import type { Call } from "./call.ts";
import { bencoder, decoder } from "./internal-codecs.ts";
import { CasRetryLimitReachedError, NotFoundError } from "./errors.ts";
import { PendingReturn, TaggedTuple } from "./tagged-tuple.ts";

export interface PendingReturnsPayload {
  readonly scheduled_at: number | undefined;
  readonly returns: unknown[][];
}

/**
 * Set of parents waiting for a child call to complete.
 *
 * When the child call is scheduled, a timestamp is added to this record to
 * indicate it doesn't need to be rescheduled.  If the record exists but with a
 * null scheduled timestamp, you cannot be sure this child has ever actually
 * been scheduled, so it should be rescheduled.
 *
 * This record is used in highly race sensitive context and is the point of a
 * lot of CASing.
 *
 * <docsync>PendingReturns</docsync>
 */
export class PendingReturns {
  public readonly scheduledAt: number | undefined;
  public readonly encodedReturns: ReadonlySet<string>;

  public constructor(
    scheduledAt: number | undefined,
    returns: Iterable<PendingReturn>,
  ) {
    this.scheduledAt = scheduledAt;
    this.encodedReturns = new Set([...returns].map(TaggedTuple.encodeToString));
  }

  public static decode(encoded: Uint8Array): PendingReturns {
    const { scheduled_at, returns } = bencoder.decode(
      encoded,
      "utf-8",
    ) as PendingReturnsPayload;
    return new this(
      scheduled_at,
      [...new Set(returns)].map((it) => {
        return TaggedTuple.fromTuple(
          PendingReturn,
          it as [number, string, string, string],
        );
      }),
    );
  }

  public encode(): Uint8Array {
    return bencoder.encode({
      scheduled_at: this.scheduledAt,
      returns: [...this.encodedReturns]
        .map((it) =>
          TaggedTuple.asTuple(TaggedTuple.decodeFromString(PendingReturn, it)),
        )
        .sort(),
    } satisfies PendingReturnsPayload);
  }
}

export interface MemKey {
  readonly type: "pending_returns" | "call" | "value";
  readonly callHash: string;
}

export interface Store {
  /**
   * Check if the store has a value for the given key.
   */
  has(key: MemKey): Promise<boolean>;

  /**
   * Get the value for the given key.
   */
  get(key: MemKey): Promise<Uint8Array | undefined>;

  /**
   * Get the value for the given key and retry if NotFound.
   */
  getWithRetry(key: MemKey): Promise<Uint8Array | undefined>;

  /**
   * Set the value for the given key.
   */
  set(key: MemKey, value: Uint8Array): Promise<void>;

  /**
   * Delete the value for the given key.
   */
  delete(key: MemKey): Promise<void>;

  /**
   * Set a new value for the given key.
   * Returns true if the value was set, false if the key already exists.
   */
  setNewValue(key: MemKey, value: Uint8Array): Promise<boolean>;

  /**
   * Compare and set a value for the given key.
   * Returns true if the value was set, false if the expected value did not match.
   */
  compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<boolean>;

  /**
   * Compare and delete a value for the given key.
   * Returns true if the value was deleted, false if the expected value did not match.
   */
  compareAndDelete(key: MemKey, expected: Uint8Array): Promise<boolean>;
}

export interface Cache {
  /**
   * Increment the value for the given key.
   */
  incr(key: string): Promise<number>;
}

export class Memory {
  private static readonly casRetryLimit = 100;

  private readonly store: Store;

  public constructor(store: Store) {
    this.store = store;
  }

  public async getCall(callHash: string): Promise<Call> {
    const memKey: MemKey = {
      type: "call",
      callHash,
    };
    const encoded = await this.store.getWithRetry(memKey);
    if (!encoded) {
      throw new NotFoundError(memKey);
    }
    const { task_name, payload } = bencoder.decode(encoded) as {
      task_name: Uint8Array;
      payload: Uint8Array;
    };
    return {
      taskName: decoder.decode(task_name),
      payload,
      callHash,
    };
  }

  public async setCall(call: Call): Promise<void> {
    const encoded = bencoder.encode({
      task_name: call.taskName,
      payload: call.payload,
    });
    await this.store.set(
      {
        type: "call",
        callHash: call.callHash,
      },
      encoded,
    );
  }

  public async hasValue(callHash: string): Promise<boolean> {
    return this.store.has({
      type: "value",
      callHash,
    });
  }

  public async getValue(callHash: string): Promise<Uint8Array | undefined> {
    return this.store.get({
      type: "value",
      callHash,
    });
  }

  public async setValue(callHash: string, payload: Uint8Array): Promise<void> {
    await this.store.set(
      {
        type: "value",
        callHash,
      },
      payload,
    );
  }

  public async addPendingReturns(
    callHash: string,
    newReturn: PendingReturn,
  ): Promise<boolean> {
    const memKey: MemKey = {
      type: "pending_returns",
      callHash,
    };
    let shouldSchedule = false;
    await this.withCas(async () => {
      shouldSchedule = false;
      const existingEncoded = await this.store.get(memKey);
      if (!existingEncoded) {
        const newReturns = new PendingReturns(Math.floor(Date.now() / 1000), [
          newReturn,
        ]);
        shouldSchedule = true;
        return await this.store.setNewValue(memKey, newReturns.encode());
      }
      const pendingReturns = PendingReturns.decode(existingEncoded);
      shouldSchedule = [...pendingReturns.encodedReturns].some((it) =>
        TaggedTuple.decodeFromString(PendingReturn, it).isRepeatedCall(
          newReturn,
        ),
      );
      const newReturns = new PendingReturns(
        pendingReturns.scheduledAt,
        pendingReturns.encodedReturns
          .union(new Set([TaggedTuple.encodeToString(newReturn)]))
          .values()
          .map((it) => TaggedTuple.decodeFromString(PendingReturn, it)),
      );
      return this.store.compareAndSet(
        memKey,
        newReturns.encode(),
        existingEncoded,
      );
    });
    return shouldSchedule;
  }

  public async withPendingReturnsRemove(
    callHash: string,
    f: (returns: ReadonlySet<PendingReturn>) => Promise<void>,
  ) {
    const memKey: MemKey = {
      type: "pending_returns",
      callHash,
    };
    const handled = new Set<PendingReturn>();
    return this.withCas(async () => {
      const pendingEncoded = await this.store.get(memKey);
      if (!pendingEncoded) {
        return true;
      }
      const toHandle = new Set(
        PendingReturns.decode(pendingEncoded)
          .encodedReturns.difference(handled)
          .values()
          .map((it) => TaggedTuple.decodeFromString(PendingReturn, it)),
      );
      await f(toHandle);
      for (const it of toHandle) {
        handled.add(it);
      }
      return this.store.compareAndDelete(memKey, pendingEncoded);
    });
  }

  private async withCas(f: () => Promise<boolean>): Promise<void> {
    for (let i = 0; i < Memory.casRetryLimit; i++) {
      if (await f()) {
        return;
      }
    }
    throw new CasRetryLimitReachedError(Memory.casRetryLimit);
  }
}
