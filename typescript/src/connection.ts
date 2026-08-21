import type { Call } from "./call.ts";
import { type Cache, Memory, type Store } from "./store.ts";
import { SpawnLimitError } from "./errors.ts";
import { randomUUID } from "node:crypto";
import type { Publisher, Subscriber } from "./emitter.ts";
import { BrrrShutdownSymbol, BrrrTaskDoneEventSymbol } from "./symbol.ts";
import { PendingReturn, ScheduleMessage, TaggedTuple } from "./tagged-tuple.ts";

export interface DeferredCall {
  readonly topic: string | undefined;
  readonly call: Call;
}

export class Defer {
  public readonly calls: DeferredCall[];

  public constructor(...calls: DeferredCall[]) {
    this.calls = calls;
  }
}

export interface Request {
  readonly call: Call;
}

export interface Response {
  readonly payload: Uint8Array;
}

export type RequestHandler = (
  request: Request,
  connection: Connection,
) => Promise<Response | Defer>;

export class Connection {
  public readonly cache: Cache;
  public readonly memory: Memory;
  public readonly emitter: Publisher;
  public readonly spawnLimit = 10_000;

  public constructor(store: Store, cache: Cache, emitter: Publisher) {
    this.cache = cache;
    this.memory = new Memory(store);
    this.emitter = emitter;
  }

  public async putJob(topic: string, job: ScheduleMessage): Promise<void> {
    if ((await this.cache.incr(`brrr/count/${job.rootId}`)) > this.spawnLimit) {
      throw new SpawnLimitError(this.spawnLimit, job.rootId, job.callHash);
    }
    await this.emitter.emit(topic, TaggedTuple.encodeToString(job));
  }

  public async scheduleRaw(topic: string, call: Call): Promise<void> {
    if (await this.memory.hasValue(call.callHash)) {
      return;
    }
    await this.memory.setCall(call);
    const rootId = randomUUID().replaceAll("-", "");
    await this.putJob(topic, new ScheduleMessage(rootId, call.callHash));
  }

  public async readRaw(callHash: string): Promise<Uint8Array | undefined> {
    return this.memory.getValue(callHash);
  }
}

export class Server extends Connection {
  public constructor(store: Store, cache: Cache, emitter: Publisher) {
    super(store, cache, emitter);
  }

  public async loop(
    topic: string,
    handler: RequestHandler,
    getMessage: () => Promise<string | typeof BrrrShutdownSymbol | undefined>,
  ) {
    while (true) {
      const message = await getMessage();
      if (!message) {
        continue;
      }
      if (message === BrrrShutdownSymbol) {
        break;
      }
      const call = await this.handleMessage(handler, topic, message);
      if (call) {
        await this.emitter.emitEventSymbol?.(BrrrTaskDoneEventSymbol, call);
      }
    }
  }

  protected async handleMessage(
    requestHandler: RequestHandler,
    topic: string,
    payload: string,
  ): Promise<Call | undefined> {
    const message = TaggedTuple.decodeFromString(ScheduleMessage, payload);
    const call = await this.memory.getCall(message.callHash);
    const handled = await requestHandler({ call }, this);
    if (handled instanceof Defer) {
      await Promise.all(
        handled.calls.map((child) => {
          return this.scheduleCallNested(topic, child, message);
        }),
      );
      return;
    }
    await this.memory.setValue(message.callHash, handled.payload);
    let spawnLimitError: SpawnLimitError;
    await this.memory.withPendingReturnsRemove(
      message.callHash,
      async (returns) => {
        for (const pending of returns) {
          try {
            await this.scheduleReturnCall(pending);
          } catch (err) {
            if (err instanceof SpawnLimitError) {
              spawnLimitError = err;
              continue;
            }
            throw err;
          }
        }
        if (spawnLimitError) {
          throw spawnLimitError;
        }
      },
    );
    return call;
  }

  private async scheduleReturnCall(
    pendingReturn: PendingReturn,
  ): Promise<void> {
    const job = new ScheduleMessage(
      pendingReturn.rootId,
      pendingReturn.callHash,
    );
    await this.putJob(pendingReturn.topic, job);
  }

  private async scheduleCallNested(
    topic: string,
    child: DeferredCall,
    parent: ScheduleMessage,
  ): Promise<void> {
    await this.memory.setCall(child.call);
    const callHash = child.call.callHash;
    const pendingReturn = new PendingReturn(
      parent.rootId,
      parent.callHash,
      topic,
    );
    const shouldSchedule = await this.memory.addPendingReturns(
      callHash,
      pendingReturn,
    );
    if (shouldSchedule) {
      const job = new ScheduleMessage(parent.rootId, callHash);
      await this.putJob(child.topic || topic, job);
    }
  }
}

export class SubscriberServer extends Server {
  public override readonly emitter: Publisher & Subscriber;

  public constructor(
    store: Store,
    cache: Cache,
    emitter: Publisher & Subscriber,
  ) {
    super(store, cache, emitter);
    this.emitter = emitter;
  }

  public listen(topic: string, handler: RequestHandler) {
    this.emitter.on(topic, async (callId: string): Promise<void> => {
      const result = await this.handleMessage(handler, topic, callId);
      if (result) {
        await this.emitter.emitEventSymbol?.(BrrrTaskDoneEventSymbol, result);
      }
    });
  }
}
