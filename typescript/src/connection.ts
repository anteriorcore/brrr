import type { Call } from "./call.ts";
import { type Cache, Memory, type Store } from "./store.ts";
import { SpawnLimitError } from "./errors.ts";
import { randomUUID } from "node:crypto";
import { PendingReturn, ScheduleMessage, TaggedTuple } from "./tagged-tuple.ts";
import type { Queue } from "./queue.ts";

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
  public readonly queue: Queue;
  public readonly spawnLimit = 10_000;

  public constructor(store: Store, cache: Cache, queue: Queue) {
    this.cache = cache;
    this.memory = new Memory(store);
    this.queue = queue;
  }

  public async putJob(topic: string, job: ScheduleMessage): Promise<void> {
    if ((await this.cache.incr(`brrr/count/${job.rootId}`)) > this.spawnLimit) {
      throw new SpawnLimitError(this.spawnLimit, job.rootId, job.callHash);
    }
    await this.queue.putMessage(topic, TaggedTuple.encodeToString(job));
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
  public constructor(store: Store, cache: Cache, queue: Queue) {
    super(store, cache, queue);
  }

  public async loop(topic: string, handler: RequestHandler) {
    while (true) {
      const response = await this.queue.getMessage(topic);
      if (!response) {
        continue;
      }
      if (response.closed) {
        break;
      }
      await this.handleMessage(handler, topic, response.message.body);
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
