import type { Call } from "./call.ts";
import { type Cache, Memory, type Store } from "./store.ts";
import { SpawnLimitError } from "./errors.ts";
import { randomUUID } from "node:crypto";
import type { Publisher, Subscriber } from "./emitter.ts";
import { BrrrShutdownSymbol, BrrrTaskDoneEventSymbol } from "./symbol.ts";
import { PendingReturn, ScheduleMessage, TaggedTuple } from "./tagged-tuple.ts";

/**
 * A call which a handler wants scheduled before it can make progress.
 */
export interface DeferredCall {
  /**
   * The topic to schedule this call on.  Undefined means self: the topic the
   * deferring handler itself is being served on.
   */
  readonly topic: string | undefined;
  readonly call: Call;
}

/**
 * When a task is called and hasn't been computed yet, the handler returns a
 * Defer.  Workers see this and schedule the deferred calls to be computed,
 * after which the deferring call is retried.
 */
export class Defer {
  public readonly calls: DeferredCall[];

  public constructor(...calls: DeferredCall[]) {
    this.calls = calls;
  }
}

/**
 * A single unit of work handed to a request handler.
 *
 * Probably some extra useful out-of-band metadata at some point?  Something
 * like "headers"?  Metadata?  For now we only have calls in a request, but it's
 * very likely we'll want to add things here very soon and this is part of the
 * public API, so let's wrap the call itself in a single-member Request type.
 */
export interface Request {
  /** The actual semantically meaningful part of the call. */
  readonly call: Call;
}

/** The successful result of a handled request. */
export interface Response {
  readonly payload: Uint8Array;
}

/**
 * User-supplied task dispatcher.
 *
 * Returns a {@link Response} when the call could be computed, or a
 * {@link Defer} listing the calls which must be computed first.
 */
export type RequestHandler = (
  request: Request,
  connection: Connection,
) => Promise<Response | Defer>;

/**
 * A connection in its most basic form, to call _out_ to brrr.
 *
 * You can read values, and you can asynchronously schedule jobs, but it doesn't
 * allow "calling" jobs using the defer-and-retry mechanism.  That's done by
 * active applications, see {@link Server}.
 */
export class Connection {
  /**
   * Non-critical, non-persistent information.  Still figuring out if it makes
   * sense to have this dichotomy supported so explicitly at the top-level of
   * the API.  We run the risk of somehow letting semantically important
   * information seep into this cache, and suddenly it is effectively just part
   * of memory again, at which point what's the split for?
   */
  public readonly cache: Cache;

  /** A storage backend for calls, values and pending returns. */
  public readonly memory: Memory;

  /** Where jobs are published to be picked up by workers. */
  public readonly emitter: Publisher;

  /**
   * Maximum task executions per root job.  Hard-coded, not intended to be
   * configurable or ever even be hit, for that matter.  If you hit this you
   * almost certainly have a pathological workflow edge case causing massive
   * reruns.  If you actually need to increase this because your flows genuinely
   * hit this limit, I'm impressed.
   */
  public readonly spawnLimit = 10_000;

  public constructor(store: Store, cache: Cache, emitter: Publisher) {
    this.cache = cache;
    this.memory = new Memory(store);
    this.emitter = emitter;
  }

  /**
   * Publish a job onto a topic, subject to the spawn limit.
   *
   * Incredibly mother-of-all ad-hoc definitions.  Doesn't use the topic for
   * counting spawn limits: the spawn limit is currently intended to never be
   * hit at all: it's a _semantic_ check, not a _runtime_ check.  It's not
   * intended for example to give paying customers a higher spawn limit than
   * free ones.  It's intended to catch infinite recursion and non-idempotent
   * call graphs.
   *
   * @throws {SpawnLimitError} when the root job has spawned too many tasks.
   * Throwing here allows the user of brrr to decide how to handle this: what
   * kind of logging?  Does the worker crash in order to flag the problem to the
   * service orchestrator, relying on auto restarts to maintain uptime while
   * allowing monitoring to go flag a bigger issue to admins?  Or just wrap it
   * in a `while (true)` loop which catches and ignores specifically this error?
   */
  public async putJob(topic: string, job: ScheduleMessage): Promise<void> {
    if ((await this.cache.incr(`brrr/count/${job.rootId}`)) > this.spawnLimit) {
      throw new SpawnLimitError(this.spawnLimit, job.rootId, job.callHash);
    }
    await this.emitter.emit(topic, TaggedTuple.encodeToString(job));
  }

  /**
   * Schedule this call on the brrr workforce.
   *
   * This method should be called for top-level workflow calls only.
   */
  public async scheduleRaw(topic: string, call: Call): Promise<void> {
    // Best effort optimization which is NOT semantically relevant.  It would in
    // fact be a good test to disable this and verify all unit tests still pass
    // (discrepancies in task call counts notwithstanding).
    if (await this.memory.hasValue(call.callHash)) {
      return;
    }
    await this.memory.setCall(call);
    // Random root id for every call so we can disambiguate retries
    const rootId = randomUUID().replaceAll("-", "");
    await this.putJob(topic, new ScheduleMessage(rootId, call.callHash));
  }

  /**
   * Returns the value of a task, or undefined if it's not present in the store.
   */
  public async readRaw(callHash: string): Promise<Uint8Array | undefined> {
    return this.memory.getValue(callHash);
  }
}

/**
 * A connection which can also _serve_ jobs: pull them off a topic, hand them to
 * a handler, store the result and kick off whoever was waiting for it.
 *
 * Separate class for now, might not need to be, although it does leave open the
 * possibility of having different emitter protocols: consumer vs producer?
 */
export class Server extends Connection {
  public constructor(store: Store, cache: Cache, emitter: Publisher) {
    super(store, cache, emitter);
  }

  /**
   * Workers take jobs from the queue, one at a time, and handle them.  They
   * have read and write access to the store, and are responsible for managing
   * the output of tasks and scheduling new ones.
   *
   * `topic` is the topic on which this brrr instance is _listening_ for new
   * jobs.  It can _call_ any jobs on any topic, but it expects to serve its
   * jobs only on this specific one.
   *
   * Every worker MUST be able to handle any job which it pulls from the queue.
   * A crucial operating principle of brrr is the lack of a central agent or
   * scheduler.  Topics are how work is segregated, but within a topic every
   * worker is equal and every worker must handle every job.  Rejecting a job
   * will be considered a job failure by the queue [in any decent queue
   * implementation, e.g. SQS dead lettering after a while].
   *
   * The loop runs until `getMessage` yields {@link BrrrShutdownSymbol}; an
   * undefined message means "nothing right now, poll again".
   */
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

  /**
   * Handle a single encoded {@link ScheduleMessage} off the wire.
   *
   * Returns the handled call if it produced a value, or undefined if the
   * handler deferred and the call still needs to be retried later.
   */
  protected async handleMessage(
    requestHandler: RequestHandler,
    topic: string,
    payload: string,
  ): Promise<Call | undefined> {
    const message = TaggedTuple.decodeFromString(ScheduleMessage, payload);
    const call = await this.memory.getCall(message.callHash);
    const handled = await requestHandler({ call }, this);
    if (handled instanceof Defer) {
      // Any of these calls could throw a SpawnLimitError: let that bubble up.
      // This is very ugly but I want to keep the contract of throwing
      // exceptions on spawn limits, even though it's _technically_ a user
      // error.  It's a very nice failure mode and it allows the user to
      // automatically lean on their fleet monitoring to measure the health of
      // their workflows, and debugging this issue can otherwise be very hard.
      await Promise.all(
        handled.calls.map((child) => {
          return this.scheduleCallNested(topic, child, message);
        }),
      );
      return;
    }
    // This can end up in a race against another worker to write the value.
    await this.memory.setValue(message.callHash, handled.payload);
    // Scheduling the returns one by one is ugly and it's tempting to reach for
    // Promise.allSettled here.  However: I don't want to blanket catch all
    // errors, only SpawnLimitError.  You'd need to do manual filtering of
    // errors, check if there are any non-spawn-limit ones, if so throw those
    // immediately, otherwise throw a spawn limit error once everything
    // finishes.  It's about as convoluted as just doing it this way, without
    // any of the clarity.
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

  /**
   * Kick off a parent which was waiting for a now-completed call.
   */
  private async scheduleReturnCall(
    pendingReturn: PendingReturn,
  ): Promise<void> {
    const job = new ScheduleMessage(
      pendingReturn.rootId,
      pendingReturn.callHash,
    );
    await this.putJob(pendingReturn.topic, job);
  }

  /**
   * Schedule this call on the brrr workforce.
   *
   * This is the real internal entrypoint which should be used by all brrr
   * internal-facing code, to avoid confusion about what's internal API and
   * what's external.
   *
   * This method is for calls which are scheduled from within another brrr call.
   * When the work scheduled by this call has completed, that worker must kick
   * off the parent (which is the flow doing the calling of this function,
   * "now").
   *
   * This will always kick off the call, it doesn't check if a return value
   * already exists for this call.
   */
  private async scheduleCallNested(
    topic: string,
    child: DeferredCall,
    parent: ScheduleMessage,
  ): Promise<void> {
    // First the call because it is perennial, it just describes the actual call
    // being made, it doesn't cause any further action and it's safe under all
    // races.
    await this.memory.setCall(child.call);
    const callHash = child.call.callHash;
    const pendingReturn = new PendingReturn(
      parent.rootId,
      parent.callHash,
      topic,
    );
    // Note this can be immediately read out by a racing return call.  The
    // pathological case is: we are late to a party and another worker is
    // actually just done handling this call, and just before it reads out the
    // addresses to which to return, it is added here.  That's still OK because
    // it will then immediately call this parent flow back, which is fine
    // because the result does in fact exist.
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

/**
 * A {@link Server} for emitters which push work to us instead of us polling for
 * it: rather than running a {@link Server.loop}, you register a handler per
 * topic and the emitter invokes it for every message.
 */
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

  /**
   * Subscribe to a topic and handle every job published on it.
   *
   * The same rule as {@link Server.loop} applies: every worker must be able to
   * handle every job on the topic it listens to.
   */
  public listen(topic: string, handler: RequestHandler) {
    this.emitter.on(topic, async (callId: string): Promise<void> => {
      const result = await this.handleMessage(handler, topic, callId);
      if (result) {
        await this.emitter.emitEventSymbol?.(BrrrTaskDoneEventSymbol, result);
      }
    });
  }
}
