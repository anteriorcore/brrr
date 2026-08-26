import {
  type Connection,
  Defer,
  type DeferredCall,
  type Request,
  type Response,
} from "./connection.ts";
import type { Codec } from "./codec.ts";
import { NotFoundError, TaskNotFoundError } from "./errors.ts";

/**
 * A brrr task: a user function which receives its environment as its first
 * argument, followed by the task's own arguments.
 *
 * `Env` is supplied by the {@link Codec}, which decides what to inject on every
 * invocation.  Usually that's an {@link ActiveWorker}, so the task can call
 * back into brrr, but a codec is free to provide anything.
 */
export type Task<Env, A extends unknown[] = any[], R = any> = AsyncFn<
  [Env, ...A],
  R
>;

/**
 * Any function of `A` returning `R`, sync or async.
 *
 * The structural primitive {@link Task} is built from, and the shape callers
 * get back from `schedule`/`read`/`call`: the task's own arguments, with the
 * environment already supplied.
 */
export type AsyncFn<A extends unknown[] = any[], R = any> = (
  ...args: A
) => R | Promise<R>;

/**
 * The tasks a worker knows how to run, keyed by task name.
 *
 * The name is what travels over the wire, so it must be stable across every
 * worker serving a topic.
 */
export type Handlers<Env> = Readonly<Record<string, Task<Env, any[], any>>>;

/**
 * Everything needed to translate between brrr's bytes and user-facing calls:
 * the codec which does the encoding, and the tasks it can encode for.
 */
export type Registry<Env> = {
  codec: Codec<Env>;
  handlers: Handlers<Env>;
};

/**
 * A task, referred to either by the function itself or by its registered name.
 *
 * Passing the function is nicer to write and keeps argument types; passing the
 * name lets you refer to tasks this process doesn't implement.
 */
export type TaskIdentifier<Env, A extends unknown[], R> =
  | Task<Env, A, R>
  | string;

/**
 * Resolve a {@link TaskIdentifier} to the task name used on the wire.
 *
 * @throws {TaskNotFoundError} if the function isn't registered in `handlers`.
 */
export function taskIdentifierToName(
  identifier: TaskIdentifier<any, any[], any>,
  handlers: Handlers<any>,
): string {
  if (typeof identifier === "string") {
    return identifier;
  }
  for (const [name, handler] of Object.entries(handlers)) {
    if (handler === identifier) {
      return name;
    }
  }
  throw new TaskNotFoundError(identifier.name);
}

/**
 * The client-facing half of a brrr app: schedule tasks and read their results.
 *
 * This is the typed, codec-aware layer on top of {@link Connection}.  Like the
 * connection it wraps, it cannot _run_ tasks and it cannot use the
 * defer-and-retry mechanism; that's {@link AppWorker}.
 */
export class AppConsumer<Env> {
  public readonly connection: Connection;
  public readonly registry: Registry<Env>;

  public constructor(
    codec: Codec<Env>,
    connection: Connection,
    handlers: Handlers<Env> = {},
  ) {
    this.connection = connection;
    this.registry = { codec, handlers };
  }

  /**
   * Public-facing one-shot schedule method.
   *
   * Returns a function which, when called with the task's arguments, encodes
   * the call and puts it on `topic`.  Scheduling is fire-and-forget: it
   * resolves once the job is queued, not once the task has run.
   */
  public schedule<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<Env, A, R>,
    topic: string,
  ): (...args: A) => Promise<void> {
    const taskName = taskIdentifierToName(
      taskIdentifier,
      this.registry.handlers,
    );
    return async (...args: A) => {
      const call = await this.registry.codec.encodeCall(taskName, args);
      await this.connection.scheduleRaw(topic, call);
    };
  }

  /**
   * Read the already-computed result of a task.
   *
   * Returns a function which, when called with the task's arguments, looks up
   * the value for that exact call.  This never schedules anything: if nobody
   * has computed this call yet, it throws.
   *
   * @throws {NotFoundError} if no value has been stored for this call.
   */
  public read<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<Env, A, R>,
  ): (...args: A) => Promise<R> {
    return async (...args: A) => {
      const taskName = taskIdentifierToName(
        taskIdentifier,
        this.registry.handlers,
      );
      const call = await this.registry.codec.encodeCall(taskName, args);
      const payload = await this.connection.memory.getValue(call.callHash);
      if (!payload) {
        throw new NotFoundError({
          type: "value",
          callHash: call.callHash,
        });
      }
      return this.registry.codec.decodeReturn(taskName, payload) as R;
    };
  }
}

/**
 * An {@link AppConsumer} which can also run tasks.
 *
 * Pass {@link AppWorker.handle} to a `Server` as its request handler and this
 * process becomes a worker for the topics that server listens on.
 */
export class AppWorker<Env> extends AppConsumer<Env> {
  /**
   * Glue between this class and the underlying `Server.loop` handler.
   *
   * Looks the task up by name, invokes it through the codec with a fresh
   * {@link ActiveWorker} to draw the environment from, and normalises the
   * outcome into the `Response | Defer` the server expects.  A {@link Defer}
   * thrown by {@link ActiveWorker.call} is a control-flow signal, not a
   * failure, so it is returned rather than propagated; any other error is a
   * genuine task failure and bubbles up.
   *
   * @throws {TaskNotFoundError} if this worker doesn't implement the requested
   * task.  Every worker must be able to handle every job on its topic, so this
   * means the fleet is misconfigured.
   */
  public readonly handle = async (
    request: Request,
    connection: Connection,
  ): Promise<Response | Defer> => {
    const handler = this.registry.handlers[request.call.taskName];
    if (!handler) {
      throw new TaskNotFoundError(request.call.taskName);
    }
    try {
      const payload = await this.registry.codec.invokeTask(
        request.call,
        handler,
        new ActiveWorker(connection, this.registry),
      );
      return { payload };
    } catch (err) {
      if (err instanceof Defer) {
        return err;
      }
      throw err;
    }
  };
}

/**
 * A task's handle onto the worker running it.
 *
 * This is how a task calls other tasks and waits for their results, using the
 * defer-and-retry mechanism.  Codecs typically pass it straight through as the
 * task's `Env`, but that is the codec's choice, not a requirement.
 */
export class ActiveWorker<Env> {
  private readonly connection: Connection;
  private readonly registry: Registry<Env>;

  public constructor(connection: Connection, registry: Registry<Env>) {
    this.connection = connection;
    this.registry = registry;
  }

  /**
   * Directly call a brrr task from within another task.
   *
   * Do not call this unless you are, yourself, already inside a brrr task.
   *
   * Returns a function which resolves to the callee's result if it has already
   * been computed.  If it hasn't, that function throws a {@link Defer} instead,
   * which unwinds you out of your task; the worker then schedules the callee
   * and re-runs your task from the top once the result exists.  This means your
   * task body may run several times and must be idempotent.
   *
   * `topic` defaults to the topic the calling task is being served on.
   */
  public call<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<Env, A, R>,
    topic?: string | undefined,
  ): (...args: A) => Promise<R> {
    const taskName = taskIdentifierToName(
      taskIdentifier,
      this.registry.handlers,
    );
    return async (...args: A): Promise<R> => {
      const call = await this.registry.codec.encodeCall(taskName, args);
      const payload = await this.connection.memory.getValue(call.callHash);
      if (!payload) {
        throw new Defer({ topic, call });
      }
      return this.registry.codec.decodeReturn(taskName, payload) as R;
    };
  }

  /**
   * Takes a number of task calls and awaits each of them.  If they've all been
   * computed, returns their values; otherwise defers the ones that haven't
   * been, all at once.
   *
   * This is the difference between fanning out and going one at a time: calling
   * {@link ActiveWorker.call} sequentially defers on the first uncomputed
   * callee, so each round trip only discovers one new call.  Gathering them
   * collects every {@link Defer} raised across the batch into a single one, so
   * the whole fan-out is scheduled in one go.
   *
   * Only errors of type {@link Defer} are absorbed this way; anything else a
   * task throws propagates as usual.
   */
  // Type annotations are modeled after Promise.all: explicit types for 1-5
  // arguments (and when all have the same type), and a catch-all for the rest.
  public async gather<T1>(t1: T1): Promise<[Awaited<T1>]>;
  public async gather<T1, T2>(
    t1: T1,
    t2: T2,
  ): Promise<[Awaited<T1>, Awaited<T2>]>;
  public async gather<T1, T2, T3>(
    t1: T1,
    t2: T2,
    t3: T3,
  ): Promise<[Awaited<T1>, Awaited<T2>, Awaited<T3>]>;
  public async gather<T1, T2, T3, T4>(
    t1: T1,
    t2: T2,
    t3: T3,
    t4: T4,
  ): Promise<[Awaited<T1>, Awaited<T2>, Awaited<T3>, Awaited<T4>]>;
  public async gather<T1, T2, T3, T4, T5>(
    t1: T1,
    t2: T2,
    t3: T3,
    t4: T4,
    t5: T5,
  ): Promise<[Awaited<T1>, Awaited<T2>, Awaited<T3>, Awaited<T4>, Awaited<T5>]>;
  public async gather<T>(...promises: Promise<T>[]): Promise<Awaited<T>[]>;
  public async gather<T>(...promises: Promise<T>[]): Promise<Awaited<T>[]> {
    function toResultWrapper(value: T) {
      return {
        type: "result",
        value: value as Awaited<T>,
      } as const;
    }

    function toDeferWrapperOrThrow(error: unknown) {
      if (error instanceof Defer) {
        return {
          type: "defer",
          defer: error,
        } as const;
      }
      throw error;
    }

    // We don't use Promise.allSettled because we only want to normalize `Defer`, not catch
    // all errors. Instead, we attach custom handlers to normalize outcomes into either a
    // `ResultWrapper` or a `DeferWrapper`.
    // Then we use Promise.all on those normalized promises to ensure they are all awaited.
    // Other errors still propagate normally.
    const results = await Promise.all(
      promises.map((promise) =>
        promise.then(toResultWrapper, toDeferWrapperOrThrow),
      ),
    );

    const values: Awaited<T>[] = [];
    const deferCalls: DeferredCall[] = [];

    for (const result of results) {
      switch (result.type) {
        case "result": {
          values.push(result.value);
          break;
        }
        case "defer": {
          deferCalls.push(...result.defer.calls);
          break;
        }
        default: {
          const _: never = result; // exhaustiveness check
        }
      }
    }

    if (deferCalls.length) {
      throw new Defer(...deferCalls);
    }
    return values;
  }
}
