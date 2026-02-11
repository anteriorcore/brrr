import {
  type Connection,
  Defer,
  type DeferredCall,
  type Request,
  type Response,
} from "./connection.ts";
import type { Codec } from "./codec.ts";
import { NotFoundError, TaskNotFoundError } from "./errors.ts";

export type Task<C, A extends unknown[] = any[], R = any> = NoContextTask<
  [C, ...A],
  R
>;

export type NoContextTask<A extends unknown[] = any[], R = any> = (
  ...args: A
) => R | Promise<R>;

export type Handlers<C> = Readonly<Record<string, Task<C, any[], any>>>;

export type Registry<C> = {
  codec: Codec<C>;
  handlers: Handlers<C>;
};

export type TaskIdentifier<C, A extends unknown[], R> = Task<C, A, R> | string;

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

export class AppConsumer<C> {
  public readonly connection: Connection;
  public readonly registry: Registry<C>;

  public constructor(
    codec: Codec<C>,
    connection: Connection,
    handlers: Handlers<C> = {},
  ) {
    this.connection = connection;
    this.registry = { codec, handlers };
  }

  public schedule<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<C, A, R>,
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

  public read<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<C, A, R>,
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

export class AppWorker<C> extends AppConsumer<C> {
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

export class ActiveWorker<C> {
  private readonly connection: Connection;
  private readonly registry: Registry<C>;

  public constructor(connection: Connection, registry: Registry<C>) {
    this.connection = connection;
    this.registry = registry;
  }

  public call<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<C, A, R>,
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
