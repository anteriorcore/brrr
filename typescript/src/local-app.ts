import { SubscriberServer } from "./connection.ts";
import {
  AppWorker,
  type AsyncFn,
  type Handlers,
  type TaskIdentifier,
  taskIdentifierToName,
  type Registry,
} from "./app.ts";
import type { Codec } from "./codec.ts";
import {
  InMemoryCache,
  InMemoryEmitter,
  InMemoryStore,
} from "./backends/in-memory.ts";
import { NotFoundError } from "./errors.ts";
import { BrrrTaskDoneEventSymbol } from "./symbol.ts";

export class LocalApp<Env> {
  public readonly topic: string;
  public readonly server: SubscriberServer;
  public readonly app: AppWorker<Env>;

  private hasRun = false;

  public constructor(
    topic: string,
    server: SubscriberServer,
    app: AppWorker<Env>,
  ) {
    this.topic = topic;
    this.server = server;
    this.app = app;
  }

  public schedule<A extends unknown[], R>(
    handler: Parameters<typeof this.app.schedule<A, R>>[0],
  ): AsyncFn<A, void> {
    return this.app.schedule(handler, this.topic);
  }

  public read<A extends unknown[], R>(
    ...args: Parameters<typeof this.app.read<A, R>>
  ): AsyncFn<A, R> {
    return this.app.read(...args);
  }

  public run(): void {
    if (this.hasRun) {
      throw new Error("LocalApp has already been run");
    }
    this.hasRun = true;
    this.server.listen(this.topic, this.app.handle);
  }
}

export class LocalBrrr<Env> {
  private readonly topic: string;
  private readonly registry: Registry<Env>;

  public constructor(topic: string, registry: Registry<Env>) {
    this.topic = topic;
    this.registry = registry;
  }

  public run<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<Env, A, R>,
  ) {
    const store = new InMemoryStore();
    const cache = new InMemoryCache();
    const emitter = new InMemoryEmitter();
    const server = new SubscriberServer(store, cache, emitter);
    const worker = new AppWorker(
      this.registry.codec,
      server,
      this.registry.handlers,
    );
    const localApp = new LocalApp(this.topic, server, worker);
    const taskName = taskIdentifierToName(
      taskIdentifier,
      this.registry.handlers,
    );
    return async (...args: A): Promise<R> => {
      localApp.run();
      await localApp.schedule(taskName)(...args);
      const call = await this.registry.codec.encodeCall(taskName, args);
      return new Promise((resolve) => {
        emitter.onEventSymbol(BrrrTaskDoneEventSymbol, async ({ callHash }) => {
          if (callHash === call.callHash) {
            const payload = await server.readRaw(callHash);
            if (!payload) {
              throw new NotFoundError({
                type: "value",
                callHash,
              });
            }
            const result = this.registry.codec.decodeReturn(
              taskName,
              payload,
            ) as R;
            resolve(result);
          }
        });
      });
    };
  }
}
