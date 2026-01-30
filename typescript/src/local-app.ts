import { SubscriberServer } from "./connection.ts";
import {
  AppWorker,
  type NoContextTask,
  type Handlers,
  type TaskIdentifier,
  taskIdentifierToName,
} from "./app.ts";
import type { Codec } from "./codec.ts";
import {
  InMemoryCache,
  InMemoryEmitter,
  InMemoryStore,
} from "./backends/in-memory.ts";
import { NotFoundError } from "./errors.ts";
import { BrrrTaskDoneEventSymbol } from "./symbol.ts";

export class LocalApp<C> {
  public readonly topic: string;
  public readonly server: SubscriberServer;
  public readonly app: AppWorker<C>;

  private hasRun = false;

  public constructor(
    topic: string,
    server: SubscriberServer,
    app: AppWorker<C>,
  ) {
    this.topic = topic;
    this.server = server;
    this.app = app;
  }

  public schedule<A extends unknown[], R>(
    handler: Parameters<typeof this.app.schedule<A, R>>[0],
  ): NoContextTask<A, void> {
    return this.app.schedule(handler, this.topic);
  }

  public read<A extends unknown[], R>(
    ...args: Parameters<typeof this.app.read<A, R>>
  ): NoContextTask<A, R> {
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

export class LocalBrrr<C> {
  private readonly topic: string;
  private readonly handlers: Handlers<C>;
  private readonly codec: Codec<C>;

  public constructor(topic: string, handlers: Handlers<C>, codec: Codec<C>) {
    this.topic = topic;
    this.handlers = handlers;
    this.codec = codec;
  }

  public run<A extends unknown[], R>(taskIdentifier: TaskIdentifier<C, A, R>) {
    const store = new InMemoryStore();
    const cache = new InMemoryCache();
    const emitter = new InMemoryEmitter();
    const server = new SubscriberServer(store, cache, emitter);
    const worker = new AppWorker(this.codec, server, this.handlers);
    const localApp = new LocalApp(this.topic, server, worker);
    const taskName = taskIdentifierToName(taskIdentifier, this.handlers);
    return async (...args: A): Promise<R> => {
      localApp.run();
      await localApp.schedule(taskName)(...args);
      const call = await this.codec.encodeCall(taskName, args);
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
            const result = this.codec.decodeReturn(taskName, payload) as R;
            resolve(result);
          }
        });
      });
    };
  }
}
