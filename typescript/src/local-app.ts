import {
  AppWorker,
  type NoContextTask,
  type Handlers,
  type TaskIdentifier,
  taskIdentifierToName,
  type Registry,
} from "./app.ts";
import type { Codec } from "./codec.ts";
import {
  CloseOnEmptyQueue,
  InMemoryCache,
  InMemoryStore,
} from "./backends/in-memory.ts";
import { NotFoundError } from "./errors.ts";
import { Server } from "./connection.ts";

export class LocalApp<C> {
  public readonly topic: string;
  public readonly server: Server;
  public readonly app: AppWorker<C>;

  private hasRun = false;

  public constructor(topic: string, server: Server, app: AppWorker<C>) {
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

  public async run(): Promise<void> {
    if (this.hasRun) {
      throw new Error("LocalApp has already been run");
    }
    this.hasRun = true;
    await this.server.loop(this.topic, this.app.handle);
  }
}

export class LocalBrrr<C> {
  private readonly topic: string;
  private readonly registry: Registry<C>;

  public constructor(topic: string, registry: Registry<C>) {
    this.topic = topic;
    this.registry = registry;
  }

  public run<A extends unknown[], R>(taskIdentifier: TaskIdentifier<C, A, R>) {
    const store = new InMemoryStore();
    const cache = new InMemoryCache();
    const queue = new CloseOnEmptyQueue([this.topic]);
    const server = new Server(store, cache, queue);
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
      await localApp.schedule(taskName)(...args);
      await localApp.run();
      return localApp.read(taskIdentifier)(...args);
    };
  }
}
