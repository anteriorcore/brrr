import { beforeEach, suite, test } from "node:test";
import { strictEqual } from "node:assert";
import {
  type ActiveWorker,
  AppConsumer,
  AppWorker,
  type Handlers,
} from "./app.ts";
import {
  type Connection,
  Defer,
  type Request,
  type Response,
  Server,
  SubscriberServer,
} from "./connection.ts";
import {
  InMemoryCache,
  InMemoryEmitter,
  InMemoryStore,
} from "./backends/in-memory.ts";
import { NaiveJsonCodec } from "./naive-json-codec.ts";
import type { Call } from "./call.ts";
import { NotFoundError, SpawnLimitError } from "./errors.ts";
import { deepStrictEqual, ok, rejects } from "node:assert/strict";
import { LocalApp, LocalBrrr } from "./local-app.ts";
import type { Cache, Store } from "./store.ts";
import type { Publisher, Subscriber } from "./emitter.ts";
import { BrrrShutdownSymbol, BrrrTaskDoneEventSymbol } from "./symbol.ts";
import { parse, stringify } from "superjson";
import { matrixSuite } from "./fixture.test.ts";

await matrixSuite(import.meta.filename, async (_, matrix) => {
  const codec = new NaiveJsonCodec();
  const topic = matrix.topic;
  const subtopics = {
    t1: "t1",
    t2: "t2",
    t3: "t3",
  } as const;

  let store: Store;
  let cache: Cache;
  let emitter: Publisher & Subscriber;
  let server: SubscriberServer;

  // Test tasks
  function bar(ctx: ActiveWorker, a: number) {
    return 456;
  }

  async function foo(app: ActiveWorker, a: number) {
    return (await app.call(bar, topic)(a + 1)) + 1;
  }

  function one(ctx: ActiveWorker, a: number): number {
    return a + 5;
  }

  async function two(app: ActiveWorker, a: number): Promise<void> {
    const result = await app.call("one", subtopics.t1)(a + 3);
    strictEqual(result, 15);
  }

  const handlers: Handlers<ActiveWorker> = { bar, foo };

  function waitFor(call: Call, predicate?: () => Promise<void>): Promise<void> {
    return new Promise((resolve) => {
      emitter.onEventSymbol?.(
        BrrrTaskDoneEventSymbol,
        async ({ callHash }: Call) => {
          if (callHash === call.callHash) {
            await predicate?.();
            resolve();
          }
        },
      );
    });
  }

  beforeEach(() => {
    store = new InMemoryStore();
    cache = new InMemoryCache();
    emitter = new InMemoryEmitter();
    server = new SubscriberServer(store, cache, emitter);
  });

  await test(AppWorker.name, async () => {
    const app = new AppWorker(codec, server, handlers);
    server.listen(topic, app.handle);

    const call = await codec.encodeCall(foo.name, [122]);

    const done = waitFor(call, async () => {
      strictEqual(await app.read(foo)(122), 457);
      strictEqual(await app.read("foo")(122), 457);
      strictEqual(await app.read(foo)(122), 457);
      strictEqual(await app.read("bar")(123), 456);
      strictEqual(await app.read(bar)(123), 456);
    });

    await app.schedule(foo, topic)(122);
    return done;
  });

  await test(AppConsumer.name, async () => {
    function foo(ctx: ActiveWorker, n: number) {
      return n * n;
    }

    const workerServer = new SubscriberServer(store, cache, emitter);
    const appWorker = new AppWorker(codec, workerServer, { foo });
    workerServer.listen(topic, appWorker.handle);

    const appConsumer = new AppConsumer(codec, workerServer);
    const call = await codec.encodeCall(foo.name, [5]);

    const done = waitFor(call, async () => {
      strictEqual(await appConsumer.read("foo")(5), 25);
      await rejects(appConsumer.read("foo")(3), NotFoundError);
      await rejects(appConsumer.read("bar")(5), NotFoundError);
    });

    await appWorker.schedule("foo", topic)(5);
    return done;
  });

  await test(LocalBrrr.name, async () => {
    const brrr = new LocalBrrr(topic, { handlers, codec });
    strictEqual(await brrr.run(foo)(122), 457);
  });

  await suite("gather", async () => {
    async function callNestedGather(useBrrGather = true): Promise<string[]> {
      const calls: string[] = [];

      function foo(ctx: ActiveWorker, a: number): number {
        calls.push(`foo(${a})`);
        return a * 2;
      }

      function bar(ctx: ActiveWorker, a: number): number {
        calls.push(`bar(${a})`);
        return a - 1;
      }

      async function notBrrrTask(
        app: ActiveWorker,
        a: number,
      ): Promise<number> {
        const b = await app.call(foo)(a);
        return app.call(bar)(b);
      }

      async function top(app: ActiveWorker, xs: number[]) {
        calls.push(`top(${xs})`);
        if (useBrrGather) {
          return app.gather(...xs.map((x) => notBrrrTask(app, x)));
        }
        return Promise.all(xs.map((x) => notBrrrTask(app, x)));
      }

      const localBrrr = new LocalBrrr(topic, {
        codec,
        handlers: { foo, bar, top },
      });
      await localBrrr.run(top)([3, 4]);
      return calls;
    }

    await test("app gather", async () => {
      const brrrCalls = await callNestedGather();
      strictEqual(brrrCalls.filter((it) => it.startsWith("top")).length, 5);
      const foo3 = brrrCalls.indexOf("foo(3)");
      const foo4 = brrrCalls.indexOf("foo(4)");
      const bar6 = brrrCalls.indexOf("bar(6)");
      const bar8 = brrrCalls.indexOf("bar(8)");
      ok(foo3 < bar6);
      ok(foo3 < bar8);
      ok(foo4 < bar6);
      ok(foo4 < bar8);
    });

    await test("Promise.all gather", async () => {
      const promises = await callNestedGather(false);
      strictEqual(promises.filter((it) => it.startsWith("top")).length, 5);
      const foo3 = promises.indexOf("foo(3)");
      const foo4 = promises.indexOf("foo(4)");
      const bar6 = promises.indexOf("bar(6)");
      const bar8 = promises.indexOf("bar(8)");
      ok(foo3 < bar6);
      ok(foo4 < bar8);
    });
  });

  await test("topics separate app same connection", async () => {
    const app1 = new AppWorker(codec, server, { one });
    const app2 = new AppWorker(codec, server, { two });

    const call = await codec.encodeCall("two", [7]);

    const done = waitFor(call);

    server.listen(subtopics.t1, app1.handle);
    server.listen(subtopics.t2, app2.handle);

    await app2.schedule(two, subtopics.t2)(7);

    return done;
  });

  await test("topics separate app separate connection", async () => {
    const server1 = new SubscriberServer(store, cache, emitter);
    const server2 = new SubscriberServer(store, cache, emitter);
    const app1 = new AppWorker(codec, server1, { one });
    const app2 = new AppWorker(codec, server2, { two });

    server1.listen(subtopics.t1, app1.handle);
    server2.listen(subtopics.t2, app2.handle);

    const call = await codec.encodeCall("two", [7]);

    const done = waitFor(call);

    await app2.schedule(two, subtopics.t2)(7);
    return done;
  });

  await test("topics same app", async () => {
    const app = new AppWorker(codec, server, { one, two });
    server.listen(subtopics.t1, app.handle);
    server.listen(subtopics.t2, app.handle);

    const call = await codec.encodeCall("two", [7]);
    await app.schedule(two, subtopics.t2)(7);

    await waitFor(call);
  });

  await test("debounce child", async () => {
    const calls = new Map<number, number>();

    async function foo(app: ActiveWorker, a: number): Promise<number> {
      calls.set(a, (calls.get(a) || 0) + 1);
      if (a === 0) {
        return a;
      }
      const results = await app.gather(
        ...Array(50)
          .keys()
          .map(() => app.call(foo)(a - 1)),
      );
      return results.reduce((sum, val) => sum + val);
    }

    const brrr = new LocalBrrr(topic, { codec, handlers: { foo } });
    await brrr.run(foo)(3);

    deepStrictEqual(Object.fromEntries(calls), { 0: 1, 1: 2, 2: 2, 3: 2 });
  });

  await test("no debounce parent", async () => {
    const calls = new Map<string, number>();

    function one(ctx: ActiveWorker, _: number): number {
      calls.set("one", (calls.get("one") || 0) + 1);
      return 1;
    }

    async function foo(app: ActiveWorker, a: number): Promise<number> {
      calls.set("foo", (calls.get("foo") || 0) + 1);
      const results = await app.gather(
        ...new Array(a).keys().map((i) => app.call(one)(i)),
      );
      return results.reduce((sum, val) => sum + val);
    }

    const brrr = new LocalBrrr(topic, {
      codec,
      handlers: { one, foo },
    });
    await brrr.run(foo)(50);

    deepStrictEqual(Object.fromEntries(calls), { one: 50, foo: 51 });
  });

  await test("app handler names", async () => {
    function foo(ctx: ActiveWorker, a: number): number {
      return a * a;
    }

    async function bar(app: ActiveWorker, a: number): Promise<number> {
      return (
        (await app.call(foo)(a)) *
        (await app.call<[number], number>("quux/zim")(a))
      );
    }

    const worker = new AppWorker(codec, server, {
      "quux/zim": foo,
      "quux/bar": bar,
    });
    const localApp = new LocalApp(topic, server, worker);
    localApp.run();

    const call = await codec.encodeCall("quux/bar", [4]);
    const done = waitFor(call, async () => {
      strictEqual(await localApp.read("quux/zim")(4), 16);
      strictEqual(await localApp.read(foo)(4), 16);
    });

    await localApp.schedule("quux/bar")(4);
    return done;
  });

  await suite("loop mode", async () => {
    let queues: Record<string, (string | typeof BrrrShutdownSymbol)[]>;
    let server: Server;

    const publisher: Publisher = {
      async emit(topic: string, callId: string | Call): Promise<void> {
        queues[topic]?.push(callId as string);
      },
    };

    async function flusher() {
      const item = queues[topic]?.shift();
      if (!item) {
        return BrrrShutdownSymbol;
      }
      return item;
    }

    beforeEach(() => {
      queues = {
        [topic]: [],
      };
      server = new Server(store, cache, publisher);
    });

    await test("basic loop", async () => {
      async function foo(app: ActiveWorker, a: number) {
        return (await app.call(bar, topic)(a + 1)) + 1;
      }

      const server = new Server(store, cache, publisher);
      const app = new AppWorker(codec, server, { ...handlers, foo });

      await app.schedule(foo, topic)(122);

      await server.loop(topic, app.handle, flusher);

      strictEqual(await app.read(foo)(122), 457);
    });

    await test("loop with no tasks", async () => {
      const app = new AppWorker(codec, server, handlers);

      let looped = false;
      await server.loop(topic, app.handle, async () => {
        if (looped) {
          return BrrrShutdownSymbol;
        }
        looped = true;
        return undefined;
      });

      ok(looped);
    });

    await test("resumable loop", async () => {
      class MyError extends Error {}

      let errors = 5;

      async function foo(ctx: ActiveWorker, a: number): Promise<number> {
        if (errors) {
          errors--;
          throw new MyError();
        }
        queues[topic]?.push(BrrrShutdownSymbol);
        return a;
      }

      const app = new AppWorker(codec, server, {
        ...handlers,
        foo,
      });

      while (true) {
        try {
          await app.schedule(foo, topic)(3);
          await server.loop(topic, app.handle, async () => {
            return queues[topic]?.pop();
          });
          break;
        } catch (err) {
          if (err instanceof MyError) {
            continue;
          }
          throw err;
        }
      }
      strictEqual(errors, 0);
    });

    await test("resumable loop nested", async () => {
      class MyError extends Error {}

      let errors = 5;

      function bar(ctx: ActiveWorker, a: number): number {
        if (errors) {
          errors--;
          throw new MyError();
        }
        return a;
      }

      async function foo(app: ActiveWorker, a: number): Promise<number> {
        return app.call(bar)(a);
      }

      const app = new AppWorker(codec, server, {
        ...handlers,
        foo,
        bar,
      });

      while (true) {
        try {
          await app.schedule(foo, topic)(3);
          await server.loop(topic, app.handle, flusher);
          break;
        } catch (err) {
          if (err instanceof MyError) {
            continue;
          }
          throw err;
        }
      }
      strictEqual(errors, 0);
    });

    await test("stress parallel", async () => {
      async function fib(app: ActiveWorker, n: bigint): Promise<bigint> {
        if (n < 2) {
          return n;
        }
        const [a, b] = await app.gather(
          app.call(fib)(n - 1n),
          app.call(fib)(n - 2n),
        );
        return a + b;
      }

      async function top(app: ActiveWorker): Promise<void> {
        const n = await app.call(fib)(1000n);
        deepStrictEqual(
          n,
          43466557686937456435688527675040625802564660517371780402481729089536555417949051890403879840079255169295922593080322634775209689623239873322471161642996440906533187938298969649928516003704476137795166849228875n,
        );
      }

      const codec = new NaiveJsonCodec({ stringify, parse });
      const app = new AppWorker(codec, server, { fib, top });
      await app.schedule(top, topic)();

      await Promise.all(
        new Array(10).keys().map(() => server.loop(topic, app.handle, flusher)),
      );
    });

    await test("app subclass", async () => {
      function bar(ctx: ActiveWorker, a: number): number {
        return a + 1;
      }

      function baz(ctx: ActiveWorker, a: number) {
        return a + 10;
      }

      async function foo(app: ActiveWorker, a: number): Promise<number> {
        return app.call(bar)(a);
      }

      class MyAppWorker extends AppWorker<ActiveWorker> {
        public readonly myHandle = async (
          request: Request,
          connection: Connection,
        ): Promise<Response | Defer> => {
          const response = await this.handle(request, connection);
          if (response instanceof Defer) {
            for (const deferredCall of response.calls) {
              Object.defineProperty(deferredCall.call, "taskName", {
                value: "baz",
              });
            }
            return new Defer(...response.calls);
          }
          return response;
        };
      }

      const app = new MyAppWorker(codec, server, { foo, bar, baz });
      await app.schedule(foo, topic)(4);
      await server.loop(topic, app.myHandle, flusher);
      strictEqual(await app.read(foo)(4), 14);
    });

    await suite("spawn limit", async () => {
      await test("spawn limit depth", async () => {
        let n = 0;

        async function foo(app: ActiveWorker, a: number): Promise<number> {
          n++;
          if (a === 0) {
            return 0;
          }
          return app.call(foo)(a - 1);
        }

        const server = new Server(store, cache, publisher);
        // override for test
        Object.defineProperty(server, "spawnLimit", {
          value: 100,
        });

        const app = new AppWorker(codec, server, { foo });
        await app.schedule(foo, topic)(server.spawnLimit + 3);

        await rejects(server.loop(topic, app.handle, flusher));
        strictEqual(n, server.spawnLimit);
      });

      await test("spawn limit recoverable", async () => {
        function one(ctx: ActiveWorker, _: number): number {
          return 1;
        }

        async function foo(app: ActiveWorker, a: number): Promise<number> {
          const results = await app.gather(
            ...new Array(a).keys().map((i) => app.call(one)(i)),
          );
          return results.reduce((sum, val) => sum + val);
        }

        const server = new Server(store, cache, publisher);
        // override for test
        Object.defineProperty(server, "spawnLimit", {
          value: 100,
        });
        const n = server.spawnLimit + 1;
        let spawnLimitEncountered = false;
        const app = new AppWorker(codec, server, { one, foo });
        while (true) {
          // reset cache
          Object.defineProperty(cache, "cache", {
            value: new Map(),
          });
          try {
            await app.schedule(foo, topic)(n);
            await server.loop(topic, app.handle, flusher);
            break;
          } catch (err) {
            if (err instanceof SpawnLimitError) {
              spawnLimitEncountered = true;
              continue;
            }
            throw err;
          }
        }
        ok(spawnLimitEncountered);
        strictEqual(await app.read(foo)(n), n);
      });

      await test("spawn limit breadth mapped", async () => {
        const calls = new Map<string, number>();

        function one(ctx: ActiveWorker, _: number): number {
          calls.set("one", (calls.get("one") || 0) + 1);
          return 1;
        }

        async function foo(app: ActiveWorker, a: number): Promise<number> {
          calls.set("foo", (calls.get("foo") || 0) + 1);
          const results = await app.gather(
            ...new Array(a).keys().map((i) => app.call(one)(i)),
          );
          return results.reduce((sum, val) => sum + val);
        }

        const app = new AppWorker(codec, server, { one, foo });
        await app.schedule(foo, topic)(server.spawnLimit + 4);

        await rejects(server.loop(topic, app.handle, flusher), SpawnLimitError);
        strictEqual(calls.get(foo.name), 1);
      });

      await test("spawn limit breadth manual", async () => {
        const calls = new Map<string, number>();

        function one(ctx: ActiveWorker, _: number): number {
          calls.set("one", (calls.get("one") || 0) + 1);
          return 1;
        }

        async function foo(app: ActiveWorker, a: number): Promise<number> {
          calls.set("foo", (calls.get("foo") || 0) + 1);
          let total = 0;
          for (let i = 0; i < a; i++) {
            total += await app.call(one)(i);
          }
          return total;
        }

        // override for test
        const server = new Server(store, cache, publisher);
        Object.defineProperty(server, "spawnLimit", {
          value: 100,
        });

        const app = new AppWorker(codec, server, { foo, one });
        await app.schedule(foo, topic)(server.spawnLimit + 3);

        await rejects(server.loop(topic, app.handle, flusher));
        deepStrictEqual(Object.fromEntries(calls), {
          one: server.spawnLimit / 2,
          foo: server.spawnLimit / 2,
        });
      });

      await test("spawn limit cached", async () => {
        let n = 0;
        let final = undefined;

        function same(ctx: ActiveWorker, a: number): number {
          n++;
          return a;
        }

        async function foo(app: ActiveWorker, a: number): Promise<number> {
          const results = await app.gather(
            ...new Array(a).fill(1).map((i) => app.call(same)(i)),
          );
          const val = results.reduce((sum, val) => sum + val);
          final = val;
          return val;
        }

        const server = new Server(store, cache, publisher);
        Object.defineProperty(server, "spawnLimit", {
          value: 100,
        });

        const app = new AppWorker(codec, server, { foo, same });
        await app.schedule(foo, topic)(server.spawnLimit + 5);

        await server.loop(topic, app.handle, flusher);
        strictEqual(n, 1);
        strictEqual(final, server.spawnLimit + 5);
      });
    });
  });
});
