import { suite, test } from "node:test";
import { strictEqual } from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { AppWorker } from "./app.ts";
import { Server } from "./connection.ts";
import { DemoJsonCodec, type DemoJsonCodecContext } from "./demo-json-codec.ts";
import type { Publisher } from "./emitter.ts";
import { Dynamo } from "./backends/dynamo.ts";
import { InMemoryCache } from "./backends/in-memory.ts";
import { BrrrShutdownSymbol } from "./symbol.ts";
import { Barrier } from "./fixture.test.ts";

type TestContext = DemoJsonCodecContext;

class DuplicatingQueue implements Publisher {
  private readonly topics = new Map<string, string[]>();

  public async emit(topic: string, message: string): Promise<void> {
    const queue = this.topics.get(topic) ?? [];
    queue.push(message, message);
    this.topics.set(topic, queue);
  }

  public closeOnEmpty(topic: string) {
    return async (): Promise<string | typeof BrrrShutdownSymbol> =>
      this.topics.get(topic)?.shift() ?? BrrrShutdownSymbol;
  }
}

suite(import.meta.filename, async () => {
  test("no overwrite return", { timeout: 60_000 }, async () => {
    const client = new DynamoDBClient();
    const tableName = `brrr_test_${randomUUID()}`;
    const store = new Dynamo(client, tableName);
    await store.createTable();

    const codec = new DemoJsonCodec();
    const topic = "test";
    const cache = new InMemoryCache();
    const queue = new DuplicatingQueue();

    let wasCalled = false;
    const barrier = new Barrier(2);

    async function foo(_: TestContext) {
      await barrier.wait();
      if (wasCalled) {
        await sleep(1000);
        return "last";
      }
      wasCalled = true;
      return "first";
    }

    const server = new Server(store, cache, queue);
    const app = new AppWorker(codec, server, { foo });
    await app.schedule(foo, topic)();
    await Promise.all([
      server.loop(topic, app.handle, queue.closeOnEmpty(topic)),
      server.loop(topic, app.handle, queue.closeOnEmpty(topic)),
    ]);
    strictEqual(await app.read(foo)(), "first");
  });
});
