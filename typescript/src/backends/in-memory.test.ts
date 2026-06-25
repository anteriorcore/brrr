import { suite, test } from "node:test";
import { CloseOnEmptyQueue, InMemoryCache, InMemoryStore } from "./in-memory.ts";
import { cacheContractTest, queueContractTest, storeContractTest } from "../store.test.ts";

await suite(import.meta.filename, async () => {
  await test(InMemoryStore.name, async () => {
    await storeContractTest(async () => ({
      store: new InMemoryStore(),
      async [Symbol.asyncDispose]() {},
    }));
  });

  await test(InMemoryCache.name, async () => {
    await cacheContractTest(async () => ({
      cache: new InMemoryCache(),
      async [Symbol.asyncDispose]() {},
    }));
  });

  await suite(CloseOnEmptyQueue.name, async () => {
    await queueContractTest(async (topics) => ({
      queue: new CloseOnEmptyQueue(topics),
      async [Symbol.asyncDispose]() {},
    }))
    await test("Basic push and pop", async () => {
    })
  })
});
