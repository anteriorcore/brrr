import { suite, test } from "node:test";
import { InMemoryCache, InMemoryStore } from "./in-memory.ts";
import { cacheContractTest, storeContractTest } from "../store.test.ts";

suite(import.meta.filename, async () => {
  test(InMemoryStore.name, async () => {
    await storeContractTest(async () => ({
      store: new InMemoryStore(),
      async [Symbol.asyncDispose]() {},
    }));
  });

  test(InMemoryCache.name, async () => {
    await cacheContractTest(async () => ({
      cache: new InMemoryCache(),
      async [Symbol.asyncDispose]() {},
    }));
  });
});
