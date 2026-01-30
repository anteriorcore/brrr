import { suite, test } from "node:test";
import { NaiveJsonCodec } from "./naive-json-codec.ts";
import { codecContractTest } from "./codec.test.ts";

await suite(import.meta.filename, async () => {
  await test(NaiveJsonCodec.name, async () => {
    await codecContractTest(new NaiveJsonCodec(), () => null);
  });
});
