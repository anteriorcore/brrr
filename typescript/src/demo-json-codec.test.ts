import { suite, test } from "node:test";
import { DemoJsonCodec } from "./demo-json-codec.ts";
import { codecContractTest } from "./codec.test.ts";

await suite(import.meta.filename, async () => {
  await test(DemoJsonCodec.name, async () => {
    await codecContractTest(new DemoJsonCodec(), () => null);
  });
});
