import { suite, test } from "node:test";
import { DemoJsonCodec, type DemoJsonCodecContext } from "./demo-json-codec.ts";
import { codecContractTest } from "./codec.test.ts";

await suite(import.meta.filename, async () => {
  await test(DemoJsonCodec.name, async () => {
    await codecContractTest(
      new DemoJsonCodec(),
      // @ts-expect-error type cheat for test
      () => null as DemoJsonCodecContext,
    );
  });
});
