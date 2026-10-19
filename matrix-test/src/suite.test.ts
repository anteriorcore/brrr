import { suite, test } from "node:test";
import { createMatrixSuite, matrixSuite } from "./suite.ts";
import { deepStrictEqual } from "node:assert";
import { type Combination } from "./matrix.ts";

suite(import.meta.filename, async () => {
  const matrix = {
    os: ["linux", "windows"],
    arch: ["x64", "arm64"],
  };

  function makeTestName(combination: Combination<typeof matrix>): string {
    return `os=${combination.os},arch=${combination.arch}`;
  }

  const combinationsExpected = [
    { os: "linux", arch: "x64" },
    { os: "linux", arch: "arm64" },
    { os: "windows", arch: "x64" },
    { os: "windows", arch: "arm64" },
  ] as const;

  const suiteNamesExpected = combinationsExpected.map(makeTestName);

  test(matrixSuite.name, async () => {
    const combinations: unknown[] = [];
    await matrixSuite("sample matrix suite", matrix, async (_, combination) => {
      combinations.push(combination);
    });
    deepStrictEqual(combinations, combinationsExpected);
  });

  test(createMatrixSuite.name, async () => {
    const combinations: unknown[] = [];
    const names: string[] = [];

    const matrixSuite = createMatrixSuite(matrix, makeTestName);

    await matrixSuite("sample matrix suite", async (context, combination) => {
      combinations.push(combination);
      names.push(context.name);
    });

    deepStrictEqual(combinations, combinationsExpected);
    deepStrictEqual(names, suiteNamesExpected);
  });
});
