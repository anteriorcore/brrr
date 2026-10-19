import { suite, test } from "node:test";
import { deepStrictEqual } from "node:assert";
import { combinations } from "./matrix.ts";

/**
 * Make sure the types of `actual` and `expected` are the same.
 */
function deepStrictEqualWithTypes<const T>(actual: T, expected: T): void {
  deepStrictEqual(actual, expected);
}

suite(import.meta.filename, async () => {
  test("basic object", async () => {
    const object = {
      a: [1, 2],
      b: ["x", "y"],
    };
    deepStrictEqualWithTypes(combinations(object), [
      { a: 1, b: "x" },
      { a: 1, b: "y" },
      { a: 2, b: "x" },
      { a: 2, b: "y" },
    ]);
  });

  test("single", async () => {
    const object = {
      color: ["red", "blue", "green"],
    };
    deepStrictEqualWithTypes(combinations(object), [
      { color: "red" },
      { color: "blue" },
      { color: "green" },
    ]);
  });

  test("empty", async () => {
    const object = {};
    deepStrictEqualWithTypes(combinations(object), [{}]);
  });

  test("single value arrays", async () => {
    const object = {
      os: ["linux"],
      arch: ["x64"],
      node: ["18"],
    };
    deepStrictEqualWithTypes(combinations(object), [
      { os: "linux", arch: "x64", node: "18" },
    ]);
  });

  test("multiple properties", async () => {
    const object = {
      os: ["linux", "windows"],
      arch: ["x64", "arm64"],
      node: ["18", "20"],
    };
    deepStrictEqualWithTypes(combinations(object), [
      { os: "linux", arch: "x64", node: "18" },
      { os: "linux", arch: "x64", node: "20" },
      { os: "linux", arch: "arm64", node: "18" },
      { os: "linux", arch: "arm64", node: "20" },
      { os: "windows", arch: "x64", node: "18" },
      { os: "windows", arch: "x64", node: "20" },
      { os: "windows", arch: "arm64", node: "18" },
      { os: "windows", arch: "arm64", node: "20" },
    ]);
  });

  test("mixed types", async () => {
    const object = {
      string: ["hello", "world"],
      number: [1, 2, 3],
      boolean: [true, false],
    };
    deepStrictEqualWithTypes(combinations(object), [
      { string: "hello", number: 1, boolean: true },
      { string: "hello", number: 1, boolean: false },
      { string: "hello", number: 2, boolean: true },
      { string: "hello", number: 2, boolean: false },
      { string: "hello", number: 3, boolean: true },
      { string: "hello", number: 3, boolean: false },
      { string: "world", number: 1, boolean: true },
      { string: "world", number: 1, boolean: false },
      { string: "world", number: 2, boolean: true },
      { string: "world", number: 2, boolean: false },
      { string: "world", number: 3, boolean: true },
      { string: "world", number: 3, boolean: false },
    ]);
  });

  test("empty array", async () => {
    const object = {
      empty: [],
      normal: ["a", "b"],
    };
    deepStrictEqualWithTypes(combinations(object), []);
  });
});
