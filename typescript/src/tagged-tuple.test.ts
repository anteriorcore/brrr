import { suite, test } from "node:test";
import { deepStrictEqual, ok } from "node:assert/strict";
import { throws } from "node:assert";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";
import { bytesField, TaggedTuple } from "./tagged-tuple.ts";

await suite(import.meta.filename, async () => {
  class Foo {
    public static readonly tag = 0;
    public static readonly fields = ["bar", "baz"] as const;

    public readonly bar: number;
    public readonly baz: string;

    public constructor(foo: number, bar: string) {
      this.bar = foo;
      this.baz = bar;
    }
  }

  await suite("fromTuple", async () => {
    await test("create instance", async () => {
      const foo = TaggedTuple.fromTuple(Foo, [0, 42, "hello"]);
      ok(foo instanceof Foo);
      ok(foo.bar === 42);
      ok(foo.baz === "hello");
    });
    await test("tag mismatch", async () => {
      throws(
        () => TaggedTuple.fromTuple(Foo, [1, 42, "hello"]),
        TagMismatchError,
      );
    });
    await test("too many args", async () => {
      throws(
        () => TaggedTuple.fromTuple(Foo, [0, 42, "hello", "a"]),
        MalformedTaggedTupleError,
      );
    });
    await test("too few args", async () => {
      throws(
        () => TaggedTuple.fromTuple(Foo, [0, 42]),
        MalformedTaggedTupleError,
      );
    });
  });

  await suite("asTuple", async () => {
    await test("basic", async () => {
      const foo: Foo = TaggedTuple.fromTuple(Foo, [0, 42, "hello"]);
      deepStrictEqual(TaggedTuple.asTuple(foo), [0, 42, "hello"]);
    });
  });

  await suite("encode and decode", async () => {
    const foo: Foo = TaggedTuple.fromTuple(Foo, [0, 42, "hello"]);
    const encoded: Uint8Array = TaggedTuple.encode(foo);
    const decoded = TaggedTuple.decode(Foo, encoded);
    deepStrictEqual(decoded, foo);
  });

  await suite("encode and decode (string)", async () => {
    const foo: Foo = TaggedTuple.fromTuple(Foo, [0, 42, "hello"]);
    const encoded: string = TaggedTuple.encodeToString(foo);
    const decoded = TaggedTuple.decodeFromString(Foo, encoded);
    deepStrictEqual(decoded, foo);
  });

  await suite("bytes fields", async () => {
    class Blob {
      public static readonly tag = 7;
      public static readonly fields = ["name", bytesField("data")] as const;

      public readonly name: string;
      public readonly data: Uint8Array;

      public constructor(name: string, data: Uint8Array) {
        this.name = name;
        this.data = data;
      }
    }

    // Not valid UTF-8, which is exactly why byte fields travel as hex.
    const data = Uint8Array.of(0x00, 0xff, 0xfe, 0x80);

    await test("go on the wire as hex", async () => {
      deepStrictEqual(TaggedTuple.asTuple(new Blob("b", data)), [
        7,
        "b",
        "00fffe80",
      ]);
    });

    await test("round trip through encode and decode", async () => {
      const original = new Blob("b", data);
      const decoded = TaggedTuple.decode(Blob, TaggedTuple.encode(original));
      deepStrictEqual(decoded, original);
    });

    await test("reject a non-hex payload instead of truncating it", async () => {
      throws(() => TaggedTuple.fromTuple(Blob, [7, "b", "00ff8"]));
    });
  });
});
