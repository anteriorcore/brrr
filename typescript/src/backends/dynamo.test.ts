import { mock, suite, test } from "node:test";

import { deepStrictEqual, strictEqual } from "node:assert/strict";
import { Dynamo } from "./dynamo.ts";
import { randomUUID } from "node:crypto";

await suite(import.meta.filename, async () => {
  await test("Retry DynamoDB GET on not found", async () => {
    let callCount = 0;
    const mockClient = {
      send: mock.fn(async () => {
        if (callCount == 2) {
          return { Item: { value: new Uint8Array([1, 2, 3]) } };
        }
        callCount++;
        return { Item: undefined };
      }),
    };

    const dynamo = new Dynamo(mockClient as any, randomUUID());
    const result = await dynamo.getWithRetry({
      type: "call",
      callHash: "testHash",
    });

    deepStrictEqual(result, new Uint8Array([1, 2, 3]));
    strictEqual(callCount, 2);
  });
  await test("Don't retry DynamoDB GET on found", async () => {
    const mockClient = {
      send: mock.fn(async () => {
        return { Item: { value: new Uint8Array([1, 2, 3]) } };
      }),
    };

    const dynamo = new Dynamo(mockClient as any, randomUUID());
    const result = await dynamo.getWithRetry({
      type: "call",
      callHash: "testHash",
    });

    deepStrictEqual(result, new Uint8Array([1, 2, 3]));
    strictEqual(mockClient.send.mock.calls.length, 1);
  });
});
