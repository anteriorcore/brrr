import { suite } from "node:test";
import { Dynamo } from "./dynamo.ts";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { storeContractTest } from "../store.test.ts";
import { randomUUID } from "node:crypto";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

suite(import.meta.filename, async () => {
  const client = DynamoDBDocumentClient.from(new DynamoDBClient());

  await storeContractTest(async () => {
    const dynamo = new Dynamo(client, randomUUID());
    await dynamo.createTable();
    return {
      store: dynamo,
      async [Symbol.asyncDispose]() {
        await dynamo.deleteTable();
      },
    };
  });
});
