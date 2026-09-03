import {
  ConditionalCheckFailedException,
  CreateTableCommand,
  DeleteTableCommand,
  type DynamoDBClient,
  ResourceInUseException,
} from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import type { MemKey, Store } from "../store.ts";
import type { NativeAttributeValue } from "@aws-sdk/util-dynamodb";

import { setTimeout } from "node:timers/promises";

export class Dynamo implements Store {
  private readonly client: DynamoDBClient;
  private readonly tableName: string;

  public constructor(client: DynamoDBClient, tableName: string) {
    this.client = client;
    this.tableName = tableName;
  }

  public async has(key: MemKey): Promise<boolean> {
    const { Item } = await this.client.send(
      new GetCommand({
        TableName: this.tableName,
        Key: this.key(key),
        ProjectionExpression: "pk",
      }),
    );
    return !!Item;
  }

  public async get(key: MemKey): Promise<Uint8Array | undefined> {
    const { Item } = await this.runGet(key);
    return Item?.value;
  }

  public async getWithRetry(
    key: MemKey,
    maxRetries: number = 4,
    baseDelayMs: number = 25,
    factor: number = 2,
    maxBackoffMs: number = 300,
  ): Promise<Uint8Array | undefined> {
    let attempt = 0;
    while (true) {
      const { Item } = await this.runGet(key);
      if (Item?.value) return Item.value;

      if (attempt++ >= maxRetries) break;

      await setTimeout(Math.min(baseDelayMs * factor ** attempt, maxBackoffMs));
    }
  }

  public async set(key: MemKey, value: Uint8Array): Promise<void> {
    await this.client.send(
      new PutCommand({
        TableName: this.tableName,
        Item: {
          ...this.key(key),
          value,
        },
      }),
    );
  }

  public async delete(key: MemKey): Promise<void> {
    await this.client.send(
      new DeleteCommand({
        TableName: this.tableName,
        Key: this.key(key),
        ReturnValues: "ALL_OLD",
      }),
    );
  }

  public async setNewValue(key: MemKey, value: Uint8Array): Promise<boolean> {
    try {
      await this.client.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: this.key(key),
          UpdateExpression: "SET #value = :value",
          ConditionExpression: "attribute_not_exists(#value)",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: { ":value": value },
        }),
      );
      return true;
    } catch (err: unknown) {
      if (err instanceof ConditionalCheckFailedException) {
        return false;
      }
      throw err;
    }
  }

  public async compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<boolean> {
    try {
      await this.client.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: this.key(key),
          UpdateExpression: "SET #value = :value",
          ConditionExpression: "#value = :expected",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: {
            ":value": value,
            ":expected": expected,
          },
        }),
      );
      return true;
    } catch (err: unknown) {
      if (err instanceof ConditionalCheckFailedException) {
        return false;
      }
      throw err;
    }
  }

  public async compareAndDelete(
    key: MemKey,
    expected: Uint8Array,
  ): Promise<boolean> {
    try {
      await this.client.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: this.key(key),
          ConditionExpression:
            "attribute_exists(#value) AND #value = :expected",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: { ":expected": expected },
        }),
      );
      return true;
    } catch (err: unknown) {
      if (err instanceof ConditionalCheckFailedException) {
        return false;
      }
      throw err;
    }
  }

  public async createTable(): Promise<void> {
    try {
      await this.client.send(
        new CreateTableCommand({
          TableName: this.tableName,
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
          ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5,
          },
        }),
      );
    } catch (err: unknown) {
      if (err instanceof ResourceInUseException) {
        return;
      }
      throw err;
    }
  }

  public async deleteTable(): Promise<void> {
    await this.client.send(
      new DeleteTableCommand({
        TableName: this.tableName,
      }),
    );
  }

  private key(key: MemKey): Record<string, NativeAttributeValue> {
    return {
      pk: key.callHash,
      sk: key.type,
    };
  }

  private async runGet(key: MemKey) {
    return this.client.send(
      new GetCommand({ TableName: this.tableName, Key: this.key(key) }),
    );
  }
}
