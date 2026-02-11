#!/usr/bin/env node --unhandled-rejections=strict
import {
  ActiveWorker,
  AppWorker,
  DemoJsonCodec,
  type DemoJsonCodecContext,
  Server,
} from "../src/index.ts";
import { Dynamo, Redis } from "../src/backends/index.ts";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { createClientPool } from "redis";
import { env } from "node:process";

async function createDynamo(): Promise<Dynamo> {
  const tableName = process.env.DYNAMODB_TABLE_NAME || "brrr";
  const client = new DynamoDBClient();
  return new Dynamo(client, tableName);
}

async function createRedis(): Promise<Redis> {
  const client = createClientPool({
    RESP: 3,
    ...(env.BRRR_DEMO_REDIS_URL && { url: env.BRRR_DEMO_REDIS_URL }),
  });
  return new Redis(client);
}

// TypeScript demo is worker only
const dynamo = await createDynamo();
const redis = await createRedis();

const topics = {
  py: "brrr-py-demo",
  ts: "brrr-ts-demo",
} as const;

// fib and lucas share the same arg type
type Arg = { n: number; salt: string | null };

/**
 * Lucas number: lucas(n) = fib(n - 1) + fib(n + 1)
 * https://en.wikipedia.org/wiki/Lucas_number
 */
async function lucas(
  app: DemoJsonCodecContext,
  { n, salt }: Arg,
): Promise<number> {
  if (n < 2) {
    return 2 - n;
  }
  const [a, b] = await app.gather(
    app.call(fib)({ n: n - 1, salt }),
    app.call(fib)({ n: n + 1, salt }),
  );
  return a + b;
}

async function fib(
  app: DemoJsonCodecContext,
  { n, salt }: Arg,
): Promise<number> {
  if (n < 2) {
    return n;
  }
  const [a, b] = await app.gather(
    app.call(fib)({ n: n - 1, salt }),
    app.call(fib)({ n: n - 2, salt }),
  );
  return a + b;
}

const server = new Server(dynamo, redis, {
  async emit(topic: string, callId: string): Promise<void> {
    await redis.push(topic, callId);
  },
});

const codec = new DemoJsonCodec();

const app = new AppWorker(codec, server, { fib, lucas });

await server.loop(topics.ts, app.handle, async () => {
  return redis.pop(topics.ts);
});
