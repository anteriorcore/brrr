import { type BinaryToTextEncoding, createHash } from "node:crypto";
import type { Call } from "./call.ts";
import type { Codec } from "./codec.ts";
import { decoder, encoder } from "./internal-codecs.ts";
import { ActiveWorker, type Task } from "./app.ts";

type Json = {
  parse: <T = unknown>(text: string) => T;
  stringify: (value: unknown) => string;
};

/**
 * An opinionated codec for demo/testing purposes.
 *
 * Uses built-in `JSON` for serialization and deserialization, and expects
 *  `ActiveWorker` as a context. Can take custom JSON implementation to
 * customize its behavior.
 *
 * It tries its best to ensure that the serialized data is deterministic by
 * sorting object keys recursively before serialization, but it's not
 * reccommended for production use; the primary purpose of this codec is
 * executable documentation.
 */
export class DemoJsonCodec implements Codec<ActiveWorker> {
  public static readonly algorithm = "sha256";
  public static readonly binaryToTextEncoding =
    "hex" satisfies BinaryToTextEncoding;

  private readonly json: Json;

  public constructor(json: Json = JSON) {
    this.json = json;
  }

  public async decodeReturn(_: string, payload: Uint8Array): Promise<unknown> {
    const decoded = decoder.decode(payload);
    return this.json.parse(decoded);
  }

  public async encodeCall<A extends unknown[]>(
    taskName: string,
    args: A,
  ): Promise<Call> {
    const sortedArgs = args.map(DemoJsonCodec.sortObjectKeys);
    const data = this.json.stringify(sortedArgs);
    const payload = encoder.encode(data);
    const callHash = await this.hashCall(taskName, sortedArgs);
    return { taskName, payload, callHash };
  }

  public async invokeTask<A extends unknown[], R>(
    call: Call,
    handler: Task<ActiveWorker, A, R>,
    activeWorkerFactory: () => ActiveWorker,
  ): Promise<Uint8Array> {
    const decoded = decoder.decode(call.payload);
    const args = this.json.parse(decoded) as A;
    const result = await handler(activeWorkerFactory(), ...args);
    const resultJson = this.json.stringify(result);
    return encoder.encode(resultJson);
  }

  protected async hashCall<A extends unknown>(
    taskName: string,
    args: A,
  ): Promise<string> {
    const data = this.json.stringify([taskName, args]);
    return createHash(DemoJsonCodec.algorithm)
      .update(data)
      .digest(DemoJsonCodec.binaryToTextEncoding);
  }

  protected static sortObjectKeys<T>(unordered: T): T {
    if (!unordered || typeof unordered !== "object") {
      return unordered;
    }
    if (Array.isArray(unordered)) {
      return unordered.map(DemoJsonCodec.sortObjectKeys) as T;
    }
    const entries = Object.keys(unordered)
      .sort()
      .map((key) => [
        key,
        DemoJsonCodec.sortObjectKeys(unordered[key as keyof typeof unordered]),
      ]);
    return Object.fromEntries(entries);
  }
}
