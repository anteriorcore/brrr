import { bencoder, decoder, encoder, encoding } from "./internal-codecs.ts";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";

export interface Tagged<T = any, A extends unknown[] = any[]> {
  new (...args: A): T;

  readonly tag: number;
}

function fromTuple<T, A extends unknown[]>(
  tagged: Tagged<T, A>,
  data: [number, ...A],
): InstanceType<typeof tagged> {
  if (data[0] !== tagged.tag) {
    throw new TagMismatchError(tagged);
  }
  if (data.length - 1 !== tagged.length) {
    throw new MalformedTaggedTupleError(tagged);
  }
  return new tagged(...(data.slice(1) as A));
}

function asTuple<T extends object, A extends unknown[]>(
  obj: InstanceType<Tagged<T, A>>,
): [number, ...A] {
  return [(obj.constructor as Tagged<T, A>).tag, ...Object.values(obj)] as [
    number,
    ...A,
  ];
}

function encode(obj: InstanceType<Tagged>): Uint8Array {
  return bencoder.encode(asTuple(obj));
}

function encodeToString(obj: InstanceType<Tagged>): string {
  return decoder.decode(encode(obj));
}

function decode<T, A extends unknown[]>(
  tagged: Tagged<T, A>,
  data: Uint8Array,
): InstanceType<typeof tagged> {
  const decoded = bencoder.decode(data, encoding) as [number, ...A];
  return fromTuple(tagged, decoded);
}

function decodeFromString<T, A extends unknown[]>(
  tagged: Tagged<T, A>,
  data: string,
): InstanceType<typeof tagged> {
  return decode(tagged, encoder.encode(data));
}

export const TaggedTuple = {
  fromTuple,
  asTuple,
  encode,
  encodeToString,
  decode,
  decodeFromString,
} as const;

export class PendingReturn {
  public static readonly tag = 1;

  public readonly rootId: string;
  public readonly callHash: string;
  public readonly topic: string;

  constructor(rootId: string, callHash: string, topic: string) {
    this.rootId = rootId;
    this.callHash = callHash;
    this.topic = topic;
  }

  public isRepeatedCall(other: PendingReturn): boolean {
    return (
      this.rootId !== other.rootId &&
      this.callHash === other.callHash &&
      this.topic === other.topic
    );
  }
}

export class ScheduleMessage {
  public static readonly tag = 2;

  public readonly rootId: string;
  public readonly callHash: string;

  constructor(rootId: string, callHash: string) {
    this.rootId = rootId;
    this.callHash = callHash;
  }
}
