import {
  bencoder,
  decoder,
  encoder,
  encoding,
  hex,
} from "./internal-codecs.ts";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";

/**
 * A single field of a tagged tuple, in wire order.
 *
 * A plain string is the property name of a field that bencode round-trips as
 * is. `bytesField` marks a property holding raw bytes, which are hex-encoded
 * on the wire. This is the counterpart of inspecting the declared field type
 * on the Python side, which has no equivalent at runtime here.
 */
export type FieldSpec =
  | string
  | { readonly name: string; readonly bytes: true };

export function bytesField(name: string) {
  return { name, bytes: true } as const;
}

const nameOf = (field: FieldSpec): string =>
  typeof field === "string" ? field : field.name;

const isBytes = (field: FieldSpec): boolean => typeof field !== "string";

export interface Tagged<T = any, A extends unknown[] = any[]> {
  new (...args: A): T;

  readonly tag: number;
  readonly fields: readonly FieldSpec[];
}

// The wire representation is not `[number, ...A]`: byte-valued fields are hex
// strings on the wire but Uint8Array in the constructor, so the slots don't
// line up with the constructor's argument types.
type WireTuple = readonly [number, ...unknown[]];

function fromTuple<T, A extends unknown[]>(
  tagged: Tagged<T, A>,
  data: WireTuple,
): InstanceType<typeof tagged> {
  if (data[0] !== tagged.tag) {
    throw new TagMismatchError(tagged);
  }
  if (data.length - 1 !== tagged.fields.length) {
    throw new MalformedTaggedTupleError(tagged);
  }
  const args = tagged.fields.map((field, i) => {
    const val = data[i + 1];
    return isBytes(field) ? hex.decode(val as string) : val;
  });
  return new tagged(...(args as A));
}

function asTuple<T extends object, A extends unknown[]>(
  obj: InstanceType<Tagged<T, A>>,
): [number, ...unknown[]] {
  const tagged = obj.constructor as Tagged<T, A>;
  return [
    tagged.tag,
    ...tagged.fields.map((field) => {
      const val = (obj as Record<string, unknown>)[nameOf(field)];
      return isBytes(field) ? hex.encode(val as Uint8Array) : val;
    }),
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
  const decoded = bencoder.decode(data, encoding) as WireTuple;
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
  public static readonly tag = 3;
  public static readonly fields = [
    "rootId",
    "callHash",
    "topic",
    bytesField("metadata"),
  ] as const;

  public readonly rootId: string;
  public readonly callHash: string;
  public readonly topic: string;
  public readonly metadata: Uint8Array;

  constructor(
    rootId: string,
    callHash: string,
    topic: string,
    metadata: Uint8Array,
  ) {
    this.rootId = rootId;
    this.callHash = callHash;
    this.topic = topic;
    this.metadata = metadata;
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
  public static readonly tag = 4;
  public static readonly fields = [
    "rootId",
    "callHash",
    bytesField("metadata"),
  ] as const;

  public readonly rootId: string;
  public readonly callHash: string;
  public readonly metadata: Uint8Array;

  constructor(rootId: string, callHash: string, metadata: Uint8Array) {
    this.rootId = rootId;
    this.callHash = callHash;
    this.metadata = metadata;
  }
}
