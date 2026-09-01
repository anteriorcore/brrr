import { bencoder, decoder, encoder, encoding } from "./internal-codecs.ts";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";

export type FieldSpec =
  | string
  | { readonly name: string; readonly optional: true };
export function optionalField(name: string) {
  return { name, optional: true } as const;
}

const nameOf = (f: FieldSpec): string => (typeof f === "string" ? f : f.name);
const isOptional = (f: FieldSpec): boolean => typeof f !== "string";

export interface Tagged<T = any, A extends unknown[] = any[]> {
  new (...args: A): T;

  readonly tag: number;
  readonly fields: readonly FieldSpec[];
}

// Note the encoded tuple is deliberately not typed as `[number, ...A]`: optional
// fields are encoded as a 0- or 1-element list, so the wire slots don't line up
// with the constructor argument types.  Arity and shape are checked at runtime.
function fromTuple<T, A extends unknown[]>(
  tagged: Tagged<T, A>,
  data: readonly [number, ...unknown[]],
): InstanceType<typeof tagged> {
  if (data[0] !== tagged.tag) {
    throw new TagMismatchError(tagged);
  }
  if (data.length - 1 !== tagged.fields.length) {
    throw new MalformedTaggedTupleError(tagged);
  }
  const args = tagged.fields.map((field, index) => {
    const value = data[index + 1];
    if (!isOptional(field)) return value;
    if (!Array.isArray(value) || value.length > 1) {
      throw new MalformedTaggedTupleError(tagged);
    }
    return value.length === 0 ? undefined : value[0];
  });
  return new tagged(...(args as A));
}

function asTuple<T extends object, A extends unknown[]>(
  obj: InstanceType<Tagged<T, A>>,
): [number, ...A] {
  const clz = obj.constructor as Tagged<T, A>;
  const values = clz.fields.map((field) => {
    const value = (obj as Record<string, unknown>)[nameOf(field)];
    if (!isOptional(field)) return value;
    return value === undefined ? [] : [value];
  });
  return [clz.tag, ...(values as A)];
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
  const decoded = bencoder.decode(data, encoding) as [number, ...unknown[]];
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
    optionalField("depthLimit"),
  ] as const;

  public readonly rootId: string;
  public readonly callHash: string;
  public readonly topic: string;
  public readonly depthLimit: number | undefined;

  constructor(
    rootId: string,
    callHash: string,
    topic: string,
    depthLimit?: number,
  ) {
    this.rootId = rootId;
    this.callHash = callHash;
    this.topic = topic;
    this.depthLimit = depthLimit;
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
    optionalField("depthLimit"),
  ] as const;

  public readonly rootId: string;
  public readonly callHash: string;
  public readonly depthLimit: number | undefined;

  constructor(rootId: string, callHash: string, depthLimit?: number) {
    this.rootId = rootId;
    this.callHash = callHash;
    this.depthLimit = depthLimit;
  }
}
