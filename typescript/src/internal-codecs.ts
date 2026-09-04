import bencode from "bencode";
import { Buffer } from "node:buffer";
import type { Encoding } from "node:crypto";
import { TextDecoder, TextEncoder } from "node:util";

/**
 * Brrr uses UTF-8 for encoding
 */
export const encoding = "utf-8" as const satisfies Encoding;

/**
 * Bencode encoding and decoding utility.
 */
export const bencoder = {
  encode(data: unknown): Uint8Array {
    return bencode.encode(data);
  },
  decode(data: Uint8Array, _encoding?: typeof encoding): unknown {
    const buffer = Buffer.from(data);
    return bencode.decode(buffer, _encoding);
  },
} as const;

/**
 * Exports TextEncoder and TextDecoder instances for UTF-8 encoding.
 */
export const encoder = new TextEncoder();
export const decoder = new TextDecoder(encoding);

/**
 * Raw bytes are hex-encoded before they go on the wire.
 *
 * Bencode has no separate byte string type and brrr decodes bencode strings as
 * UTF-8, which is lossy for arbitrary bytes, so byte-valued fields travel as
 * their hex representation instead.
 */
export const hex = {
  encode(data: Uint8Array): string {
    return Buffer.from(data).toString("hex");
  },
  decode(data: string): Uint8Array {
    // Buffer.from(_, "hex") stops at the first invalid character instead of
    // failing, which would silently truncate; reject up front like Python's
    // bytes.fromhex does.
    if (!/^([0-9a-fA-F]{2})*$/.test(data)) {
      throw new Error(`Not a hex string: ${JSON.stringify(data)}`);
    }
    return Uint8Array.from(Buffer.from(data, "hex"));
  },
} as const;
