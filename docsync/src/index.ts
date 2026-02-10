#!/usr/bin/env node --enable-source-maps
import { glob } from "node:fs/promises";
import { extname } from "node:path";
import { readFile } from "node:fs/promises";
import { deepStrictEqual } from "node:assert";

export async function docsync(
  sentinelRegex: RegExp,
  globs: readonly string[],
  extractors: Record<string, (text: string) => Generator<string>>,
) {
  const [head, ...tail] = await Promise.all(
    globs.map(async (it) => {
      const map = new Map<string, string>();
      for await (const file of glob(it)) {
        const extractor = extractors[extname(file) as keyof typeof extractors];
        if (!extractor) {
          throw new Error(`Unknown extractor for extension: ${file}`);
        }
        const content = await readFile(file, "utf-8");
        for (const docstring of extractor(content)) {
          const sentinel = docstring.match(sentinelRegex)?.at(1);
          if (!sentinel) {
            continue;
          }
          if (map.has(sentinel)) {
            throw new Error(`Duplicate sentinel: ${sentinel}`);
          }
          map.set(sentinel, docstring.replace(sentinelRegex, ""));
        }
      }
      return map;
    }),
  );
  if (!head) {
    throw new Error("empty inputs");
  }
  for (const it of tail) {
    deepStrictEqual(head, it);
  }
}
