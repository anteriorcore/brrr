import { deepStrictEqual } from "node:assert";
import { join } from "node:path";
import process from "node:process";

import { PathParser } from "./path-parser.ts";

export async function docsyncCheck(): Promise<void> {
  const [dirA, dirB] = process.argv.slice(2);
  if (!dirA || !dirB) {
    console.error(`
Usage: docsync-check <A> <B>

E.g.:

    $ docsync-check ./typescript/src ./python/src

`);
    process.exit(1);
  }

  console.log("Comparing", dirA, "and", dirB);

  const parser = new PathParser();
  const [a, b] = await Promise.all([
    parser.getPath(dirA),
    parser.getPath(dirB),
  ]);

  deepStrictEqual(a, b);
}
