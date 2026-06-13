import { deepStrictEqual } from "node:assert";
import { join } from "node:path";
import process from "node:process";

import { PathParser } from "./path-parser.ts";

/**
 * docsync-check checks if two directories' doc tags are in sync.
 *
 * <docsync>CmdCheck</docsync>
 */
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

/**
 * docsync-get extracts all docsync nodes under a path.
 *
 * <docsync>CmdGet</docsync>
 */
export async function docsyncGet(): Promise<void> {
  const [path, slug] = process.argv.slice(2);
  if (!path) {
    console.error(`
Usage: docsync-get <PATH> [SLUG]

Example:

    $ docsync-get ./src/cmds.ts | jq
    {
      "CmdCheck": "docsync-check checks if two directories' doc tags are in sync.",
      "CmdGet": "docsync-get extracts all docsync nodes under a path."
    }

    $ docsync-get ./src/cmds.ts CmdGet
    docsync-get extracts all docsync nodes under a path.
`);
    process.exit(1);
  }

  const parser = new PathParser();
  const m = await parser.getPath(path);
  if (slug !== undefined) {
    if (!m.has(slug)) {
      console.error(`No such key ${slug} in docsync tags for ${path}`);
      process.exit(1);
    }
    console.log(m.get(slug));
  } else {
    const o = Object.fromEntries(m.entries());
    console.log(JSON.stringify(o));
  }
}
