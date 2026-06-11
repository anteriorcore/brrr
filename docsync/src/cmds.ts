import { deepStrictEqual } from "node:assert";
import { join } from "node:path";
import process from "node:process";

import { pythonGetDir } from "./python.ts";
import { tsGetDir } from "./typescript.ts";

export async function docsyncCheck(): Promise<void> {
  const [pythonDir, tsDir] = process.argv.slice(2);
  if (!pythonDir || !tsDir) {
    console.error("Usage: docsync-check <PYTHON_DIR> <TYPESCRIPT_DIR>");
    process.exit(1);
  }

  console.log("Comparing", pythonDir, "and", tsDir);

  const [python, ts] = await Promise.all([
    pythonGetDir(pythonDir),
    tsGetDir(tsDir),
  ]);

  deepStrictEqual(python, ts);
}
