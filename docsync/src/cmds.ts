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

  console.time("python");
  const python = await pythonGetDir(pythonDir);
  console.timeEnd("python");
  console.time("ts");
  const ts = await tsGetDir(tsDir);
  console.timeEnd("ts");

  deepStrictEqual(python, ts);
}
