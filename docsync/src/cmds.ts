import { deepStrictEqual } from "node:assert";
import { join } from "node:path";
import process from "node:process";

import { pythonGetDir } from "./python.ts";
import { getTypeScriptDocStrings } from "./typescript.ts";

export async function docsyncCheck(): Promise<void> {
  const [pythonDir, tsDir] = process.argv.slice(2);
  if (!pythonDir || !tsDir) {
    console.error("Usage: docsync-check <PYTHON_DIR> <TYPESCRIPT_DIR>");
    process.exit(1);
  }

  console.log("Comparing", pythonDir, "and", tsDir);
  const path = {
    python: pythonDir,
    typescript: join(tsDir, "src/**/*.ts"),
  } as const;

  const python = await pythonGetDir(path.python);
  const ts = await getTypeScriptDocStrings(path.typescript);

  deepStrictEqual(python, ts);
}
