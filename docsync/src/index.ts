#!/usr/bin/env node --enable-source-maps

import { deepStrictEqual } from "node:assert";
import { join } from "node:path";
import process from "node:process";

import { getPythonDocsStrings } from "./python.ts";
import { getTypeScriptDocStrings } from "./typescript.ts";

async function main() {
  const [pythonDir, tsDir] = process.argv.slice(2);
  if (!pythonDir || !tsDir) {
    console.error("Usage: docsync <PYTHON_DIR> <TYPESCRIPT_DIR>");
    process.exit(1);
  }

  console.log("Comparing", pythonDir, "and", tsDir);
  const path = {
    python: join(pythonDir, "src/**/*.py"),
    typescript: join(tsDir, "src/**/*.ts"),
  } as const;

  const python = await getPythonDocsStrings(path.python);
  const ts = await getTypeScriptDocStrings(path.typescript);

  deepStrictEqual(python, ts);

  console.log("Docsync completed successfully");
}

main();
