#!/usr/bin/env node --enable-source-maps
import { parseArgs } from "node:util";
import { docsync } from "./index.ts";
import { createTreeSitterExtractors } from "./tree-sitter-extractors.ts";

const { values, positionals } = parseArgs({
  args: process.argv.slice(2),
  options: {
    sentinel: { type: "string", default: "<docsync>(.*?)</docsync>" },
  },
  allowPositionals: true,
});

const regex = new RegExp(values.sentinel);

await docsync(regex, positionals, createTreeSitterExtractors());
