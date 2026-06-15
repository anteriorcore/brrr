import { glob, stat } from "node:fs/promises";
import { extname } from "node:path";

import { PythonParser } from "./python.ts";
import { SentinelParser } from "./sentinel-parser.ts";
import { TsParser } from "./typescript.ts";

import { mergeMaps } from "./utils.ts";

export class PathParser {
  private readonly parsers: Record<string, SentinelParser>;

  constructor() {
    this.parsers = {
      ".py": new PythonParser(),
      ".ts": new TsParser(),
    };
  }

  private getParser(path: string): SentinelParser | null {
    return this.parsers[extname(path)] || null;
  }

  async getFile(path: string): Promise<Map<string, string>> {
    const parser = this.getParser(path);
    if (!parser) {
      throw new Error(`No parser for ${path}`);
    }
    return parser.parseFile(path);
  }

  async getGlob(globstr: string): Promise<Map<string, string>> {
    const files = await Array.fromAsync(glob(globstr));
    const docstringMap = new Map<string, string>();
    return mergeMaps(await Promise.all(files.map((x) => this.getFile(x))));
  }

  async getDir(path: string): Promise<Map<string, string>> {
    const exts = Object.keys(this.parsers).join(",");
    return this.getGlob(`${path}/**/*{${exts}}`);
  }

  async getPath(path: string): Promise<Map<string, string>> {
    const s = await stat(path);
    if (s.isDirectory()) {
      return this.getDir(path);
    } else {
      return this.getFile(path);
    }
  }
}
