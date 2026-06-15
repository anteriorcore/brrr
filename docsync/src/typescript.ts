import Parser, { type SyntaxNode } from "tree-sitter";
import TS from "tree-sitter-typescript";
import { glob, readFile } from "node:fs/promises";
import { type SentinelDocstring, SentinelParser } from "./sentinel-parser.ts";

export class TsParser extends SentinelParser {
  constructor() {
    const parser = new Parser();
    parser.setLanguage(TS.typescript as any);
    super(parser);
  }

  protected override extractDocString(node: SyntaxNode): string | null {
    const prev = node.previousSibling;
    return prev && prev.type === "comment" ? prev.text : null;
  }

  protected override cleanDocstring(docstring: string): string {
    return docstring
      .replace(/<docsync>.*?<\/docsync>/g, "") // remove <docsync> tags
      .replace(/^\/\*\*?/, "") // remove leading "/**" or "/*"
      .replace(/\*\/$/, "") // remove trailing "*/"
      .replace(/^\s*\*\s?/gm, "") // remove leading "*"
      .replace(/\/\/\s?/g, "") // remove line comment prefix
      .replace(/\s+/g, " ")
      .trim();
  }
}
