import Parser, { type SyntaxNode } from "tree-sitter";
import TS from "tree-sitter-typescript";
import { glob, readFile } from "node:fs/promises";
import { parseSentinel } from "./utils.ts";

const parser = new Parser();
parser.setLanguage(TS.typescript as any);

function extractDocString(node: SyntaxNode): string | undefined {
  const prevSibling = node.previousSibling;
  if (!prevSibling || prevSibling.type !== "comment") {
    return;
  }
  return prevSibling.text;
}

function fetchDocStrings(root: SyntaxNode): string[] {
  const docstrings: string[] = [];

  function go(node: SyntaxNode): void {
    const comment = extractDocString(node);
    if (comment) {
      docstrings.push(comment);
    }
    for (const child of node.namedChildren) {
      go(child);
    }
  }

  go(root);
  return docstrings;
}

export async function tsGetDir(path: string): Promise<Map<string, string>> {
  const files = await Array.fromAsync(glob(path + "/**/*.ts"));
  const docstringMap = new Map<string, string>();
  for (const file of files) {
    const content = await readFile(file, "utf-8");
    const tree = parser.parse(content);
    const docstrings = fetchDocStrings(tree.rootNode);
    for (const docstring of docstrings) {
      const sentinel = parseSentinel(docstring);
      if (!sentinel) {
        continue;
      }
      const cleaned = docstring
        .replace(/<docsync>.*?<\/docsync>/g, "") // remove <docsync> tags
        .replace(/^\/\*\*?/, "") // remove leading "/**" or "/*"
        .replace(/\*\/$/, "") // remove trailing "*/"
        .replace(/^\s*\*\s?/gm, "") // remove leading "*"
        .replace(/\/\/\s?/g, "") // remove line comment prefix
        .replace(/\s+/g, " ")
        .trim();
      docstringMap.set(sentinel, cleaned);
    }
  }
  return docstringMap;
}
