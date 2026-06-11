import Parser, { type SyntaxNode } from "tree-sitter";
import TS from "tree-sitter-typescript";
import { glob, readFile } from "node:fs/promises";
import { mergeMaps, parseSentinel } from "./utils.ts";

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

function tsParse(docstring: string): null | [string, string] {
  const sentinel = parseSentinel(docstring);
  if (!sentinel) {
    return null;
  }
  const cleaned = docstring
    .replace(/<docsync>.*?<\/docsync>/g, "") // remove <docsync> tags
    .replace(/^\/\*\*?/, "") // remove leading "/**" or "/*"
    .replace(/\*\/$/, "") // remove trailing "*/"
    .replace(/^\s*\*\s?/gm, "") // remove leading "*"
    .replace(/\/\/\s?/g, "") // remove line comment prefix
    .replace(/\s+/g, " ")
    .trim();
  return [sentinel, cleaned];
}

export async function tsGetFile(file: string): Promise<Map<string, string>> {
  const content = await readFile(file, "utf-8");
  const tree = parser.parse(content);
  const docstrings = fetchDocStrings(tree.rootNode);
  return new Map(
    docstrings.map(tsParse).filter((x) => x) as [string, string][],
  );
}

export async function tsGetDir(path: string): Promise<Map<string, string>> {
  const files = await Array.fromAsync(glob(path + "/**/*.ts"));
  const docstringMap = new Map<string, string>();
  return mergeMaps(await Promise.all(files.map(tsGetFile)));
}
