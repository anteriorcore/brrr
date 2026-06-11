import Parser, { type SyntaxNode } from "tree-sitter";
import Python from "tree-sitter-python";
import { glob, readFile } from "node:fs/promises";
import { parseSentinel } from "./utils.ts";

const parser = new Parser();
parser.setLanguage(Python as any);

function extractDocString(node: SyntaxNode): string | undefined {
  const body = node.namedChildren.find(
    ({ type }) => type === "block" || type === "suite",
  );
  const stmt = body?.namedChildren?.at(0);
  if (
    stmt?.type === "expression_statement" &&
    stmt.firstNamedChild?.type === "string"
  ) {
    return stmt.firstNamedChild.text;
  }
}

function fetchDocStrings(rootNode: SyntaxNode): string[] {
  const docstrings: string[] = [];

  function go(node: SyntaxNode) {
    if (
      node.type === "module" ||
      node.type === "class_definition" ||
      node.type === "function_definition"
    ) {
      const doc = extractDocString(node);
      if (doc) {
        docstrings.push(doc);
      }
    }
    node.namedChildren.map(go);
  }

  go(rootNode);
  return docstrings;
}

function sentinel2docstring(docstring: string): null | [string, string] {
  const sentinel = parseSentinel(docstring);
  if (!sentinel) {
    return null;
  }
  const cleaned = docstring
    .replace(/<docsync>.*?<\/docsync>/g, "") // remove <docsync> tags
    .replace(/^[ \t]*"""/, "") // remove leading triple quotes
    .replace(/"""[ \t]*$/, "") // remove trailing triple quotes
    .replace(/\s*\n\s*/g, " ") // replace newlines with spaces
    .replace(/\s+/g, " ") // normalize whitespace
    .trim();
  return [sentinel, cleaned];
}

export async function pythonGetFile(
  path: string,
): Promise<Map<string, string>> {
  const content = await readFile(path, "utf-8");
  const tree = parser.parse(content);
  const docstrings = fetchDocStrings(tree.rootNode);
  return new Map(docstrings.map(sentinel2docstring).filter((x) => x) as [string, string][]);
}

function mergeMaps<T, U>(maps: Map<T, U>[]): Map<T, U> {
  return new Map(maps.flatMap((x) => [...x]));
}

export async function pythonGetDir(root: string): Promise<Map<string, string>> {
  const files = await Array.fromAsync(glob(root));
  return mergeMaps(await Promise.all(files.map(pythonGetFile)));
}
