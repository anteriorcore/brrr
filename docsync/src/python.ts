import Parser, { type SyntaxNode } from "tree-sitter";
import Python from "tree-sitter-python";
import { glob, readFile } from "node:fs/promises";
import { mergeMaps, parseSentinel, treeFold } from "./utils.ts";

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

function fetchDocStrings(root: SyntaxNode): string[] {
  return treeFold(root, (acc: string[], node: SyntaxNode) => {
    if (
      node.type === "module" ||
      node.type === "class_definition" ||
      node.type === "function_definition"
    ) {
      const doc = extractDocString(node);
      if (doc) {
        acc.push(doc);
      }
    }
    return acc;
  }, []);
}

function pyParse(docstring: string): null | [string, string] {
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
  return new Map(
    docstrings.map(pyParse).filter((x) => x) as [string, string][],
  );
}

export async function pythonGetDir(root: string): Promise<Map<string, string>> {
  const files = await Array.fromAsync(glob(root + "/**/*.py"));
  return mergeMaps(await Promise.all(files.map(pythonGetFile)));
}
