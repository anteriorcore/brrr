import { type SyntaxNode } from "tree-sitter";

const sentinelRegex = /<docsync>(.*?)<\/docsync>/;

export function mergeMaps<T, U>(maps: Map<T, U>[]): Map<T, U> {
  return new Map(maps.flatMap((x) => [...x]));
}

export function parseSentinel(docstring: string) {
  return docstring.match(sentinelRegex)?.at(1);
}

export function treeFold<T>(
  node: SyntaxNode,
  callback: (acc: T, node: SyntaxNode) => T,
  init: T,
): T {
  let acc = callback(init, node);
  for (const child of node.namedChildren) {
    acc = treeFold(child, callback, acc);
  }
  return acc;
}
