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
  return callback(
    node.namedChildren.reduce(
      (acc, child) => treeFold(child, callback, acc),
      init,
    ),
    node,
  );
}
