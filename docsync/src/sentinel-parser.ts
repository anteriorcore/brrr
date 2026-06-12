import { readFile } from "node:fs/promises";

import Parser, { type SyntaxNode } from "tree-sitter";

import { parseSentinel, treeFold } from "./utils.ts";

export interface SentinelDocstring {
  sentinel: string;
  docstring: string;
}

export abstract class SentinelParser {
  protected abstract cleanDocstring(docstring: string): string;
  protected abstract extractDocString(node: SyntaxNode): null | string;
  protected abstract readonly extension: string;
  private readonly parser: Parser;

  constructor(parser: Parser) {
    this.parser = parser;
  }

  protected fetchDocStrings(root: SyntaxNode): string[] {
    return treeFold(
      root,
      (acc: string[], node: SyntaxNode) => {
        const comment = this.extractDocString(node);
        if (comment) {
          acc.push(comment);
        }
        return acc;
      },
      [],
    );
  }

  private parseSentinel(docstring: string): null | SentinelDocstring {
    const sentinel = parseSentinel(docstring);
    if (!sentinel) {
      return null;
    }
    return { sentinel, docstring: this.cleanDocstring(docstring) };
  }

  async parseFile(path: string): Promise<Map<string, string>> {
    const content = await readFile(path, "utf-8");
    const tree = this.parser.parse(content);
    const docstrings = this.fetchDocStrings(tree.rootNode);
    return new Map(
      docstrings
        .map((x) => this.parseSentinel(x))
        .flatMap((x) => (x === null ? [] : [[x.sentinel, x.docstring]])) as [
        string,
        string,
      ][],
    );
  }
}
