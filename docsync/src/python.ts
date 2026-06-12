import Parser, { type SyntaxNode } from "tree-sitter";
import Python from "tree-sitter-python";

import { type SentinelDocstring, SentinelParser } from "./sentinel-parser.ts";

export class PythonParser extends SentinelParser {
  protected override readonly extension = "py";

  constructor() {
    const parser = new Parser();
    parser.setLanguage(Python as any);
    super(parser);
  }

  protected override extractDocString(node: SyntaxNode): string | null {
    if (
      !["module", "class_definition", "function_definition"].includes(node.type)
    ) {
      return null;
    }
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
    return null;
  }

  protected override cleanDocstring(docstring: string): string {
    return docstring
      .replace(/<docsync>.*?<\/docsync>/g, "") // remove <docsync> tags
      .replace(/^[ \t]*"""/, "") // remove leading triple quotes
      .replace(/"""[ \t]*$/, "") // remove trailing triple quotes
      .replace(/\s*\n\s*/g, " ") // replace newlines with spaces
      .replace(/\s+/g, " ") // normalize whitespace
      .trim();
  }
}
