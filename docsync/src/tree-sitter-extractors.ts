import Parser, { type SyntaxNode } from "tree-sitter";
import TS from "tree-sitter-typescript";
import Python from "tree-sitter-python";

/**
 * Built-in, default extractors that uses tree-sitter.
 */
export function createTreeSitterExtractors(): Record<
  string,
  (text: string) => Generator<string>
> {
  const pyParser = new Parser();
  pyParser.setLanguage(TS.typescript as any);

  const tsParser = new Parser();
  tsParser.setLanguage(Python as any);

  return {
    ".ts": function* (content: string): Generator<string> {
      function* go(node: SyntaxNode): Generator<string> {
        const { previousSibling, namedChildren } = node;
        if (previousSibling?.type === "comment") {
          yield previousSibling.text
            .replace(/^\/\*\*?/, "") // remove leading "/**" or "/*"
            .replace(/\*\/$/, "") // remove trailing "*/"
            .replace(/^\s*\*\s?/gm, "") // remove leading "*"
            .replace(/\/\/\s?/g, "") // remove line comment prefix
            .replace(/\s+/g, " ")
            .trim();
        }
        for (const child of namedChildren) {
          yield* go(child);
        }
      }

      const { rootNode } = pyParser.parse(content);
      yield* go(rootNode);
    },

    ".py": function* (content: string): Generator<string> {
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

      function* go(node: SyntaxNode): Generator<string> {
        if (
          node.type === "module" ||
          node.type === "class_definition" ||
          node.type === "function_definition"
        ) {
          const docstring = extractDocString(node);
          if (docstring) {
            yield docstring
              .replace(/^[ \t]*"""/, "") // remove leading triple quotes
              .replace(/"""[ \t]*$/, "") // remove trailing triple quotes
              .replace(/^[ \t]*'''/, "") // remove leading single triple quotes
              .replace(/'''[ \t]*$/, "") // remove trailing single triple quotes
              .replace(/\s*\n\s*/g, " ") // replace newlines with spaces
              .replace(/\s+/g, " ") // normalize whitespace
              .trim();
          }
        }
        for (const child of node.namedChildren) {
          yield* go(child);
        }
      }

      const { rootNode } = tsParser.parse(content);
      yield* go(rootNode);
    },
  };
}
