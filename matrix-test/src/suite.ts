import { suite, type SuiteContext } from "node:test";
import { type Combination, combinations } from "./matrix.ts";

export function matrixSuite<const M extends Record<string, any>>(
  name: string,
  matrix: M,
  fn: (_: SuiteContext, combination: Combination<M>) => void | Promise<void>,
  childSuiteNameFn?: (combination: Combination<M>) => string,
): void {
  suite(name, async (_: SuiteContext): Promise<void> => {
    for (const combination of combinations(matrix)) {
      suite(
        childSuiteNameFn?.(combination) || name,
        async (context: SuiteContext): Promise<void> => {
          await fn(context, combination);
        },
      );
    }
  });
}

export function createMatrixSuite<const M extends Record<string, any>>(
  matrix: M,
  childSuiteNameFn?: (combination: Combination<M>) => string,
): (
  name: string,
  fn: (_: SuiteContext, combination: Combination<M>) => void | Promise<void>,
) => void {
  return (
    name: string,
    fn: (_: SuiteContext, combination: Combination<M>) => void | Promise<void>,
  ): void => {
    matrixSuite(name, matrix, fn, childSuiteNameFn);
  };
}
