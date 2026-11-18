import { createMatrixSuite } from "matrix-test";

const matrices = {
  topic: ["test-topic", "//':\"~`\\", "🇺🇸"],
} as const;

export const matrixSuite = createMatrixSuite(matrices, (combination) => {
  return `Combination: ${JSON.stringify(combination)}`;
});

/**
 * Barrier lock.  Compare to python's asyncio.Barrier.
 */
export class Barrier {
  private count: number;
  private resolve: () => void;
  private promise: Promise<void>;

  constructor(count: number) {
    this.count = count;
    ({ resolve: this.resolve, promise: this.promise } =
      Promise.withResolvers<void>());
  }

  wait(): Promise<void> {
    if (this.count === 0) {
      return Promise.reject(new Error("Barrier awaited >n times"));
    }
    this.count--;
    if (this.count === 0) {
      this.resolve();
    }
    return this.promise;
  }
}
