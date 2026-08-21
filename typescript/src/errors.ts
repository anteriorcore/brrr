import type { MemKey } from "./store.ts";
import type { Tagged } from "./tagged-tuple.ts";

abstract class BrrrError extends Error {
  protected constructor(message: string) {
    super();
    this.name = this.constructor.name;
    this.message = message;
  }
}

export class NotFoundError extends BrrrError {
  public constructor(key: MemKey) {
    super(`Not found: ${key.type}/${key.callHash}`);
  }
}

export class CasRetryLimitReachedError extends BrrrError {
  public constructor(retryLimit: number) {
    super(`CAS retry limit reached (${retryLimit})`);
  }
}

export class SpawnLimitError extends BrrrError {
  public constructor(limit: number, rootId: string, callHash: string) {
    super(
      `Spawn limit of ${limit} reached for rootId ${rootId} and callHash ${callHash}`,
    );
  }
}

export class TaskNotFoundError extends BrrrError {
  public constructor(taskName: string) {
    super(`Task not found: ${taskName}`);
  }
}

export class TagMismatchError extends BrrrError {
  public constructor(clz: Tagged) {
    super(`Tag mismatch for ${clz.name}: expected ${clz.tag}`);
  }
}

export class MalformedTaggedTupleError extends BrrrError {
  public constructor(clz: Tagged) {
    super(
      `Malformed tagged tuple for ${clz.name}, expected ${clz.length} elements`,
    );
  }
}
