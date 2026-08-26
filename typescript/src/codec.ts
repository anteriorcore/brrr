import type { ActiveWorker, Task } from "./app.ts";
import type { Call } from "./call.ts";

export interface Codec<Env> {
  encodeCall<A extends unknown[]>(taskName: string, args: A): Promise<Call>;

  invokeTask<A extends unknown[], R>(
    call: Call,
    task: Task<Env, A, R>,
    activeWorker: ActiveWorker<Env>,
  ): Promise<Uint8Array>;

  decodeReturn(taskName: string, payload: Uint8Array): unknown;
}
