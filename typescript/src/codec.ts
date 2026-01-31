import type { ActiveWorker, Task } from "./app.ts";
import type { Call } from "./call.ts";

export interface Codec<C> {
  encodeCall<A extends unknown[]>(taskName: string, args: A): Promise<Call>;

  invokeTask<A extends unknown[], R>(
    call: Call,
    task: Task<C, A, R>,
    activeWorkerFactory: () => ActiveWorker<C>,
  ): Promise<Uint8Array>;

  decodeReturn(taskName: string, payload: Uint8Array): unknown;
}
