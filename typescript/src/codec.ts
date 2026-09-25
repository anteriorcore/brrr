import type { ActiveWorker, Task } from "./app.ts";
import type { Call } from "./call.ts";
import type { DemoJsonCodecContext } from "./demo-json-codec.ts";

export interface Codec<C> {
  encodeCall<A extends unknown[]>(taskName: string, args: A): Promise<Call>;

  invokeTask<A extends unknown[], R>({
    call,
    task,
    activeWorker,
    signal,
    metadata,
  }: {
    call: Call;
    task: Task<C, A, R>;
    activeWorker: ActiveWorker<C>;
    signal: Uint8Array;
    metadata: Uint8Array;
  }): Promise<Uint8Array>;

  decodeReturn(taskName: string, payload: Uint8Array): unknown;
}
