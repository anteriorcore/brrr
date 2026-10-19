import type { ActiveWorker, Task } from "./app.ts";
import type { Call } from "./call.ts";

export interface Codec<C> {
  encodeCall<A extends unknown[]>(taskName: string, args: A): Promise<Call>;

  invokeTask<A extends unknown[], R>(
    call: Call,
    task: Task<C, A, R>,
    activeWorker: ActiveWorker<C>,
    signal: Uint8Array,
  ): Promise<Uint8Array>;

  decodeReturn(taskName: string, payload: Uint8Array): unknown;

  // Sync, but could be made async, if necessary.  Wait and see.
  /**
   * Amount of seconds before which a worker who took on this job is
   * expected to return a result.
   *
   * This takes effect retroactively.  If a worker has indicated, less than
   * ‘visibility_timeout’ seconds ago, that it is working on this call, then
   * the call is considered in progress.  It won’t be retried, if rescheduled.
   * If however the last invocation was more than this many seconds ago
   * (assuming it did not return yet), that job is considered unresponsive, and
   * this call will be scheduled.
   *
   * This does not have any effect proactively: if a worker exceeds this
   * timeout, nothing really happens with that, until a call is retried.  In
   * that sense this value is different from the eponymous SQS attribute.
   *
   * This is call-local and can change on a task-per-task and call-per-call
   * basis.  The intended use is for all calls with the same call hash to have
   * the same visibility timeout; this would make most intuitive sense: a call
   * is a call, and it takes X minutes, the end.  However, technically
   * speaking, a “force refresh” operation could choose to set this to 0.  That
   * would be valid and it would force a re-execution of this call immediately,
   * no matter what.  Of course at the risk of concurrent execution (which is
   * always a risk).
   *
   * /Technically/ technically speaking this has nothing to do with "codec"
   * in the original sense of the word ("Encoder / decoder") because it's not
   * about data.  That is a problem with the name of this class, not with the
   * existence of this method.
   *
   * <docsync>visibility_timeout</docsync>
   *
   */
  visibilityTimeout(call: Call): number;
}
