from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable

from brrr.call import Call

if TYPE_CHECKING:
    from brrr.app import ActiveWorker, Task


class Codec[C](ABC):
    """Codec for values that pass around the brrr datastore.

    If you want inter-language calling you'll need to ensure both languages
    can compute this.

    The serializations must be deterministic, whatever that means for you.
    E.g. if you use dictionaries, make sure to order them before serializing.

    For any serious use you want strict control over the types you accept here
    and explicit serialization routines.

    """

    @abstractmethod
    def encode_call(
        self, task_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Call:
        raise NotImplementedError()

    @abstractmethod
    async def invoke_task(
        self,
        call: Call,
        task: Task[C, ..., Awaitable[Any]],
        active_worker: ActiveWorker[C],
        signal: bytes,
    ) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def decode_return(self, task_name: str, payload: bytes) -> Any:
        raise NotImplementedError()

    # One could argue this should also take a topic.  I think YAGNI, but it
    # would be fair.  Maybe one day.  Let’s wait and see.
    @abstractmethod
    def visibility_timeout(self, call: Call) -> int:
        """Amount of seconds before which a worker who took on this job is
        expected to return a result.

        This takes effect retroactively.  If a worker has indicated, less than
        ‘visibility_timeout’ seconds ago, that it is working on this call, then
        the call is considered in progress.  It won’t be retried, if rescheduled.
        If however the last invocation was more than this many seconds ago
        (assuming it did not return yet), that job is considered unresponsive, and
        this call will be scheduled.

        This does not have any effect proactively: if a worker exceeds this
        timeout, nothing really happens with that, until a call is retried.  In
        that sense this value is different from the eponymous SQS attribute.

        This is call-local and can change on a task-per-task and call-per-call
        basis.  The intended use is for all calls with the same call hash to have
        the same visibility timeout; this would make most intuitive sense: a call
        is a call, and it takes X minutes, the end.  However, technically
        speaking, a “force refresh” operation could choose to set this to 0.  That
        would be valid and it would force a re-execution of this call immediately,
        no matter what.  Of course at the risk of concurrent execution (which is
        always a risk).

        /Technically/ technically speaking this has nothing to do with "codec"
        in the original sense of the word ("Encoder / decoder") because it's not
        about data.  That is a problem with the name of this class, not with the
        existence of this method.

        <docsync>visibility_timeout</docsync>
        """
        raise NotImplementedError()
