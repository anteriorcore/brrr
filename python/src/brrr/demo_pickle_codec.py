import hashlib
import pickle
from typing import Any, override

from brrr.app import ActiveWorker, Task

from .call import Call
from .codec import Codec

type DemoPickleCodecContext = ActiveWorker[DemoPickleCodecContext]


class DemoPickleCodec(Codec[DemoPickleCodecContext]):
    """
    An opinionated codec for demo/testing purposes. It expects `ActiveWorker` as
    a context.

    Don't use this in production because you run the risk of non-deterministic
    serialization, e.g. dicts with arbitrary order.

    The primary purpose of this codec is executable documentation.

    """

    def _hash_call(
        self, task_name: str, args: tuple[Any, ...], kwargs: dict[Any, Any]
    ) -> str:
        h = hashlib.new("sha256")
        h.update(repr([task_name, args, list(sorted(kwargs.items()))]).encode())
        return h.hexdigest()

    @override
    def encode_call(
        self, task_name: str, args: tuple[Any, ...], kwargs: dict[Any, Any]
    ) -> Call:
        payload = pickle.dumps((args, kwargs))
        call_hash = self._hash_call(task_name, args, kwargs)
        return Call(task_name=task_name, payload=payload, call_hash=call_hash)

    @override
    async def invoke_task(
        self,
        call: Call,
        task: Task[DemoPickleCodecContext, ..., Any],
        active_worker: ActiveWorker[DemoPickleCodecContext],
        signal: bytes,
    ) -> bytes:
        args, kwargs = pickle.loads(call.payload)
        return pickle.dumps(await task(active_worker, *args, **kwargs))

    @override
    def decode_return(self, task_name: str, payload: bytes) -> Any:
        return pickle.loads(payload)

    @override
    def visibility_timeout(self, call: Call) -> int:
        # Completely arbitrary: after 10 minutes, consider a call expired.
        return 60 * 10
