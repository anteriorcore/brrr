import hashlib
import pickle
from typing import Any

from brrr.app import ActiveWorker, Task

from .call import Call
from .codec import Codec


class DemoPickleCodec(Codec[ActiveWorker]):
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

    def encode_call(
        self, task_name: str, args: tuple[Any, ...], kwargs: dict[Any, Any]
    ) -> Call:
        payload = pickle.dumps((args, kwargs))
        call_hash = self._hash_call(task_name, args, kwargs)
        return Call(task_name=task_name, payload=payload, call_hash=call_hash)

    async def invoke_task(
        self,
        call: Call,
        task: Task[ActiveWorker, ..., Any],
        active_worker: ActiveWorker,
    ) -> bytes:
        args, kwargs = pickle.loads(call.payload)
        return pickle.dumps(await task(active_worker, *args, **kwargs))

    def decode_return(self, task_name: str, payload: bytes) -> Any:
        return pickle.loads(payload)
