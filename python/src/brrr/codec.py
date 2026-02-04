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
    ) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def decode_return(self, task_name: str, payload: bytes) -> Any:
        raise NotImplementedError()
