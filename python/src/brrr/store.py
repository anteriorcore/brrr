from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Literal, Self

import bencodepy

from .call import Call
from .tagged_tuple import PendingReturn

logger = logging.getLogger(__name__)


@dataclass
class Info:
    """
    Optional information about a task.
    Does not affect the computation, but may instruct orchestration
    """

    description: str | None
    timeout_seconds: int | None
    retries: int | None
    retry_delay_seconds: int | None
    log_prints: bool | None


_bc = bencodepy.Bencode(encoding="utf-8")


@dataclass
class PendingReturns:
    """Set of parents waiting for a child call to complete.

    When the child call is scheduled, a timestamp is added to this record to
    indicate it doesn't need to be rescheduled.  If the record exists but with a
    null scheduled timestamp, you cannot be sure this child has ever actually
    been scheduled, so it should be rescheduled.

    This record is used in highly race sensitive context and is the point of a
    lot of CASing.

    <docsync>PendingReturns</docsync>

    """

    # Unix time, in seconds.  Purposefully coarse to drive home that this value
    # is not meant for synchronization, only for measuring age.  Don’t use this
    # to determine which pending return record was written later than another or
    # any such event serialization where order matters.  This is for expiring
    # entries in a stale cache, that’s all.
    scheduled_at: int | None
    returns: set[PendingReturn]

    def encode(self) -> bytes:
        x: bytes = _bc.encode(
            {
                "returns": list(sorted(map(lambda x: x.astuple(), self.returns))),
                **({"scheduled_at": self.scheduled_at} if self.scheduled_at else {}),
            }
        )
        return x

    @classmethod
    def decode(cls, enc: bytes) -> Self:
        decoded = _bc.decode(enc)
        returns = decoded["returns"]
        return cls(
            decoded.get("scheduled_at"),
            set(map(PendingReturn.fromtuple, returns)),
        )


@dataclass
class MemKey:
    type: Literal["pending_returns", "call", "value"]
    # Hashes only contain printable us-ascii characters
    call_hash: str


class CompareMismatch(Exception): ...


class NotFoundError(Exception):
    def __init__(self, key: MemKey):
        super().__init__(f"Not found: {key!r}")


class Store(ABC):
    """A key-value store with a dict-like interface.

    This expresses the requirements for a store to be suitable as a Memory
    backend.

    """

    @abstractmethod
    async def has(self, key: MemKey) -> bool:
        """Inherently racy operation, be careful when using this"""
        raise NotImplementedError()

    @abstractmethod
    async def get(self, key: MemKey) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    async def get_with_retry(self, key: MemKey) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    async def set(self, key: MemKey, value: bytes) -> None:
        """Set a value, overriding any existing value if present.

        You don't have to provide read-after-write consistency, nor even
        write-after-write consistency if you don't want it.  The guarantees
        offered by your storage layer bubble up to the application layer, it's
        up to you where you want to spend your effort dealing with the inherent
        complexity in distributed storage.

        """
        raise NotImplementedError()

    @abstractmethod
    async def delete(self, key: MemKey) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def set_new_value(self, key: MemKey, value: bytes) -> None:
        """Set a fresh value, throwing if any value already exists.

        This must provide a hard detection of whether this was the first write.
        This CAS operation allows validating / invalidating related external
        work depending on if this write ended up being "final".  If the only
        importance of this operation is the final value itself, use
        `set' instead.

        """
        raise NotImplementedError()

    @abstractmethod
    async def compare_and_set(self, key: MemKey, value: bytes, expected: bytes) -> None:
        """
        Only set the value, as a transaction, if the existing value matches the expected value
        Or, if expected value is None, if the key does not exist
        """
        raise NotImplementedError()

    @abstractmethod
    async def compare_and_delete(self, key: MemKey, expected: bytes) -> None:
        """Delete the value, iff the current value equals the given expected value.

        The expected value CANNOT be None.  If the expected value is None,
        meaning there currently is no value, then don't call this function.

        """
        raise NotImplementedError()


class Cache(ABC):
    """A best-effort store for light-weight, non-critical data.

    Values in this cache are allowed, even encouraged to expire within a few
    minutes time.  They don't need to be consistent across nodes, there is no
    requirement for read-after-write nor even write-after-write consistency.
    It's all best effort and the worst case consequence of returning invalid
    data to brrr is just that more duplicated work might happen.  No correctness
    guarantees would be violated by brrr if this cache returns incorrect /
    incomplete / out-of-date data.

    Basically a formalization of the subset of Redis which we use.

    This is technically a "store" and it could be implemented by the exact same
    class which implements the Store interface.  It has only been separated out
    because it could be nice to implement this separately.  Concretely, it makes
    sense to use Dynamo for the store, and Redis for the cache, but do what you
    want.

    Note the required guarantees on this interface are very lax, both in
    persistence and immediately, i.e. it's ok to return speculative responses.

    It's undefined what happens if the keys for these elements are shared
    between cache, memory and/or queue.  It's probably worth being explicit
    about it at some point.

    """

    @abstractmethod
    async def incr(self, key: str) -> int:
        """Increase by 1 and return the new value.

        In reality this is used for spawn limit tracking but 🤫 that's an
        implementation detail.

        """
        raise NotImplementedError()


class Memory:
    def __init__(self, store: Store):
        self.store = store

    async def get_call(self, call_hash: str) -> Call:
        enc = await self.store.get_with_retry(MemKey("call", call_hash))
        decoded = bencodepy.decode(enc)
        task_name = decoded[b"task_name"]
        payload = decoded[b"payload"]
        return Call(
            task_name=task_name.decode("utf-8"), payload=payload, call_hash=call_hash
        )

    async def set_call(self, call: Call) -> None:
        """Store this call in the storage layer.

        If you override an existing call (i.e. same hash), ensure that the
        payload round trips through the codec unchanged.  It doesn't need to be
        the exact same byte representation, but it must _decode_ to the same
        call.

        """
        enc = bencodepy.encode(
            {
                b"task_name": call.task_name.encode("utf-8"),
                b"payload": call.payload,
            }
        )
        await self.store.set(MemKey(type="call", call_hash=call.call_hash), enc)

    async def has_value(self, call_hash: str) -> bool:
        """Inherently racy check for existence of a value.

        If this returns true you can be sure there is a value.  If it returns
        false you don't know anything because a value might be created by the
        time the function returned.

        """
        return await self.store.has(MemKey("value", call_hash))

    async def get_value(self, call_hash: str) -> bytes:
        return await self.store.get(MemKey("value", call_hash))

    async def set_value(self, call_hash: str, payload: bytes) -> None:
        """Set a [return] value for this call.

        It is the responsibility of the storage layer to ensure
        write-after-write consistency here by throwing an error if you are
        trying to overwrite an incompatible value to a pre-existing call.  The
        store is allowed to not offer that protection, at which point brrr
        itself will also not offer that protection, and suddenly it becomes the
        responsibility of the application layer to never try and do that in the
        first place, or to accept the fact that if you return a different value
        for the same call, you might also observe different results for the same
        call in different parts of the system.  This could be a sensible
        approach if you trust your encoder to generate different representations
        for the same input, which all do still decode back to the same original
        input (e.g. python's pickle).

        It's your choice where you want to solve this: application or storage
        layer?

        """
        await self.store.set(MemKey("value", call_hash), payload)

    async def _with_cas[T](self, f: Callable[[], Awaitable[T]]) -> T:
        """Wrap a CAS exception generating body.

        This abstracts the retry nature of a CAS gated operation.  The with
        block will be retried as long as it keeps throwing CompareMismatch
        exceptions.  Once it completes without throwing that, this with block
        will exit.  The retries are capped at a hard-coded 100, after which a
        generic error is returned (don't reach that, I guess).

        """
        i = 0
        while True:
            try:
                return await f()
            except CompareMismatch as e:
                i += 1
                # Do this within the catch so we can attach the last
                # CompareMismatch exception to the new exception.
                if i > 100:
                    # Very ad-hoc.  This should never be encountered, but let’s
                    # at least set _some_ kind of error message here so someone
                    # could debug this, if it ever happens.  It almost certainly
                    # indicates an issue in the underlying store’s
                    # compare_and_set implementation.
                    raise Exception("exceeded CAS retry limit") from e
                continue

    async def add_pending_return(
        self, call_hash: str, new_return: PendingReturn
    ) -> bool:
        """Register a pending return address for a call.

        Note this is inherently racy: as soon as this call completes, another
        worker could swoop in and immediately read the pending returns for this
        call and clear them.  You can't trust that the new return is ever
        visible to the thread that writes it--you can only trust that it is
        visible to _some_ worker.

        Return value indicates whether we need to schedule this call.

        """

        def is_repeated_call(existing: PendingReturn) -> bool:
            return (
                existing.root_id != new_return.root_id
                and existing.call_hash == new_return.call_hash
                and existing.topic == new_return.topic
            )

        # Beware race conditions here!  Be aware of concurrency corner cases on
        # every single line.
        async def cas_body() -> bool:
            memkey = MemKey("pending_returns", call_hash)

            logger.debug(f"Looking for existing pending returns for {call_hash}...")
            try:
                existing_enc = await self.store.get(memkey)
                logger.debug(f"    ... found! {existing_enc!r}")
                existing = PendingReturns.decode(existing_enc)
            except NotFoundError:
                existing = PendingReturns(int(time.time()), {new_return})
                existing_enc = existing.encode()
                logger.debug(f"    ... none found. Creating new: {existing_enc!r}")
                await self.store.set_new_value(memkey, existing_enc)
                return True

            should_schedule = any(map(is_repeated_call, existing.returns))
            existing.returns.add(new_return)
            await self.store.compare_and_set(memkey, existing.encode(), existing_enc)
            return should_schedule

        return await self._with_cas(cas_body)

    async def with_pending_returns_remove(
        self, call_hash: str, f: Callable[[Iterable[PendingReturn]], Awaitable[None]]
    ) -> None:
        memkey = MemKey("pending_returns", call_hash)
        handled: set[PendingReturn] = set()

        async def cas_body() -> None:
            nonlocal handled
            try:
                pending_enc = await self.store.get(memkey)
            except NotFoundError:
                # No pending returns means we were raced by a concurrent
                # execution of the same call with the same parent.
                # Unfortunately because of how Python context managers work, we
                # must yield _something_.  Yuck.
                #
                # https://stackoverflow.com/a/34519857
                return await f([])
            to_handle = PendingReturns.decode(pending_enc).returns - handled
            logger.debug(f"Handling returns for {call_hash}: {to_handle}...")
            await f(to_handle)
            handled |= to_handle
            await self.store.compare_and_delete(memkey, pending_enc)

        return await self._with_cas(cas_body)
