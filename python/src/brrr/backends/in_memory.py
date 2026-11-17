from __future__ import annotations

import asyncio
import typing
from collections.abc import Mapping, Sequence
from typing import override

from brrr.store import CompareMismatch, NotFoundError

from ..queue import Message, Queue, QueueInfo, QueueIsClosed
from ..store import Cache, MemKey, Store


class CloseOnEmptyQueue(Queue):
    """In-memory queue (for testing) which closes when it's empty.

    Closure happens when there is a `get` operation, the queue is empty, and
    every other worker is also waiting in a `get` (see `workers` below).
    Doing a get on a queue with only one item will return that item, leave the
    queue empty, but not close it yet.  If the next operation is a put, it is
    accepted, and nothing is closed.

    The queue is topic aware: doing a get on topic A, when topic A is empty, but
    there is a message on topic B, will block the get until a message is
    available on topic A (or: until the queue is closed, either directly or by
    another coroutine doing gets until the queue is entirely empty).

    Closure of one topic affects all topics.  Existing messages on other topics
    remain available for reading.

    Closure is not triggered before the first message has been put.

    With multiple concurrent consumers, pass their exact number as `workers`:
    the queue then only closes once all of them wait in a `get` with nothing
    left, i.e. nothing is running that could still put new work.  This relies
    on a worker only doing its next get once it is done handling its previous
    message, which is how brrr workers behave.  Too low a count closes early,
    too high never closes.

    Good fit for deterministic tests, single threaded or with N parallel
    `conn.loop`s.

    >>> q = CloseOnEmptyQueue(["t1", "t2"])
    >>> def get(t):
    ...     async def inner():
    ...         async with asyncio.timeout(1):
    ...             return await q.get_message(t)
    ...     return asyncio.run(inner())
    ...
    >>> asyncio.run(q.put_message("t1", "foo"))
    >>> asyncio.run(q.put_message("t2", "bar"))
    >>> get("t1")
    Message(body='foo')
    >>> get("t1")
    Traceback (most recent call last):
        ...
    TimeoutError
    >>> asyncio.run(q.put_message("t1", "frob"))
    >>> get("t2")
    Message(body='bar')
    >>> get("t1")
    Message(body='frob')
    >>> get("t2")
    Traceback (most recent call last):
        ...
    brrr.queue.QueueIsClosed

    With two workers, one busy worker keeps the queue open:

    >>> async def two_workers():
    ...     q = CloseOnEmptyQueue(["t1"], workers=2)
    ...     await q.put_message("t1", "foo")
    ...     print(await q.get_message("t1"))
    ...     # Stays open: worker 1 is still busy and may put more work
    ...     w2 = asyncio.create_task(q.get_message("t1"))
    ...     await asyncio.sleep(0)
    ...     await q.put_message("t1", "bar")
    ...     print(await w2)
    ...     w1 = asyncio.create_task(q.get_message("t1"))
    ...     await asyncio.sleep(0)
    ...     print(w1.done())
    ...     # Closes: nobody is busy, so no new work can appear
    ...     try:
    ...         await q.get_message("t1")
    ...     except QueueIsClosed:
    ...         print("worker 2: closed")
    ...     try:
    ...         await w1
    ...     except QueueIsClosed:
    ...         print("worker 1: closed")
    ...
    >>> asyncio.run(two_workers())
    Message(body='foo')
    Message(body='bar')
    False
    worker 2: closed
    worker 1: closed

    """

    _queues: Mapping[str, asyncio.Queue[str]]

    def __init__(self, topics: Sequence[str], workers: int = 1):
        if workers < 1:
            raise ValueError(f"workers must be at least 1, got {workers}")
        # Could be updated to allow dynamically creating topics on-demand but
        # this is probably a bit nicer for now.
        self._queues = {k: asyncio.Queue() for k in topics}
        self._workers = workers
        self._waiting = 0
        self._had_message = False

    @property
    def workers(self) -> int:
        """Number of concurrent consumers this queue waits for before closing."""
        return self._workers

    @typing.override
    async def get_message(self, topic: str) -> Message:
        q = self._queues[topic]
        self._waiting += 1
        try:
            if self._waiting >= self._workers and self._had_message and self._empty():
                self.close()
            payload = await q.get()
        except asyncio.QueueShutDown:
            raise QueueIsClosed()
        finally:
            self._waiting -= 1

        q.task_done()
        return Message(body=payload)

    @typing.override
    async def put_message(self, topic: str, body: str) -> None:
        self._had_message = True
        await self._queues[topic].put(body)

    def close(self) -> None:
        for q in self._queues.values():
            q.shutdown()

    def _empty(self) -> bool:
        return all(map(lambda q: q.empty(), self._queues.values()))

    async def get_info(self, topic: str) -> QueueInfo:
        return QueueInfo(num_messages=self._queues[topic].qsize())


class CloseOnSilenceQueue(Queue):
    """In-memory queue (for testing) which closes when it's unused for 1 second.

    Activity on any topic is considered activity for all topics.  Closure
    happens when no message is put on the channel for at least 1 second.  Any
    pending gets are resolved with a QueueIsClosed exception.  Same for any
    subsequent gets, to any topic.  Pending messages are still allowed to be
    retrieved.  No new messages can be put.

    Good fit for unit tests involving parallel, synthetic workers which will
    definitely take <1 second.  Poor fit for tests involving long running tasks.
    For tests involving only a single consumer, prefer the (deterministic)
    CloseOnEmptyQueue.

    """

    _queues: Mapping[str, asyncio.Queue[str]]
    _watchdog: asyncio.Handle | None

    def __init__(self, topics: Sequence[str]):
        self._queues = {k: asyncio.Queue() for k in topics}
        self._watchdog = None

    def _kick_watchdog(self) -> None:
        if self._watchdog is not None:
            self._watchdog.cancel()
        self._start_watchdog()

    def _start_watchdog(self) -> None:
        self._watchdog = asyncio.get_running_loop().call_later(1, self._shutdown)

    @typing.override
    async def get_message(self, topic: str) -> Message:
        if self._watchdog is None:
            self._start_watchdog()

        q = self._queues[topic]
        try:
            payload = await q.get()
        except asyncio.QueueShutDown:
            raise QueueIsClosed()

        q.task_done()
        return Message(body=payload)

    @typing.override
    async def put_message(self, topic: str, body: str) -> None:
        self._kick_watchdog()
        await self._queues[topic].put(body)

    def _shutdown(self) -> None:
        for q in self._queues.values():
            q.shutdown()

    async def get_info(self, topic: str) -> QueueInfo:
        return QueueInfo(num_messages=self._queues[topic].qsize())


def _key2str(key: MemKey) -> str:
    return f"{key.type}/{key.call_hash}"


# Just to drive the point home
class InMemoryByteStore(Store, Cache):
    """
    A store that stores bytes
    """

    inner: dict[str, bytes]
    cache: dict[str, int]

    def __init__(self) -> None:
        self.inner = {}
        self.cache = {}

    @override
    async def has(self, key: MemKey) -> bool:
        return _key2str(key) in self.inner

    @override
    async def get(self, key: MemKey) -> bytes:
        full_hash = _key2str(key)
        if full_hash not in self.inner:
            raise NotFoundError(key)
        return self.inner[full_hash]

    @override
    async def get_with_retry(self, key: MemKey) -> bytes:
        return await self.get(key=key)

    @override
    async def set(self, key: MemKey, value: bytes) -> None:
        self.inner.setdefault(_key2str(key), value)

    @override
    async def delete(self, key: MemKey) -> None:
        try:
            del self.inner[_key2str(key)]
        except KeyError:
            pass

    @override
    async def set_new_value(self, key: MemKey, value: bytes) -> None:
        k = _key2str(key)
        if k in self.inner:
            raise CompareMismatch()
        self.inner[k] = value

    @override
    async def compare_and_set(self, key: MemKey, value: bytes, expected: bytes) -> None:
        k = _key2str(key)
        if (k not in self.inner) or (self.inner[k] != expected):
            raise CompareMismatch()
        self.inner[k] = value

    @override
    async def compare_and_delete(self, key: MemKey, expected: bytes) -> None:
        k = _key2str(key)
        if (k not in self.inner) or (self.inner[k] != expected):
            raise CompareMismatch()
        del self.inner[k]

    @override
    async def incr(self, key: str) -> int:
        n: int = self.cache.get(key, 0) + 1
        self.cache[key] = n
        return n
