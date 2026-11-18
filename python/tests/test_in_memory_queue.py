import asyncio
import time

import pytest
from brrr.backends.in_memory import (
    CloseOnEmptyQueue,
    CloseOnSilenceQueue,
)
from brrr.queue import Message, Queue, QueueIsClosed


async def test_close_on_silence() -> None:
    q = CloseOnSilenceQueue(["t1", "t2"])
    await q.put_message("t1", "foo")
    await q.put_message("t2", "bar")
    checkpoint = time.monotonic()
    assert (await q.get_message("t1")).body == "foo"
    with pytest.raises(QueueIsClosed):
        await q.get_message("t1")
    # This blocked for around a second:
    assert 0.9 < time.monotonic() - checkpoint < 1.1
    checkpoint = time.monotonic()
    with pytest.raises(asyncio.QueueShutDown):
        await q.put_message("t1", "frob")
    with pytest.raises(asyncio.QueueShutDown):
        await q.put_message("t2", "brap")
    assert (await q.get_message("t2")).body == "bar"
    with pytest.raises(QueueIsClosed):
        await q.get_message("t2")
    # None of that blocked
    assert time.monotonic() - checkpoint < 0.1


async def _waiting(q: Queue, topic: str) -> asyncio.Task[Message]:
    task: asyncio.Task[Message] = asyncio.create_task(q.get_message(topic))
    await asyncio.sleep(0)
    assert not task.done()
    return task


async def test_close_on_empty_workers_waits_for_all_workers() -> None:
    q = CloseOnEmptyQueue(["t1"], workers=3)
    await q.put_message("t1", "foo")
    assert (await q.get_message("t1")).body == "foo"
    w2 = await _waiting(q, "t1")
    w3 = await _waiting(q, "t1")
    with pytest.raises(QueueIsClosed):
        await q.get_message("t1")
    for w in (w2, w3):
        with pytest.raises(QueueIsClosed):
            await w
    with pytest.raises(QueueIsClosed):
        await q.get_message("t1")
    with pytest.raises(asyncio.QueueShutDown):
        await q.put_message("t1", "frob")


async def test_close_on_empty_workers_topics() -> None:
    q = CloseOnEmptyQueue(["t1", "t2"], workers=2)
    await q.put_message("t1", "foo")
    await q.put_message("t2", "bar")
    assert (await q.get_message("t1")).body == "foo"
    # Stays open: t2 still has work for worker 2
    w1 = await _waiting(q, "t1")
    assert (await q.get_message("t2")).body == "bar"
    await q.put_message("t1", "frob")
    assert (await w1) == Message(body="frob")
    w2 = await _waiting(q, "t2")
    with pytest.raises(QueueIsClosed):
        await q.get_message("t1")
    with pytest.raises(QueueIsClosed):
        await w2
