import asyncio
import time

import pytest
from brrr.backends.in_memory import CloseOnSilenceQueue
from brrr.queue import QueueIsClosed


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
