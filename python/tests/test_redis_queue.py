import os
from collections.abc import Sequence
from contextlib import asynccontextmanager
from typing import AsyncIterator

import pytest
import redis.asyncio as redis
from brrr.backends.redis import RedisQueue
from brrr.queue import Queue

from tests.contract_queue import QueueContract


@asynccontextmanager
async def with_redis(redurl: str | None) -> AsyncIterator[redis.Redis]:  # type: ignore[type-arg]
    rkwargs = dict(
        health_check_interval=10,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        socket_keepalive=True,
        protocol=3,
    )
    if redurl is None:
        rc = redis.Redis(**rkwargs)  # type: ignore[call-overload]
    else:
        rc = redis.from_url(redurl, **rkwargs)

    await rc.ping()
    try:
        yield rc
    finally:
        await rc.aclose()


@pytest.mark.dependencies
class TestRedisQueue(QueueContract):
    has_accurate_info = True

    @asynccontextmanager
    async def with_queue(self, topics: Sequence[str]) -> AsyncIterator[Queue]:
        # Hack but worth it for testing
        RedisQueue.recv_block_secs = 1
        async with with_redis(os.environ.get("BRRR_TEST_REDIS_URL")) as rc:
            yield RedisQueue(rc)
