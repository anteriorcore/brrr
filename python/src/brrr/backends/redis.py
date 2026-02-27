from __future__ import annotations

import logging
import typing

from ..queue import Message, Queue, QueueInfo, QueueIsEmpty
from ..store import Cache

if typing.TYPE_CHECKING:
    from redis.asyncio import Redis


logger = logging.getLogger(__name__)


class RedisQueue(Queue, Cache):
    client: Redis[typing.Any]

    def __init__(self, client: Redis[typing.Any]) -> None:
        self.client = client

    async def setup(self) -> None:
        pass

    async def put_message(self, topic: str, body: str) -> None:
        logger.debug(f"Putting new message on {topic}")
        await self.client.rpush(f"brrr/messages/{topic}", body.encode("utf-8"))

    async def get_message(self, topic: str) -> Message:
        response = await self.client.blpop(
            f"brrr/messages/{topic}", self.recv_block_secs
        )
        if not response:
            raise QueueIsEmpty()
        return Message(response[1].decode("utf-8"))

    async def get_info(self, topic: str) -> QueueInfo:
        total = await self.client.llen(f"brrr/messages/{topic}")
        return QueueInfo(num_messages=total)

    async def incr(self, key: str) -> int:
        return await self.client.incr(key)
