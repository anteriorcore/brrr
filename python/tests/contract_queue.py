from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import AsyncContextManager, Protocol, cast

import pytest
from brrr.queue import Queue, QueueInfo, QueueIsEmpty


class HasInfo(Protocol):
    async def get_info(self, topic: str) -> QueueInfo: ...


class QueueContract(ABC):
    has_accurate_info: bool

    @abstractmethod
    def with_queue(self, topics: Sequence[str]) -> AsyncContextManager[Queue]:
        """
        A context manager which calls test function f with a queue
        """
        raise NotImplementedError()

    async def test_queue_raises_empty(self) -> None:
        async with self.with_queue(["foo"]) as queue:
            with pytest.raises(QueueIsEmpty):
                await queue.get_message("foo")

    async def test_queue_enqueues(self) -> None:
        async with self.with_queue(["test-topic"]) as queue:
            messages = {"message-1", "message-2", "message-3"}

            if self.has_accurate_info:
                assert (await self.get_info(queue, "test-topic")).num_messages == 0

            for i, msg in enumerate(messages):
                await queue.put_message("test-topic", msg)
                if self.has_accurate_info:
                    assert (
                        await self.get_info(queue, "test-topic")
                    ).num_messages == i + 1

            for i, msg in enumerate(set(messages)):
                message = await queue.get_message("test-topic")
                assert message.body in messages
                messages.remove(message.body)
                if self.has_accurate_info:
                    assert (
                        await self.get_info(queue, "test-topic")
                    ).num_messages == len(messages)

            with pytest.raises(QueueIsEmpty):
                await queue.get_message("test-topic")

    async def test_topics(self) -> None:
        async with self.with_queue(["test1", "test2"]) as queue:
            await queue.put_message("test1", "one")
            await queue.put_message("test2", "two")
            await queue.put_message("test1", "one")
            assert (await queue.get_message("test2")).body == "two"
            assert (await queue.get_message("test1")).body == "one"
            assert (await queue.get_message("test1")).body == "one"

    async def get_info(self, queue: Queue, topic: str) -> QueueInfo:
        assert self.has_accurate_info, (
            "Can only get info if queue is assumed to have accurate info"
        )
        queue_with_info = cast(HasInfo, queue)
        return await queue_with_info.get_info(topic)
