import dataclasses
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import AsyncContextManager, Awaitable, Callable, Iterable

import pytest
from brrr.call import Call
from brrr.store import (
    CompareMismatch,
    MemKey,
    Memory,
    NotFoundError,
    Store,
)
from brrr.tagged_tuple import PendingReturn


class FakeError(Exception):
    pass


class ByteStoreContract(ABC):
    @abstractmethod
    def with_store(self) -> AsyncContextManager[Store]: ...

    # There are tests in this file which require read-after-write consistency.
    # Technically that’s not actually required by brrr, nor by the datastores.
    # What’s really needed in fact, is a store which is explicitly NOT
    # read-after-write consistent so we can check which guarantees are and
    # aren’t violated at the application layer.  To make these (actually
    # fundamentally spurious) tests more "future proof" and be more explicit
    # about allowing stores which are not read-after-write consistent, any tests
    # which read after a write will be passed as a closure to this wrapper.
    # Most practical non-read-after-write consistent stores are in fact
    # consistent after a certain timeout, so you can put an asyncio.sleep here
    # to bridge that gap between theory and reality, or you could execute it in
    # a loop with a max timeout, or you can just #yolo it and leave the default
    # implementation which assumes RAW consistency.
    async def read_after_write[T](self, f: Callable[[], Awaitable[T]]) -> T:
        return await f()

    async def test_has(self) -> None:
        async with self.with_store() as store:
            a1 = MemKey("pending_returns", "id-1")
            a2 = MemKey("pending_returns", "id-2")
            b1 = MemKey("call", "id-1")

            assert not await store.has(a1)
            assert not await store.has(a2)
            assert not await store.has(b1)

            await store.set(a1, b"value-1")

            async def r1() -> None:
                assert await store.has(a1)
                assert not await store.has(a2)
                assert not await store.has(b1)

            await self.read_after_write(r1)

            await store.set(a2, b"value-2")

            async def r2() -> None:
                assert await store.has(a1)
                assert await store.has(a2)
                assert not await store.has(b1)

            await self.read_after_write(r2)

            await store.set(b1, b"value-3")

            async def r3() -> None:
                assert await store.has(a1)
                assert await store.has(a2)
                assert await store.has(b1)

            await self.read_after_write(r3)

            await store.delete(a1)

            async def r4() -> None:
                assert not await store.has(a1)
                assert await store.has(a2)
                assert await store.has(b1)

            await self.read_after_write(r4)

            await store.delete(a2)

            async def r5() -> None:
                assert not await store.has(a1)
                assert not await store.has(a2)
                assert await store.has(b1)

            await self.read_after_write(r5)

            await store.delete(b1)

            async def r6() -> None:
                assert not await store.has(a1)
                assert not await store.has(a2)
                assert not await store.has(b1)

            await self.read_after_write(r6)

    async def test_read_after_write(self) -> None:
        async with self.with_store() as store:
            a1 = MemKey("call", "id-1")
            a2 = MemKey("call", "id-2")
            b1 = MemKey("pending_returns", "id-1")

            await store.set(a1, b"value-1")
            await store.set(a2, b"value-2")
            await store.set(b1, b"value-3")

            async def r1() -> None:
                assert await store.get(a1) == b"value-1"
                assert await store.get(a2) == b"value-2"
                assert await store.get(b1) == b"value-3"

            await self.read_after_write(r1)

    async def test_key_error(self) -> None:
        async with self.with_store() as store:
            a1 = MemKey("value", "id-1")

            with pytest.raises(NotFoundError):
                await store.get(a1)

            await store.delete(a1)
            with pytest.raises(NotFoundError):
                await store.get(a1)

            await store.set(a1, b"value-1")

            async def r1() -> None:
                assert await store.get(a1) == b"value-1"
                await store.delete(a1)

            await self.read_after_write(r1)

            async def r2() -> None:
                with pytest.raises(NotFoundError):
                    await store.get(a1)

            await self.read_after_write(r2)

    async def test_set_new_value(self) -> None:
        async with self.with_store() as store:
            a1 = MemKey("value", "id-1")

            await store.set_new_value(a1, b"value-1")

            async def r1() -> None:
                assert await store.get(a1) == b"value-1"

            await self.read_after_write(r1)

            with pytest.raises(CompareMismatch):
                await store.set_new_value(a1, b"value-2")

            # Even overriding with the _same_ value is not allowed for a CAS
            # operation:
            with pytest.raises(CompareMismatch):
                await store.set_new_value(a1, b"value-1")

            await self.read_after_write(r1)

    async def test_set(self) -> None:
        async with self.with_store() as store:
            a1 = MemKey("value", "id-1")

            await store.set(a1, b"value-1")

            async def r1() -> None:
                assert await store.get(a1) == b"value-1"

            await self.read_after_write(r1)

            # Overriding with a different value is allowed
            await store.set(a1, b"value-2")

            async def r2() -> None:
                assert await store.get(a1) == b"value-2"

            await self.read_after_write(r2)

    async def test_compare_and_set(self) -> None:
        async with self.with_store() as store:
            a1 = MemKey("value", "id-1")

            await store.set(a1, b"value-1")

            async def r1() -> None:
                with pytest.raises(CompareMismatch):
                    await store.compare_and_set(a1, b"value-2", b"value-3")

            await self.read_after_write(r1)

            await store.compare_and_set(a1, b"value-2", b"value-1")

            async def r2() -> None:
                assert await store.get(a1) == b"value-2"

            await self.read_after_write(r2)

    async def test_compare_and_delete(self) -> None:
        async with self.with_store() as store:
            a1 = MemKey("value", "id-1")

            with pytest.raises(CompareMismatch):
                await store.compare_and_delete(a1, b"value-2")

            await store.set(a1, b"value-1")

            async def r1() -> None:
                with pytest.raises(CompareMismatch):
                    await store.compare_and_delete(a1, b"value-2")

            await self.read_after_write(r1)

            assert await store.get(a1) == b"value-1"

            await store.compare_and_delete(a1, b"value-1")

            async def r2() -> None:
                with pytest.raises(NotFoundError):
                    await store.get(a1)

            await self.read_after_write(r2)


class MemoryContract(ByteStoreContract):
    @asynccontextmanager
    async def with_memory(self) -> AsyncIterator[Memory]:
        async with self.with_store() as store:
            yield Memory(store)

    async def test_call(self) -> None:
        async with self.with_memory() as memory:
            with pytest.raises(NotFoundError):
                await memory.get_call("non-existent")

            call = Call(task_name="task", payload=b"foo", call_hash="abc")

            await memory.set_call(call)

            async def r1() -> None:
                call_round = await memory.get_call(call.call_hash)
                assert call == call_round
                # Call.__eq__ was overridden and I’m not sure if it was the
                # right idea or not but in this case I really definitely want to
                # ensure all properties round tripped correctly.
                assert call.call_hash == call_round.call_hash
                assert call.task_name == call_round.task_name
                assert call.payload == call_round.payload

            await self.read_after_write(r1)

    async def test_value(self) -> None:
        async with self.with_memory() as memory:
            call_hash = "abc"

            await memory.set_value(call_hash, b"123")

            async def r1() -> None:
                assert await memory.has_value(call_hash)
                assert await memory.get_value(call_hash) == b"123"

            await self.read_after_write(r1)

            await memory.set_value(call_hash, b"456")

            async def r2() -> None:
                assert await memory.get_value(call_hash) == b"456"

            await self.read_after_write(r2)

    async def test_pending_returns(self, topic: str) -> None:
        async with self.with_memory() as memory:

            async def body(keys: Iterable[PendingReturn]) -> None:
                assert not keys

            await memory.with_pending_returns_remove("key", body)

            call_hash = "key"
            one = PendingReturn(
                root_id="root",
                call_hash="parent",
                topic=topic,
            )

            # base case
            assert await memory.add_pending_return(call_hash, one)
            # same one, shouldn't schedule again
            assert not await memory.add_pending_return(call_hash, one)
            # different root, should schedule - it's a retry
            two = dataclasses.replace(one, root_id="different")
            assert await memory.add_pending_return(call_hash, two)
            # new callHash, new PR, should schedule
            assert await memory.add_pending_return("different-hash", one)
            # continuation, shouldn't schedule again
            three = dataclasses.replace(one, topic="different")
            assert not await memory.add_pending_return(call_hash, three)
            four = dataclasses.replace(one, call_hash="different")
            assert not await memory.add_pending_return(call_hash, four)
            five = dataclasses.replace(one, call_hash="one", topic="two")
            assert not await memory.add_pending_return(call_hash, five)

            with pytest.raises(FakeError):

                async def body(keys: Iterable[PendingReturn]) -> None:
                    assert set(keys) == {one, two, three, four, five}
                    raise FakeError()

                await memory.with_pending_returns_remove("key", body)

            async def body2(keys: Iterable[PendingReturn]) -> None:
                assert set(keys) == {one, two, three, four, five}

            await memory.with_pending_returns_remove("key", body2)

            async def body3(keys: Iterable[PendingReturn]) -> None:
                assert not keys

            async def r1() -> None:
                await memory.with_pending_returns_remove("key", body3)

            await self.read_after_write(r1)
