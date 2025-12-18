from collections import Counter

import brrr
import pytest
from brrr import ActiveWorker, AppWorker, SpawnLimitError
from brrr.backends.in_memory import InMemoryByteStore, InMemoryQueue
from brrr.pickle_codec import PickleCodec

from .parametrize import names


async def test_spawn_limit_depth(topic: str, task_name: str) -> None:
    queue = InMemoryQueue([topic])
    store = InMemoryByteStore()
    n = 0

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        nonlocal n
        n += 1
        if a == 0:
            # Prevent false positives from this test by exiting cleanly at some point
            return 0
        return await app.call(foo)(a - 1)

    async with brrr.serve(queue, store, store) as conn:
        conn._spawn_limit = 100
        app = AppWorker(handlers={task_name: foo}, codec=PickleCodec(), connection=conn)
        await app.schedule(task_name, topic=topic)(conn._spawn_limit + 3)
        queue.flush()

        with pytest.raises(SpawnLimitError):
            await conn.loop(topic, app.handle)

        assert n == conn._spawn_limit


async def test_spawn_limit_breadth_mapped(topic: str, task_name: str) -> None:
    queue = InMemoryQueue([topic])
    store = InMemoryByteStore()
    calls = Counter[str]()
    name_one, name_foo = names(task_name, ("one", "foo"))

    @brrr.handler_no_arg
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        calls["foo"] += 1
        # Pass a different argument to avoid the debouncer
        return sum(await app.gather(*map(app.call(one), range(a))))

    async with brrr.serve(queue, store, store) as conn:
        conn._spawn_limit = 100
        app = AppWorker(
            handlers={name_foo: foo, name_one: one},
            codec=PickleCodec(),
            connection=conn,
        )
        await app.schedule(name_foo, topic=topic)(conn._spawn_limit + 4)
        queue.flush()

        with pytest.raises(SpawnLimitError):
            await conn.loop(topic, app.handle)

    assert calls["foo"] == 1


async def test_spawn_limit_recoverable(topic: str, task_name: str) -> None:
    queue = InMemoryQueue([topic])
    store = InMemoryByteStore()
    cache = InMemoryByteStore()
    name_one, name_foo = names(task_name, ("one", "foo"))

    @brrr.handler_no_arg
    async def one(_: int) -> int:
        return 1

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        # Pass a different argument to avoid the debouncer
        return sum(await app.gather(*map(app.call(one), range(a))))

    async with brrr.serve(queue, store, cache) as conn:
        conn._spawn_limit = 100
        spawn_limit_encountered = False
        n = conn._spawn_limit + 1
        app = AppWorker(
            handlers={name_foo: foo, name_one: one},
            codec=PickleCodec(),
            connection=conn,
        )

        while True:
            # Very ugly but this works for testing
            cache.inner = {}
            try:
                await app.schedule(name_foo, topic=topic)(n)
                queue.flush()
                await conn.loop(topic, app.handle)
                break
            except SpawnLimitError:
                spawn_limit_encountered = True

    # I expect messages to be left pending as unhandled here, that’s the point:
    assert spawn_limit_encountered
    assert await app.read(name_foo)(n) == n


async def test_spawn_limit_breadth_manual(topic: str, task_name: str) -> None:
    queue = InMemoryQueue([topic])
    store = InMemoryByteStore()
    calls = Counter[str]()
    name_one, name_foo = names(task_name, ("one", "foo"))

    @brrr.handler_no_arg
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        calls["foo"] += 1
        total = 0
        for i in range(a):
            # Pass a different argument to avoid the debouncer
            total += await app.call(one)(i)

        return total

    async with brrr.serve(queue, store, store) as conn:
        conn._spawn_limit = 100
        app = AppWorker(
            handlers={name_foo: foo, name_one: one},
            codec=PickleCodec(),
            connection=conn,
        )
        await app.schedule(name_foo, topic=topic)(conn._spawn_limit + 3)
        queue.flush()
        with pytest.raises(SpawnLimitError):
            await conn.loop(topic, app.handle)

        assert calls == Counter(
            dict(one=conn._spawn_limit / 2, foo=conn._spawn_limit / 2)
        )


async def test_spawn_limit_cached(topic: str, task_name: str) -> None:
    queue = InMemoryQueue([topic])
    store = InMemoryByteStore()
    name_foo, name_same = names(task_name, ("foo", "same"))
    n = 0
    final = None

    @brrr.handler_no_arg
    async def same(a: int) -> int:
        nonlocal n
        n += 1
        return a

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        val = sum(await app.gather(*map(app.call(same), [1] * a)))
        nonlocal final
        final = val
        return val

    async with brrr.serve(queue, store, store) as conn:
        conn._spawn_limit = 100
        app = AppWorker(
            handlers={name_foo: foo, name_same: same},
            codec=PickleCodec(),
            connection=conn,
        )
        await app.schedule(name_foo, topic=topic)(conn._spawn_limit + 5)
        queue.flush()
        await conn.loop(topic, app.handle)

        assert n == 1
        assert final == conn._spawn_limit + 5
