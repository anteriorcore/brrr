from __future__ import annotations

import functools
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, AsyncContextManager, Awaitable, Callable, Generic, overload

from .app import A, AppWorker, Task
from .backends.in_memory import InMemoryByteStore, InMemoryQueue
from .codec import Codec
from .connection import Server, serve


class LocalApp(Generic[A]):
    """
    Low(er)-level primitive for local dev, mimics App* types.
    """

    def __init__(
        self, *, topic: str, conn: Server, queue: InMemoryQueue, app: AppWorker[A]
    ) -> None:
        self._conn = conn
        self._app = app
        self._queue = queue
        self._topic = topic
        self.schedule = functools.partial(app.schedule, topic=topic)
        self.read = app.read
        self._has_run = False

    async def run(self) -> None:
        if self._has_run:
            raise ValueError("LocalApp has already run")
        self._has_run = True
        self._queue.flush()
        await self._conn.loop(self._topic, self._app.handle)


@overload
def local_app(
    topic: str,
    handlers: Mapping[str, Task[..., Any, None]],
    codec: Codec,
) -> AsyncContextManager[LocalApp[None]]: ...
@overload
def local_app[A](
    topic: str,
    handlers: Mapping[str, Task[..., Any, A]],
    codec: Codec,
    context: A,
) -> AsyncContextManager[LocalApp[A]]: ...


@asynccontextmanager
async def local_app[A](
    topic: str,
    handlers: Mapping[str, Task[..., Any, A]],
    codec: Codec,
    context: A | None = None,
) -> AsyncIterator[LocalApp[A]]:
    """
    Helper function for unit tests which use brrr
    """
    store = InMemoryByteStore()
    queue = InMemoryQueue([topic])

    async with serve(queue, store, store) as conn:
        app = AppWorker[A](
            handlers=handlers,
            codec=codec,
            connection=conn,
            context=context,  # type: ignore[arg-type]
        )
        yield LocalApp(topic=topic, conn=conn, queue=queue, app=app)


class LocalBrrr(Generic[A]):
    """Helper class for your unit tests to use an ephemeral in-memory brrr.

    >>> async def plus(app: brrr.ActiveWorker, x: int, y: int) -> int: return x + y
    ...
    >>> b = LocalBrrr(topic="test", handlers=dict(plus=plus), codec=PickleCodec())
    >>> await b.run(plus)(x=1, y=2)
    3

    The full state is cleared between each .call.  There is no brrr caching
    between calls in this local instance.

    """

    @overload
    def __init__(
        self: LocalBrrr[None],
        topic: str,
        handlers: Mapping[str, Task[..., Any, None]],
        codec: Codec,
    ) -> None: ...
    @overload
    def __init__(
        self,
        topic: str,
        handlers: Mapping[str, Task[..., Any, A]],
        codec: Codec,
        context: A,
    ) -> None: ...

    def __init__(
        self,
        topic: str,
        handlers: Mapping[str, Task[..., Any, Any]],
        codec: Codec,
        context: Any = None,
    ) -> None:
        self.topic = topic
        self.handlers: Mapping[str, Task[..., Any, A]] = handlers
        self.codec = codec
        self.context: A = context

    def run[**P, R](self, f: Task[P, R, A] | str) -> Callable[P, Awaitable[R]]:
        """Create an ephemeral brrr app and runt his entire task to completion.

        Named `run' to emphasize this is different from app.call.  This isn't
        just a singular call from within a brrr task: this is a full in-memory
        brrr instance with memory and queue, running the entire call graph, and
        returning its result.

        """

        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            async with local_app(
                topic=self.topic,
                handlers=self.handlers,
                codec=self.codec,
                context=self.context,
            ) as app:
                await app.schedule(f)(*args, **kwargs)
                await app.run()
                return await app.read(f)(*args, **kwargs)

        return wrapper
