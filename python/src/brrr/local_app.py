import asyncio
import functools
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable

from .app import AppWorker, Task
from .backends.in_memory import CloseOnEmptyQueue, InMemoryByteStore
from .codec import Codec
from .connection import Server, serve


class LocalApp[C]:
    """
    Low(er)-level primitive for local dev, mimics App* types.
    """

    def __init__(
        self,
        *,
        topic: str,
        conn: Server,
        queue: CloseOnEmptyQueue,
        app: AppWorker[C],
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
        # One loop per worker the queue expects, so the two can't disagree.
        loops = [
            asyncio.create_task(self._conn.loop(self._topic, self._app.handle))
            for _ in range(self._queue.workers)
        ]
        # Not asyncio.TaskGroup on purpose: it wraps failures in an
        # ExceptionGroup, which would change the exception type callers see
        # (even with workers=1) and break their pytest.raises checks.
        try:
            await asyncio.gather(*loops)
        finally:
            # If a loop raised (or run() was cancelled, e.g. by a test
            # timeout), the others can never see the queue close: stop them
            # and wait, so none hang around or keep running jobs.  On success
            # they're all done already and this is a no-op.
            for t in loops:
                t.cancel()
            await asyncio.gather(*loops, return_exceptions=True)


@asynccontextmanager
async def local_app[C](
    topic: str,
    handlers: Mapping[str, Task[C, ..., Any]],
    codec: Codec[C],
    workers: int = 1,
) -> AsyncIterator[LocalApp[C]]:
    """
    Helper function for unit tests which use brrr

    With workers > 1, `run` runs that many concurrent worker loops, e.g. to
    surface races between overlapping re-runs of a parent task.
    """
    store = InMemoryByteStore()
    queue = CloseOnEmptyQueue([topic], workers=workers)

    async with serve(queue, store, store) as conn:
        app = AppWorker(handlers=handlers, codec=codec, connection=conn)
        yield LocalApp(topic=topic, conn=conn, queue=queue, app=app)


class LocalBrrr[C]:
    """Helper class for your unit tests to use an ephemeral in-memory brrr.

    >>> import asyncio
    >>> from .demo_pickle_codec import DemoPickleCodec, DemoPickleCodecContext
    >>>
    >>> async def plus(app: AppWorker, x: int, y: int) -> int: return x + y
    ...
    >>> b = LocalBrrr(topic="test", handlers=dict(plus=plus), codec=DemoPickleCodec())
    >>> asyncio.run(b.run(plus)(x=1, y=2))
    3

    The full state is cleared between each .call.  There is no brrr caching
    between calls in this local instance.

    Pass workers > 1 to run that many worker loops concurrently.

    """

    def __init__(
        self,
        topic: str,
        handlers: Mapping[str, Task[C, ..., Any]],
        codec: Codec[C],
        workers: int = 1,
    ):
        self.topic = topic
        self.workers = workers
        self.handlers = handlers
        self.codec = codec

    def run[**P, R](self, f: Task[C, P, R] | str) -> Callable[P, Awaitable[R]]:
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
                workers=self.workers,
            ) as app:
                await app.schedule(f)(*args, **kwargs)
                await app.run()
                return await app.read(f)(*args, **kwargs)

        return wrapper
