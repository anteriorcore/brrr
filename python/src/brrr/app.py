from __future__ import annotations

import functools
from abc import abstractmethod
from collections import UserDict
from collections.abc import (
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from typing import Any, Concatenate, Protocol, overload

from brrr.store import NotFoundError

from .codec import Codec
from .connection import Connection, Defer, DeferredCall, Request, Response
from .only import allow_only


class WrappedTask(Protocol):
    """Base class for functions which have been registered as app handlers.

    Artificial because of mypy struggles.  Ideally you'd give this a member
    _brrr_handler with type Callable[..., Awaitable[Any]], but when you do mypy
    loses the ability to automatically infer the GCD base class between
    different type-specific instances of the typed WrappedTaskT.  That's a
    desirable property because it makes intuitive the passing of a dictionary of
    such functions as the `handlers' argument.

    """

    pass


class WrappedTaskT[**H, **U, R, T](WrappedTask):
    """Type-aware wrapped brrr handler for apps.

    - H: argskwargs of the original handler
    - U: unwrapped argskwargs without ActiveWorker no matter what
    - R: Unwrapped return value (no awaitable)

    H could be U (no-arg handler) or H could include an ActiveWorker.

    """

    @abstractmethod
    async def __call__(self, *args: H.args, **kwargs: H.kwargs) -> R: ...

    _brrr_handler: Callable[Concatenate[ActiveWorker[T], U], Awaitable[R]]


class NotInBrrrError(Exception):
    """Trying to access worker context from outside a worker"""

    pass


def _val2key[K, V](d: Mapping[K, V], val: V) -> K:
    for k, v in d.items():
        if v == val:
            return k
    raise KeyError(val)


def handler_no_arg[**P, R, T](
    f: Callable[P, Awaitable[R]],
) -> WrappedTaskT[P, P, R, T]:
    """A brrr handler which does not care for the worker app as an argument."""

    # A brrr task API compatible version of this function is one which just
    # ignores its first arg
    async def _brrr_handler(_: ActiveWorker[T], *args: P.args, **kwargs: P.kwargs) -> R:
        return await f(*args, **kwargs)

    # Rather than create a wrapper for the original arg, we can tack this
    # brrr-internal property onto that function instead.  This makes the
    # decorator inherently low-touch: we don’t mess with the function at all.
    # This also saves us from having to muck with __call__.
    setattr(f, "_brrr_handler", _brrr_handler)
    return f  # type: ignore[return-value]


def handler[**P, R, T](
    f: Callable[Concatenate[ActiveWorker[T], P], Awaitable[R]],
) -> WrappedTaskT[Concatenate[ActiveWorker[T], P], P, R, T]:
    # This function already satisfies the brrr task API
    setattr(f, "_brrr_handler", f)
    return f  # type: ignore[return-value]


class TaskCollection(UserDict[str, WrappedTask]):
    def task2name(self, task: WrappedTask) -> str:
        return _val2key(self, task)

    def spec2name(self, spec: str | WrappedTask) -> str:
        return spec if isinstance(spec, str) else self.task2name(spec)


class AppConsumer[T]:
    _codec: Codec
    _connection: Connection
    _context: T
    tasks: TaskCollection

    def __init__(
        self,
        codec: Codec,
        connection: Connection,
        *,
        context: T,
        handlers: Mapping[str, WrappedTask] | None = None,
    ):
        self._codec = codec
        self._connection = connection
        self._context = context
        self.tasks = TaskCollection(**(handlers or {}))

    @overload
    def schedule[**P, R](
        self,
        task_spec: Callable[Concatenate[ActiveWorker[T], P], Awaitable[R]],
        *,
        topic: str,
    ) -> Callable[P, Awaitable[None]]: ...
    @overload
    def schedule[**P, R](
        self,
        task_spec: Callable[P, Awaitable[R]],
        *,
        topic: str,
    ) -> Callable[P, Awaitable[None]]: ...
    @overload
    def schedule(
        self, task_spec: str, *, topic: str
    ) -> Callable[..., Awaitable[None]]: ...
    def schedule(self, task_spec: Any, *, topic: str) -> Callable[..., Awaitable[None]]:
        """Public-facing one-shot schedule method."""
        task_name = self.tasks.spec2name(task_spec)

        async def f(*args: Any, **kwargs: Any) -> None:
            call = self._codec.encode_call(task_name, args, kwargs)
            await self._connection.schedule_raw(
                topic, call.call_hash, task_name, call.payload
            )

        return f

    @overload
    def read[**P, R](
        self, task_spec: Callable[Concatenate[ActiveWorker[T], P], Awaitable[R]]
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def read[**P, R](
        self, task_spec: Callable[P, Awaitable[R]]
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def read(self, task_spec: str) -> Callable[..., Awaitable[Any]]: ...
    def read(self, task_spec: Any) -> Callable[..., Awaitable[Any]]:
        task_name = self.tasks.spec2name(task_spec)

        async def f(*args: Any, **kwargs: Any) -> Any:
            call = self._codec.encode_call(task_name, args, kwargs)
            payload = await self._connection._memory.get_value(call.call_hash)
            return self._codec.decode_return(task_name, payload)

        return f


class AppWorker[T](AppConsumer[T]):
    async def handle(self, request: Request, conn: Connection) -> Response | Defer:
        """Glue between this class and the underlying Connection.loop handler"""
        task_name = request.call.task_name
        # This is such an odd place to be wrapping this... the carpet keeps
        # bubbling up somewhere and no matter how often I push it down, it pops
        # up somewhere else.
        handler = functools.partial(
            # If WrappedTask exposed ._brrr_handler as a property we would be
            # able to access it here.  Unfortunately, doing so defeats mypy’s
            # ability to recognize it as the common base class for different
            # WrappedTaskT instantiations (with different types substituted for
            # the generic parameters).  Currently, if you have multiple
            # different WrappedTaskT objects, with different concrete types,
            # mypy still recognizes that they’re all WrappedTask, which is
            # useful for the api (see its own docstring).  But this is the price
            # you pay:
            getattr(self.tasks[task_name], "_brrr_handler"),
            ActiveWorker(conn, self._codec, self.tasks, context=self._context),
        )
        with allow_only():
            try:
                resp = await self._codec.invoke_task(request.call, handler)
            except Defer as e:
                return e
            return Response(payload=resp)


class ActiveWorker[T]:
    _connection: Connection
    _codec: Codec
    _handlers: TaskCollection
    _context: T

    def __init__(
        self, conn: Connection, codec: Codec, tasks: TaskCollection, *, context: T
    ):
        self._connection = conn
        self._codec = codec
        self._handlers = tasks
        self._context = context

    def get_context(self) -> T:
        """Get the context object associated with this worker instance."""
        return self._context

    @overload
    def call[**P, R](
        self,
        task_spec: Callable[Concatenate[ActiveWorker[T], P], Awaitable[R]],
        *,
        topic: str | None = None,
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def call[**P, R](
        self,
        task_spec: Callable[P, Awaitable[R]],
        *,
        topic: str | None = None,
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def call(
        self, task_spec: str, *, topic: str | None = None
    ) -> Callable[..., Awaitable[Any]]: ...
    def call(
        self, task_spec: Any, *, topic: str | None = None
    ) -> Callable[..., Awaitable[Any]]:
        """Directly call a brrr task from within another task.

        Do not call this unless you are, yourself, already inside a brrr task.

        """
        task_name = self._handlers.spec2name(task_spec)

        async def f(*args: Any, **kwargs: Any) -> Any:
            call = self._codec.encode_call(task_name, args, kwargs)
            try:
                payload = await self._connection._memory.get_value(call.call_hash)
            except NotFoundError:
                raise Defer([DeferredCall(topic, call)])
            else:
                return self._codec.decode_return(task_name, payload)

        return f

    # Type annotations for Brrr.gather are modeled after asyncio.gather:
    # support explicit types for 1-5 arguments (and when all have the same type),
    # and a catch-all for the rest.
    @overload
    async def gather[T1](self, coro_or_future1: Awaitable[T1]) -> tuple[T1]: ...
    @overload
    async def gather[T1, T2](
        self, coro_or_future1: Awaitable[T1], coro_or_future2: Awaitable[T2]
    ) -> tuple[T1, T2]: ...
    @overload
    async def gather[T1, T2, T3](
        self,
        coro_or_future1: Awaitable[T1],
        coro_or_future2: Awaitable[T2],
        coro_or_future3: Awaitable[T3],
    ) -> tuple[T1, T2, T3]: ...
    @overload
    async def gather[T1, T2, T3, T4](
        self,
        coro_or_future1: Awaitable[T1],
        coro_or_future2: Awaitable[T2],
        coro_or_future3: Awaitable[T3],
        coro_or_future4: Awaitable[T4],
    ) -> tuple[T1, T2, T3, T4]: ...
    @overload
    async def gather[T1, T2, T3, T4, T5](
        self,
        coro_or_future1: Awaitable[T1],
        coro_or_future2: Awaitable[T2],
        coro_or_future3: Awaitable[T3],
        coro_or_future4: Awaitable[T4],
        coro_or_future5: Awaitable[T5],
    ) -> tuple[T1, T2, T3, T4, T5]: ...
    @overload
    async def gather[Ts](self, *coro_or_futures: Awaitable[Ts]) -> list[Ts]: ...
    @overload
    async def gather(
        self,
        coro_or_future1: Awaitable[Any],
        coro_or_future2: Awaitable[Any],
        coro_or_future3: Awaitable[Any],
        coro_or_future4: Awaitable[Any],
        coro_or_future5: Awaitable[Any],
        *coro_or_futures: Awaitable[Any],
    ) -> list[Any]: ...
    async def gather(self, *task_awaitables: Awaitable[Any]) -> Sequence[Any]:  # type: ignore[misc]
        """
        Takes a number of task lambdas and calls each of them.
        If they've all been computed, return their values,
        Otherwise raise jobs for those that haven't been computed
        """
        defers: list[DeferredCall] = []
        values = []

        for task_awaitable in task_awaitables:
            try:
                values.append(await task_awaitable)
            except Defer as d:
                defers.extend(d.calls)

        if defers:
            raise Defer(defers)

        return values
