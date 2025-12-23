from __future__ import annotations

import functools
from collections import UserDict
from collections.abc import (
    Awaitable,
    Callable,
    Coroutine,
    Mapping,
    Sequence,
)
from typing import Any, Concatenate, Self, overload

from brrr.store import NotFoundError

from .codec import Codec
from .connection import Connection, Defer, DeferredCall, Request, Response

type Task[**P, R, A] = Callable[Concatenate[A, P], Coroutine[Any, Any, R]]


class NotInBrrrError(Exception):
    """Trying to access worker context from outside a worker"""

    pass


def _val2key[K, V](d: Mapping[K, V], val: V) -> K:
    for k, v in d.items():
        if v == val:
            return k
    raise KeyError(val)


class TaskCollection[A](UserDict[str, Task[..., Any, A]]):
    def task2name(self, task: Task[..., Any, A]) -> str:
        return _val2key(self, task)

    def spec2name(self, spec: str | Task[..., Any, A]) -> str:
        return spec if isinstance(spec, str) else self.task2name(spec)


class AppConsumer[A]:
    _codec: Codec
    _connection: Connection
    tasks: TaskCollection[A]

    def __init__(
        self,
        codec: Codec,
        connection: Connection,
        handlers: Mapping[str, Task[..., Any, A]] | None = None,
    ) -> None:
        self._codec = codec
        self._connection = connection
        self.tasks = TaskCollection(**(handlers or {}))

    @overload
    def schedule[**P, R](
        self,
        task_spec: Callable[Concatenate[A, P], Awaitable[R]],
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
        self, task_spec: Callable[Concatenate[A, P], Awaitable[R]]
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


class AppWorker[A](AppConsumer[A]):
    @overload
    def __init__(
        self: AppWorker[ActiveWorker],
        codec: Codec,
        connection: Connection,
        *,
        handlers: Mapping[str, Task[..., Any, A]] | None = None,
        active_worker_init: None = None,
    ) -> None: ...
    @overload
    def __init__(
        self,
        codec: Codec,
        connection: Connection,
        *,
        handlers: Mapping[str, Task[..., Any, A]] | None = None,
        active_worker_init: Callable[[Connection, Codec, TaskCollection[A]], A],
    ) -> None: ...

    def __init__(
        self,
        codec: Codec,
        connection: Connection,
        *,
        handlers: Mapping[str, Task[..., Any, A]] | None = None,
        active_worker_init: Callable[[Connection, Codec, TaskCollection[A]], A]
        | None = None,
    ) -> None:
        super().__init__(codec, connection, handlers=handlers)
        self._active_worker_init = active_worker_init or ActiveWorker.__call__

    async def handle(
        self,
        request: Request,
        conn: Connection,
    ) -> Response | Defer:
        """Glue between this class and the underlying Connection.loop handler"""
        task_name = request.call.task_name
        # This is such an odd place to be wrapping this... the carpet keeps
        # bubbling up somewhere and no matter how often I push it down, it pops
        # up somewhere else.
        active_worker = self._active_worker_init(conn, self._codec, self.tasks)
        handler = functools.partial(self.tasks[task_name], active_worker)
        try:
            resp = await self._codec.invoke_task(request.call, handler)
        except Defer as e:
            return e
        return Response(payload=resp)


class ActiveWorker:
    _connection: Connection
    _codec: Codec
    _handlers: TaskCollection[Self]

    def __init__(self, conn: Connection, codec: Codec, tasks: TaskCollection[Self]):
        self._connection = conn
        self._codec = codec
        self._handlers = tasks

    @overload
    def call[**P, R](
        self,
        task_spec: Callable[Concatenate[Self, P], Awaitable[R]],
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
    async def gather[T](self, *coro_or_futures: Awaitable[T]) -> list[T]: ...
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
