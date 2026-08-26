from __future__ import annotations

import asyncio
from collections import UserDict
from collections.abc import (
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from typing import Any, Concatenate, Final, NewType, assert_never, overload

from brrr.store import NotFoundError

from .codec import Codec
from .connection import Connection, Defer, DeferredCall, Request, Response

# Any async function of P returning R.  The structural primitive Task is built
# from, and the shape callers get back from schedule/read/call: the task's own
# arguments, with the environment already supplied.
type AsyncFn[**P, R] = Callable[P, Awaitable[R]]

# A brrr task: a user function which receives its environment as its first
# argument, followed by the task's own arguments.  Env is supplied by the Codec,
# which decides what to inject on every invocation.  Usually that's an
# ActiveWorker, so the task can call back into brrr, but a codec is free to
# provide anything.
type Task[Env, **P, R] = AsyncFn[Concatenate[Env, P], R]

RootId = NewType("RootId", str)


@dataclass
class Registry[Env]:
    codec: Codec[Env]
    handlers: TaskCollection[Env]


class NotInBrrrError(Exception):
    """Trying to access worker context from outside a worker"""

    pass


def _val2key[K, V](d: Mapping[K, V], val: V) -> K:
    for k, v in d.items():
        if v == val:
            return k
    raise KeyError(val)


class TaskCollection[Env](UserDict[str, Task[Env, ..., Any]]):
    def task2name(self, task: Task[Env, ..., Any]) -> str:
        return _val2key(self, task)

    def spec2name(self, spec: str | Task[Env, ..., Any]) -> str:
        return spec if isinstance(spec, str) else self.task2name(spec)


class AppConsumer[Env]:
    _connection: Connection
    _registry: Registry[Env]

    def __init__(
        self,
        codec: Codec[Env],
        connection: Connection,
        handlers: Mapping[str, Task[Env, ..., Any]] | None = None,
    ):
        self._connection = connection
        self._registry = Registry(codec, TaskCollection(handlers or {}))

    @overload
    def schedule[**P, R](
        self,
        task_spec: Task[Env, P, R],
        *,
        topic: str,
    ) -> AsyncFn[P, None]: ...
    @overload
    def schedule(self, task_spec: str, *, topic: str) -> AsyncFn[..., None]: ...
    def schedule(self, task_spec: Any, *, topic: str) -> AsyncFn[..., None]:
        """Public-facing one-shot schedule method."""
        task_name = self._registry.handlers.spec2name(task_spec)

        async def f(*args: Any, **kwargs: Any) -> None:
            call = self._registry.codec.encode_call(task_name, args, kwargs)
            await self._connection.schedule_raw(
                topic, call.call_hash, task_name, call.payload
            )

        return f

    @overload
    def read[**P, R](self, task_spec: Task[Env, P, R]) -> AsyncFn[P, R]: ...
    @overload
    def read(self, task_spec: str) -> AsyncFn[..., Any]: ...
    def read(self, task_spec: Any) -> AsyncFn[..., Any]:
        task_name = self._registry.handlers.spec2name(task_spec)

        async def f(*args: Any, **kwargs: Any) -> Any:
            call = self._registry.codec.encode_call(task_name, args, kwargs)
            payload = await self._connection._memory.get_value(call.call_hash)
            return self._registry.codec.decode_return(task_name, payload)

        return f


class AppWorker[Env](AppConsumer[Env]):
    async def handle(self, request: Request, conn: Connection) -> Response | Defer:
        """Glue between this class and the underlying Connection.loop handler"""
        task_name = request.call.task_name
        handler = self._registry.handlers[task_name]
        try:
            resp = await self._registry.codec.invoke_task(
                request.call,
                handler,
                ActiveWorker(conn, self._registry, RootId(request.root_id)),
            )
        except Defer as e:
            return e
        return Response(payload=resp)


class ActiveWorker[Env]:
    _connection: Connection
    _registry: Registry[Env]
    # Exposed only for reference sake of the handler of a call; changing this
    # value has no effect on how this class behaves.  This class’ implementation
    # does not read nor care about this value.
    root_id: Final[RootId]

    def __init__(self, conn: Connection, registry: Registry[Env], root_id: RootId):
        self._connection = conn
        self._registry = registry
        self.root_id = root_id

    @overload
    def call[**P, R](
        self,
        task_spec: Task[Env, P, R],
        *,
        topic: str | None = None,
    ) -> AsyncFn[P, R]: ...
    @overload
    def call(
        self, task_spec: str, *, topic: str | None = None
    ) -> AsyncFn[..., Any]: ...
    def call(self, task_spec: Any, *, topic: str | None = None) -> AsyncFn[..., Any]:
        """Directly call a brrr task from within another task.

        Do not call this unless you are, yourself, already inside a brrr task.

        """
        task_name = self._registry.handlers.spec2name(task_spec)

        async def f(*args: Any, **kwargs: Any) -> Any:
            call = self._registry.codec.encode_call(task_name, args, kwargs)
            try:
                payload = await self._connection._memory.get_value(call.call_hash)
            except NotFoundError:
                raise Defer([DeferredCall(topic, call)])
            else:
                return self._registry.codec.decode_return(task_name, payload)

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
        return await _gather(task_awaitables)


async def _get_deferrable(task: Awaitable[Any]) -> Response | Defer:
    try:
        return Response(payload=await task)
    except Defer as e:
        return e


# Don’t use me directly.  Only ever legal to use from within an ActiveWorker.
async def _gather(task_awaitables: Sequence[Awaitable[Any]]) -> Sequence[Any]:
    rets = await asyncio.gather(*map(_get_deferrable, task_awaitables))

    defers: list[DeferredCall] = []
    values: list[Any] = []
    for ret in rets:
        match ret:
            case Response(payload=payload):
                values.append(payload)
            case Defer(calls=calls):
                defers.extend(calls)
            case never:
                assert_never(never)

    if defers:
        raise Defer(defers)

    return values
