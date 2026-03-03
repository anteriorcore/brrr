from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
)
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4

from .call import Call
from .queue import Queue, QueueIsClosed, QueueIsEmpty
from .store import (
    Cache,
    Memory,
    NotFoundError,
    Store,
)
from .tagged_tuple import PendingReturn, ScheduleMessage

logger = logging.getLogger(__name__)


class SpawnLimitError(Exception): ...


@dataclass
class DeferredCall:
    # None means self
    topic: str | None
    call: Call


class Defer(Exception):
    """
    When a task is called and hasn't been computed yet, a Defer exception is raised
    Workers catch this exception and schedule the task to be computed
    """

    calls: Iterable[DeferredCall]

    def __init__(self, calls: Iterable[DeferredCall]):
        self.calls = calls


@dataclass
class Request:
    # The actual semantically meaningful part of the call
    call: Call
    # Probably some extra useful out-of-band metadata at some point?  Something
    # like "headers"?  Metadata?  For now we only have calls in a request, but
    # it’s very likely we’ll want to add things here very soon and this is part
    # of the public API, so let’s wrap the call itself in a single-member
    # Request class.


@dataclass
class Response:
    payload: bytes


type Handler = Callable[[Request, Connection], Awaitable[Response | Defer]]


@asynccontextmanager
async def connect(
    queue: Queue, store: Store, cache: Cache
) -> AsyncIterator[Connection]:
    """Create a client-only connection without the ability to handle jobs.

    It can schedule new tasks and read values from the kv store though!

    """
    # Technically unnecessary for this to be a contextmanager today but it fits
    # the expected API for this, and it leaves the door open for some activity
    # on join / leave (e.g. counting active clients in the cache).
    yield Connection(queue, store, cache)


@asynccontextmanager
async def serve(queue: Queue, store: Store, cache: Cache) -> AsyncIterator[Server]:
    # I guess you could give the end user even more control but at some
    # point I think this API is fine.  Who really needs access to the server
    # instance?
    yield Server(queue, store, cache)


class Connection:
    """A connection in its most basic form, to call _out_ to brr.

    You can read values, and you can asynchronously schedule jobs, but it
    doesn't allow "calling" jobs using the defer-and-retry mechanism.  That's
    done by active applications.

    """

    # Maximum task executions per root job.  Hard-coded, not intended to be
    # configurable or ever even be hit, for that matter.  If you hit this you almost
    # certainly have a pathological workflow edge case causing massive reruns.  If
    # you actually need to increase this because your flows genuinely hit this
    # limit, I’m impressed.
    _spawn_limit: int = 10_000

    # Non-critical, non-persistent information.  Still figuring out if it makes
    # sense to have this dichotomy supported so explicitly at the top-level of
    # the API.  We run the risk of somehow letting semantically important
    # information seep into this cache, and suddenly it is effectively just part
    # of memory again, at which point what’s the split for?
    _cache: Cache
    # A storage backend for calls, values and pending returns
    _memory: Memory
    # A queue of call keys to be processed
    _queue: Queue

    def __init__(self, queue: Queue, store: Store, cache: Cache):
        self._cache = cache
        self._memory = Memory(store)
        self._queue = queue

    async def _put_job(self, topic: str, job: ScheduleMessage) -> None:
        # Incredibly mother-of-all ad-hoc definitions.  Doesn’t use the topic
        # for counting spawn limits: the spawn limit is currently intended to
        # never be hit at all: it’s a /semantic/ check, not a /runtime/ check.
        # It’s not intended for example to give paying customers a higher spawn
        # limit than free ones.  It’s intended to catch infinite recursion and
        # non-idempotent call graphs.
        if (await self._cache.incr(f"brrr/count/{job.root_id}")) > self._spawn_limit:
            msg = f"Spawn limit {self._spawn_limit} reached for {job.root_id} at job {job.call_hash}"
            logger.error(msg)
            # Throw here because it allows the user of brrrlib to decide how to
            # handle this: what kind of logging?  Does the worker crash in order
            # to flag the problem to the service orchestrator, relying on auto
            # restarts to maintain uptime while allowing monitoring to go flag a
            # bigger issue to admins?  Or just wrap it in a while True loop
            # which catches and ignores specifically this error?
            raise SpawnLimitError(msg)

        await self._queue.put_message(topic, job.encode().decode("utf-8"))

    async def schedule_raw(
        self, topic: str, idempotency_key: str, task_name: str, payload: bytes
    ) -> None:
        """Schedule this call on the brrr workforce.

        This method should be called for top-level workflow calls only.

        """
        # Best effort optimization which is NOT semantically relevant.  It would
        # in fact be a good test to disable this and verify all unit tests still
        # pass (discrepancies in task call counts notwithstanding).
        if await self._memory.has_value(idempotency_key):
            return
        call = Call(task_name=task_name, payload=payload, call_hash=idempotency_key)
        await self._memory.set_call(call)
        # Random root id for every call so we can disambiguate retries
        root_id = base64.urlsafe_b64encode(uuid4().bytes).decode("ascii").strip("=")
        job = ScheduleMessage(
            call_hash=idempotency_key,
            root_id=root_id,
        )
        await self._put_job(topic, job)

    async def read_raw(self, call_hash: str) -> bytes | None:
        """
        Returns the value of a task, or None if it's not present in the store
        """
        try:
            return await self._memory.get_value(call_hash)
        except NotFoundError:
            return None


# Separate classes for now, might not need to be, although it does leave open
# the possibility of having different queue protocols: consumer vs producer
# queue?
class Server(Connection):
    _cache: Cache
    # A storage backend for calls, values and pending returns
    _memory: Memory
    # A queue of call keys to be processed
    _queue: Queue

    _n: int
    # Singleton to count global number of workers for logging purposes only
    _total_workers = 0

    def __init__(self, queue: Queue, store: Store, cache: Cache):
        super().__init__(queue, store, cache)
        self._n = Server._total_workers
        Server._total_workers += 1

    async def _schedule_return_call(self, ret: PendingReturn) -> None:
        job = ScheduleMessage(root_id=ret.root_id, call_hash=ret.call_hash)
        await self._put_job(ret.topic, job)

    async def _schedule_call_nested(
        self,
        my_topic: str,
        child: DeferredCall,
        parent: ScheduleMessage,
    ) -> None:
        """Schedule this call on the brrr workforce.

        This is the real internal entrypoint which should be used by all brrr
        internal-facing code, to avoid confusion about what's internal API and
        what's external.

        This method is for calls which are scheduled from within another brrr
        call.  When the work scheduled by this call has completed, that worker
        must kick off the parent (which is the thread doing the calling of this
        function, "now").

        This will always kick off the call, it doesn't check if a return value
        already exists for this call.

        """
        # First the call because it is perennial, it just describes the actual
        # call being made, it doesn’t cause any further action and it’s safe
        # under all races.
        await self._memory.set_call(child.call)
        # Note this can be immediately read out by a racing return call. The
        # pathological case is: we are late to a party and another worker is
        # actually just done handling this call, and just before it reads out
        # the addresses to which to return, it is added here.  That’s still OK
        # because it will then immediately call this parent flow back, which is
        # fine because the result does in fact exist.
        child_topic = child.topic or my_topic
        call_hash = child.call.call_hash
        ret = PendingReturn(
            root_id=parent.root_id,
            call_hash=parent.call_hash,
            topic=my_topic,
        )
        should_schedule = await self._memory.add_pending_return(call_hash, ret)
        if should_schedule:
            job = ScheduleMessage(
                call_hash=call_hash,
                root_id=parent.root_id,
            )
            await self._put_job(child_topic, job)

    async def _handle_msg(self, handler: Handler, my_topic: str, payload: str) -> None:
        msg = ScheduleMessage.decode(payload.encode("utf-8"))
        call = await self._memory.get_call(msg.call_hash)

        logger.debug(
            f"Calling {my_topic} -> {msg.root_id}/{msg.call_hash} -> {call.task_name}"
        )
        req = Request(call=call)
        ret = await handler(req, self)
        if isinstance(ret, Defer):
            logger.debug(f"Deferring {msg.root_id}/{msg.call_hash}: {call.task_name}")

            # Any of these calls could throw a SpawnLimitError: let that bubble
            # up.  This is very ugly but I want to keep the contract of throwing
            # exceptions on spawn limits, even though it’s _technically_ a user
            # error.  It’s a very nice failure mode and it allows the user to
            # automatically lean on their fleet monitoring to measure the health
            # of their workflows, and debugging this issue can otherwise be very
            # hard.  Of course the “proper” way for a language to support this
            # is Lisp’s restarts, where an exception doesn’t unroll the stack
            # but allows the caller to handle it from the point at which it
            # occurs.
            async def handle_child(child: DeferredCall) -> None:
                await self._schedule_call_nested(my_topic, child, msg)

            await asyncio.gather(*map(handle_child, ret.calls))
            return

        elif isinstance(ret, Response):
            logger.info(
                f"Handled {my_topic} -> {msg.root_id}/{msg.call_hash} -> {call.task_name}"
            )

            # This can end up in a race against another worker to write the
            # value.
            await self._memory.set_value(msg.call_hash, ret.payload)

            # This is ugly and it’s tempting to use asyncio.gather with
            # ‘return_exceptions=True’.  However note I don’t want to blanket catch
            # all errors: only SpawnLimitError.  You’d need to do manual filtering
            # of errors, check if there are any non-spawnlimiterrors, if so throw
            # those immediately from the context block, otherwise throw a spawnlimit
            # error once the context finishes.  It’s about as convoluted as just
            # doing it this way, without any of the clarity.
            spawn_limit_err = None

            async def schedule_returns(returns: Iterable[PendingReturn]) -> None:
                for pending in returns:
                    try:
                        await self._schedule_return_call(pending)
                    except SpawnLimitError as e:
                        logger.info(
                            f"Spawn limit reached returning from {msg.call_hash} to {pending}; clearing the return"
                        )
                        nonlocal spawn_limit_err
                        spawn_limit_err = e

            await self._memory.with_pending_returns_remove(
                msg.call_hash, schedule_returns
            )
            if spawn_limit_err is not None:
                raise spawn_limit_err
            return

        else:
            raise ValueError("Unexpected return value from handler")

    async def loop(self, topic: str, handler: Handler) -> None:
        """Workers take jobs from the queue, one at a time, and handle them.
        They have read and write access to the store, and are responsible for
        Managing the output of tasks and scheduling new ones

        The default topic on which this brrr instance is _listening_ for new
        jobs.  It can _call_ any jobs in the given queue, but it expects to
        serve its jobs only in this specific queue.

        Every worker MUST be able to handle any job which it pulls from the
        queue.  A crucial operating principle of brrr is the lack of a central
        agent or scheduler.  Topics are how work is segregated, but within a
        topic every worker is equal and every worker must handle every job.
        Rejecting a job will be considered a job failure by the queue [in any
        decent queue implementation, e.g. SQS dead lettering after a while].

        """
        num = f"{self._n}/{Server._total_workers}"
        logger.info(f"Worker {num} listening on {topic}")
        while True:
            try:
                # This is presumed to be a long poll
                message = await self._queue.get_message(topic)
                logger.debug(f"Worker {num} got {topic} message {repr(message)}")
            except QueueIsEmpty:
                logger.debug(f"Worker {num}'s queue {topic} is empty")
                continue
            except QueueIsClosed:
                logger.info(f"Worker {num}'s queue {topic} is closed")
                return

            await self._handle_msg(handler, topic, message.body)
