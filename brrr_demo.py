#!/usr/bin/env python3

import ast
import asyncio
import hashlib
import json
import logging
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from pprint import pprint
from typing import Any, Iterable

import aioboto3
import brrr
import redis.asyncio as redis
from aiohttp import web
from brrr import ActiveWorker, AppWorker, NotFoundError
from brrr.backends.dynamo import DynamoDbMemStore
from brrr.backends.redis import RedisQueue
from brrr.call import Call
from brrr.codec import Codec
from types_aiobotocore_dynamodb import DynamoDBClient

logger = logging.getLogger(__name__)
routes = web.RouteTableDef()

brrr_app: ContextVar[AppWorker] = ContextVar("brrr_demo.app")

topic_py = "brrr-py-demo"
topic_ts = "brrr-ts-demo"


### Brrr handlers


async def hello(app: ActiveWorker, greetee: str):
    greeting = f"Hello, {greetee}!"
    print(greeting, flush=True)
    return greeting


async def calc_and_print(app: ActiveWorker, op: str, n: str, salt=None):
    result = await app.call(op, topic=topic_ts)(n=int(n), salt=salt)
    print(f"{op}({n}) = {result}", flush=True)
    return result


class JsonKwargsCodec(Codec):
    def encode_call(self, task_name: str, args: tuple, kwargs: dict) -> Call:
        if args:
            raise ValueError("This codec only supports keyword arguments")
        payload = self._json_bytes([kwargs])
        call_hash = self._hash_call(task_name, kwargs)
        return Call(task_name=task_name, payload=payload, call_hash=call_hash)

    async def invoke_task(self, call: Call, task) -> bytes:
        [kwargs] = json.loads(call.payload.decode())
        result = await task(**kwargs)
        return self._json_bytes(result)

    def decode_return(self, task_name: str, payload: bytes) -> Any:
        return json.loads(payload.decode())

    @classmethod
    def _json_bytes(cls, value) -> bytes:
        return json.dumps(value, sort_keys=True).encode()

    @classmethod
    def _hash_call(cls, task_name: str, kwargs: dict) -> str:
        data = [task_name, [kwargs]]
        h = hashlib.new("sha256")
        h.update(cls._json_bytes(data))
        return h.hexdigest()


### Brrr setup


def table_name() -> str:
    """
    Get table name from environment
    """
    return os.environ.get("DYNAMODB_TABLE_NAME", "brrr")


# ... where is the python contextmanager monad?


@asynccontextmanager
async def with_redis(
    redurl: str | None = os.environ.get("BRRR_DEMO_REDIS_URL"),
) -> AsyncIterator[redis.Redis]:
    rkwargs = dict(
        health_check_interval=10,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        socket_keepalive=True,
        protocol=3,
    )
    if redurl is None:
        rc = redis.Redis(**rkwargs)
    else:
        rc = redis.from_url(redurl, **rkwargs)
    await rc.ping()
    try:
        yield rc
    finally:
        await rc.aclose()


@asynccontextmanager
async def with_resources() -> AsyncIterator[tuple[redis.Redis, DynamoDBClient]]:
    async with with_redis() as rc:
        dync: DynamoDBClient
        async with aioboto3.Session().client("dynamodb") as dync:
            yield (rc, dync)


@asynccontextmanager
async def with_brrr_resources() -> AsyncIterator[tuple[RedisQueue, DynamoDbMemStore]]:
    async with with_resources() as (rc, dync):
        store = DynamoDbMemStore(dync, table_name())
        queue = RedisQueue(rc)
        yield (queue, store)


@asynccontextmanager
async def with_brrr(
    reset_backends,
) -> AsyncIterator[tuple[brrr.Server, brrr.AppWorker]]:
    async with with_brrr_resources() as (redis, dynamo):
        if reset_backends:
            await redis.setup()
            await dynamo.create_table()
        async with brrr.serve(redis, dynamo, redis) as conn:
            app = AppWorker(
                handlers=dict(
                    hello=hello,
                    calc_and_print=calc_and_print,
                ),
                codec=JsonKwargsCodec(),
                connection=conn,
            )
            token = brrr_app.set(app)
            try:
                yield (conn, app)
            finally:
                brrr_app.reset(token)


### Demo HTTP API implementation


def response(status: int, content: dict):
    return web.Response(status=status, text=json.dumps(content))


@routes.get("/{task_name}")
async def get_task_result(request: web.BaseRequest):
    # aiohttp uses a multidict but we don’t need that for this demo.
    kwargs = dict(request.query)

    task_name = request.match_info["task_name"]
    if task_name not in brrr_app.get().tasks:
        return response(404, {"error": "No such task"})

    try:
        result = await brrr_app.get().read(task_name)(**kwargs)
    except NotFoundError:
        return response(404, dict(error="No result for this task"))
    return response(200, dict(status="ok", result=result))


@routes.post("/{task_name}")
async def schedule_task(request: web.BaseRequest):
    kwargs = dict(request.query)

    task_name = request.match_info["task_name"]
    if task_name not in brrr_app.get().tasks:
        return response(404, {"error": "No such task"})

    await brrr_app.get().schedule(task_name, topic=topic_py)(**kwargs)
    return response(202, {"status": "accepted"})


### Demo CLI

cmds = {}


def cmd(f):
    cmds[f.__name__] = f
    return f


@cmd
async def brrr_worker():
    async with with_brrr(False) as (conn, app):
        await conn.loop(topic_py, app.handle)


@cmd
async def web_server():
    bind_addr = os.environ.get("BRRR_DEMO_LISTEN_HOST", "127.0.0.1")
    bind_port = int(os.environ.get("BRRR_DEMO_LISTEN_PORT", "8080"))
    async with with_brrr(True):
        app = web.Application()
        app.add_routes(routes)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, bind_addr, bind_port)
        await site.start()
        logger.info(f"Listening on http://{bind_addr}:{bind_port}")
        await asyncio.Event().wait()


def args2dict(args: Iterable[str]) -> dict[str, Any]:
    """
    Extremely rudimentary arbitrary argparser.

    args2dict(["--foo", "bar", "--zim", "zom"])
    => {"foo": "bar", "zim": "zom"}

    """
    it = iter(args)

    # Best effort eval()
    def maybeval(x: str):
        try:
            return ast.literal_eval(x)
        except ValueError:
            return x

    return {k.lstrip("-"): maybeval(v) for k, v in zip(it, it)}


@cmd
async def schedule(topic: str, job: str, *args: str):
    """
    Put a single job onto the queue
    """
    async with with_brrr(False) as (_, app):
        await app.schedule(job, topic=topic)(**args2dict(args))


@cmd
async def monitor():
    async with with_brrr_resources() as (queue, _):
        while True:
            pprint(await queue.get_info(topic_py))
            await asyncio.sleep(1)


@cmd
async def reset():
    async with with_resources() as (rc, dync):
        try:
            await dync.delete_table(TableName=table_name())
        except Exception as e:
            # Table does not exist
            if "ResourceNotFoundException" not in str(e):
                raise

        await rc.flushall()


async def amain():
    # To log _all_ messages at DEBGUG level (very noisy)
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)
    # To log all brrr messages at DEBUG level (quite noisy)
    # logging.getLogger('brrr').setLevel(logging.DEBUG)
    f = cmds.get(sys.argv[1]) if len(sys.argv) > 1 else None
    if f:
        await f(*sys.argv[2:])
    else:
        print(f"Usage: brrr_demo.py <{' | '.join(cmds.keys())}>")
        sys.exit(1)


def main():
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
