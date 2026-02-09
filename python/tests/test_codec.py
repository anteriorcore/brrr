import dataclasses
import pickle
from collections import Counter
from typing import Any
from unittest.mock import Mock, call

from brrr.call import Call
from brrr.demo_pickle_codec import DemoPickleCodec, DemoPickleCodecContext
from brrr.local_app import LocalBrrr

TOPIC = "test"

type TestContext = DemoPickleCodecContext


async def test_codec_key_no_args() -> None:
    calls = Counter[str]()
    codec = DemoPickleCodec()

    old = codec.encode_call

    def encode_call(task_name: str, args: Any, kwargs: Any) -> Call:
        call = old(task_name, args, kwargs)
        bare_call = old(task_name, (), {})
        return dataclasses.replace(call, call_hash=bare_call.call_hash)

    codec.encode_call = Mock(side_effect=encode_call)  # type: ignore[method-assign]

    async def same(app: TestContext, a: int) -> int:
        assert a == 1
        calls[f"same({a})"] += 1
        return a

    async def foo(app: TestContext, a: int) -> int:
        calls[f"foo({a})"] += 1

        val = 0
        # Call in deterministic order for the test’s sake
        for i in range(1, a + 1):
            val += await app.call(same)(i)

        assert val == a
        return val

    b = LocalBrrr(topic=TOPIC, handlers=dict(foo=foo, same=same), codec=codec)
    await b.run(foo)(50)

    assert calls == Counter(
        {
            "same(1)": 1,
            "foo(50)": 2,
        }
    )
    codec.encode_call.assert_called()


async def test_codec_determinstic() -> None:
    call1 = DemoPickleCodec().encode_call("foo", (1, 2), dict(b=4, a=3))
    call2 = DemoPickleCodec().encode_call("foo", (1, 2), dict(a=3, b=4))
    assert call1.call_hash == call2.call_hash


async def test_codec_api() -> None:
    codec = Mock(wraps=DemoPickleCodec())

    async def plus(app: TestContext, x: int, y: str) -> int:
        return x + int(y)

    async def foo(app: TestContext) -> int:
        val = (
            await app.call(plus)(1, "2")
            + await app.call(plus)(x=3, y="4")
            + await app.call(plus)(*(5, "6"))
            + await app.call(plus)(**dict(x=7, y="8"))  # type: ignore[arg-type]
        )
        assert val == sum(range(9))
        return val

    b = LocalBrrr[TestContext](
        topic=TOPIC, handlers=dict(foo=foo, plus=plus), codec=codec
    )
    await b.run("foo")()

    codec.encode_call.assert_has_calls(
        [
            call("foo", (), {}),
            call("plus", (1, "2"), {}),
            call("plus", (), {"x": 3, "y": "4"}),
            call("plus", (5, "6"), {}),
            call("plus", (), {"x": 7, "y": "8"}),
        ],
        any_order=True,
    )

    # The Call argument’s task_name to invoke_task is easiest to test.
    assert Counter(foo=5, plus=4) == Counter(
        map(lambda c: c[0][0].task_name, codec.invoke_task.call_args_list)
    )

    for c in codec.decode_return.call_args_list:
        ret = c[0][0], pickle.loads(c[0][1])
        # I don’t want to hard-code too much of the implementation in the test
        assert ret in {
            ("plus", 3),
            ("plus", 7),
            ("plus", 11),
            ("plus", 15),
            ("foo", sum(range(9))),
        }
