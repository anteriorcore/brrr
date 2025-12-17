import brrr
import pytest
from brrr import OnlyInBrrrError
from brrr.local_app import LocalBrrr
from brrr.pickle_codec import PickleCodec


async def test_only_no_brrr() -> None:
    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        return a * 2

    with pytest.raises(OnlyInBrrrError):
        await foo(3)


async def test_only_in_brrr(topic: str, task_name: str) -> None:
    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        return a * 2

    b = LocalBrrr(
        topic=topic, handlers={task_name: foo}, codec=PickleCodec(), context=None
    )
    assert await b.run(foo)(5) == 10


async def test_only_in_fake_brrr() -> None:
    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        return a * 2

    with brrr.allow_only():
        assert await foo(7) == 14
