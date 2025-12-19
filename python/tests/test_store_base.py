import functools

import pytest
from brrr.backends.in_memory import InMemoryByteStore
from brrr.store import (
    CompareMismatch,
    MemKey,
    Memory,
    PendingReturns,
)
from brrr.tagged_tuple import PendingReturn


class FlakyStore(InMemoryByteStore):
    # fail every other CAS operation
    fail_cas: bool

    def __init__(self) -> None:
        super().__init__()
        self.fail_cas = True

    async def set_new_value(self, key: MemKey, value: bytes) -> None:
        self.fail_cas = not self.fail_cas
        if not self.fail_cas:
            raise CompareMismatch()
        return await super().set_new_value(key, value)

    async def compare_and_set(self, key: MemKey, value: bytes, expected: bytes) -> None:
        self.fail_cas = not self.fail_cas
        if not self.fail_cas:
            raise CompareMismatch()
        return await super().compare_and_set(key, value, expected)

    async def compare_and_delete(self, key: MemKey, expected: bytes) -> None:
        self.fail_cas = not self.fail_cas
        if not self.fail_cas:
            raise CompareMismatch()
        return await super().compare_and_delete(key, expected)


async def test_memory_cas() -> None:
    store = FlakyStore()
    memory = Memory(store)
    key = MemKey("value", "bar")
    # Testing a private method is technically a bit of an anti pattern.  Tbh I
    # don’t think the API for the store is correct to begin with and we should
    # probably just remove it entirely.  This primitive though seems broken and
    # I need to test it now without rewriting the entire store API.
    await memory._with_cas(functools.partial(store.set_new_value, key, b"123"))
    assert b"123" == await store.get(key)
    await memory._with_cas(
        functools.partial(store.compare_and_set, key, b"999", b"123")
    )
    assert b"999" == await store.get(key)


@pytest.mark.parametrize(
    "scheduled_at,returns",
    [
        (1000, {"a", "b", "c"}),
        (None, {"x", "y"}),
        (2000, {"single"}),
        (None, set()),
    ],
)
async def test_encode_decode_mixed_cases(scheduled_at: int, returns: set[str]) -> None:
    def make_pr(tag: str) -> PendingReturn:
        return PendingReturn(root_id=tag, call_hash=tag, topic=tag)

    pending_returns = PendingReturns(
        scheduled_at=scheduled_at, returns=set(map(make_pr, returns))
    )
    encoded = pending_returns.encode()
    decoded = PendingReturns.decode(encoded)
    assert decoded == pending_returns
    assert decoded.scheduled_at == pending_returns.scheduled_at
    assert decoded.returns == pending_returns.returns
