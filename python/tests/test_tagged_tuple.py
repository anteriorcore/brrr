from dataclasses import dataclass

from brrr.tagged_tuple import TaggedTupleStrings, optional_field


@dataclass(frozen=True)
class ExampleTT(TaggedTupleStrings):
    tag = 10
    id: str
    attr: str
    optional_attr: int | None = optional_field()


def test_astuple() -> None:
    tt_with_optional = ExampleTT(
        id='id',
        attr='attr',
        optional_attr=5,
    )
    assert tt_with_optional.astuple() == (10, 'id', 'attr', (5,))
    tt_without_optional = ExampleTT(
        id='id',
        attr='attr',
        optional_attr=None,
    )
    assert tt_without_optional.astuple() == (10, 'id', 'attr', ())


def test_from_tuple() -> None:
    tt_with_optional = (10, "id", "attr", (5,))
    assert ExampleTT.fromtuple(tt_with_optional) == ExampleTT("id", "attr", 5)
    tt_without_optional = (10, "id", "attr", ())
    assert ExampleTT.fromtuple(tt_without_optional) == ExampleTT("id", "attr", None)


def test_encode_decode_roundtrip() -> None:
    for tt in [
        ExampleTT("id", "attr", 5),
        ExampleTT("id", "attr", None),
    ]:
        assert ExampleTT.decode(tt.encode()) == tt
