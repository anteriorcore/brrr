from collections.abc import Sequence


def names(base_name: str, names: Sequence[str]) -> list[str]:
    return [f"{base_name}{name}" for name in names]
