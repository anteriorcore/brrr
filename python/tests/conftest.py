import pytest


@pytest.fixture(params=("brrr-test", "'/:/\"~`\\", "🇰🇳"))
def topic(request: pytest.FixtureRequest) -> str:
    assert isinstance(request.param, str)
    return request.param


@pytest.fixture(params=("task", "`'\"\\/~$!@:", "🏭"))
def task_name(request: pytest.FixtureRequest) -> str:
    assert isinstance(request.param, str)
    return request.param
