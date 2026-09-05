import logging

import pytest


def pytest_configure(config):
    sub_logger = logging.getLogger("brrr")
    sub_logger.setLevel(logging.DEBUG)


@pytest.fixture(params=("brrr-test", "'/:/\"~`\\", "🇰🇳"))
def topic(request: pytest.FixtureRequest) -> str:
    assert isinstance(request.param, str)
    return request.param


@pytest.fixture(params=("task", "`'\"\\/~$!@:", "🏭"))
def task_name(request: pytest.FixtureRequest) -> str:
    assert isinstance(request.param, str)
    return request.param
