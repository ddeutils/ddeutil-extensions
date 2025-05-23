import pytest


@pytest.fixture(scope="session")
def example_path(data_path):
    return data_path / "examples"


@pytest.fixture(scope="session")
def target_path(data_path):
    return data_path / "target"
