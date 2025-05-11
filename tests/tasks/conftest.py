import pytest


@pytest.fixture(scope="session")
def example_path(data_path):
    return data_path / "examples"
