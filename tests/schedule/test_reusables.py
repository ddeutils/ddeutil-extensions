import pytest
from ddeutil.extensions.schedule.reusables import batch


def test_batch():
    with pytest.raises(ValueError):
        next(batch(range(10), n=-1))

    assert [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]] == [
        list(i) for i in batch(range(10), n=2)
    ]
