from collections.abc import Iterator
from itertools import chain, islice
from typing import Any, Union


def batch(iterable: Union[Iterator[Any], range], n: int) -> Iterator[Any]:
    """Batch data into iterators of length n. The last batch may be shorter.

    Example:
        >>> for b in batch(iter('ABCDEFG'), 3):
        ...     print(list(b))
        ['A', 'B', 'C']
        ['D', 'E', 'F']
        ['G']

    :param iterable:
    :param n: (int) A number of returning batch size.

    :rtype: Iterator[Any]
    """
    if n < 1:
        raise ValueError("n must be at least one")

    it: Iterator[Any] = iter(iterable)
    while True:
        chunk_it = islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield chain((first_el,), chunk_it)
