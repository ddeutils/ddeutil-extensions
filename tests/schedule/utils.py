from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Any, Union

import yaml


@contextmanager
def dump_yaml_context(
    filename: Union[str, Path], data: Union[dict[str, Any], str]
) -> Iterator[Path]:  # pragma: no cov
    """Dump the context data to the target yaml file.

    :param filename: (str | Path) A file path or filename of a YAML config.
    :param data: A YAML data context that want to write to the target file path.
    """
    test_file: Path = Path(filename) if isinstance(filename, str) else filename
    with test_file.open(mode="w") as f:
        if isinstance(data, str):
            f.write(dedent(data.strip("\n")))
        else:
            yaml.dump(data, f)

    yield test_file

    # NOTE: Remove the testing file.
    test_file.unlink(missing_ok=True)
