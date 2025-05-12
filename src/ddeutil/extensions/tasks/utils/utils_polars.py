from typing import Optional

import polars as pl


def pipe_condition(
    lf: pl.LazyFrame,
    condition: Optional[str] = None,
) -> pl.LazyFrame:
    """Pip function for filter the LazyFrame with SQL condition statement.

    :param lf: A LazyFrame.
    :param condition: (str) A SQL condition statement.
    """
    if condition:
        return lf.sql(f"SELECT * FROM self WHERE {condition}")
    return lf
