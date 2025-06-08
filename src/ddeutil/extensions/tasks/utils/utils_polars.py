from typing import Optional, TypedDict

import polars as pl


def pipe_condition(
    lf: pl.LazyFrame,
    *,
    condition: Optional[str] = None,
    conversion: Optional[str] = None,
) -> pl.LazyFrame:
    """Pipe function for filter the LazyFrame with SQL condition statement.

    :param lf: (pl.LazyFrame) A LazyFrame.
    :param condition: (str) A SQL condition statement.
    :param conversion: (str)

    :rtype: pl.LazyFrame
    """
    conversion: str = (conversion + ", _src") if conversion else "*"
    if condition:
        return lf.sql(f"SELECT {conversion} FROM self WHERE {condition}")
    return lf.sql(f"SELECT {conversion} FROM self")


class Column(TypedDict):
    type: str
    name: str


def pipe_type_convert(
    lf: pl.LazyFrame,
    schema: dict[str, Column],
) -> pl.LazyFrame:
    """Pipe function for convert data type and name conversion.

    :param lf: (pl.LazyFrame)
    :param schema: (dict[str, Column])
    """
    return lf.with_columns(
        (pl.col(c).cast(col["type"]).alias(col["name"]) for c, col in schema)
    )
