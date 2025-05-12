# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

try:
    import polars as pl
except ImportError:
    raise ImportError(
        "Please install polars if you want to use any relate task"
    ) from None

try:
    import pyarrow.parquet as pq

    # NOTE:
    # import pyarrow as pa
except ImportError:
    raise ImportError(
        "Please install pyarrow if you want to use any relate task"
    ) from None

from ddeutil.workflow import Result, tag

from ..__types import DictData
from .models import CountResult

POLARS_TAG = partial(tag, name="polars")


@POLARS_TAG(alias="count-parquet")
def local_count_parquet_task(
    source: str,
    result: Result,
    limit: int = 5,
    condition: Optional[str] = None,
) -> CountResult:
    """Count the target Parquet file on the local.

    :param source: (str) A source path of a parquet file.
    :param result: (Result)
    :param limit: (int)
    :param condition: (str) A SQL condition statement that will use before
        counting record.

    :rtype:
    """
    source_path: Path = Path(source)
    result.trace.info(
        f"Start Counting the CSV file"
        f"||=> Source Path: {source_path.resolve()}"
        f"||=> Exists or Not: {source_path.exists()}||"
    )

    df: pl.DataFrame = pl.read_parquet(source)

    if condition:
        df: pl.DataFrame = df.sql(f"SELECT * FROM self WHERE {condition}")

    result.trace.info(f"Display Polars DataFrame:||{df.limit(limit)}")
    record_count: int = len(df)
    result.trace.info(f"Records count: {record_count}")
    return {"records": 1}


@POLARS_TAG(alias="count-csv")
def local_count_csv_task(
    source: str,
    result: Result,
    limit: int = 5,
    condition: Optional[str] = None,
) -> CountResult:
    """Count the target CSV file on the local.

    :param source: (str) A source path of a csv file.
    :param result: (Result)
    :param limit: (int)
    :param condition: (str)

    :rtype:
    """
    source_path: Path = Path(source)
    result.trace.info(
        f"Start Counting the CSV file"
        f"||=> Source Path: {source_path.resolve()}"
        f"||=> Exists or Not: {source_path.exists()}||"
    )

    df: pl.DataFrame = pl.read_csv(source)

    if condition:
        df: pl.DataFrame = df.sql(f"SELECT * FROM self WHERE {condition}")

    result.trace.info(f"Display Polars DataFrame:||{df.limit(limit)}")
    record_count: int = len(df)
    result.trace.info(f"Records count: {record_count}")
    return {"records": record_count}


@POLARS_TAG(alias="count-xlsx")
def local_count_xlsx_task(
    source: str,
    result: Result,
    limit: int = 5,
    sheet_name: Optional[str] = None,
    condition: Optional[str] = None,
) -> CountResult:
    """Count the target XLSX file on the local.

    :param source: (str) A source path of a xlsx file.
    :param result: (Result)
    :param limit: (int)
    :param sheet_name: (str)
    :param condition: (str)

    :rtype:
    """
    source_path: Path = Path(source)
    result.trace.info(
        f"Start Counting the XLSX file"
        f"||=> Source Path: {source_path.resolve()}"
        f"||=> Exists or Not: {source_path.exists()}||"
    )

    df: pl.DataFrame = pl.read_excel(
        source,
        sheet_id=sheet_name,
        engine="calamine",
        has_header=True,
        drop_empty_rows=True,
        drop_empty_cols=True,
        raise_if_empty=False,
    )

    if condition:
        df: pl.DataFrame = df.sql(f"SELECT * FROM self WHERE {condition}")

    result.trace.info(f"Display Polars DataFrame:||{df.limit(limit)}")
    record_count: int = len(df)
    result.trace.info(f"Records count: {record_count}")
    return {"records": record_count}


def polars_dtype() -> dict[str, Any]:
    """Return mapping of the Polars datatype and Python variable type."""
    return {
        "str": pl.Utf8,
        "int": pl.Int32,
    }


@POLARS_TAG(alias="convert-csv-to-parquet")
def local_convert_csv_to_parquet(
    source: str,
    sink: str,
    result: Result,
    conversion: Optional[dict[str, Any]] = None,
) -> DictData:
    """Covert data from CSV to Parquet file.

    :param source:
    :param sink:
    :param result:
    :param conversion:
    """
    source_path: Path = Path(source)
    sink_path: Path = Path(sink)
    result.trace.info(
        f"Start Convert the CSV file"
        f"||=> Source Path: {source_path.resolve()}"
        f"||=> Exists or Not: {source_path.exists()}"
        f"||To Parquet file"
        f"||=> Sick Path: {sink_path.resolve()}"
    )

    lf: pl.LazyFrame = pl.scan_csv(source)

    # STEP 02: Schema conversion on Polars DataFrame.
    conversion: dict[str, Any] = conversion or {}
    if conversion:
        lf = lf.with_columns(
            *[pl.col(c).cast(col.type).alias(col.name) for c, col in conversion]
        )

    pq.write_to_dataset(
        table=lf.collect().to_arrow(),
        root_path=sink,
        compression="snappy",
        basename_template=f"{uuid4().hex}-{{i}}.snappy.parquet",
    )
    return {"records": len(lf.collect())}
