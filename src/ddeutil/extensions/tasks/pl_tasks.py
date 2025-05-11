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
    # import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError(
        "Please install pyarrow if you want to use any relate task"
    ) from None

from ddeutil.workflow import Result
from ddeutil.workflow.reusables import tag

from ..__types import TupleStr
from ..datasets.pl import PolarsCsv, PolarsParq

__all__: TupleStr = (
    "local_count_parquet_task",
    "local_count_csv_task",
    "local_convert_csv_to_parquet",
)


POLARS_TAG = partial(tag, name="polars")


@POLARS_TAG(alias="count-parquet")
def local_count_parquet_task(
    source: str,
    result: Result,
    condition: Optional[str] = None,
) -> dict[str, int]:
    """"""
    result.trace.info("[CALLER]: count-parquet@polars")
    result.trace.debug("... Start Count Records with Polars Engine")

    source_path: Path = Path(source)
    result.trace.debug(f"... Reading data from {source_path!r}")

    if condition:
        result.trace.info(f"... Filter data with {condition!r}")

    return {"records": 1}


@POLARS_TAG(alias="count-csv")
def local_count_csv_task(
    source: str,
    result: Result,
    limit: int = 5,
):
    """Count the target CSV file on the local.

    :param source:
    :param result: (Result)
    :param limit: (int)

    :rtype:
    """
    source_path: Path = Path(source)
    result.trace.info(
        f"Start Counting the CSV file||=> Source Path: {source_path.resolve()}"
        f"||=> Exists or Not: {source_path.exists()}"
    )

    df: pl.DataFrame = pl.read_csv(source)
    result.trace.info(f"Display: {df.limit(limit)}")
    return {"records": len(df)}


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
    conversion: Optional[dict[str, Any]] = None,
) -> dict[str, int]:
    """Covert data from CSV to Parquet file.

    :param source:
    :param sink:
    :param conversion:
    """
    print("Start EL for CSV to Parquet with Polars Engine")
    print("---")
    # STEP 01: Read the source data to Polars.
    src_dataset: PolarsCsv = PolarsCsv.from_loader(name=source, externals={})
    src_df: pl.DataFrame = src_dataset.load()
    print(src_df)

    # STEP 02: Schema conversion on Polars DataFrame.
    conversion: dict[str, Any] = conversion or {}
    if conversion:
        src_df = src_df.with_columns(
            *[pl.col(c).cast(col.type).alias(col.name) for c, col in conversion]
        )
        print("Start Schema Conversion ...")

    # STEP 03: Write data to parquet file format.
    sink = PolarsParq.from_loader(name=sink, externals={})
    pq.write_to_dataset(
        table=src_df.to_arrow(),
        root_path=f"{sink.conn.endpoint}/{sink.object}",
        compression="snappy",
        basename_template=f"{sink.object}-{uuid4().hex}-{{i}}.snappy.parquet",
    )
    return {"records": src_df.select(pl.len()).item()}


@tag("polars-dir-scan", alias="el-csv-to-parquet")
def csv_to_parquet_dir_scan(
    source: str,
    sink: str,
    conversion: Optional[dict[str, Any]] = None,
) -> dict[str, int]:
    print("Start EL for CSV to Parquet with Polars Engine")
    print("---")
    # STEP 01: Read the source data to Polars.
    src_dataset: PolarsCsv = PolarsCsv.from_loader(name=source, externals={})
    src_df: pl.LazyFrame = src_dataset.scan()

    if conversion:
        ...

    sink = PolarsParq.from_loader(name=sink, externals={})
    pq.write_to_dataset(
        table=src_df.collect().to_arrow(),
        root_path=f"{sink.conn.endpoint}/{sink.object}",
        compression="snappy",
        basename_template=f"{sink.object}-{uuid4().hex}-{{i}}.snappy.parquet",
    )
    return {"records": src_df.select(pl.len()).collect().item()}
