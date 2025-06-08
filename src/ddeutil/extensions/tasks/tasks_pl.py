# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Optional
from uuid import uuid4

from ddeutil.workflow import Result, tag

from ..__types import DictData
from ..utils import Lazy
from .models import CountResult
from .utils.utils_polars import pipe_condition

if TYPE_CHECKING:
    import polars as pl
else:
    pl = Lazy("polars")

POLARS_TAG = partial(tag, name="polars")


@POLARS_TAG(alias="count-parquet")
def local_pl_count_parquet_task(
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

    df: pl.DataFrame = pl.read_parquet(source).pipe(
        pipe_condition, condition=condition
    )
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

    lf: pl.LazyFrame = pl.scan_csv(
        source,
        infer_schema=False,
    ).pipe(pipe_condition, condition=condition)
    result.trace.info(f"Display Polars DataFrame:||{lf.limit(limit).collect()}")
    record_count: int = len(lf.collect())
    result.trace.info(f"Records count: {record_count}")
    return {"records": record_count}


@POLARS_TAG(alias="count-excel")
def local_pl_count_excel_task(
    source: str,
    result: Result,
    limit: int = 5,
    sheet_name: Optional[str] = None,
    condition: Optional[str] = None,
) -> CountResult:
    """Count the target Excel file on the local.

    :param source: (str) A source path of an Excel file.
    :param result: (Result)
    :param limit: (int)
    :param sheet_name: (str)
    :param condition: (str)

    :rtype:
    """
    source_path: Path = Path(source)
    result.trace.info(
        f"Start Counting the Excel file"
        f"||=> Source Path: {source_path.resolve()}"
        f"||=> Exists or Not: {source_path.exists()}||"
    )

    lf: pl.LazyFrame = (
        pl.read_excel(
            source,
            sheet_id=sheet_name,
            engine="calamine",
            has_header=True,
            infer_schema_length=False,
            drop_empty_rows=True,
            drop_empty_cols=True,
            raise_if_empty=False,
        )
        .lazy()
        .pipe(pipe_condition, condition=condition)
    )

    result.trace.info(f"Display Polars DataFrame:||{lf.limit(limit)}")
    record_count: int = len(lf.collect())
    result.trace.info(f"Records count: {record_count}")
    return {"records": record_count}


@POLARS_TAG(alias="convert-excel-to-parquet")
def local_pl_convert_excel_to_parquet(
    source: str,
    sink: str,
    result: Result,
    audit_date: datetime,
    sheet_name: Optional[str] = None,
    conversion: Optional[str] = None,
    condition: Optional[str] = None,
    compression: str = "zstd",
    partition_by: Optional[list[str]] = None,
    limit: int = 3,
) -> DictData:
    """Covert data from Excel to Parquet file.

    :param source: (str) A source path.
    :param sink: (str) A sink path.
    :param result: (Result)
    :param audit_date: (datetime) An audit datetime.
    :param sheet_name: (str) A sheet name that want to extract data from an
        Excel file.
    :param conversion: (str) A select conversion string SQL statement.
    :param condition: (str) A condition SQL statement.
    :param compression: (str)
    :param partition_by: (list[str])
    :param limit: (int)
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError(
            "Please install pyarrow if you want to use any relate task"
        ) from None

    source_path: Path = Path(source)
    sink_path: Path = Path(sink)
    result.trace.info(
        f"Start Convert the CSV file"
        f"||=> Source Path: {source_path.resolve()}"
        f"||=> Exists or Not: {source_path.exists()}"
        f"||To Parquet file"
        f"||=> Sick Path: {sink_path.resolve()}||"
    )
    lf: pl.LazyFrame = (
        pl.read_excel(
            source,
            sheet_id=sheet_name,
            engine="calamine",
            infer_schema_length=False,
            include_file_paths="_src",
        )
        .lazy()
        .pipe(pipe_condition, condition=condition, conversion=conversion)
        .with_columns(
            # NOTE: Convert the audit date to integer for partitioning.
            (
                pl.lit(audit_date.strftime("%Y%m%d"))
                .str.to_integer()
                .alias("audit_date")
            ),
            # NOTE: Sprit only filename from path.
            (pl.col("_src").str.split("/").list.last().alias("_src")),
        )
    )

    result.trace.info(f"Display Polars DataFrame:||{lf.limit(limit).collect()}")
    row_records: int = len(lf.collect())
    result.trace.info(f"Start Sink Data with {row_records} records.")

    # TODO: Change to use native polars that support write parquet with folder
    #   refs issue: https://github.com/pola-rs/polars/issues/21899
    # WARNING: This case do not work when we move to use `use_pyarrow` flag on
    #   native Polars.
    # ---
    # (
    #     lf
    #     .collect()
    #     .write_parquet(
    #         file=sink_path,
    #         compression="zstd",
    #         use_pyarrow=True,
    #         row_group_size=1000000,
    #         pyarrow_options={
    #             "partition_cols": partition_by or [],
    #         }
    #     )
    # )

    pq.write_to_dataset(
        table=lf.collect().to_arrow(),
        root_path=sink,
        compression=compression,
        partition_cols=partition_by or [],
        basename_template=f"{uuid4().hex}-{{i}}.{compression}.parquet",
        # NOTE: Write as `overwrite` mode.
        existing_data_behavior="delete_matching",
    )
    return {
        "sick": sink,
        "records": row_records,
        # NOTE: Extract OrderedDict to dict.
        "schema": {**lf.collect_schema()},
    }


@POLARS_TAG(alias="convert-csv-to-parquet")
def local_pl_convert_csv_to_parquet(
    source: str,
    sink: str,
    result: Result,
    audit_date: datetime,
    conversion: Optional[str] = None,
    condition: Optional[str] = None,
    compression: str = "zstd",
    partition_by: Optional[list[str]] = None,
    limit: int = 3,
) -> DictData:
    """Covert data from CSV to Parquet file.

    :param source: (str) A source path.
    :param sink: (str) A sink path.
    :param result: (Result)
    :param audit_date: (datetime) An audit datetime.
    :param conversion: (str) A select conversion string SQL statement.
    :param condition: (str) A condition SQL statement.
    :param compression: (str)
    :param partition_by: (list[str])
    :param limit: (int)
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError(
            "Please install pyarrow if you want to use any relate task"
        ) from None

    source_path: Path = Path(source)
    sink_path: Path = Path(sink)
    result.trace.info(
        f"Start Convert the CSV file"
        f"||=> Source Path: {source_path.resolve()}"
        f"||=> Exists or Not: {source_path.exists()}"
        f"||To Parquet file"
        f"||=> Sick Path: {sink_path.resolve()}||"
    )
    lf: pl.LazyFrame = (
        pl.scan_csv(
            source,
            has_header=True,
            include_file_paths="_src",
        )
        .pipe(
            pipe_condition,
            condition=condition,
            conversion=conversion,
        )
        .with_columns(
            # NOTE: Convert the audit date to integer for partitioning.
            (
                pl.lit(audit_date.strftime("%Y%m%d"))
                .str.to_integer()
                .alias("audit_date")
            ),
            # NOTE: Sprit only filename from path.
            (pl.col("_src").str.split("/").list.last().alias("_src")),
        )
    )

    result.trace.info(f"Display Polars DataFrame:||{lf.limit(limit).collect()}")
    row_records: int = len(lf.collect())
    result.trace.info(f"Start Sink Data with {row_records} records.")
    pq.write_to_dataset(
        table=lf.collect().to_arrow(),
        root_path=sink,
        compression=compression,
        partition_cols=partition_by or [],
        basename_template=f"{uuid4().hex}-{{i}}.{compression}.parquet",
        # NOTE: Write as `overwrite` mode.
        existing_data_behavior="delete_matching",
    )
    return {
        "records": len(lf.collect()),
        "schema": {**lf.collect_schema()},
    }
