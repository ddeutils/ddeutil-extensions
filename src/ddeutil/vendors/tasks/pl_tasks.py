# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import logging
from typing import Any

from ddeutil.workflow.hook import tag

logger = logging.getLogger("ddeutil.workflow")

__all__: tuple[str, ...] = (
    "dummy_task_polars_dir",
    "dummy_task_polars_dir_scan",
)


@tag("polars-dir", alias="el-csv-to-parquet")
def dummy_task_polars_dir(
    source: str,
    sink: str,
    conversion: dict[str, Any] | None = None,
) -> dict[str, int]:
    logger.info("[HOOK]: el-csv-to-parquet@polars-dir")
    logger.debug("... Start EL for CSV to Parquet with Polars Engine")
    logger.debug(f"... Reading data from {source}")

    conversion: dict[str, Any] = conversion or {}
    if conversion:
        logger.debug("... Start Schema Conversion ...")
    logger.debug(f"... Writing data to {sink}")
    return {"records": 1}


@tag("polars-dir-scan", alias="el-csv-to-parquet")
def dummy_task_polars_dir_scan(
    source: str,
    sink: str,
    conversion: dict[str, Any] | None = None,
) -> dict[str, int]:
    logger.info("[HOOK]: el-csv-to-parquet@polars-dir-scan")
    logger.debug("... Start EL for CSV to Parquet with Polars Engine")
    logger.debug("... ---")
    logger.debug(f"... Reading data from {source}")

    conversion: dict[str, Any] = conversion or {}
    if conversion:
        logger.debug("... Start Schema Conversion ...")
    logger.debug(f"... Writing data to {sink}")
    return {"records": 1}
