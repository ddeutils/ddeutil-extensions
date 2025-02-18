# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import logging
from pathlib import Path

from ddeutil.workflow.hook import tag

logger = logging.getLogger("ddeutil.workflow")

__all__: tuple[str, ...] = ("task_polars_count",)


@tag("polars", alias="count-parquet")
def task_polars_count(
    source: str,
    condition: str | None = None,
) -> dict[str, int]:
    logger.info("[HOOK]: count-parquet@polars")
    logger.debug("... Start Count Records with Polars Engine")

    source_path: Path = Path(source)
    logger.debug(f"... Reading data from {source_path!r}")

    if condition:
        logger.info(f"... Filter data with {condition!r}")

    return {"records": 1}
