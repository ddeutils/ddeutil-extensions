# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

from ddeutil.workflow import Result, tag

from ..__types import DictData
from ..utils import Lazy

if TYPE_CHECKING:
    import deltalake as dl
    import pandas as pd
else:
    pd = Lazy("pandas")
    dl = Lazy("deltalake")

PANDAS_TAG = partial(tag, name="pandas")


@PANDAS_TAG(alias="convert-excel-to-fabric")
def local_pd_convert_excel_to_deltalake(
    source: str,
    sink: str,
    token: str,
    result: Result,
    header: int = 1,
    sheet_name: Optional[Union[str, int]] = 0,
    limit: int = 3,
) -> DictData:
    """Covert data from Excel to Microsoft Fabric.

    :param source: (str) A source path.
    :param sink: (str) A sink path.
    :param token: (str)
    :param result: (Result)
    :param header:
    :param sheet_name:
    :param limit: (int)
    """
    source_path: Path = Path(source)
    result.trace.debug(
        "... [CALLER]: Loading xlsx file via Pandas Dataframe API"
    )

    df: pd.DataFrame = pd.read_excel(
        source_path,
        header=header,
        sheet_name=sheet_name,
        engine="calamine",
    )
    result.trace.info(f"Display Pandas DataFrame:||{df.head(limit)}")
    row_records: int = len(df)
    dl.write_deltalake(
        sink,
        data=df,
        mode="overwrite",
        storage_options={
            "use_fabric_endpoint": "true",
            "allow_unsafe_rename": "true",
            "bearer_token": token,
        },
    )
    return {"sink": sink, "records": row_records}


@PANDAS_TAG(alias="describe-fabric")
def fabric_pd_describe(
    source: str,
    token: str,
    result: Result,
) -> DictData:
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise ImportError(
            "Task `describe-fabric` need to install `deltalake` "
            "package before execution."
        ) from None

    result.trace.info("Start Describe Microsoft Fabric.")
    dt: DeltaTable = DeltaTable(
        f"abfss://<host>/flats.Lakehouse/Tables/{source}",
        storage_options={
            "bearer_token": token,
            "use_fabric_endpoint": "true",
        },
    )
    limited_data: pd.DataFrame = dt.to_pyarrow_dataset().head(1000).to_pandas()
    result.trace.info(f"Describe:||{limited_data.describe()}||")
    return {"source": source}
