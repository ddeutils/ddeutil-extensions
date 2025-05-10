import logging
from functools import partial
from typing import Optional

try:
    import pandas as pd

    logging.debug(f"Pandas version: {pd.__version__}")
except ImportError:
    raise ImportError(
        "Please install pandas if you want to use any relate task"
    ) from None

from ddeutil.workflow import Result, config
from ddeutil.workflow.reusables import tag

from ..__types import DictData

logger = logging.getLogger("ddeutil.workflow")
PANDAS_TAG = partial(tag, name="pandas")


@PANDAS_TAG(alias="xlsx-to-fabric-with-deltalake")
def task_pandas_excel_to_fabric_with_deltalake(
    source: str,
    target: str,
    token: str,
    result: Result,
    header: int = 1,
    usecols: Optional[str] = None,
    sheet_name: Optional[str] = None,
):
    """Task for loading excel data from the local storage and save to deltalake
    format on the target Microsoft Fabric.
    """

    result.trace.debug(
        "... [CALLER]: Loading xlsx file via Pandas Dataframe API"
    )

    df: pd.DataFrame = pd.read_excel(
        # NOTE: dest_path/file.xlsx
        config.root_path / source,
        header=header,
        usecols=usecols,
        sheet_name=sheet_name,
    )

    print(df)

    storage_options = {
        "use_fabric_endpoint": "true",
        "allow_unsafe_rename": "true",
        "bearer_token": token,
    }

    print(storage_options)

    print(target)
    # write_deltalake(
    #     target,
    #     df,
    #     mode="overwrite",
    #     engine="rust",
    #     storage_options=storage_options,
    # )
    return {"records": 1}


@PANDAS_TAG(alias="fabric-describe")
def task_pandas_fabric_describe(
    source: str, token: str, result: Result
) -> DictData:
    from deltalake import DeltaTable

    result.trace.info("Start Describe Microsoft Fabric.")
    table_path: str = f"abfss://xxxxx/flats.Lakehouse/Tables/{source}"
    storage_options = {"bearer_token": token, "use_fabric_endpoint": "true"}
    dt = DeltaTable(table_path, storage_options=storage_options)

    limited_data: pd.DataFrame = dt.to_pyarrow_dataset().head(1000).to_pandas()
    print(limited_data.describe())

    return {"records": 1}
