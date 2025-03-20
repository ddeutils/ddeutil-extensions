import logging
from typing import Optional

try:
    import pandas as pd

    logging.debug(f"Polars version: {pd.__version__}")
except ImportError:
    raise ImportError(
        "Please install polars if you want to use any relate task"
    ) from None

from ddeutil.workflow.caller import tag

logger = logging.getLogger("ddeutil.workflow")


@tag("pandas", alias="xlsx-to-fabric")
def task_pandas_excel_to_delta(
    source: str,
    target: str,
    token: str,
    header: int = 1,
    usecols: Optional[str] = None,
    sheet_name: Optional[str] = None,
):
    from deltalake import write_deltalake

    df = pd.read_excel(
        # NOTE: dest_path/file.xlsx
        source,
        header=header,
        usecols=usecols,
        sheet_name=sheet_name,
    )

    storage_options = {
        "use_fabric_endpoint": "true",
        "allow_unsafe_rename": "true",
        "bearer_token": token,
    }

    write_deltalake(
        target,
        df,
        mode="overwrite",
        engine="rust",
        storage_options=storage_options,
    )
    return {"records": 1}
