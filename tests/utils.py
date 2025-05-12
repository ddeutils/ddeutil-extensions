import logging
import os
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

OUTSIDE_PATH: Path = Path(__file__).parent.parent
DEFAULT_TZ: str = "Asia/Bangkok"


def dotenv_setting() -> None:
    env_path: Path = OUTSIDE_PATH / ".env"

    # NOTE: Get pre-defined secret value when use GH Action.
    sftp_pass: str = os.getenv("SFTP_PASSWORD", "P@ssW0rd")

    if not env_path.exists():
        logging.warning("Dot env file does not exists")
        # NOTE: for ROOT_PATH value on the different OS:
        #   * Windows: D:\user\path\...\ddeutil-workflow
        #   * Ubuntu: /home/runner/work/ddeutil-workflow/ddeutil-workflow
        env_str: str = dedent(
            f"""
            WORKFLOW_CORE_CONF_PATH={OUTSIDE_PATH.resolve()}/tests/data
            WORKFLOW_CORE_REGISTRY_CALLER=src.ddeutil.extensions

            SFTP_HOST='50.100.200.123'
            SFTP_USER='bastion'
            SFTP_PASSWORD='{sftp_pass}'

            AWS_ACCESS_ID='dummy_access_id'
            AWS_ACCESS_SECRET_KEY='dummy_access_secret_key'
            """
        ).strip()
        env_path.write_text(env_str)
    load_dotenv(env_path)


def initial_sqlite() -> None:
    import sqlite3

    example_path: Path = OUTSIDE_PATH / "tests/data/examples"
    example_path.mkdir(parents=True, exist_ok=True)
    if not (db := (example_path / "demo_sqlite.db")).exists():
        sqlite3.connect(db, timeout=20, isolation_level=None)


def str2dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(ZoneInfo(DEFAULT_TZ))
