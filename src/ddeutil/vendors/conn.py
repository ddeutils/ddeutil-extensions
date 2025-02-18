# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any, Literal, Optional, TypeVar

from ddeutil.workflow import Loader
from pydantic import BaseModel, ConfigDict, Field
from pydantic.functional_validators import field_validator
from pydantic.types import SecretStr
from typing_extensions import Self

from .__types import DictData, TupleStr
from .models.conn import Conn as ConnModel

EXCLUDED_EXTRAS: TupleStr = (
    "type",
    "url",
)


class BaseConn(BaseModel):
    """Base Conn (Connection) Model"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # NOTE: This is fields
    dialect: str
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    pwd: Optional[SecretStr] = None
    endpoint: str
    extras: Annotated[
        DictData,
        Field(default_factory=dict, description="Extras mapping of parameters"),
    ]

    @classmethod
    def from_dict(cls, values: DictData) -> Self:
        """Construct Connection Model from dict data. This construct is
        different with ``.model_validate()`` because it will prepare the values
        before using it if the data do not have 'url'.

        :param values: A dict data that use to construct this model.
        """
        # NOTE: filter out the fields of this model.
        filter_data: DictData = {
            k: values.pop(k)
            for k in values.copy()
            if k not in cls.model_fields and k not in EXCLUDED_EXTRAS
        }
        if "url" in values:
            url: ConnModel = ConnModel.from_url(values.pop("url"))
            return cls(
                dialect=url.dialect,
                host=url.host,
                port=url.port,
                user=url.user,
                pwd=url.pwd,
                # NOTE:
                #   I will replace None endpoint with memory value for SQLite
                #   connection string.
                endpoint=(url.endpoint or "memory"),
                # NOTE: This order will show that externals this the top level.
                extras=(url.options | filter_data),
            )
        return cls.model_validate(
            obj={
                "extras": (values.pop("extras", {}) | filter_data),
                **values,
            }
        )

    @classmethod
    def from_loader(cls, name: str, externals: DictData) -> Self:
        """Construct Connection with Loader object with specific config name.

        :param name: A config name.
        :param externals: An external data that want to add to extras.
        """
        loader: Loader = Loader(name, externals=externals)
        # NOTE: Validate the config type match with current connection model
        if loader.type != cls:
            raise ValueError(f"Type {loader.type} does not match with {cls}")
        return cls.from_dict(
            {
                "extras": (loader.data.pop("extras", {}) | externals),
                **loader.data,
            }
        )

    @field_validator("endpoint")
    def __prepare_slash(cls, value: str) -> str:
        """Prepare slash character that map double form URL model loading."""
        if value.startswith("//"):
            return value[1:]
        return value


class Conn(BaseConn):
    """Conn (Connection) Model that implement any necessary methods. This object
    should be the base for abstraction to any connection model object.
    """

    def get_spec(self) -> str:
        """Return full connection url that construct from all fields."""
        return (
            f"{self.dialect}://{self.user or ''}"
            f"{f':{self.pwd}' if self.pwd else ''}"
            f"{self.host or ''}{f':{self.port}' if self.port else ''}"
            f"/{self.endpoint}"
        )

    def ping(self) -> bool:
        """Ping the connection that able to use with this field value."""
        raise NotImplementedError("Ping does not implement")

    def glob(self, pattern: str) -> Iterator[Any]:
        """Return a list of object from the endpoint of this connection."""
        raise NotImplementedError("Glob does not implement")

    def find_object(self, _object: str):
        raise NotImplementedError("Glob does not implement")


class FlSys(Conn):
    """File System Connection."""

    dialect: Literal["local"] = "local"

    def ping(self) -> bool:
        return Path(self.endpoint).exists()

    def glob(self, pattern: str) -> Iterator[Path]:
        yield from Path(self.endpoint).rglob(pattern=pattern)

    def find_object(self, _object: str) -> bool:
        return (Path(self.endpoint) / _object).exists()


class SFTP(Conn):
    """SFTP Server Connection."""

    dialect: Literal["sftp"] = "sftp"

    def __client(self):
        from .datasets.sftp import WrapSFTP

        return WrapSFTP(
            host=self.host,
            port=self.port,
            user=self.user,
            pwd=self.pwd.get_secret_value(),
        )

    def ping(self) -> bool:
        with self.__client().simple_client():
            return True

    def glob(self, pattern: str) -> Iterator[str]:
        yield from self.__client().walk(pattern=pattern)


class Db(Conn):
    """RDBMS System Connection"""

    def ping(self) -> bool:
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL, Engine
        from sqlalchemy.exc import OperationalError

        engine: Engine = create_engine(
            url=URL.create(
                self.dialect,
                username=self.user,
                password=self.pwd.get_secret_value() if self.pwd else None,
                host=self.host,
                port=self.port,
                database=self.endpoint,
                query={},
            ),
            execution_options={},
        )
        try:
            return engine.connect()
        except OperationalError as err:
            logging.warning(str(err))
            return False


class SQLite(Db):
    dialect: Literal["sqlite"]


class ODBC(Conn): ...


class Doc(Conn):
    """No SQL System Connection"""


class Mongo(Doc): ...


class SSHCred(BaseModel):
    ssh_host: str
    ssh_user: str
    ssh_password: Optional[SecretStr] = Field(default=None)
    ssh_private_key: Optional[str] = Field(default=None)
    ssh_private_key_pwd: Optional[SecretStr] = Field(default=None)
    ssh_port: int = Field(default=22)


class S3Cred(BaseModel):
    aws_access_key: str
    aws_secret_access_key: SecretStr
    region: str = Field(default="ap-southeast-1")
    role_arn: Optional[str] = Field(default=None)
    role_name: Optional[str] = Field(default=None)
    mfa_serial: Optional[str] = Field(default=None)


class AZServPrinCred(BaseModel):
    tenant: str
    client_id: str
    secret_id: SecretStr


class GoogleCred(BaseModel):
    google_json_path: str


SubclassConn = TypeVar("SubclassConn", bound=Conn)
