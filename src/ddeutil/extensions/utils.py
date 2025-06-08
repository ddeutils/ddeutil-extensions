# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from typing import Any

from .exceptions import LazyImportError


class Lazy:
    def __init__(self, module_name: str):
        self.module_name = module_name
        self.module = None

    def __getattr__(self, name: str) -> Any:
        if self.module is None:
            try:
                self.module = __import__(self.module_name)
            except ImportError as e:
                raise LazyImportError(
                    f"Please install `{self.module_name}` before using this "
                    f"extension."
                ) from e
        return getattr(self.module, name)
