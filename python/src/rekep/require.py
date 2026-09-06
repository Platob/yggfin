"""Optional dependencies, imported where they are used."""

from __future__ import annotations

import importlib
from typing import Any


def require(module: str, extra: str) -> Any:
    """Import an optional dependency, or name the extra that provides it.

    Importing optional storage or dataframe packages at module load would make
    the base package unusable without them. This names the install at the call
    that needs it.
    """
    try:
        return importlib.import_module(module)
    except ImportError as error:
        raise ImportError(
            f"{module} is required for this operation; install it with: pip install rekep[{extra}]"
        ) from error
