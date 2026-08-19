"""Optional dependencies, imported where they are used."""

from __future__ import annotations

import importlib
from typing import Any


def require(module: str, extra: str) -> Any:
    """Import an optional dependency, or name the extra that provides it.

    A top-level `import yaml` would turn a missing extra into an unimportable
    package; going through here turns it into a clear error at the one call
    that needed it, naming the install that fixes it.
    """
    try:
        return importlib.import_module(module)
    except ImportError as error:
        raise ImportError(
            f"{module} is required for this format; install it with: pip install rekep[{extra}]"
        ) from error
