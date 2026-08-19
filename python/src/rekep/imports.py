"""Resolving dotted paths to the objects they name."""

from __future__ import annotations

import importlib
from typing import Any


def locate(dotted: str) -> Any:
    """`"rekep.models.Log"` -> the class itself.

    The rightmost dot splits module from attribute, which is enough for
    everything this package points at -- records, flows, callables -- without
    inventing a path syntax.
    """
    module_name, _, attribute = dotted.rpartition(".")
    if not module_name:
        raise ValueError(f"{dotted!r} is not a dotted path")
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise ImportError(f"cannot import {module_name!r} while resolving {dotted!r}") from error
    try:
        return getattr(module, attribute)
    except AttributeError as error:
        raise ImportError(f"{module_name!r} has no attribute {attribute!r}") from error
