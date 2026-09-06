"""Type-hint utilities shared by generic document conversion."""

from __future__ import annotations

import collections.abc
from typing import Annotated, Any, get_args, get_origin

NONE_TYPE = type(None)

SEQUENCE_ORIGINS = {list, collections.abc.Sequence, collections.abc.MutableSequence}
SET_ORIGINS = {set, frozenset, collections.abc.Set, collections.abc.MutableSet}
MAPPING_ORIGINS = {dict, collections.abc.Mapping, collections.abc.MutableMapping}


def unwrap_annotated(annotation: Any) -> tuple[tuple[Any, ...], Any]:
    """Split ``Annotated[X, ...]`` into its extras and inner type."""
    if get_origin(annotation) is not Annotated:
        return (), annotation
    inner, *extras = get_args(annotation)
    return tuple(extras), inner


def item_annotation(annotation: Any) -> Any:
    """Return a container hint's item type, or ``Any`` when absent."""
    arguments = get_args(annotation)
    return arguments[0] if arguments else Any
