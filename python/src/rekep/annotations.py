"""Type-hint utilities shared by generic document conversion."""

from __future__ import annotations

import collections.abc
import sys
from typing import Annotated, Any, get_args, get_origin

# `Self` is 3.11's, and a hint here is read rather than merely written --
# `get_type_hints` evaluates every one of them -- so the name has to exist at
# runtime on the oldest interpreter this package supports and not only in a
# checker. It is resolved once, here, because this module is already what
# every reader of an annotation imports; nothing else spells the branch, and
# the redundant alias is what says the name is re-exported rather than used.
if sys.version_info >= (3, 11):
    from typing import Self as Self
else:  # pragma: no cover - the back-port is what the older interpreter has
    from typing_extensions import Self as Self

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
