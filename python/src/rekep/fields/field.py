"""Generic Arrow boundaries over Yggdryl's native ``Field``."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from yggdryl import Field
from yggdryl import field as field_of

DESCRIPTION = "description"
DIGEST_ALGORITHM = "digest:algorithm"
DIGEST_ROLE = "digest:role"
DIGEST_SOURCES = "digest:sources"
ICEBERG = "iceberg"
PRIMARY_KEY = "iceberg:primary_key"
PARTITION_KEY = "iceberg:partition_key"
FIELD_ID = "iceberg:field_id"
SORT_KEY = "iceberg:sort_key"
SORT_ORDER = "iceberg:sort_order"

_MISSING = object()


def field_options(
    *,
    dtype: Any = None,
    nullable: bool | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Options for one native ``Field.from_pyhint`` annotation."""
    options: dict[str, Any] = {}
    if dtype is not None:
        options["arrow_type"] = dtype
    if nullable is not None:
        options["nullable"] = nullable
    if metadata:
        options["metadata"] = {str(key): str(value) for key, value in metadata.items()}
    return options


def primary_key(**declared: Any) -> dict[str, Any]:
    """Mark one non-null member as part of its Iceberg identity."""
    metadata = dict(declared.pop("metadata", None) or {})
    metadata[PRIMARY_KEY] = "true"
    return field_options(metadata=metadata, **declared)


def partition_key(
    transform: bool | str = True,
    **declared: Any,
) -> dict[str, Any]:
    """Mark one member as an Iceberg partition column."""
    metadata = dict(declared.pop("metadata", None) or {})
    identity = transform is True or str(transform).casefold() == "identity"
    existing_identity = str(metadata.get("field:partition") or "").casefold() == "true"
    existing_transform = str(metadata.get(PARTITION_KEY) or "")
    if existing_transform.casefold() == "false":
        existing_transform = ""
    if identity and existing_transform:
        raise ValueError("'field:partition' and 'iceberg:partition_key' are mutually exclusive")
    if transform is not False and not identity and existing_identity:
        raise ValueError("'field:partition' and 'iceberg:partition_key' are mutually exclusive")
    metadata.pop("field:partition", None)
    metadata.pop(PARTITION_KEY, None)
    if identity:
        marker = Field("", "null")
        marker.set_partition(True)
        metadata.update(marker.metadata)
    elif transform is not False:
        metadata[PARTITION_KEY] = str(transform)
    return field_options(metadata=metadata, **declared)


def derived_from(
    sources: str | Sequence[str],
    transform: str | None = None,
    **declared: Any,
) -> dict[str, Any]:
    """Declare a column computed by Yggdryl from the named source fields."""
    metadata = dict(declared.pop("metadata", None) or {})
    protocol = Field("", "null")
    protocol.partition.sources = [sources] if isinstance(sources, str) else sources
    if transform is not None:
        protocol.partition.transform = transform
    metadata.update(protocol.metadata)
    return field_options(metadata=metadata, **declared)


def digest_key(
    sources: str | Sequence[str] | None = None,
    algorithm: str = "xxh3-128",
    **declared: Any,
) -> dict[str, Any]:
    """Mark one member as the row digest Yggdryl computes beside it.

    The member holds the digest; the members it reads stay ordinary columns,
    so `sources` is the only place the input is named. Omitting `sources`
    selects every field beside the holder. The declared datatype must be the
    algorithm's exact width -- `fixed_size_binary[16]` for the 128-bit default.
    """
    metadata = dict(declared.pop("metadata", None) or {})
    metadata[DIGEST_ROLE] = "holder"
    metadata[DIGEST_ALGORITHM] = str(algorithm)
    if sources is not None:
        named = [sources] if isinstance(sources, str) else list(sources)
        metadata[DIGEST_SOURCES] = json.dumps(named, separators=(",", ":"))
    return field_options(metadata=metadata, **declared)


def sort_key(direction: bool | str = True, **declared: Any) -> dict[str, Any]:
    """Mark one member as an Iceberg sort column."""
    metadata = dict(declared.pop("metadata", None) or {})
    if direction is not False:
        metadata[SORT_KEY] = "asc" if direction is True else str(direction)
    return field_options(metadata=metadata, **declared)


def replace_field(
    source: Field,
    *,
    name: str | object = _MISSING,
    dtype: Any = _MISSING,
    nullable: bool | object = _MISSING,
    metadata: Mapping[str, Any] | object = _MISSING,
) -> Field:
    """Clone a native field with the named immutable parts replaced."""
    return Field(
        source.name if name is _MISSING else str(name),
        source.dtype if dtype is _MISSING else dtype,
        source.nullable if nullable is _MISSING else bool(nullable),
        dict(source.metadata) if metadata is _MISSING else metadata,
    )


def leaf_names(source: Field) -> list[str]:
    """Return every scalar path below a native field."""
    children = tuple(source)
    if not children:
        return [""]
    return [
        f"{member.name}.{leaf}" if leaf else member.name
        for member in children
        for leaf in leaf_names(member)
    ]


__all__ = [
    "DESCRIPTION",
    "DIGEST_ALGORITHM",
    "DIGEST_ROLE",
    "DIGEST_SOURCES",
    "FIELD_ID",
    "ICEBERG",
    "PARTITION_KEY",
    "PRIMARY_KEY",
    "SORT_KEY",
    "SORT_ORDER",
    "Field",
    "derived_from",
    "digest_key",
    "field_of",
    "field_options",
    "leaf_names",
    "partition_key",
    "primary_key",
    "replace_field",
    "sort_key",
]
