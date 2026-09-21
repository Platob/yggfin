"""Generic Arrow boundaries over rekep's native ``Field``."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import pyarrow
from yggdryl import Field
from yggdryl import field as field_of

#: The metadata keys this package writes, spelled as the core spells them: a
#: namespace in capitals, the key after it in lower case. The core reads a
#: namespace in any case and answers it in this one, so a key spelled here is
#: the key a declaration reads back -- and a lookup against what a field
#: answers needs no folding.
DESCRIPTION = "description"
DIGEST_ALGORITHM = "DIGEST:algorithm"
DIGEST_ROLE = "DIGEST:role"
DIGEST_SOURCES = "DIGEST:sources"
ICEBERG = "iceberg"
PRIMARY_KEY = "ICEBERG:primary_key"
PARTITION_KEY = "ICEBERG:partition_key"
FIELD_ID = "ICEBERG:field_id"
SORT_KEY = "ICEBERG:sort_key"
SORT_ORDER = "ICEBERG:sort_order"

#: The core's own mark for an identity partition, the one `set_partition`
#: writes; a transform is this package's `PARTITION_KEY` beside it.
IDENTITY_PARTITION = "FIELD:partition"

#: The transform every capture table is laid out by, named once.
#:
#: Every published table takes it over `currunix`, the instant its own read
#: settled: the hour a line was printed in on `logs.messages`, the hour its
#: message happened in on `fix.bronze` and `fix.silver`. One hour of a busy
#: bridge is a file a scan can skip whole, and a run's window is a whole
#: number of them, so a replay replaces exactly the partitions it covers.
#: What tells the three apart is the key, not the layout.
HOUR = "hour"

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
    # A caller spells a namespace as it likes and the core reads any case, so
    # what a caller already stated is read the same way here.
    metadata = {
        (IDENTITY_PARTITION if key.casefold() == IDENTITY_PARTITION.casefold() else key): value
        for key, value in (declared.pop("metadata", None) or {}).items()
    }
    metadata = {
        (PARTITION_KEY if key.casefold() == PARTITION_KEY.casefold() else key): value
        for key, value in metadata.items()
    }
    identity = transform is True or str(transform).casefold() == "identity"
    existing_identity = str(metadata.get(IDENTITY_PARTITION) or "").casefold() == "true"
    existing_transform = str(metadata.get(PARTITION_KEY) or "")
    if existing_transform.casefold() == "false":
        existing_transform = ""
    if identity and existing_transform:
        raise ValueError(f"'{IDENTITY_PARTITION}' and '{PARTITION_KEY}' are mutually exclusive")
    if transform is not False and not identity and existing_identity:
        raise ValueError(f"'{IDENTITY_PARTITION}' and '{PARTITION_KEY}' are mutually exclusive")
    metadata.pop(IDENTITY_PARTITION, None)
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
    """Declare a column computed natively from the named source fields."""
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
    """Mark one member as the row digest computed beside it.

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


def stored_arrow_reader(
    source: pyarrow.RecordBatchReader,
    field: Field,
) -> pyarrow.RecordBatchReader:
    """Any stage's rows as a table stores them, ready for the write.

    The one storage boundary every task crosses: the text read's rows on
    their way into `logs.messages`, the parse's into `fix.bronze`, the walk's
    into `fix.silver`. Projecting onto `field` is most of it, and the field
    apply does that for free -- a column the stage carried and the table does
    not declare is dropped here rather than written.

    The rest exists for one column type. A content code is an unsigned
    sixty-four-bit integer and Iceberg's only sixty-four-bit integer is
    signed, so a code above 2**63 has no `int64` to be cast to: the native
    cast refuses it rather than wrapping, and PyIceberg's own metrics refuse
    it a second time when it packs the column's bounds. The same eight bytes
    read as signed are the code, so the column is *viewed* rather than
    converted: one zero-copy reinterpretation per batch, no row pass, and a
    filter on either side names the same rows.

    The field apply runs after it, in its native order, so the cast, the
    derived partitions and the digests still happen where they always did.
    """
    stored = field.into_arrow_schema()
    unsigned = [
        member.name
        for member in source.schema
        if pyarrow.types.is_unsigned_integer(member.type)
        and member.name in stored.names
        and pyarrow.types.is_signed_integer(stored.field(member.name).type)
    ]
    if not unsigned:
        return field.apply_arrow_reader(source, safe=False, nullability="strict")
    viewed = pyarrow.schema(
        [
            member.with_type(stored.field(member.name).type) if member.name in unsigned else member
            for member in source.schema
        ],
        metadata=source.schema.metadata,
    )

    def _viewed() -> Iterator[pyarrow.RecordBatch]:
        for batch in source:
            columns = [
                batch.column(index).view(viewed.field(index).type)
                if viewed.field(index).name in unsigned
                else batch.column(index)
                for index in range(batch.num_columns)
            ]
            yield pyarrow.RecordBatch.from_arrays(columns, schema=viewed)

    return field.apply_arrow_reader(
        pyarrow.RecordBatchReader.from_batches(viewed, _viewed()),
        safe=False,
        nullability="strict",
    )


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
    "stored_arrow_reader",
]
