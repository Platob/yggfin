"""Generic Arrow boundaries over Yggdryl's native ``Field``."""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import pyarrow
from yggdryl import Field
from yggdryl import field as field_of

from rekep.arrow_reader import OwnedRecordBatchReader

LOGGER = logging.getLogger(__name__)

DESCRIPTION = "description"
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


def sort_key(direction: bool | str = True, **declared: Any) -> dict[str, Any]:
    """Mark one member as an Iceberg sort column."""
    metadata = dict(declared.pop("metadata", None) or {})
    if direction is not False:
        metadata[SORT_KEY] = "asc" if direction is True else str(direction)
    return field_options(metadata=metadata, **declared)


def arrow_type(source: Field) -> pyarrow.DataType:
    """Return one native field's PyArrow datatype."""
    return source.dtype.into_arrow()


def fields(source: Field) -> tuple[Field, ...]:
    """Return a native container's immediate children."""
    return tuple(source)


def field_names(source: Field) -> list[str]:
    """Return a native struct's member names in declaration order."""
    return [member.name for member in source]


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


def strict_cast_batch(
    target: Field,
    batch: pyarrow.RecordBatch,
    *,
    safe: bool = False,
) -> pyarrow.RecordBatch:
    """Apply one native schema after rejecting missing required values."""
    schema = target.into_arrow_schema()
    partition, digest = _schema_protocols(schema)
    _validate_required_batch(schema, batch, allow_generated=True)
    cast = target.cast_arrow_batch(batch, safe=safe)
    if not partition and not digest:
        return cast
    applied = target.apply_arrow_batch(
        cast,
        partition=partition,
        digest=digest,
        cast=False,
    )
    _validate_required_batch(schema, applied)
    return applied


def strict_cast_table(
    target: Field,
    table: pyarrow.Table,
    *,
    safe: bool = False,
) -> pyarrow.Table:
    """Apply one native schema to a memory-sized table after strict preflight."""
    schema = target.into_arrow_schema()
    partition, digest = _schema_protocols(schema)
    _validate_required_schema(schema, table.schema, allow_generated=True)
    for batch in table.to_batches():
        _validate_required_batch(schema, batch, allow_generated=True)
    cast = target.cast_arrow(table, safe=safe)
    if not partition and not digest:
        return cast
    applied_schema = target.apply_arrow_schema(
        cast.schema,
        partition=partition,
        digest=digest,
        cast=False,
    )
    applied = pyarrow.Table.from_batches(
        [
            target.apply_arrow_batch(
                batch,
                partition=partition,
                digest=digest,
                cast=False,
            )
            for batch in cast.to_batches()
        ],
        schema=applied_schema,
    )
    for batch in applied.to_batches():
        _validate_required_batch(schema, batch)
    return applied


def strict_cast_reader(
    target: Field,
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
    *,
    safe: bool = False,
) -> pyarrow.RecordBatchReader:
    """Apply one native schema lazily after strict per-batch preflight."""
    LOGGER.debug("applying a stream onto %s: %d columns", target.name or "row", len(target))
    schema = target.into_arrow_schema()
    partition, digest = _schema_protocols(schema)
    source_schema = getattr(source, "schema", None)
    if isinstance(source_schema, pyarrow.Schema):
        _validate_required_schema(schema, source_schema, allow_generated=True)

    def generate() -> Iterator[pyarrow.RecordBatch]:
        for batch in source:
            yield strict_cast_batch(target, batch, safe=safe)

    close = getattr(source, "close", None)
    applied_schema = target.apply_arrow_schema(
        schema,
        partition=partition,
        digest=digest,
        cast=False,
    )
    return OwnedRecordBatchReader(
        applied_schema,
        generate(),
        close if close is not None else lambda: None,
    )


def _field_index(fields: Sequence[pyarrow.Field], name: str) -> int | None:
    folded = name.encode().lower()
    matched = [index for index, field in enumerate(fields) if field.name.encode().lower() == folded]
    return matched[0] if len(matched) == 1 else None


def _validate_required(
    target: pyarrow.Field,
    source: Any,
    path: str,
    *,
    allow_generated: bool = False,
) -> None:
    if pyarrow.types.is_dictionary(source.type):
        source = pyarrow.compute.dictionary_decode(source)
    if (
        not target.nullable
        and source.null_count
        and not (allow_generated and _is_generated(target))
    ):
        raise ValueError(
            f"column {path!r} is not nullable and {source.null_count} of {len(source)} values "
            "are null; fill them upstream or make the field optional"
        )
    target_type, source_type = target.type, source.type
    kinds = pyarrow.types
    if kinds.is_struct(target_type) and kinds.is_struct(source_type):
        nested = (
            source if source.null_count == 0 else pyarrow.compute.filter(source, source.is_valid())
        )
        if len(nested) == 0:
            return
        source_fields = list(nested.type)
        for member in target_type:
            member_path = f"{path}.{member.name}" if path else member.name
            index = _field_index(source_fields, member.name)
            if index is None:
                if not member.nullable and not (allow_generated and _is_generated(member)):
                    raise ValueError(
                        f"column {member_path!r} is missing and not nullable, so it cannot be "
                        "filled with nulls; produce it upstream or make the field optional"
                    )
                continue
            _validate_required(
                member,
                nested.field(index),
                member_path,
                allow_generated=allow_generated,
            )
        return
    if kinds.is_map(target_type) and kinds.is_map(source_type):
        nested = (
            source if source.null_count == 0 else pyarrow.compute.filter(source, source.is_valid())
        )
        if len(nested) == 0:
            return
        _validate_required(
            target_type.key_field,
            nested.keys,
            f"{path}.key",
            allow_generated=False,
        )
        _validate_required(
            target_type.item_field,
            nested.items,
            f"{path}.value",
            allow_generated=False,
        )
        return
    if (
        not kinds.is_map(target_type)
        and not kinds.is_map(source_type)
        and _is_list_like(target_type)
        and _is_list_like(source_type)
    ):
        _validate_required(
            target_type.value_field,
            source.flatten(),
            f"{path}.item",
            allow_generated=False,
        )


def _validate_required_schema(
    target: pyarrow.Schema,
    source: pyarrow.Schema,
    *,
    allow_generated: bool = False,
) -> None:
    """Reject absent required columns even when a source contains no batches."""
    source_fields = list(source)
    for member in target:
        if (
            not member.nullable
            and _field_index(source_fields, member.name) is None
            and not (allow_generated and _is_generated(member))
        ):
            raise ValueError(
                f"column {member.name!r} is missing and not nullable, so it cannot be "
                "filled with nulls; produce it upstream or make the field optional"
            )


def _validate_required_batch(
    target: pyarrow.Schema,
    source: pyarrow.RecordBatch,
    *,
    allow_generated: bool = False,
) -> None:
    source_fields = list(source.schema)
    for member in target:
        index = _field_index(source_fields, member.name)
        if index is None:
            if not member.nullable and not (allow_generated and _is_generated(member)):
                raise ValueError(
                    f"column {member.name!r} is missing and not nullable, so it cannot be "
                    "filled with nulls; produce it upstream or make the field optional"
                )
            continue
        _validate_required(
            member,
            source.column(index),
            member.name,
            allow_generated=allow_generated,
        )


def _is_generated(field: pyarrow.Field) -> bool:
    """Whether a native protocol owns filling this field."""
    metadata = field.metadata or {}
    return b"partition:sources" in metadata or (
        metadata.get(b"digest:role", b"").lower() == b"holder"
    )


def _schema_protocols(schema: pyarrow.Schema) -> tuple[bool, bool]:
    """Whether partition or digest metadata asks for native application."""
    partition = False
    digest = False
    for field in schema:
        field_partition, field_digest = _field_protocols(field, field.name)
        partition = partition or field_partition
        digest = digest or field_digest
    return partition, digest


def _field_protocols(
    field: pyarrow.Field,
    path: str,
    *,
    materializable: bool = True,
) -> tuple[bool, bool]:
    metadata = field.metadata or {}
    partition = any(key.startswith(b"partition:") for key in metadata)
    digest = any(key.startswith(b"digest:") for key in metadata)
    if (partition or digest) and not materializable:
        kinds = []
        if partition:
            kinds.append("partition")
        if digest:
            kinds.append("digest")
        raise ValueError(
            f"field {path!r} declares native {' and '.join(kinds)} metadata below a list or "
            "map; Yggdryl materializes protocols only through Struct fields"
        )
    dtype = field.type
    kinds = pyarrow.types
    if kinds.is_struct(dtype):
        children = tuple((member, member.name, materializable) for member in dtype)
    elif kinds.is_map(dtype):
        children = (
            (dtype.key_field, "key", False),
            (dtype.item_field, "value", False),
        )
    elif _is_list_like(dtype):
        children = ((dtype.value_field, "item", False),)
    else:
        children = ()
    for child, child_name, child_materializable in children:
        child_path = f"{path}.{child_name}" if path else child_name
        child_partition, child_digest = _field_protocols(
            child,
            child_path,
            materializable=child_materializable,
        )
        partition = partition or child_partition
        digest = digest or child_digest
    return partition, digest


def _is_list_like(dtype: pyarrow.DataType) -> bool:
    kinds = pyarrow.types
    return (
        kinds.is_list(dtype)
        or kinds.is_large_list(dtype)
        or kinds.is_list_view(dtype)
        or kinds.is_large_list_view(dtype)
        or kinds.is_fixed_size_list(dtype)
    )


__all__ = [
    "DESCRIPTION",
    "FIELD_ID",
    "ICEBERG",
    "PARTITION_KEY",
    "PRIMARY_KEY",
    "SORT_KEY",
    "SORT_ORDER",
    "Field",
    "arrow_type",
    "derived_from",
    "field_names",
    "field_of",
    "field_options",
    "fields",
    "leaf_names",
    "partition_key",
    "primary_key",
    "replace_field",
    "sort_key",
    "strict_cast_batch",
    "strict_cast_reader",
    "strict_cast_table",
]
