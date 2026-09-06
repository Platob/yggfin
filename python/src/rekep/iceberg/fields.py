"""Projecting a field onto Iceberg, and reading one back.

pyiceberg already converts between its types and Arrow's, so this is not a
second walk of the type system: it is the identity Arrow does not carry --
field ids, documentation, identifier fields, partition transforms -- put on top
of pyiceberg's own conversion.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Sequence
from typing import Any

import pyarrow

from rekep.fields import (
    DESCRIPTION,
    FIELD_ID,
    SORT_ORDER,
    Field,
    arrow_type,
    field_of,
    fields,
    replace_field,
)
from rekep.require import require

#: Arrow field metadata key pyiceberg reads a column comment from, and writes
#: one back to. Ours is `description`; this is the bridge between the two.
DOC = b"doc"

#: Iceberg numbers partition fields from 1000 by convention, so a spec's ids
#: never collide with a schema's.
FIRST_PARTITION_ID = 1000

#: Arrow field metadata key the ecosystem stores Iceberg's column ids under --
#: parquet's own, which is why the prefix is not ours. Ours is
#: `iceberg:field_id`, beside the other keys the protocol owns; this is the
#: bridge between the two, and it is crossed here and nowhere else.
PARQUET_FIELD_ID = b"PARQUET:field_id"
ICEBERG_FIELD_ID = FIELD_ID.encode()


def primary_keys(source: Field) -> list[str]:
    """Top-level identifier columns in declaration order."""
    keys = [
        member
        for member in fields(source)
        if str(member.iceberg.get("primary_key") or "").casefold() == "true"
    ]
    nullable = [member.name for member in keys if member.nullable]
    if nullable:
        raise ValueError(f"primary key columns must not be nullable: {', '.join(nullable)}")
    return [member.name for member in keys]


def partition_keys(source: Field) -> dict[str, str]:
    """Top-level partition columns mapped to their transforms."""
    declared = {}
    for member in fields(source):
        transform = _enabled(member.iceberg.get("partition_key"))
        if member.is_partition and transform:
            raise ValueError(
                f"partition column {member.name!r} declares both identity and {transform!r}"
            )
        if member.is_partition:
            declared[member.name] = "identity"
        elif transform:
            declared[member.name] = transform
    return declared


def sort_keys(source: Field) -> dict[str, str]:
    """Top-level sort columns mapped to directions in physical order."""
    encoded = source.metadata.get(SORT_ORDER)
    declared = {
        member.name: direction
        for member in fields(source)
        if (direction := _enabled(member.iceberg.get("sort_key")))
    }
    if not encoded:
        return declared
    try:
        ordered = [(str(name), str(direction)) for name, direction in json.loads(encoded)]
    except (TypeError, ValueError):
        raise ValueError(f"field {source.name!r} has an invalid {SORT_ORDER!r}") from None
    if len({name for name, _ in ordered}) != len(ordered):
        raise ValueError(f"field {source.name!r} repeats a column in {SORT_ORDER!r}")
    if declared != dict(ordered):
        raise ValueError(f"field {source.name!r} has inconsistent {SORT_ORDER!r} metadata")
    return dict(ordered)


def derived_keys(source: Field) -> dict[str, tuple[str, ...]]:
    """Top-level derived columns mapped to their source columns."""
    return {
        member.name: tuple(sources)
        for member in fields(source)
        if (sources := member.partition.sources) is not None
    }


def _enabled(value: Any) -> str:
    """One enabled string metadata value; empty and false are unset."""
    spelled = str(value or "")
    return "" if spelled.casefold() == "false" else spelled


def iceberg_schema(source: Field) -> Any:
    """`source` as a `pyiceberg.schema.Schema`, ids numbered from one.

    The ids come from pyiceberg's own fresh assignment -- siblings before any
    descent -- so a schema built here and one pyiceberg builds from the same
    Arrow schema agree on which column is which.
    """
    require("pyiceberg", "iceberg")
    from pyiceberg.schema import Schema

    schema = _fresh(_documented(source.into_arrow_schema()))
    keys = primary_keys(source)
    if not keys:
        return schema
    return Schema(
        *schema.fields,
        schema_id=schema.schema_id,
        identifier_field_ids=[schema.find_field(key).field_id for key in keys],
    )


def iceberg_field(source: Field, field_id: int = 1) -> Any:
    """One field as a `pyiceberg` NestedField, ids numbered from `field_id`."""
    require("pyiceberg", "iceberg")
    counter = itertools.count(field_id)
    documented = _documented(pyarrow.schema([source.into_arrow()]))
    return _fresh(documented, next_id=lambda: next(counter)).fields[0]


def iceberg_partition_spec(source: Field, schema: Any = None) -> Any:
    """The `pyiceberg.partitioning.PartitionSpec` `source` declares."""
    require("pyiceberg", "iceberg")
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import parse_transform

    schema = schema if schema is not None else iceberg_schema(source)
    partitions = []
    for index, (name, transform) in enumerate(partition_keys(source).items()):
        partitions.append(
            PartitionField(
                source_id=schema.find_field(name).field_id,
                field_id=FIRST_PARTITION_ID + index,
                transform=parse_transform(transform),
                name=name if transform == "identity" else f"{name}_{_kind(transform)}",
            )
        )
    return PartitionSpec(*partitions)


def _kind(transform: str) -> str:
    """`bucket[16]` is a `bucket`; the width belongs to the spec, not the name."""
    return transform.split("[", 1)[0]


def iceberg_sort_order(
    source: Field,
    schema: Any = None,
    sort_by: Sequence[str] | None = None,
) -> Any:
    """The `pyiceberg.table.sorting.SortOrder` `source` declares, in declaration order.

    Iceberg records it and every engine that writes through the table honours
    it; nothing here has to sort on read. `SortOrder()` with no fields is
    Iceberg's own "unsorted", which is what a shape declaring none gets.

    What a sort key *is* -- where a row sits inside its file, against a
    partition, which decides which file -- is in its ``iceberg:`` metadata.
    """
    require("pyiceberg", "iceberg")
    from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
    from pyiceberg.transforms import IdentityTransform

    declared = sort_keys(source) if sort_by is None else dict.fromkeys(sort_by, "ascending")
    if not declared:
        return SortOrder()
    schema = schema if schema is not None else iceberg_schema(source)
    return SortOrder(
        *[
            SortField(
                source_id=schema.find_field(name).field_id,
                transform=IdentityTransform(),
                direction=(
                    SortDirection.DESC
                    if str(direction).lower().startswith("desc")
                    else SortDirection.ASC
                ),
                null_order=NullOrder.NULLS_LAST,
            )
            for name, direction in declared.items()
        ]
    )


def iceberg_struct_field(
    schema: Any,
    name: str = "",
    spec: Any = None,
    sort_order: Any = None,
) -> Field:
    """A `pyiceberg` schema as a struct field: types, docs and keys.

    This side keeps the pyiceberg import lazy and restores Rekep's table
    metadata onto a native field.
    """
    require("pyiceberg", "iceberg")
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    arrow = _described(narrowed(schema_to_pyarrow(schema, include_field_ids=True)))
    field = field_of(arrow, name)
    members = {member.name: member for member in fields(field)}
    for field_id in schema.identifier_field_ids:
        column = schema.find_column_name(field_id)
        if column and "." not in column:  # a nested key is Iceberg's, not a column here
            members[column].iceberg["primary_key"] = "true"
    partitioned: dict[str, list[Any]] = {}
    for partition in getattr(spec, "fields", ()):
        column = schema.find_column_name(partition.source_id)
        if column and "." not in column:
            partitioned.setdefault(column, []).append(partition)
    for column, partitions in partitioned.items():
        if len(partitions) != 1:
            # Iceberg permits more than one transform over the same source.
            # One Field member has one physical slot, so keep table.spec()
            # authoritative instead of publishing a false partial projection.
            continue
        transform = str(partitions[0].transform)
        if transform == "identity":
            members[column].set_partition(True)
        else:
            members[column].set_partition(False)
            members[column].iceberg["partition_key"] = transform
    metadata = dict(field.metadata)
    if sort_order is not None:
        from pyiceberg.table.sorting import NullOrder, SortDirection
        from pyiceberg.transforms import IdentityTransform

        ordered: list[tuple[str, str]] = []
        for sorting in getattr(sort_order, "fields", ()):
            column = schema.find_column_name(sorting.source_id)
            if (
                not column
                or "." in column
                or not isinstance(sorting.transform, IdentityTransform)
                or sorting.null_order != NullOrder.NULLS_LAST
            ):
                ordered = []
                break
            direction = "desc" if sorting.direction == SortDirection.DESC else "asc"
            ordered.append((column, direction))
        if ordered:
            for column, direction in ordered:
                members[column].iceberg["sort_key"] = direction
            metadata[SORT_ORDER] = json.dumps(ordered, separators=(",", ":"))
    return replace_field(
        field,
        dtype=pyarrow.struct([member.into_arrow() for member in members.values()]),
        metadata=metadata,
    )


# -- arrow metadata: `description` is ours, `doc` is pyiceberg's -------------


def _fresh(arrow: pyarrow.Schema, next_id: Any = None) -> Any:
    """An Arrow schema as an Iceberg schema, keeping the ids it already carries."""
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    from pyiceberg.schema import assign_fresh_schema_ids

    plain = _pyarrow_to_schema_without_ids(arrow)
    if next_id is not None:
        return assign_fresh_schema_ids(plain, next_id)

    planned = _planned_ids(arrow)
    declared = [(path, field_id) for path, field_id in planned if field_id is not None]
    paths_by_id: dict[int, str] = {}
    for path, field_id in declared:
        assert field_id is not None
        if previous := paths_by_id.get(field_id):
            raise ValueError(
                f"Iceberg field id {field_id} is repeated by {previous!r} and {path!r}"
            )
        paths_by_id[field_id] = path

    fresh = itertools.count(max(paths_by_id, default=0) + 1)
    ids = iter(field_id for _, field_id in planned)

    def allocate() -> int:
        return next(ids) or next(fresh)

    return assign_fresh_schema_ids(plain, allocate)


def _planned_ids(schema: pyarrow.Schema) -> list[tuple[str, int | None]]:
    """Declared ids in PyIceberg's sibling-first assignment order."""
    return _planned_field_ids(list(schema), "")


def _planned_field_ids(members: list[pyarrow.Field], prefix: str) -> list[tuple[str, int | None]]:
    planned = [
        (f"{prefix}.{member.name}" if prefix else member.name, _declared_id(member))
        for member in members
    ]
    for member in members:
        path = f"{prefix}.{member.name}" if prefix else member.name
        planned.extend(_planned_type_ids(member.type, path))
    return planned


def _planned_type_ids(dtype: pyarrow.DataType, path: str) -> list[tuple[str, int | None]]:
    kinds = pyarrow.types
    if kinds.is_struct(dtype):
        return _planned_field_ids(list(dtype), path)
    if kinds.is_list(dtype) or kinds.is_large_list(dtype):
        item = dtype.value_field
        return [(f"{path}.item", _declared_id(item)), *_planned_type_ids(item.type, f"{path}.item")]
    if kinds.is_map(dtype):
        key, value = dtype.key_field, dtype.item_field
        return [
            (f"{path}.key", _declared_id(key)),
            (f"{path}.value", _declared_id(value)),
            *_planned_type_ids(key.type, f"{path}.key"),
            *_planned_type_ids(value.type, f"{path}.value"),
        ]
    return []


def _declared_id(field: pyarrow.Field) -> int | None:
    """One positive Iceberg id from Arrow's bridge metadata."""
    encoded = (field.metadata or {}).get(PARQUET_FIELD_ID)
    if encoded is None:
        return None
    try:
        field_id = int(encoded)
    except (TypeError, ValueError):
        raise ValueError(f"field {field.name!r} has invalid Iceberg field id {encoded!r}") from None
    if field_id <= 0:
        raise ValueError(f"field {field.name!r} has non-positive Iceberg field id {field_id}")
    return field_id


def _documented(schema: pyarrow.Schema) -> pyarrow.Schema:
    """The schema with every description copied to the key pyiceberg reads."""
    return pyarrow.schema([_document(field) for field in schema], metadata=schema.metadata)


def _document(field: pyarrow.Field) -> pyarrow.Field:
    metadata = dict(field.metadata or {})
    description = metadata.get(DESCRIPTION.encode())
    if description:
        metadata[DOC] = description
    column_id = metadata.get(ICEBERG_FIELD_ID)
    if column_id is not None:
        # A declaration that names its ids is one that came from a table, and
        # keeping them is what makes the round trip an identity rather than a
        # rename of every column. pyiceberg reads them under parquet's key.
        metadata[PARQUET_FIELD_ID] = column_id
    return pyarrow.field(
        field.name, _document_type(field.type), nullable=field.nullable, metadata=metadata
    )


def narrowed(schema: pyarrow.Schema) -> pyarrow.Schema:
    """A scan's shape with Arrow's 32-bit offsets rather than its 64-bit ones.

    `schema_to_pyarrow` answers `large_string` and `large_binary` for every
    Iceberg string and binary, with nothing to ask it otherwise:
    `_ConvertToArrowSchema` returns them outright, and the
    `pyarrow.use-large-types-on-read` the configuration page documents is read
    nowhere in 0.11.1. So a table written from `string` columns read back as
    `large_string`, and every join against a value this package built itself
    needed the two widths reconciled somewhere.

    Reconciled here, once, at the seam where the width is chosen -- which is
    also what makes the reading stable across a pyiceberg that changes its
    mind, since `pyiceberg>=0.11.1` names no upper bound.
    """
    return pyarrow.schema([_narrow(field) for field in schema], metadata=schema.metadata)


def _narrow(field: pyarrow.Field) -> pyarrow.Field:
    return pyarrow.field(
        field.name, _narrow_type(field.type), nullable=field.nullable, metadata=field.metadata
    )


def _narrow_type(dtype: pyarrow.DataType) -> pyarrow.DataType:
    kinds = pyarrow.types
    if kinds.is_large_string(dtype):
        return pyarrow.string()
    if kinds.is_large_binary(dtype):
        return pyarrow.binary()
    if kinds.is_struct(dtype):
        return pyarrow.struct([_narrow(dtype.field(index)) for index in range(dtype.num_fields)])
    if kinds.is_large_list(dtype):
        return pyarrow.list_(_narrow(dtype.field(0)))
    if kinds.is_list(dtype):
        return pyarrow.list_(_narrow(dtype.field(0)))
    if kinds.is_map(dtype):
        return pyarrow.map_(_narrow(dtype.key_field), _narrow(dtype.item_field))
    return dtype


def _document_type(dtype: pyarrow.DataType) -> pyarrow.DataType:
    kinds = pyarrow.types
    if kinds.is_struct(dtype):
        return pyarrow.struct([_document(dtype.field(index)) for index in range(dtype.num_fields)])
    if kinds.is_list(dtype):
        return pyarrow.list_(_document(dtype.field(0)))
    if kinds.is_large_list(dtype):
        return pyarrow.large_list(_document(dtype.field(0)))
    if kinds.is_map(dtype):
        return pyarrow.map_(_document(dtype.key_field), _document(dtype.item_field))
    return dtype


def _described(schema: pyarrow.Schema) -> pyarrow.Schema:
    """The schema with pyiceberg's `doc` read back as our description."""
    return pyarrow.schema([_describe(field) for field in schema], metadata=schema.metadata)


def _describe(field: pyarrow.Field) -> pyarrow.Field:
    metadata = dict(field.metadata or {})
    doc = metadata.pop(DOC, None)
    if doc:
        metadata[DESCRIPTION.encode()] = doc
    column_id = metadata.pop(PARQUET_FIELD_ID, None)
    if column_id:
        metadata[ICEBERG_FIELD_ID] = column_id
    return pyarrow.field(
        field.name, _describe_type(field.type), nullable=field.nullable, metadata=metadata or None
    )


def _describe_type(dtype: pyarrow.DataType) -> pyarrow.DataType:
    kinds = pyarrow.types
    if kinds.is_struct(dtype):
        return pyarrow.struct([_describe(dtype.field(index)) for index in range(dtype.num_fields)])
    if kinds.is_list(dtype):
        return pyarrow.list_(_describe(dtype.field(0)))
    if kinds.is_large_list(dtype):
        return pyarrow.large_list(_describe(dtype.field(0)))
    if kinds.is_map(dtype):
        return pyarrow.map_(_describe(dtype.key_field), _describe(dtype.item_field))
    return dtype


#: How many **leaf** columns Iceberg collects bounds for by default, in
#: declaration order, and the property that says so. Past the hundredth leaf a
#: column is written with no lower or upper bound, so a filter on it reads
#: every file and still returns the right answer -- which is exactly why
#: nothing notices.
INFERRED_METRICS = "write.metadata.metrics.max-inferred-column-defaults"
DEFAULT_INFERRED = 100

#: The ceiling `metrics_for` raises that to. Bounds cost bytes in every
#: manifest entry, so this is not unbounded: a shape wider than this should
#: say which of its columns a reader filters on rather than asking for all.
MAX_INFERRED = 1_000

#: The prefix under which one column's metrics mode is declared. A column
#: named here is collected whatever its position, which is the whole point:
#: the budget above only ever applies to columns nothing asked for.
COLUMN_METRICS = "write.metadata.metrics.column"


def metrics_for(source: Field) -> dict[str, str]:
    """Table properties that keep the columns a reader filters on prunable."""
    declared = {
        **partition_keys(source),
        **sort_keys(source),
        **dict.fromkeys(primary_keys(source), ""),
    }
    properties = {
        f"{COLUMN_METRICS}.{name}": _mode(arrow_type(source.field(name))) for name in declared
    }
    counted = len(_leaves(arrow_type(source)))
    if counted > DEFAULT_INFERRED:
        properties[INFERRED_METRICS] = str(min(counted, MAX_INFERRED))
    return properties


def _mode(dtype: pyarrow.DataType) -> str:
    """The metrics mode a column of `dtype` is worth collecting under.

    A bound on a long string is the string, in every manifest entry that names
    the file; sixteen characters is Iceberg's own default and prunes a prefix
    filter just as well. Anything fixed-width is cheap enough to keep whole.
    """
    kinds = pyarrow.types
    if kinds.is_string(dtype) or kinds.is_large_string(dtype):
        return "truncate(16)"
    if kinds.is_binary(dtype) or kinds.is_large_binary(dtype):
        return "truncate(16)"
    return "full"


def _leaves(dtype: pyarrow.DataType) -> list[str]:
    """Every leaf of `dtype`, in the pre-order Iceberg counts in.

    A list contributes its item's leaves and a map its key and value, which is
    what makes a nested member expensive against a budget it can never benefit
    from: Iceberg collects no bounds for a field under a repeated one.
    """
    kinds = pyarrow.types
    if kinds.is_struct(dtype):
        found: list[str] = []
        for index in range(dtype.num_fields):
            member = dtype.field(index)
            found += _under(member.name, member.type)
        return found
    if kinds.is_list(dtype) or kinds.is_large_list(dtype):
        return _under("item", dtype.field(0).type)
    if kinds.is_map(dtype):
        return _under("key", dtype.key_type) + _under("value", dtype.item_type)
    return [""]


def _under(name: str, dtype: pyarrow.DataType) -> list[str]:
    """`_leaves` of `dtype`, each spelled below `name`."""
    return [f"{name}.{one}" if one else name for one in _leaves(dtype)]
