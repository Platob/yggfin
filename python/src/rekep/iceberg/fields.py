"""Projecting a field onto Iceberg, and reading one back.

pyiceberg already converts between its types and Arrow's, so this is not a
second walk of the type system: it is the identity Arrow does not carry --
field ids, documentation, identifier fields, partition transforms -- put on top
of pyiceberg's own conversion.
"""

from __future__ import annotations

import itertools
from collections.abc import Sequence
from typing import Any

import pyarrow

from rekep.fields import DESCRIPTION, FIELD_ID, Field, StructField
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


def iceberg_schema(source: StructField) -> Any:
    """`source` as a `pyiceberg.schema.Schema`, ids numbered from one.

    The ids come from pyiceberg's own fresh assignment -- siblings before any
    descent -- so a schema built here and one pyiceberg builds from the same
    Arrow schema agree on which column is which.
    """
    require("pyiceberg", "iceberg")
    from pyiceberg.schema import Schema

    schema = _fresh(_documented(source.into_arrow_schema()))
    keys = source.primary_keys()
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
    documented = _documented(pyarrow.schema([source.into_arrow_field()]))
    return _fresh(documented, next_id=lambda: next(counter)).fields[0]


def iceberg_partition_spec(source: StructField, schema: Any = None) -> Any:
    """The `pyiceberg.partitioning.PartitionSpec` `source` declares."""
    require("pyiceberg", "iceberg")
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import parse_transform

    schema = schema if schema is not None else iceberg_schema(source)
    partitions = []
    for index, (name, transform) in enumerate(source.partition_keys().items()):
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
    source: StructField,
    schema: Any = None,
    sort_by: Sequence[str] | None = None,
) -> Any:
    """The `pyiceberg.table.sorting.SortOrder` `source` declares, in declaration order.

    Iceberg records it and every engine that writes through the table honours
    it; nothing here has to sort on read. `SortOrder()` with no fields is
    Iceberg's own "unsorted", which is what a shape declaring none gets.

    What a sort key *is* -- where a row sits inside its file, against a
    partition, which decides which file -- is on `Field.is_sort_key`.
    """
    require("pyiceberg", "iceberg")
    from pyiceberg.table.sorting import SortDirection, SortField, SortOrder
    from pyiceberg.transforms import IdentityTransform

    declared = source.sort_keys() if sort_by is None else dict.fromkeys(sort_by, "ascending")
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
            )
            for name, direction in declared.items()
        ]
    )


def struct_field_of(schema: Any, name: str = "", spec: Any = None) -> StructField:
    """A `pyiceberg` schema as a struct field: types, docs and keys."""
    require("pyiceberg", "iceberg")
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    arrow = _described(schema_to_pyarrow(schema, include_field_ids=True))
    field = Field.from_arrow_schema(arrow, name)
    for field_id in schema.identifier_field_ids:
        column = schema.find_column_name(field_id)
        if column and "." not in column:  # a nested key is Iceberg's, not a column here
            field.field(column).is_primary_key = True
    for partition in getattr(spec, "fields", ()):
        column = schema.find_column_name(partition.source_id)
        if column and "." not in column:
            field.field(column).is_partition_key = str(partition.transform)
    return field


# -- arrow metadata: `description` is ours, `doc` is pyiceberg's -------------


def _fresh(arrow: pyarrow.Schema, next_id: Any = None) -> Any:
    """An Arrow schema as an Iceberg schema, keeping the ids it already carries."""
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids, pyarrow_to_schema
    from pyiceberg.schema import assign_fresh_schema_ids

    if next_id is None and all(_has_ids(field) for field in arrow):
        return pyarrow_to_schema(arrow)
    return assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(arrow), next_id)


def _has_ids(field: pyarrow.Field) -> bool:
    """Whether `field` and everything under it carries an Iceberg column id."""
    if PARQUET_FIELD_ID not in (field.metadata or {}):
        return False
    data_type = field.type
    return all(_has_ids(data_type.field(index)) for index in range(data_type.num_fields))


def _documented(schema: pyarrow.Schema) -> pyarrow.Schema:
    """The schema with every description copied to the key pyiceberg reads."""
    return pyarrow.schema([_document(field) for field in schema], metadata=schema.metadata)


def _document(field: pyarrow.Field) -> pyarrow.Field:
    metadata = dict(field.metadata or {})
    description = metadata.get(DESCRIPTION.encode())
    if description:
        metadata[DOC] = description
    column_id = metadata.get(ICEBERG_FIELD_ID)
    if column_id:
        # A declaration that names its ids is one that came from a table, and
        # keeping them is what makes the round trip an identity rather than a
        # rename of every column. pyiceberg reads them under parquet's key.
        metadata[PARQUET_FIELD_ID] = column_id
    return pyarrow.field(
        field.name, _document_type(field.type), nullable=field.nullable, metadata=metadata
    )


def _document_type(data_type: pyarrow.DataType) -> pyarrow.DataType:
    kinds = pyarrow.types
    if kinds.is_struct(data_type):
        return pyarrow.struct(
            [_document(data_type.field(index)) for index in range(data_type.num_fields)]
        )
    if kinds.is_list(data_type):
        return pyarrow.list_(_document(data_type.field(0)))
    if kinds.is_large_list(data_type):
        return pyarrow.large_list(_document(data_type.field(0)))
    if kinds.is_map(data_type):
        return pyarrow.map_(_document(data_type.key_field), _document(data_type.item_field))
    return data_type


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


def _describe_type(data_type: pyarrow.DataType) -> pyarrow.DataType:
    kinds = pyarrow.types
    if kinds.is_struct(data_type):
        return pyarrow.struct(
            [_describe(data_type.field(index)) for index in range(data_type.num_fields)]
        )
    if kinds.is_list(data_type):
        return pyarrow.list_(_describe(data_type.field(0)))
    if kinds.is_large_list(data_type):
        return pyarrow.large_list(_describe(data_type.field(0)))
    if kinds.is_map(data_type):
        return pyarrow.map_(_describe(data_type.key_field), _describe(data_type.item_field))
    return data_type


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


def metrics_for(source: StructField) -> dict[str, str]:
    """Table properties that keep the columns a reader filters on prunable."""
    declared = {
        **source.partition_keys(),
        **source.sort_keys(),
        **dict.fromkeys(source.primary_keys(), ""),
    }
    properties = {
        f"{COLUMN_METRICS}.{name}": _mode(source.field(name).arrow_type) for name in declared
    }
    counted = len(_leaves(source.arrow_type))
    if counted > DEFAULT_INFERRED:
        properties[INFERRED_METRICS] = str(min(counted, MAX_INFERRED))
    return properties


def _mode(arrow_type: pyarrow.DataType) -> str:
    """The metrics mode a column of `arrow_type` is worth collecting under.

    A bound on a long string is the string, in every manifest entry that names
    the file; sixteen characters is Iceberg's own default and prunes a prefix
    filter just as well. Anything fixed-width is cheap enough to keep whole.
    """
    kinds = pyarrow.types
    if kinds.is_string(arrow_type) or kinds.is_large_string(arrow_type):
        return "truncate(16)"
    if kinds.is_binary(arrow_type) or kinds.is_large_binary(arrow_type):
        return "truncate(16)"
    return "full"


def _leaves(arrow_type: pyarrow.DataType) -> list[str]:
    """Every leaf of `arrow_type`, in the pre-order Iceberg counts in.

    A list contributes its item's leaves and a map its key and value, which is
    what makes a nested member expensive against a budget it can never benefit
    from: Iceberg collects no bounds for a field under a repeated one.
    """
    kinds = pyarrow.types
    if kinds.is_struct(arrow_type):
        found: list[str] = []
        for index in range(arrow_type.num_fields):
            member = arrow_type.field(index)
            found += _under(member.name, member.type)
        return found
    if kinds.is_list(arrow_type) or kinds.is_large_list(arrow_type):
        return _under("item", arrow_type.field(0).type)
    if kinds.is_map(arrow_type):
        return _under("key", arrow_type.key_type) + _under("value", arrow_type.item_type)
    return [""]


def _under(name: str, arrow_type: pyarrow.DataType) -> list[str]:
    """`_leaves` of `arrow_type`, each spelled below `name`."""
    return [f"{name}.{one}" if one else name for one in _leaves(arrow_type)]
