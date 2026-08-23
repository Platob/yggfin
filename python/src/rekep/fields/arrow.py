"""Widening one Arrow schema with another, at every level.

Everything here speaks pyarrow only, so a `Field` can be built on it: casting
data onto a shape lives on the field that declares the shape.
"""

from __future__ import annotations

import pyarrow


def merge_fields(source: pyarrow.Field, target: pyarrow.Field) -> pyarrow.Field:
    """`source` merged into `target`, all the way down."""
    merged = _merge_type(source.type, target.type)
    if merged.equals(target.type):
        return target
    return pyarrow.field(target.name, merged, nullable=target.nullable, metadata=target.metadata)


def merge_schemas(source: pyarrow.Schema, target: pyarrow.Schema) -> pyarrow.Schema:
    """`merge_fields` over two schemas: the same rule, one level up.

    A schema is a list of fields and a struct is a list of fields, so this
    is the field merge with the ends changed. Nothing about the rule is
    restated here, which is why a nested addition behaves exactly like a
    top-level one.
    """
    merged = _merge_field_lists(list(source), list(target))
    if merged == list(target):
        return target
    return pyarrow.schema(merged, metadata=target.metadata)


# -- helpers ----------------------------------------------------------------


def _merge_type(source: pyarrow.DataType, target: pyarrow.DataType) -> pyarrow.DataType:
    """The container cases; anything else is the target, unchanged."""
    kinds = pyarrow.types
    if kinds.is_struct(source) and kinds.is_struct(target):
        return pyarrow.struct(
            _merge_field_lists(
                [source.field(i) for i in range(source.num_fields)],
                [target.field(i) for i in range(target.num_fields)],
            )
        )
    if kinds.is_map(source) and kinds.is_map(target):
        # Only the value side can grow: a key is what identifies an entry,
        # so changing its shape changes which entries exist.
        return pyarrow.map_(target.key_field, merge_fields(source.item_field, target.item_field))
    if _same_list_kind(source, target):
        # Every list flavour through one rebuilder rather than a branch each:
        # spelling out `list` and `large_list` by hand is what left the three
        # view flavours silently dropping a member their items had grown.
        from rekep.fields.arrays import list_type_like

        return list_type_like(target, merge_fields(source.field(0), target.field(0)))
    return target


def _same_list_kind(source: pyarrow.DataType, target: pyarrow.DataType) -> bool:
    """Whether both are lists, of the same flavour and (if fixed) the same width."""
    kinds = pyarrow.types
    tests = (
        kinds.is_list,
        kinds.is_large_list,
        kinds.is_list_view,
        kinds.is_large_list_view,
        kinds.is_fixed_size_list,
    )
    if not any(test(source) and test(target) for test in tests):
        return False
    return not kinds.is_fixed_size_list(target) or source.list_size == target.list_size


def _merge_field_lists(
    source: list[pyarrow.Field], target: list[pyarrow.Field]
) -> list[pyarrow.Field]:
    """Target order, each field merged with its namesake, then the additions."""
    by_name = {field.name: field for field in source}
    merged = [
        merge_fields(by_name[field.name], field) if field.name in by_name else field
        for field in target
    ]
    known = {field.name for field in target}
    merged.extend(field.with_nullable(True) for field in source if field.name not in known)
    return merged
