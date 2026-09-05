"""Widening one Arrow schema with another, at every level.

Everything here speaks pyarrow only, so a `Field` can be built on it: casting
data onto a shape lives on the field that declares the shape.

One recursion serves two opposite contracts, which is what `leaf` selects
between. Casting onto a declared shape keeps the target's leaf -- the
declaration is the answer -- while folding two readings of one identity may
lose neither, so it widens. Everything above the leaves is the same walk.
"""

from __future__ import annotations

from collections.abc import Callable

import pyarrow

#: How two readings of one scalar are settled at the bottom of the recursion.
LeafRule = Callable[[pyarrow.DataType, pyarrow.DataType], pyarrow.DataType]

#: Coarsest first, so `max` over two of them widens rather than narrows.
_TIMESTAMP_UNITS = ("s", "ms", "us", "ns")


def merge_fields(
    source: pyarrow.Field, target: pyarrow.Field, *, leaf: LeafRule | None = None
) -> pyarrow.Field:
    """`source` merged into `target`, all the way down."""
    merged = _merge_type(source.type, target.type, leaf=leaf)
    if merged.equals(target.type):
        return target
    return pyarrow.field(target.name, merged, nullable=target.nullable, metadata=target.metadata)


def promoted_type(
    source: pyarrow.DataType | None, target: pyarrow.DataType | None
) -> pyarrow.DataType | None:
    """The one type both readings fit in: `merge_fields` without a field around it.

    For callers holding two types and no names, which is how a registry folds
    two readings of one identity.
    """
    if source is None or target is None:
        return target if source is None else source
    return _merge_type(source, target, leaf=promoted_scalar)


def promoted_scalar(source: pyarrow.DataType, target: pyarrow.DataType) -> pyarrow.DataType:
    """One scalar type holding both readings; never refuses.

    A field already known to be one identity must not lose a reading, so text
    beats an error -- the same reasoning `arrow_type_of` gives for an unknown
    FIX datatype, because every FIX value is representable as text.
    """
    promoted = _promotion(source, target)
    if promoted is not None:
        return promoted
    # Mismatched containers are not two readings of one field, so the target
    # stands; anything else is two scalars and text holds both.
    kinds = pyarrow.types
    return target if kinds.is_nested(source) or kinds.is_nested(target) else pyarrow.string()


def reconcilable(source: pyarrow.DataType, target: pyarrow.DataType) -> bool:
    """Whether two readings can be *one column*.

    Deciding whether two readings are one identity must be able to say no, so
    text is not an answer here and the refusal is the evidence: only what
    Arrow declines to unify marks two readings as two identities.
    """
    return _promotion(source, target) is not None


# -- helpers ----------------------------------------------------------------


def _promotion(source: pyarrow.DataType, target: pyarrow.DataType) -> pyarrow.DataType | None:
    """The type holding both readings, or None where the pair is two identities.

    The one triage both public questions ask; they differ only in what they
    make of a refusal. `unify_schemas` refuses through more than one exception
    type -- pyarrow 25.0.1 raises `ArrowTypeError` for `int32` against
    `string` -- so the catch is the base every Arrow error shares, on purpose:
    which one a pair raises is not part of Arrow's contract, and narrowing it
    crashes on the next pair.
    """
    kinds = pyarrow.types
    if source.equals(target) or kinds.is_null(source):
        return target
    if kinds.is_null(target):
        return source
    if kinds.is_nested(source) or kinds.is_nested(target):
        return None
    if kinds.is_timestamp(source) and kinds.is_timestamp(target):
        unit = max((source.unit, target.unit), key=_TIMESTAMP_UNITS.index)
        return pyarrow.timestamp(unit, tz=target.tz or source.tz)
    try:
        unified = pyarrow.unify_schemas(
            [
                pyarrow.schema([pyarrow.field("", source)]),
                pyarrow.schema([pyarrow.field("", target)]),
            ],
            promote_options="permissive",
        )
    except pyarrow.ArrowException:
        return None
    return unified.field(0).type


def _merge_type(
    source: pyarrow.DataType, target: pyarrow.DataType, *, leaf: LeafRule | None = None
) -> pyarrow.DataType:
    """The container cases; a leaf is `leaf`'s to settle, or the target."""
    kinds = pyarrow.types
    if kinds.is_struct(source) and kinds.is_struct(target):
        return pyarrow.struct(
            _merge_field_lists(
                [source.field(i) for i in range(source.num_fields)],
                [target.field(i) for i in range(target.num_fields)],
                leaf=leaf,
            )
        )
    if kinds.is_map(source) and kinds.is_map(target):
        # Only the value side can grow: a key is what identifies an entry,
        # so changing its shape changes which entries exist.
        return pyarrow.map_(
            target.key_field, merge_fields(source.item_field, target.item_field, leaf=leaf)
        )
    if _same_list_kind(source, target):
        # Every list flavour through one rebuilder rather than a branch each:
        # spelling out `list` and `large_list` by hand is what left the three
        # view flavours silently dropping a member their items had grown.
        from rekep.fields.arrays import list_type_like

        return list_type_like(target, merge_fields(source.field(0), target.field(0), leaf=leaf))
    return leaf(source, target) if leaf is not None else target


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
    source: list[pyarrow.Field], target: list[pyarrow.Field], *, leaf: LeafRule | None = None
) -> list[pyarrow.Field]:
    """Target order, each field merged with its namesake, then the additions."""
    by_name = {field.name: field for field in source}
    merged = [
        merge_fields(by_name[field.name], field, leaf=leaf) if field.name in by_name else field
        for field in target
    ]
    known = {field.name for field in target}
    merged.extend(field.with_nullable(True) for field in source if field.name not in known)
    return merged
