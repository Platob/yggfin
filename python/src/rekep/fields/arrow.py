"""Arrow schema surgery: reshaping a batch, and widening a schema.

Everything here speaks pyarrow only. A `Field` is what *declares* a shape;
these are the operations that make real data agree with one.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterator
from typing import Any

import pyarrow


def cast_batch(
    batch: pyarrow.RecordBatch, schema: pyarrow.Schema, *, safe: bool = False
) -> pyarrow.RecordBatch:
    """`batch` reshaped to `schema`: columns cast, missing filled, extras dropped.

    The gap this closes is the one every real pipeline hits: a transform
    produces *almost* the target shape -- an `int64` where the target wants
    `int32`, a column the source never had, its columns in another order --
    and the write fails on a schema comparison rather than on the data.

    `safe=False` by default, deliberately: this is `pyarrow.compute.cast`'s
    unsafe mode, the one that lets a value narrow or a timestamp lose
    precision instead of raising. A cast to a *target schema* is a
    declaration that the target's types are the authority, so the
    truncation is the intent, not an accident; pass `safe=True` to get
    Arrow's checking back.

    A column the batch does not have is filled with nulls -- but only if the
    target field is nullable. A missing non-nullable field is refused by
    name: filling a NOT NULL column with nulls builds a batch that only
    fails later, at the write, where the cause is much harder to see.
    """
    if batch.schema.equals(schema):
        return batch
    arrays = []
    for field in schema:
        if field.name in batch.schema.names:
            column = batch.column(field.name)
            arrays.append(column if column.type == field.type else column.cast(field.type, safe))
        elif field.nullable:
            arrays.append(pyarrow.nulls(batch.num_rows, field.type))
        else:
            raise ValueError(
                f"column {field.name!r} is missing and not nullable, so it cannot be filled "
                "with nulls; produce it upstream or make the field optional"
            )
    return pyarrow.RecordBatch.from_arrays(arrays, schema=schema)


def cast_reader(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
    schema: pyarrow.Schema,
    *,
    safe: bool = False,
    merge_schema: bool = False,
) -> pyarrow.RecordBatchReader:
    """`cast_batch` over a whole stream, still one batch at a time.

    Takes a plain iterator of batches too, so a transform's output becomes a
    reader of the target shape in one step, without the caller building a
    `RecordBatchReader` by hand first.

    `merge_schema=True` widens the target with `merge_schemas` first, so a
    column the source has and the target does not is **kept** instead of
    dropped. It has to look at the incoming schema to do that, which for a
    plain iterator means pulling one batch early (put straight back, so
    nothing is lost or read twice); a reader already declares its schema and
    is not touched. An empty iterator leaves the target as it was: there was
    no incoming schema to merge.

    The widened schema is decided **once**, from the reader's own schema or
    the first batch, and every later batch is cast onto it -- a stream is
    one shape, and a `RecordBatchReader` cannot say otherwise. A hand-rolled
    iterator whose batches disagree is therefore resolved in the target's
    favour: a column a later batch drops comes back as nulls, a column only
    a later batch has is dropped. Widening again mid-stream would mean a
    reader whose schema changes under its consumer, which no downstream
    writer accepts.
    """
    if merge_schema:
        source, incoming = _peek_schema(source)
        if incoming is not None:
            schema = merge_schemas(incoming, schema)

    def generate() -> Iterator[pyarrow.RecordBatch]:
        for batch in source:
            yield cast_batch(batch, schema, safe=safe)

    return pyarrow.RecordBatchReader.from_batches(schema, generate())


def merge_fields(source: pyarrow.Field, target: pyarrow.Field) -> pyarrow.Field:
    """`source` merged into `target`, all the way down.

    The one merge rule, applied recursively rather than only at the top:

    - `target` wins wherever both have something -- its type, its
      nullability, its metadata. That is what makes this a *merge* and not a
      takeover: shared columns stay the target's, so data is cast onto them
      (`cast_batch`), never the other way round. A source calling a column
      `int64` does not get to widen a target that declared `int32`.
    - Whatever `source` has and `target` does not is **added**, forced
      nullable: values already stored under `target` predate the field and
      have nothing to put in it.

    "Whatever source has" means at every level, which is the point of doing
    this per field rather than per schema. A struct that grew a member, a
    list whose items grew one, a map whose values grew one -- each merges
    the same way, because each is a field with fields inside it. Only
    matching containers recurse: a struct in the target and a scalar in the
    source is a type conflict, and the target simply wins.
    """
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
    if kinds.is_list(source) and kinds.is_list(target):
        return pyarrow.list_(merge_fields(source.field(0), target.field(0)))
    if kinds.is_large_list(source) and kinds.is_large_list(target):
        return pyarrow.large_list(merge_fields(source.field(0), target.field(0)))
    if kinds.is_map(source) and kinds.is_map(target):
        # Only the value side can grow: a key is what identifies an entry,
        # so changing its shape changes which entries exist.
        return pyarrow.map_(target.key_field, merge_fields(source.item_field, target.item_field))
    return target


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


def _peek_schema(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
) -> tuple[Any, pyarrow.Schema | None]:
    """`(source, its schema)`, reading one batch only when it has to.

    A `RecordBatchReader` states its schema up front, so it comes back
    untouched and still fully lazy. A plain iterator only reveals its shape
    by producing a batch, so one is pulled and then chained back on the
    front -- the caller still sees every batch, in order, exactly once.
    """
    if isinstance(source, pyarrow.RecordBatchReader):
        return source, source.schema
    iterator = iter(source)
    first = next(iterator, None)
    if first is None:
        return iter(()), None
    return itertools.chain([first], iterator), first.schema
