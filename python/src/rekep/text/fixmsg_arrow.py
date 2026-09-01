"""Arrow transcription for already numbered flat FIX messages."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow
import pyarrow.compute as compute

from rekep.entries import ENTRIES
from rekep.enums import Protocol
from rekep.fields.arrays import build_list, dense_counts, sequence
from rekep.fix.columns import COLUMNS, TYPES
from rekep.fix.fields import cast_arrow_field
from rekep.fix.transcribe import BEGIN_STRING_SOURCE, _raw_spelling_changed, _version_key


def into_flat_fixmsg_batch(
    shape: type[Any],
    batch: pyarrow.RecordBatch,
    codec: Any,
    columns: Mapping[str, pyarrow.Array],
    protocols: pyarrow.Array,
) -> pyarrow.RecordBatch | None:
    """Transcribe numeric standard fields, or return None for registry fallback."""
    rows = batch.num_rows
    entries = columns.get("entries")
    if (
        not rows
        or entries is None
        or entries.null_count
        or not _supports(codec)
        or protocols.null_count
        or not compute.all(
            compute.equal(protocols, Protocol.FIX.into_stored()), min_count=0
        ).as_py()
    ):
        return None
    items = compute.list_flatten(entries)
    tags = compute.struct_field(items, "tag")
    keys = compute.struct_field(items, "key")
    values = compute.struct_field(items, "value")
    component = compute.struct_field(items, "comp")
    column_tags = _namespaced_column_tags(codec, tags.type)
    if (
        tags.null_count
        or values.null_count
        or compute.any(compute.less_equal(tags, 0), min_count=0).as_py()
        or component.null_count < len(component)
        or compute.any(compute.equal(tags, 213), min_count=0).as_py()
        or not compute.all(compute.equal(keys, tags.cast(pyarrow.string())), min_count=0).as_py()
        or not compute.all(
            compute.equal(values, compute.utf8_trim_whitespace(values)), min_count=0
        ).as_py()
        or (
            len(column_tags)
            and compute.any(compute.is_in(tags, value_set=column_tags), min_count=0).as_py()
        )
    ):
        return None
    if codec.null_values:
        absent = compute.is_in(
            compute.utf8_lower(values),
            value_set=pyarrow.array(sorted(codec.null_values), pyarrow.string()),
        )
        if compute.any(absent, min_count=0).as_py():
            return None
    parents = compute.list_parent_indices(entries).cast(pyarrow.int64())
    identities = compute.add(
        compute.multiply(parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
        tags.cast(pyarrow.int64()),
    )
    if len(compute.unique(identities)) != len(identities) or not _checksum_is_last(
        entries, parents, tags
    ):
        return None

    versions, _ = _versions(
        codec, entries, tags, values, rows, columns.get("beginstring"), columns.get("applverid")
    )
    distinct_versions = compute.drop_null(compute.unique(versions))
    if versions.null_count or len(distinct_versions) != 1:
        return None
    version = distinct_versions[0].as_py()
    group_tags = codec.registry.group_count_tags(version)
    if (
        group_tags
        and compute.any(
            compute.is_in(tags, value_set=pyarrow.array(sorted(group_tags), tags.type)),
            min_count=0,
        ).as_py()
    ):
        return None

    fields = codec.flat_fields(version)
    resolved = _complete_tagged(codec, entries, version)
    lifted = _lifted_columns(resolved, fields, rows)
    if lifted is None:
        return None
    promoted, residual = lifted
    residual, unmap = shape._partition_entries(residual, codec, version)
    schema = shape._message_schema(batch.schema)
    output = dict(columns)
    output.update(
        {
            "protocol": Protocol.with_versions_arrow(protocols, versions),
            "entries": residual,
            "unmap": unmap,
            **promoted,
            **shape._wire_session_columns(columns, codec, version, promoted),
        }
    )
    for field in schema:
        output.setdefault(field.name, pyarrow.nulls(rows, field.type))

    from rekep.text.fixmsg import _lastmkt_arrow

    output["lastmkt"] = _lastmkt_arrow(output, rows)
    return shape.identified(output, schema, rows, codec.registry)


def _supports(codec: Any) -> bool:
    """Whether the codec has exactly the behavior this specialization mirrors."""
    from rekep.fix.transcribe import FixCodec

    rule = codec.rules.rule(Protocol.FIX)
    return (
        type(codec) is FixCodec
        and not bool(codec.fields)
        and rule.named is False
        and rule.entry_separator is None
    )


def _namespaced_column_tags(codec: Any, dtype: pyarrow.DataType) -> pyarrow.Array:
    """Numeric identities whose registry fields target named log columns."""
    return pyarrow.array(
        sorted(
            {
                int(field.fix.tag)
                for field in codec.named_fields().values()
                if field.fix.tag is not None
            }
        ),
        dtype,
    )


def _versions(
    codec: Any,
    entries: pyarrow.Array,
    tags: pyarrow.Array,
    values: pyarrow.Array,
    rows: int,
    begin_strings: pyarrow.Array | None,
    application_versions: pyarrow.Array | None = None,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Resolve one common non-transport BeginString once for the whole batch.

    `begin_strings` and `application_versions` are the columns the raw stage
    lifted those two tags into. Each leads and `entries` fills it, which is
    the one rule every lifted column is read under: a column that is null and
    a column a projection dropped are the same absence, and the tag is still
    in the list either way. Stated rather than defaulted, so a caller says
    which of the two it is handing over.
    """
    spelled = _begin_strings(entries, tags, values, rows, begin_strings)
    if spelled is not None and spelled.null_count == 0:
        distinct = compute.unique(spelled)
        if len(distinct) == 1:
            spelling = distinct[0].as_py()
            if not _version_key(spelling).startswith("FIXT"):
                version = codec.version_named(spelling)
                if version is not None:
                    return (
                        pyarrow.repeat(pyarrow.scalar(version), rows),
                        pyarrow.repeat(pyarrow.scalar(BEGIN_STRING_SOURCE), rows),
                    )
    return codec.versions_of_entries(entries, begin_strings, application_versions)


def _begin_strings(
    entries: pyarrow.Array,
    tags: pyarrow.Array,
    values: pyarrow.Array,
    rows: int,
    lifted: pyarrow.Array | None,
) -> pyarrow.Array | None:
    """One `BeginString` per row, from the column first and the tag second."""
    begins = compute.equal(tags, 8)
    inline = None
    if compute.sum(begins, min_count=0).as_py() == rows:
        inline = compute.filter(values, begins)
    if lifted is None:
        return inline
    column = lifted.combine_chunks() if isinstance(lifted, pyarrow.ChunkedArray) else lifted
    column = column.cast(pyarrow.string(), safe=False)
    return column if inline is None else compute.coalesce(column, inline)


def _complete_tagged(codec: Any, entries: pyarrow.Array, version: str) -> pyarrow.Array:
    """Canonicalize values whose numeric identities are already authoritative."""
    items = compute.list_flatten(entries)
    tags = compute.struct_field(items, "tag")
    keys = compute.struct_field(items, "key")
    values = compute.struct_field(items, "value")
    completed = pyarrow.StructArray.from_arrays(
        [
            tags,
            codec._canonical(keys, tags, version),
            codec._encoded(tags, values, version),
            compute.struct_field(items, "comp"),
        ],
        fields=list(items.type),
    )
    return build_list(
        ENTRIES,
        compute.list_value_length(entries).cast(pyarrow.int32()),
        completed,
    )


def _lifted_columns(
    entries: pyarrow.Array, fields: Mapping[int, Any], rows: int
) -> tuple[dict[str, pyarrow.Array], pyarrow.Array] | None:
    """Lift one occurrence per tag and retain raw text a typed column loses."""
    items = compute.list_flatten(entries)
    parents = compute.list_parent_indices(entries).cast(pyarrow.int64())
    tags = compute.struct_field(items, "tag")
    values = compute.struct_field(items, "value")
    wanted = compute.is_in(tags, value_set=pyarrow.array(sorted(fields), tags.type))
    row_ids = sequence(rows)
    columns: dict[str, pyarrow.Array] = {}
    order = compute.array_sort_indices(compute.filter(tags, wanted))
    sorted_tags = compute.take(compute.filter(tags, wanted), order)
    sorted_parents = compute.take(compute.filter(parents, wanted), order)
    sorted_values = compute.take(compute.filter(values, wanted), order)
    sorted_identities = compute.add(
        compute.multiply(sorted_parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
        sorted_tags.cast(pyarrow.int64()),
    )
    retained_identities: list[pyarrow.Array] = []
    at = 0
    for counted in compute.value_counts(sorted_tags).to_pylist():
        tag, run = counted["values"], counted["counts"]
        raw = sorted_values.slice(at, run)
        column_rows = sorted_parents.slice(at, run)
        identities = sorted_identities.slice(at, run)
        at += run
        column = cast_arrow_field(raw, fields[tag], TYPES[tag])
        changed = _raw_spelling_changed(raw, column)
        if compute.any(changed, min_count=0).as_py():
            retained_identities.append(compute.filter(identities, changed))
        if run != rows:
            column = compute.take(column, compute.index_in(row_ids, value_set=column_rows))
        columns[COLUMNS[tag]] = column

    keep = compute.invert(wanted)
    if retained_identities:
        identities = compute.add(
            compute.multiply(parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
            tags.cast(pyarrow.int64()),
        )
        keep = compute.or_(
            keep,
            compute.is_in(
                identities,
                value_set=pyarrow.concat_arrays(retained_identities),
            ),
        )
    kept_parents = compute.filter(parents, keep)
    kept = pyarrow.StructArray.from_arrays(
        [compute.filter(compute.struct_field(items, field.name), keep) for field in items.type],
        fields=list(items.type),
    )
    residual = build_list(ENTRIES, dense_counts(kept_parents, rows), kept)
    return columns, residual


def _checksum_is_last(entries: pyarrow.Array, parents: pyarrow.Array, tags: pyarrow.Array) -> bool:
    """Whether every present CheckSum is its row's final field."""
    checksum = compute.equal(tags, 10)
    if not compute.any(checksum, min_count=0).as_py():
        return True
    sizes = compute.list_value_length(entries).cast(pyarrow.int64())
    ends = compute.subtract(compute.cumulative_sum(sizes), 1)
    checksum_parents = compute.filter(parents, checksum)
    checksum_positions = compute.filter(sequence(len(tags)), checksum)
    expected = compute.take(ends, checksum_parents)
    return bool(compute.all(compute.equal(checksum_positions, expected), min_count=0).as_py())
