"""Arrow transcription for already numbered flat FIX messages."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import pyarrow
import pyarrow.compute as compute

from rekep.entries import ENTRIES
from rekep.fields.arrays import build_list, dense_counts, sequence
from rekep.fix.columns import COLUMNS, TYPES
from rekep.fix.fields import cast_arrow_fix
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
        or not compute.all(compute.equal(protocols, "FIX"), min_count=0).as_py()
    ):
        return None
    items = compute.list_flatten(entries)
    tags = compute.struct_field(items, "tag")
    keys = compute.struct_field(items, "key")
    values = compute.struct_field(items, "value")
    namespace = compute.struct_field(items, "namespace")
    component = compute.struct_field(items, "comp")
    if (
        tags.null_count
        or values.null_count
        or compute.any(compute.less_equal(tags, 0), min_count=0).as_py()
        or namespace.null_count < len(namespace)
        or component.null_count < len(component)
        or compute.any(compute.equal(tags, 213), min_count=0).as_py()
        or not compute.all(compute.equal(keys, tags.cast(pyarrow.string())), min_count=0).as_py()
        or not compute.all(
            compute.equal(values, compute.utf8_trim_whitespace(values)), min_count=0
        ).as_py()
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

    protocolversion, protocolversionsource = _versions(
        codec, entries, tags, values, rows, columns.get("beginstring")
    )
    versions = compute.drop_null(compute.unique(protocolversion))
    if protocolversion.null_count or len(versions) != 1:
        return None
    version = versions[0].as_py()
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
    schema = shape._message_schema(batch.schema)
    output = dict(columns)
    output.update(
        {
            "protocolcode": protocols,
            "protocolversion": protocolversion,
            "protocolversionsource": protocolversionsource,
            "entries": residual,
            **promoted,
        }
    )
    for field in schema:
        output.setdefault(field.name, pyarrow.nulls(rows, field.type))

    from rekep.text.fixmsg import _mic_arrow

    output["mic"] = _mic_arrow(output, rows)
    return shape.identified(output, schema, rows)


def flat_fixmsg_positions(
    codec: Any,
    columns: Mapping[str, pyarrow.Array],
    protocols: pyarrow.Array,
) -> Iterator[pyarrow.Array]:
    """Yield version-homogeneous rows accepted by the flat transcription."""
    entries = columns.get("entries")
    rows = len(protocols)
    if not rows or entries is None or not _supports(codec):
        return
    items = compute.list_flatten(entries)
    tags = compute.struct_field(items, "tag")
    keys = compute.struct_field(items, "key")
    values = compute.struct_field(items, "value")
    namespace = compute.struct_field(items, "namespace")
    component = compute.struct_field(items, "comp")
    parents = compute.list_parent_indices(entries).cast(pyarrow.int64())

    good = compute.and_(compute.is_valid(tags), compute.greater(tags, 0))
    good = compute.and_(good, compute.is_valid(values))
    good = compute.and_(good, compute.is_null(namespace))
    good = compute.and_(good, compute.is_null(component))
    good = compute.and_(good, compute.not_equal(tags, 213))
    good = compute.and_(good, compute.equal(keys, tags.cast(pyarrow.string())))
    good = compute.and_(good, compute.equal(values, compute.utf8_trim_whitespace(values)))
    if codec.null_values:
        absent = compute.is_in(
            compute.utf8_lower(values),
            value_set=pyarrow.array(sorted(codec.null_values), pyarrow.string()),
        )
        good = compute.and_(good, compute.invert(compute.fill_null(absent, True)))
    eligible = compute.and_(compute.is_valid(entries), compute.equal(protocols, "FIX"))
    invalid_rows = _marked_rows(parents, compute.invert(good), rows)
    eligible = compute.and_(eligible, compute.invert(invalid_rows))
    eligible = compute.and_(eligible, compute.invert(_duplicate_rows(parents, tags, rows)))
    misplaced = _misplaced_checksum_rows(entries, parents, tags)
    eligible = compute.and_(eligible, compute.invert(misplaced))

    versions, _ = _versions(codec, entries, tags, values, rows, columns.get("beginstring"))
    eligible = compute.and_(eligible, compute.is_valid(versions))
    positions = sequence(rows)
    for version in compute.drop_null(compute.unique(compute.filter(versions, eligible))).sort():
        selected = compute.and_(eligible, compute.equal(versions, version))
        group_tags = codec.registry.group_count_tags(version.as_py())
        if group_tags:
            grouped = compute.is_in(
                tags,
                value_set=pyarrow.array(sorted(group_tags), tags.type),
            )
            selected = compute.and_(selected, compute.invert(_marked_rows(parents, grouped, rows)))
        where = compute.filter(positions, selected)
        if len(where):
            yield where


def _supports(codec: Any) -> bool:
    """Whether the codec has exactly the behavior this specialization mirrors."""
    from rekep.fix.transcribe import FixCodec

    rule = codec.rules.rule("FIX")
    return (
        type(codec) is FixCodec
        and not bool(codec.fields)
        and rule.named is False
        and rule.entry_separator is None
    )


def _marked_rows(parents: pyarrow.Array, marked: pyarrow.Array, rows: int) -> pyarrow.Array:
    """Mark rows having at least one selected child entry."""
    bad = compute.unique(compute.filter(parents, compute.fill_null(marked, True)))
    return compute.is_in(sequence(rows), value_set=bad)


def _duplicate_rows(parents: pyarrow.Array, tags: pyarrow.Array, rows: int) -> pyarrow.Array:
    """Mark rows carrying one numeric tag more than once."""
    valid = compute.and_(compute.is_valid(tags), compute.greater(tags, 0))
    valid_parents = compute.filter(parents, valid)
    valid_tags = compute.filter(tags, valid).cast(pyarrow.int64())
    identities = compute.add(
        compute.multiply(valid_parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
        valid_tags,
    )
    counted = compute.value_counts(identities)
    repeated = compute.filter(counted.field("values"), compute.greater(counted.field("counts"), 1))
    if not len(repeated):
        return pyarrow.repeat(pyarrow.scalar(False), rows)
    found = compute.index_in(repeated, value_set=identities)
    return compute.is_in(sequence(rows), value_set=compute.take(valid_parents, found))


def _misplaced_checksum_rows(
    entries: pyarrow.Array, parents: pyarrow.Array, tags: pyarrow.Array
) -> pyarrow.Array:
    """Mark rows whose CheckSum is not their final field."""
    rows = len(entries)
    checksum = compute.fill_null(compute.equal(tags, 10), False)
    if not compute.any(checksum, min_count=0).as_py():
        return pyarrow.repeat(pyarrow.scalar(False), rows)
    sizes = compute.fill_null(compute.list_value_length(entries), 0).cast(pyarrow.int64())
    ends = compute.subtract(compute.cumulative_sum(sizes), 1)
    checksum_parents = compute.filter(parents, checksum)
    checksum_positions = compute.filter(sequence(len(tags)), checksum)
    misplaced = compute.not_equal(checksum_positions, compute.take(ends, checksum_parents))
    return compute.is_in(sequence(rows), value_set=compute.filter(checksum_parents, misplaced))


def _versions(
    codec: Any,
    entries: pyarrow.Array,
    tags: pyarrow.Array,
    values: pyarrow.Array,
    rows: int,
    begin_strings: pyarrow.Array | None,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Resolve one common non-transport BeginString once for the whole batch.

    `begin_strings` is the column the raw stage lifted the tag into. It leads
    and `entries` fills it, which is the one rule every lifted column is read
    under: a column that is null and a column a projection dropped are the
    same absence, and the tag is still in the list either way. Stated rather
    than defaulted, so a caller says which of the two it is handing over.
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
    return codec.versions_of_entries(entries, begin_strings)


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
            compute.struct_field(items, "namespace"),
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
        column = _cast(raw, fields[tag].dtype, TYPES[tag])
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


def _cast(
    column: pyarrow.Array,
    source_type: pyarrow.DataType,
    target_type: pyarrow.DataType,
) -> pyarrow.Array:
    """Read one lifted column at the physical contract width."""
    read = cast_arrow_fix(column, source_type)
    if read.type.equals(target_type):
        return read
    if pyarrow.types.is_string(read.type) or pyarrow.types.is_large_string(read.type):
        return cast_arrow_fix(read, target_type)
    return read.cast(target_type, safe=False)


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
