"""Arrow transcription for already numbered flat FIX messages."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import pyarrow
import pyarrow.compute as compute

from rekep.fields.arrays import build_list, dense_counts, sequence
from rekep.fix.columns import COLUMNS, TYPES
from rekep.fix.fields import cast_arrow_fix
from rekep.fix.transcribe import BEGIN_STRING_SOURCE, _raw_spelling_changed, _version_key
from rekep.kwargs import KWARGS


def into_flat_fixmsg_batch(
    shape: type[Any],
    batch: pyarrow.RecordBatch,
    codec: Any,
    columns: Mapping[str, pyarrow.Array],
    protocols: pyarrow.Array,
) -> pyarrow.RecordBatch | None:
    """Transcribe numeric standard fields, or return None for registry fallback."""
    rows = batch.num_rows
    kwargs = columns.get("kwargs")
    if (
        not rows
        or kwargs is None
        or kwargs.null_count
        or not _supports(codec)
        or protocols.null_count
        or not compute.all(compute.equal(protocols, "FIX"), min_count=0).as_py()
    ):
        return None
    entries = compute.list_flatten(kwargs)
    tags = compute.struct_field(entries, "tag")
    keys = compute.struct_field(entries, "key")
    values = compute.struct_field(entries, "value")
    namespace = compute.struct_field(entries, "namespace")
    component = compute.struct_field(entries, "comp")
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
    parents = compute.list_parent_indices(kwargs).cast(pyarrow.int64())
    identities = compute.add(
        compute.multiply(parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
        tags.cast(pyarrow.int64()),
    )
    if len(compute.unique(identities)) != len(identities) or not _checksum_is_last(
        kwargs, parents, tags
    ):
        return None

    protocol_version, protocol_version_source = _versions(codec, kwargs, tags, values, rows)
    versions = compute.drop_null(compute.unique(protocol_version))
    if protocol_version.null_count or len(versions) != 1:
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
    resolved = _complete_tagged(codec, kwargs, version)
    lifted = _lifted_columns(resolved, fields, rows)
    if lifted is None:
        return None
    promoted, residual = lifted
    schema = shape._message_schema(batch.schema)
    output = dict(columns)
    output.update(
        {
            "protocol_code": protocols,
            "protocol_version": protocol_version,
            "protocol_version_source": protocol_version_source,
            "kwargs": residual,
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
    kwargs = columns.get("kwargs")
    rows = len(protocols)
    if not rows or kwargs is None or not _supports(codec):
        return
    entries = compute.list_flatten(kwargs)
    tags = compute.struct_field(entries, "tag")
    keys = compute.struct_field(entries, "key")
    values = compute.struct_field(entries, "value")
    namespace = compute.struct_field(entries, "namespace")
    component = compute.struct_field(entries, "comp")
    parents = compute.list_parent_indices(kwargs).cast(pyarrow.int64())

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
    eligible = compute.and_(compute.is_valid(kwargs), compute.equal(protocols, "FIX"))
    invalid_rows = _marked_rows(parents, compute.invert(good), rows)
    eligible = compute.and_(eligible, compute.invert(invalid_rows))
    eligible = compute.and_(eligible, compute.invert(_duplicate_rows(parents, tags, rows)))
    misplaced = _misplaced_checksum_rows(kwargs, parents, tags)
    eligible = compute.and_(eligible, compute.invert(misplaced))

    versions, _ = _versions(codec, kwargs, tags, values, rows)
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
    kwargs: pyarrow.Array, parents: pyarrow.Array, tags: pyarrow.Array
) -> pyarrow.Array:
    """Mark rows whose CheckSum is not their final field."""
    rows = len(kwargs)
    checksum = compute.fill_null(compute.equal(tags, 10), False)
    if not compute.any(checksum, min_count=0).as_py():
        return pyarrow.repeat(pyarrow.scalar(False), rows)
    sizes = compute.fill_null(compute.list_value_length(kwargs), 0).cast(pyarrow.int64())
    ends = compute.subtract(compute.cumulative_sum(sizes), 1)
    checksum_parents = compute.filter(parents, checksum)
    checksum_positions = compute.filter(sequence(len(tags)), checksum)
    misplaced = compute.not_equal(checksum_positions, compute.take(ends, checksum_parents))
    return compute.is_in(sequence(rows), value_set=compute.filter(checksum_parents, misplaced))


def _versions(
    codec: Any,
    kwargs: pyarrow.Array,
    tags: pyarrow.Array,
    values: pyarrow.Array,
    rows: int,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Resolve one common non-transport BeginString once for the whole batch."""
    begins = compute.equal(tags, 8)
    if compute.sum(begins).as_py() == rows:
        distinct = compute.unique(compute.filter(values, begins))
        if len(distinct) == 1:
            spelling = distinct[0].as_py()
            if not _version_key(spelling).startswith("FIXT"):
                version = codec.version_named(spelling)
                if version is not None:
                    return (
                        pyarrow.repeat(pyarrow.scalar(version), rows),
                        pyarrow.repeat(pyarrow.scalar(BEGIN_STRING_SOURCE), rows),
                    )
    return codec.versions_of_kwargs(kwargs)


def _complete_tagged(codec: Any, kwargs: pyarrow.Array, version: str) -> pyarrow.Array:
    """Canonicalize values whose numeric identities are already authoritative."""
    entries = compute.list_flatten(kwargs)
    tags = compute.struct_field(entries, "tag")
    keys = compute.struct_field(entries, "key")
    values = compute.struct_field(entries, "value")
    completed = pyarrow.StructArray.from_arrays(
        [
            tags,
            codec._canonical(keys, tags, version),
            codec._encoded(tags, values, version),
            compute.struct_field(entries, "namespace"),
            compute.struct_field(entries, "comp"),
        ],
        fields=list(entries.type),
    )
    return build_list(
        KWARGS,
        compute.list_value_length(kwargs).cast(pyarrow.int32()),
        completed,
    )


def _lifted_columns(
    kwargs: pyarrow.Array, fields: Mapping[int, Any], rows: int
) -> tuple[dict[str, pyarrow.Array], pyarrow.Array] | None:
    """Lift one occurrence per tag and retain raw text a typed column loses."""
    entries = compute.list_flatten(kwargs)
    parents = compute.list_parent_indices(kwargs).cast(pyarrow.int64())
    tags = compute.struct_field(entries, "tag")
    values = compute.struct_field(entries, "value")
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
        column = _cast(raw, fields[tag].arrow_type, TYPES[tag])
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
        [compute.filter(compute.struct_field(entries, field.name), keep) for field in entries.type],
        fields=list(entries.type),
    )
    residual = build_list(KWARGS, dense_counts(kept_parents, rows), kept)
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


def _checksum_is_last(kwargs: pyarrow.Array, parents: pyarrow.Array, tags: pyarrow.Array) -> bool:
    """Whether every present CheckSum is its row's final field."""
    checksum = compute.equal(tags, 10)
    if not compute.any(checksum, min_count=0).as_py():
        return True
    sizes = compute.list_value_length(kwargs).cast(pyarrow.int64())
    ends = compute.subtract(compute.cumulative_sum(sizes), 1)
    checksum_parents = compute.filter(parents, checksum)
    checksum_positions = compute.filter(sequence(len(tags)), checksum)
    expected = compute.take(ends, checksum_parents)
    return bool(compute.all(compute.equal(checksum_positions, expected), min_count=0).as_py())
