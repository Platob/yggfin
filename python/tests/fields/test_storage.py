"""The storage boundary retains unsigned bits through nested Arrow buffers."""

from __future__ import annotations

import pyarrow
import pytest

from rekep.fields import Field, stored_arrow_reader
from rekep.fix import _plain, _stored


def stored(array: pyarrow.Array, dtype: pyarrow.DataType) -> pyarrow.Array:
    source = pyarrow.record_batch([array], names=["values"])
    field = Field.from_arrow_schema(pyarrow.schema([pyarrow.field("values", dtype)]))
    reader = stored_arrow_reader(
        pyarrow.RecordBatchReader.from_batches(source.schema, [source]), field
    )
    return reader.read_next_batch().column(0)


class ApplyBoundary:
    """Capture the prepared reader before the native cast owns its buffers."""

    def __init__(self, schema: pyarrow.Schema) -> None:
        self.schema = schema

    def into_arrow_schema(self) -> pyarrow.Schema:
        return self.schema

    def apply_arrow_reader(
        self, source: pyarrow.RecordBatchReader, *, safe: bool, nullability: str
    ) -> pyarrow.RecordBatchReader:
        assert safe is False
        assert nullability == "strict"
        return source


def viewed(array: pyarrow.Array, dtype: pyarrow.DataType) -> pyarrow.Array:
    source = pyarrow.record_batch([array], names=["values"])
    boundary = ApplyBoundary(pyarrow.schema([pyarrow.field("values", dtype)]))
    reader = stored_arrow_reader(
        pyarrow.RecordBatchReader.from_batches(source.schema, [source]), boundary
    )
    return reader.read_next_batch().column(0)


@pytest.mark.parametrize("nested", ["struct", "list", "large_list", "map"])
def test_nested_unsigned_bits_keep_nulls_slices_and_buffers(nested: str) -> None:
    unsigned = pyarrow.struct([("hash", pyarrow.uint64()), ("label", pyarrow.string())])
    signed = pyarrow.struct([("hash", pyarrow.int64()), ("label", pyarrow.string())])
    high = {"hash": 2**64 - 1, "label": "high"}
    low = {"hash": 3, "label": None}
    negative = {"hash": -1, "label": "high"}
    if nested == "struct":
        source_type = pyarrow.struct([("entry", unsigned)])
        target_type = pyarrow.struct([("entry", signed)])
        values = [{"entry": low}, {"entry": high}, None, {"entry": None}]
        expected = [{"entry": negative}, None, {"entry": None}]
    elif nested == "map":
        source_type = pyarrow.map_(pyarrow.string(), unsigned)
        target_type = pyarrow.map_(pyarrow.string(), signed)
        values = [[("before", low)], [("one", high), ("two", None)], None, []]
        expected = [[("one", negative), ("two", None)], None, []]
    else:
        factory = pyarrow.list_ if nested == "list" else pyarrow.large_list
        source_type = factory(unsigned)
        target_type = factory(signed)
        values = [[low], [high, None], None, []]
        expected = [[negative, None], None, []]
    complete = pyarrow.array(values, type=source_type)
    source = complete.slice(1)

    result = viewed(source, target_type)

    assert result.to_pylist() == expected
    assert result.offset == source.offset
    assert [buffer.address if buffer else None for buffer in result.buffers()] == [
        buffer.address if buffer else None for buffer in source.buffers()
    ]
    assert stored(complete, target_type).slice(1).to_pylist() == expected


def test_nested_list_and_map_unsigned_keys_are_reinterpreted_once() -> None:
    source_type = pyarrow.list_(pyarrow.map_(pyarrow.uint64(), pyarrow.list_(pyarrow.uint64())))
    target_type = pyarrow.list_(pyarrow.map_(pyarrow.int64(), pyarrow.list_(pyarrow.int64())))
    complete = pyarrow.array([None, [[(2**63, [2**64 - 1, None, 0])]], []], type=source_type)
    source = complete.slice(1)

    result = viewed(source, target_type)

    expected = [[[(-(2**63), [-1, None, 0])]], []]
    assert result.to_pylist() == expected
    assert [buffer.address if buffer else None for buffer in result.buffers()] == [
        buffer.address if buffer else None for buffer in source.buffers()
    ]
    assert stored(complete, target_type).slice(1).to_pylist() == expected


def test_a_sliced_struct_reaches_the_native_apply_with_signed_bits() -> None:
    source = pyarrow.array(
        [{"hash": 0}, {"hash": 2**64 - 1}, None, {"hash": None}],
        type=pyarrow.struct([("hash", pyarrow.uint64())]),
    ).slice(1)
    assert stored(source, pyarrow.struct([("hash", pyarrow.int64())])).to_pylist() == [
        {"hash": -1},
        None,
        {"hash": None},
    ]


def test_an_unchanged_reader_reaches_native_apply_directly() -> None:
    schema = pyarrow.schema([("values", pyarrow.list_(pyarrow.int64()))])
    source = pyarrow.RecordBatchReader.from_batches(schema, [])
    assert stored_arrow_reader(source, ApplyBoundary(schema)) is source


def test_top_level_unsigned_bits_and_ordinary_columns_keep_their_buffers() -> None:
    for source, dtype, expected in [
        (
            pyarrow.array([0, 2**63, None, 2**64 - 1], type=pyarrow.uint64()),
            pyarrow.int64(),
            [0, -(2**63), None, -1],
        ),
        (pyarrow.array([1, None, 3], type=pyarrow.int64()), pyarrow.int64(), [1, None, 3]),
    ]:
        result = stored(source, dtype)
        assert result.to_pylist() == expected
        assert [buffer.address if buffer else None for buffer in result.buffers()] == [
            buffer.address if buffer else None for buffer in source.buffers()
        ]


def test_unsigned_narrower_than_storage_is_widened_without_reinterpreting() -> None:
    source = pyarrow.array([0, 2**32 - 1, None], type=pyarrow.uint32())
    assert stored(source, pyarrow.int64()).to_pylist() == [0, 2**32 - 1, None]


def test_empty_reader_keeps_the_declared_nested_storage_schema() -> None:
    source = pyarrow.schema([("values", pyarrow.list_(pyarrow.uint64()))])
    target = pyarrow.schema([("values", pyarrow.list_(pyarrow.int64()))])
    field = Field.from_arrow_schema(target)
    reader = stored_arrow_reader(pyarrow.RecordBatchReader.from_batches(source, []), field)

    assert reader.schema.equals(field.into_arrow_schema(), check_metadata=True)
    assert reader.read_all().num_rows == 0


def test_semantic_child_metadata_is_removed_at_every_nested_field() -> None:
    metadata = {
        b"ARROW:extension:name": b"yggdryl.currency",
        b"ARROW:extension:metadata": b"{}",
        b"description": b"kept",
    }
    item = pyarrow.field("item", pyarrow.uint64(), metadata=metadata)
    key = pyarrow.field("key", pyarrow.string(), nullable=False, metadata=metadata)
    source = pyarrow.field(
        "root",
        pyarrow.struct(
            [
                pyarrow.field("small", pyarrow.list_(item), metadata=metadata),
                pyarrow.field("large", pyarrow.large_list(item), metadata=metadata),
                pyarrow.field(
                    "mapping", pyarrow.map_(key, item, keys_sorted=True), metadata=metadata
                ),
            ]
        ),
        metadata=metadata,
    )

    result = _plain(source)

    assert result.metadata == {b"description": b"kept"}
    for child in result.type:
        assert child.metadata == {b"description": b"kept"}
    for name in ("small", "large"):
        item = result.type.field(name).type.value_field
        assert item.metadata == {b"description": b"kept"}
        assert item.type == pyarrow.int64()
    mapping = result.type.field("mapping").type
    assert mapping.keys_sorted
    assert mapping.key_field.metadata == mapping.item_field.metadata == {b"description": b"kept"}
    assert mapping.item_type == pyarrow.int64()
    assert _stored(source.type).equals(result.type, check_metadata=True)
