"""The strict Rekep boundary around native Yggdryl casts."""

import pyarrow
import pytest
import yggdryl

from rekep.fields import Field, strict_cast_batch, strict_cast_reader, strict_cast_table


def row_field() -> Field:
    return Field.from_arrow_schema(
        pyarrow.schema(
            [
                pyarrow.field("required", pyarrow.int64(), nullable=False),
                pyarrow.field("optional", pyarrow.string()),
            ]
        ),
        "Row",
    )


def generated_field() -> Field:
    year = Field("year", "int32", nullable=False)
    year.partition.sources = ["event"]
    year.partition.transform = "year"
    digest = Field("digest", "uint64", nullable=False)
    digest.digest["role"] = "holder"
    digest.digest["sources"] = '["event","year"]'
    return Field(
        "Row",
        yggdryl.DataType.from_fields([Field("event", "date32", nullable=False), year, digest]),
        nullable=False,
    )


def test_field_is_the_native_runtime_type() -> None:
    assert Field is yggdryl.Field
    assert type(row_field()) is yggdryl.Field


def test_strict_cast_rejects_missing_required_columns() -> None:
    batch = pyarrow.record_batch([pyarrow.array(["x"])], names=["optional"])
    with pytest.raises(ValueError, match="required.*missing"):
        strict_cast_batch(row_field(), batch)


def test_strict_cast_rejects_null_required_values() -> None:
    batch = pyarrow.record_batch(
        [pyarrow.array([1, None], pyarrow.int64()), pyarrow.array(["a", "b"])],
        names=["required", "optional"],
    )
    with pytest.raises(ValueError, match="not nullable"):
        strict_cast_batch(row_field(), batch)


def test_strict_cast_rejects_dictionary_values_that_decode_to_null() -> None:
    encoded = pyarrow.DictionaryArray.from_arrays(
        pyarrow.array([0, 1]), pyarrow.array([None, 7], pyarrow.int64())
    )
    batch = pyarrow.record_batch(
        [encoded, pyarrow.array(["a", "b"])], names=["required", "optional"]
    )

    with pytest.raises(ValueError, match="not nullable"):
        strict_cast_batch(row_field(), batch)


def test_strict_cast_rejects_missing_required_column_in_empty_table() -> None:
    table = pyarrow.table({"optional": pyarrow.array([], pyarrow.string())})

    with pytest.raises(ValueError, match="required.*missing"):
        strict_cast_table(row_field(), table)


def test_strict_cast_rejects_missing_required_column_in_empty_reader() -> None:
    source = pyarrow.RecordBatchReader.from_batches(
        pyarrow.schema([("optional", pyarrow.string())]), []
    )

    with pytest.raises(ValueError, match="required.*missing"):
        strict_cast_reader(row_field(), source)


def test_strict_cast_ignores_required_child_null_hidden_by_nullable_parent() -> None:
    child = pyarrow.field("child", pyarrow.int64(), nullable=False)
    parent = pyarrow.StructArray.from_arrays(
        [pyarrow.array([None, 1])],
        fields=[child],
        mask=pyarrow.array([True, False]),
    )
    batch = pyarrow.record_batch([parent], names=["parent"])
    target = Field.from_arrow_schema(
        pyarrow.schema([pyarrow.field("parent", parent.type, nullable=True)]),
        "Row",
    )

    assert strict_cast_batch(target, batch).equals(batch)


def test_strict_cast_ignores_missing_child_under_all_null_parent() -> None:
    source = pyarrow.StructArray.from_arrays(
        [pyarrow.array([None], pyarrow.int64())],
        fields=[pyarrow.field("other", pyarrow.int64())],
        mask=pyarrow.array([True]),
    )
    batch = pyarrow.record_batch([source], names=["parent"])
    target = Field.from_arrow_schema(
        pyarrow.schema(
            [
                pyarrow.field(
                    "parent",
                    pyarrow.struct([pyarrow.field("child", pyarrow.int64(), nullable=False)]),
                    nullable=True,
                )
            ]
        ),
        "Row",
    )

    cast = strict_cast_batch(target, batch)
    assert cast.column("parent").null_count == 1


def test_strict_reader_stays_lazy_and_uses_native_casts() -> None:
    source = pyarrow.RecordBatchReader.from_batches(
        pyarrow.schema([("required", pyarrow.int32())]),
        [pyarrow.record_batch([pyarrow.array([1, 2], pyarrow.int32())], names=["required"])],
    )
    reader = strict_cast_reader(row_field(), source)
    batch = reader.read_next_batch()
    assert batch.schema == row_field().into_arrow_schema()
    assert batch.column("required").to_pylist() == [1, 2]
    assert batch.column("optional").null_count == 2


def test_strict_batch_applies_native_partitions_before_digests() -> None:
    target = generated_field()
    source = pyarrow.record_batch({"event": pyarrow.array([19_723], pyarrow.date32())})

    applied = strict_cast_batch(target, source)

    assert applied.schema == target.into_arrow_schema()
    assert applied.column("year").to_pylist() == [2024]
    assert applied.column("digest").null_count == 0
    assert strict_cast_batch(target, applied).equals(applied)


def test_strict_table_applies_native_generated_columns() -> None:
    target = generated_field()
    source = pyarrow.table({"event": pyarrow.array([19_723, 20_089], pyarrow.date32())})

    applied = strict_cast_table(target, source)

    assert applied.column("year").to_pylist() == [2024, 2025]
    assert applied.column("digest").null_count == 0


def test_strict_reader_reports_and_applies_the_native_schema_lazily() -> None:
    target = generated_field()
    source_schema = pyarrow.schema([pyarrow.field("event", pyarrow.date32(), nullable=False)])
    source = pyarrow.RecordBatchReader.from_batches(
        source_schema,
        [pyarrow.record_batch({"event": pyarrow.array([19_723], pyarrow.date32())})],
    )

    applied = strict_cast_reader(target, source)

    assert applied.schema == target.into_arrow_schema()
    batch = applied.read_next_batch()
    assert batch.column("year").to_pylist() == [2024]
    assert batch.column("digest").null_count == 0


@pytest.mark.parametrize(
    ("metadata", "message"),
    [
        ({"partition:transform": "year"}, "partition:sources"),
        ({"digest:sources": '["event"]'}, "digest:sources.*digest holder"),
        ({"digest:algorithm": "xxh3"}, "digest:algorithm.*digest holder"),
    ],
)
@pytest.mark.parametrize("boundary", ["batch", "table", "reader"])
def test_strict_cast_routes_incomplete_protocols_to_native_validation(
    metadata: dict[str, str],
    message: str,
    boundary: str,
) -> None:
    generated = pyarrow.field("generated", pyarrow.int32(), nullable=False, metadata=metadata)
    target = Field.from_arrow_schema(
        pyarrow.schema([pyarrow.field("event", pyarrow.int32(), nullable=False), generated]),
        "Row",
    )
    source = pyarrow.record_batch(
        {"event": pyarrow.array([1], pyarrow.int32()), "generated": pyarrow.array([0])}
    )
    value = {
        "batch": source,
        "table": pyarrow.Table.from_batches([source]),
        "reader": pyarrow.RecordBatchReader.from_batches(source.schema, [source]),
    }[boundary]
    apply = {
        "batch": strict_cast_batch,
        "table": strict_cast_table,
        "reader": strict_cast_reader,
    }[boundary]

    with pytest.raises(ValueError, match=message):
        apply(target, value)


def test_layout_partition_marker_does_not_generate_a_missing_column() -> None:
    day = Field("day", "date32", nullable=False)
    day.set_partition(True)
    target = Field(
        "Row",
        yggdryl.DataType.from_fields([day]),
        nullable=False,
    )
    source = pyarrow.record_batch({"payload": pyarrow.array([1])})

    with pytest.raises(ValueError, match="day.*missing"):
        strict_cast_batch(target, source)


def test_required_generated_field_in_direct_nested_struct_is_materialized() -> None:
    generated = pyarrow.field(
        "year",
        pyarrow.int32(),
        nullable=False,
        metadata={"partition:sources": '["event"]', "partition:transform": "year"},
    )
    target_item = pyarrow.struct(
        [pyarrow.field("event", pyarrow.date32(), nullable=False), generated]
    )
    target = Field.from_arrow_schema(
        pyarrow.schema([pyarrow.field("item", target_item, nullable=False)]),
        "Row",
    )
    source_item = pyarrow.struct([pyarrow.field("event", pyarrow.date32(), nullable=False)])
    source = pyarrow.record_batch(
        [pyarrow.array([{"event": 19_723}], type=source_item)],
        names=["item"],
    )

    applied = strict_cast_batch(target, source)

    assert applied.column("item").field("year").to_pylist() == [2024]


@pytest.mark.parametrize("container", ["list", "map"])
@pytest.mark.parametrize("generated_value", [None, 0])
def test_protocols_below_list_and_map_elements_are_rejected(
    container: str,
    generated_value: int | None,
) -> None:
    generated = pyarrow.field(
        "year",
        pyarrow.int32(),
        nullable=False,
        metadata={"partition:sources": '["event"]', "partition:transform": "year"},
    )
    target_item = pyarrow.struct(
        [pyarrow.field("event", pyarrow.date32(), nullable=False), generated]
    )
    source_item = pyarrow.struct([pyarrow.field("event", pyarrow.date32(), nullable=False)])
    if container == "list":
        target_type = pyarrow.list_(target_item)
        row = {"event": 19_723}
        if generated_value is not None:
            source_item = target_item
            row["year"] = generated_value
        source_array = pyarrow.array([[row]], type=pyarrow.list_(source_item))
    else:
        target_type = pyarrow.map_(pyarrow.string(), target_item)
        row = {"event": 19_723}
        if generated_value is not None:
            source_item = target_item
            row["year"] = generated_value
        source_array = pyarrow.array(
            [[("x", row)]],
            type=pyarrow.map_(pyarrow.string(), source_item),
        )
    target = Field.from_arrow_schema(
        pyarrow.schema([pyarrow.field("items", target_type, nullable=False)]),
        "Row",
    )
    source = pyarrow.record_batch([source_array], names=["items"])

    with pytest.raises(ValueError, match=r"items\.(?:item|value)\.year.*below a list or map"):
        strict_cast_batch(target, source)
