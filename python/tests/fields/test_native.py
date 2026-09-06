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
