"""`TimestampField`: the clock castings, spelled once and parametrized."""

import pyarrow
import pytest

from rekep.fields import Field, TimestampField


def test_a_timestamp_declaration_answers_as_a_timestamp_field() -> None:
    built = Field(name="stamp", dtype=pyarrow.timestamp("us", tz="UTC"))
    assert isinstance(built, TimestampField)
    assert built.unit == "us"
    assert built.timezone == "UTC"
    naive = TimestampField.of("ns", name="t")
    assert naive.dtype == pyarrow.timestamp("ns")
    assert naive.timezone is None


def test_the_factor_table_is_the_one_every_cast_shares() -> None:
    assert TimestampField.factor_of("s") == 1_000_000_000
    assert TimestampField.factor_of("ms") == 1_000_000
    assert TimestampField.factor_of("us") == 1_000
    assert TimestampField.factor_of("ns") == 1
    with pytest.raises(ValueError, match="unit"):
        TimestampField.factor_of("weeks")


def test_epoch_integers_round_trip_through_a_declared_clock() -> None:
    nanos = pyarrow.array([1_700_000_000_000_000_000, 0, -1_000], pyarrow.int64())
    stamps = TimestampField.of("us", "UTC").from_unix_arrow(nanos)
    assert stamps.type == pyarrow.timestamp("us", tz="UTC")
    back = TimestampField.into_unix_arrow(stamps)
    assert back.to_pylist() == [1_700_000_000_000_000_000, 0, -1_000]
    assert TimestampField.into_unix_arrow(stamps, "us").to_pylist() == [
        1_700_000_000_000_000,
        0,
        -1,
    ]


def test_a_same_unit_read_neither_scales_nor_copies_meaning() -> None:
    micros = pyarrow.array([5], pyarrow.int64())
    stamps = TimestampField.of("us").from_unix_arrow(micros, unit="us")
    assert stamps.cast(pyarrow.int64()).to_pylist() == [5]


def test_a_zoned_column_drops_its_zone_without_a_shift() -> None:
    """The stored ticks are epoch-anchored either way; the zone is a reading."""
    zoned = pyarrow.array([1_700_000_000_000_000], pyarrow.timestamp("us", tz="Europe/Paris"))
    assert TimestampField.into_unix_arrow(zoned).to_pylist() == [1_700_000_000_000_000_000]


def test_a_second_clock_widens_exactly() -> None:
    seconds = pyarrow.array([1_700_000_000], pyarrow.timestamp("s"))
    assert TimestampField.into_unix_arrow(seconds).to_pylist() == [1_700_000_000_000_000_000]
    assert TimestampField.into_unix_arrow(seconds, "ms").to_pylist() == [1_700_000_000_000]


def test_replacing_the_dtype_re_dispatches_the_kind() -> None:
    """`dataclasses.replace` follows the type, so a retyped field equals a
    fresh declaration instead of staying a mislabeled timestamp."""
    import dataclasses

    stamp = Field(name="when", dtype=pyarrow.timestamp("ns"))
    retyped = dataclasses.replace(stamp, dtype=pyarrow.date32())
    assert type(retyped) is Field
    assert retyped == Field(name="when", dtype=pyarrow.date32())
    still = dataclasses.replace(stamp, name="later")
    assert isinstance(still, TimestampField) and still.unit == "ns"
