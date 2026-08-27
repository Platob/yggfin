"""What a field's value is in Python, and what it is in bytes."""

import datetime
import decimal

import pyarrow
import pytest

from rekep import Field
from rekep.fields import BinaryField, DictionaryField, TimestampField


def field(dtype: pyarrow.DataType) -> Field:
    return Field(name="value", dtype=dtype)


@pytest.mark.parametrize(
    ("dtype", "given", "expected"),
    [
        (pyarrow.int32(), "42", 42),
        (pyarrow.int64(), 42.0, 42),
        (pyarrow.float64(), "1.5", 1.5),
        (pyarrow.bool_(), 1, True),
        (pyarrow.string(), 7, "7"),
        (pyarrow.binary(), bytearray(b"ab"), b"ab"),
        (pyarrow.decimal128(38, 9), "1.25", decimal.Decimal("1.25")),
    ],
)
def test_a_leaf_reads_as_the_python_type_it_stands_for(dtype, given, expected) -> None:
    assert field(dtype).cast_py(given) == expected
    assert type(field(dtype).cast_py(given)) is type(expected)


def test_an_instant_reads_as_a_datetime_on_its_own_clock() -> None:
    zoned = field(pyarrow.timestamp("us", tz="UTC"))
    assert zoned.cast_py(1_700_000_000_000_000) == datetime.datetime(
        2023, 11, 14, 22, 13, 20, tzinfo=datetime.UTC
    )
    naive = field(pyarrow.timestamp("s"))
    assert naive.cast_py(0) == datetime.datetime(1970, 1, 1)
    assert field(pyarrow.date32()).cast_py("2024-01-02") == datetime.date(2024, 1, 2)
    assert field(pyarrow.time64("us")).cast_py("01:02:03") == datetime.time(1, 2, 3)
    assert field(pyarrow.duration("s")).cast_py(90) == datetime.timedelta(seconds=90)


def test_a_null_stays_a_null() -> None:
    assert field(pyarrow.int32()).cast_py(None) is None
    assert field(pyarrow.int32()).cast_py(pyarrow.scalar(None, pyarrow.int32())) is None


def test_a_struct_reads_as_the_dataclass_its_declaration_spells() -> None:
    shape = field(pyarrow.struct([("mic", pyarrow.string()), ("size", pyarrow.int32())]))
    built = shape.cast_py({"mic": "XPAR", "size": "2"})
    assert (built.mic, built.size) == ("XPAR", 2)
    assert shape.cast_py(built) is built, "one already built is what it is"


def test_a_list_and_a_map_read_through_their_members() -> None:
    assert field(pyarrow.list_(pyarrow.int32())).cast_py(["1", 2]) == [1, 2]
    mapped = field(pyarrow.map_(pyarrow.string(), pyarrow.int32()))
    assert mapped.cast_py({"a": "1"}) == {"a": 1}


def test_a_scalar_comes_back_typed() -> None:
    assert field(pyarrow.int32()).cast_arrow_scalar("7") == pyarrow.scalar(7, pyarrow.int32())
    already = pyarrow.scalar(7, pyarrow.int64())
    assert field(pyarrow.int32()).cast_arrow_scalar(already).type == pyarrow.int32()


# -- bytes -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dtype", "given", "expected"),
    [
        (pyarrow.int8(), 1, b"\x01"),
        (pyarrow.int16(), 1, b"\x00\x01"),
        (pyarrow.int32(), 1, b"\x00\x00\x00\x01"),
        (pyarrow.int64(), 1, b"\x00" * 7 + b"\x01"),
        (pyarrow.int32(), -1, b"\xff\xff\xff\xff"),
        (pyarrow.bool_(), True, b"\x01"),
        (pyarrow.string(), "ab", b"ab"),
        (pyarrow.binary(), b"ab", b"ab"),
    ],
)
def test_a_number_is_exactly_its_width_big_endian(dtype, given, expected) -> None:
    assert field(dtype).into_bytes(given) == expected


def test_a_float_is_its_ieee_bytes_at_its_own_width() -> None:
    assert field(pyarrow.float64()).into_bytes(1.5) == bytes.fromhex("3ff8000000000000")
    assert field(pyarrow.float32()).into_bytes(1.5) == bytes.fromhex("3fc00000")


def test_an_instant_is_its_epoch_integer_in_the_declared_unit() -> None:
    when = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=datetime.UTC)
    micros = field(pyarrow.timestamp("us", tz="UTC")).into_bytes(when)
    assert int.from_bytes(micros, "big", signed=True) == 1_700_000_000_000_000
    seconds = field(pyarrow.timestamp("s")).into_bytes(when)
    assert int.from_bytes(seconds, "big", signed=True) == 1_700_000_000
    day = field(pyarrow.date32()).into_bytes(datetime.date(1970, 1, 2))
    assert int.from_bytes(day, "big", signed=True) == 1


def test_a_nested_value_is_its_members_concatenated() -> None:
    shape = field(pyarrow.struct([("mic", pyarrow.string()), ("size", pyarrow.int32())]))
    assert shape.into_bytes({"mic": "XPAR", "size": 2}) == b"XPAR" + b"\x00\x00\x00\x02"
    listed = field(pyarrow.list_(pyarrow.int32()))
    assert listed.into_bytes([1, 2]) == b"\x00\x00\x00\x01\x00\x00\x00\x02"
    mapped = field(pyarrow.map_(pyarrow.string(), pyarrow.int32()))
    assert mapped.into_bytes({"a": 1}) == b"a" + b"\x00\x00\x00\x01"
    assert shape.into_bytes(None) == b""


# -- the kinds ---------------------------------------------------------------


def test_a_binary_declaration_is_a_binary_field() -> None:
    for dtype in (pyarrow.binary(), pyarrow.large_binary(), pyarrow.binary(4)):
        assert isinstance(field(dtype), BinaryField)
    assert field(pyarrow.binary()).cast_py(bytearray(b"x")) == b"x"


def test_a_dictionary_declaration_isolates_its_own_casts() -> None:
    declared = field(pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8()))
    assert isinstance(declared, DictionaryField)
    assert declared.index_type == pyarrow.int32()
    assert declared.value_type == pyarrow.utf8()
    assert declared.cast_py(7) == "7"
    assert declared.into_bytes("ab") == b"ab"
    encoded = declared.cast_arrow_array(pyarrow.array(["a", "b", "a"]))
    assert encoded.type == declared.dtype
    assert encoded.to_pylist() == ["a", "b", "a"]
    assert declared.cast_arrow_array(encoded) is encoded


def test_a_timestamp_declaration_keeps_its_clock_casts() -> None:
    assert isinstance(field(pyarrow.timestamp("ns")), TimestampField)
    assert field(pyarrow.timestamp("ns")).unit == "ns"
