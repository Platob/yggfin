"""The FIX datatype projection, and the deliberately forgiving value readings."""

import datetime

import pyarrow
import pytest

import rekep.fix.fields
import rekep.market
import rekep.market.fix
from rekep.fix import arrow_type_of, cast_arrow_bool, cast_arrow_fix, fix_field, unix_of

# -- datatypes ---------------------------------------------------------------


def test_char_is_a_string_not_one_character() -> None:
    assert arrow_type_of("char") == pyarrow.string()


@pytest.mark.parametrize(
    ("datatype", "expected"),
    [
        ("Boolean", pyarrow.bool_()),
        ("int", pyarrow.int64()),
        ("SeqNum", pyarrow.int64()),
        ("NumInGroup", pyarrow.int64()),
        ("Price", pyarrow.float64()),
        ("Qty", pyarrow.float64()),
        ("String", pyarrow.string()),
        ("Currency", pyarrow.string()),
        ("UTCTimestamp", pyarrow.timestamp("ns")),
        ("LocalMktDate", pyarrow.date32()),
        ("UTCTimeOnly", pyarrow.time64("ns")),
        ("data", pyarrow.binary()),
    ],
)
def test_the_projection_by_datatype(datatype: str, expected: pyarrow.DataType) -> None:
    assert arrow_type_of(datatype) == expected


def test_spelling_is_forgiven_and_the_unknown_is_a_string() -> None:
    assert arrow_type_of("BOOLEAN") == pyarrow.bool_()
    assert arrow_type_of(" price ") == pyarrow.float64()
    assert arrow_type_of("SomeVendorThing") == pyarrow.string()
    assert arrow_type_of(None) == pyarrow.string()
    assert arrow_type_of("") == pyarrow.string()


def test_the_timezone_carrying_types_stay_text() -> None:
    """A naive Arrow type would drop the offset that is part of the value."""
    assert arrow_type_of("TZTimestamp") == pyarrow.string()
    assert arrow_type_of("TZTimeOnly") == pyarrow.string()


# -- building ----------------------------------------------------------------


def test_a_fix_field_is_a_generic_field_with_fix_metadata() -> None:
    built = fix_field(
        "Side",
        54,
        "char",
        description="Side of order.",
        version="4.4",
        values={"1": "Buy", "2": "Sell"},
    )
    assert built.name == "Side"
    assert built.arrow_type == pyarrow.string()
    assert built.nullable, "required-ness belongs to messages, not to the field"
    assert built.description == "Side of order."
    assert built.fix["tag"] == "54"
    assert built.fix["type"] == "char"
    assert built.fix["version"] == "4.4"
    assert '"1":"Buy"' in built.fix["values"]


# -- booleans ----------------------------------------------------------------


def test_the_standard_and_the_common_spellings_all_read() -> None:
    spelled = ["Y", "n", "TRUE", "faLse", "yes", "No", "oui", "NON", "1", "0", "on", "off"]
    expected = [True, False, True, False, True, False, True, False, True, False, True, False]
    assert cast_arrow_bool(pyarrow.array(spelled)).to_pylist() == expected


def test_what_is_not_a_flag_reads_null_never_guessed() -> None:
    assert cast_arrow_bool(pyarrow.array(["maybe", "", None, "2"])).to_pylist() == [
        None,
        None,
        None,
        None,
    ]


def test_whitespace_is_trimmed_first() -> None:
    assert cast_arrow_bool(pyarrow.array([" yes ", "\tN"])).to_pylist() == [True, False]


def test_an_already_boolean_column_is_untouched() -> None:
    column = pyarrow.array([True, None, False])
    assert cast_arrow_bool(column) is column


def test_a_chunked_column_casts_chunk_by_chunk() -> None:
    chunked = pyarrow.chunked_array([["Y"], ["non"]])
    assert cast_arrow_bool(chunked).to_pylist() == [True, False]


# -- reading a column ---------------------------------------------------------


def test_text_declared_as_text_is_handed_back_untouched() -> None:
    """`check_sum` is three digits with leading zeros, so nothing may narrow it."""
    column = pyarrow.array(["FIX.4.2", "007", "", None])
    assert cast_arrow_fix(column, pyarrow.string()) is column


def test_a_flag_column_reads_as_a_boolean_and_a_word_that_is_not_one_reads_null() -> None:
    read = cast_arrow_fix(pyarrow.array(["Y", "N", "maybe", None]), pyarrow.bool_())
    assert read.type == pyarrow.bool_()
    assert read.to_pylist() == [True, False, None, None]


def test_digits_read_as_an_integer_and_everything_else_reads_null() -> None:
    """`body_length` is int64 and the wire is text: a bad one may not cost the batch."""
    spelled = ["176", "-3", "12x", "", None]
    read = cast_arrow_fix(pyarrow.array(spelled), pyarrow.int64()).to_pylist()
    assert read == [176, -3, None, None, None]
    assert read.count(None) == 3, "a value the type cannot hold costs its own row and no other"


def test_a_leading_plus_is_a_valid_fix_integer() -> None:
    read = cast_arrow_fix(pyarrow.array(["+1", "-2", "3"]), pyarrow.int64())
    assert read.to_pylist() == [1, -2, 3]


def test_a_number_wider_than_the_target_reads_null_where_arrow_itself_raises() -> None:
    """The range guard preserves both int64 limits and nulls only overflow."""
    spelled = [
        "9223372036854775807",
        "9223372036854775808",
        "-9223372036854775808",
        "-9223372036854775809",
        "7",
    ]
    with pytest.raises(pyarrow.ArrowInvalid):
        pyarrow.array(spelled).cast(pyarrow.int64())
    assert cast_arrow_fix(pyarrow.array(spelled), pyarrow.int64()).to_pylist() == [
        9223372036854775807,
        None,
        -9223372036854775808,
        None,
        7,
    ]


def test_a_price_reads_as_a_float_and_a_word_beside_it_reads_null() -> None:
    read = cast_arrow_fix(pyarrow.array(["1200", "1.5", "abc", None]), pyarrow.float64())
    assert read.to_pylist() == [1200.0, 1.5, None, None]


def test_a_stamp_a_date_and_a_time_of_day_each_read_as_the_type_declared() -> None:
    """One FIX spelling per temporal type, and the fraction keeps its own scale."""
    stamp = cast_arrow_fix(pyarrow.array(["20260821-10:30:00.123456789"]), pyarrow.timestamp("ns"))
    nanos = stamp.cast(pyarrow.int64()).to_pylist()
    assert nanos == [unix_of("20260821-10:30:00.123456789")]
    assert nanos == [1787308200123456789], "to the nanosecond, not to the microsecond"
    date = cast_arrow_fix(pyarrow.array(["20260821", "20260821-10:30:00"]), pyarrow.date32())
    assert date.to_pylist() == [datetime.date(2026, 8, 21)] * 2
    clock = cast_arrow_fix(pyarrow.array(["10:30:00.5"]), pyarrow.time64("ns"))
    assert clock.to_pylist() == [datetime.time(10, 30, 0, 500000)]
    seconds = cast_arrow_fix(pyarrow.array(["10:30:00.5"]), pyarrow.time32("s"))
    millis = cast_arrow_fix(pyarrow.array(["10:30:00.5"]), pyarrow.time32("ms"))
    assert seconds.to_pylist() == [datetime.time(10, 30)]
    assert millis.to_pylist() == [datetime.time(10, 30, 0, 500000)]


def test_binary_keeps_the_bytes_the_line_carried() -> None:
    """`secure_data`, `xml_data` and `signature` are binary; the wire is still text."""
    read = cast_arrow_fix(pyarrow.array(["<x>1</x>", None]), pyarrow.binary())
    assert read.to_pylist() == [b"<x>1</x>", None]


def test_the_column_reading_answers_what_the_value_reading_does(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The vectorised path is what a good column takes, so the scalar one is made to raise."""
    spelled = [
        "20260821-10:30:00.123456789",
        "20260821T10:30:00",
        "20260821 10:30:00Z",
        "  20260821-10:30:00  ",
        "20260821",
        "10:30:00.5",
        "nope",
        "",
        None,
    ]
    expected = [unix_of(one) for one in spelled]
    assert expected.count(None) == 3, "what is not a stamp reads null, never the epoch"
    monkeypatch.setattr(rekep.fix.fields, "unix_of", _unreachable)
    read = cast_arrow_fix(pyarrow.array(spelled), pyarrow.timestamp("ns"))
    assert read.cast(pyarrow.int64()).to_pylist() == expected


def test_a_bad_civil_time_nulls_only_its_row_without_scalar_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spelled = [
        "20260230-00:00:00",
        "20260821-10:30:00",
        "20250229",
        "00000101",
        "20260821",
        "20260630-23:59:60",
        "20260821-24:00:00",
        "20260821-23:60:00",
        "20260821-23:59:61",
    ]
    expected = [unix_of(one) for one in spelled]
    with pytest.raises(pyarrow.ArrowInvalid):
        pyarrow.array(["2026-02-30T00:00:00.0"]).cast(pyarrow.timestamp("ns"))
    monkeypatch.setattr(rekep.fix.fields, "unix_of", _unreachable)
    read = cast_arrow_fix(pyarrow.array(spelled), pyarrow.timestamp("ns")).cast(pyarrow.int64())
    assert read.to_pylist() == expected
    assert read.null_count == 6, "only the impossible dates and clocks"
    assert read[1].as_py() == 1787308200000000000


def test_a_valid_date_outside_nanosecond_range_reads_null_without_overflow() -> None:
    read = cast_arrow_fix(
        pyarrow.array(["99991231-23:59:59", "20260821-10:30:00"]),
        pyarrow.timestamp("ns"),
    )
    assert read.is_null().to_pylist() == [True, False]


def test_a_valid_date_uses_the_destination_temporal_range() -> None:
    spelled = pyarrow.array(["16000101-00:00:00.123456789", "99991231-23:59:59.987654321"])
    dates = cast_arrow_fix(spelled, pyarrow.date32())
    assert dates.to_pylist() == [datetime.date(1600, 1, 1), datetime.date(9999, 12, 31)]
    stamps = cast_arrow_fix(spelled, pyarrow.timestamp("us"))
    assert stamps.to_pylist() == [
        datetime.datetime(1600, 1, 1, 0, 0, 0, 123457),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 987654),
    ]


@pytest.mark.parametrize(
    "arrow_type", [pyarrow.date32(), pyarrow.timestamp("s"), pyarrow.timestamp("us")]
)
def test_year_zero_is_not_a_valid_fix_date(arrow_type: pyarrow.DataType) -> None:
    read = cast_arrow_fix(pyarrow.array(["00000101", "00000101-00:00:00"]), arrow_type)
    assert read.null_count == 2


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_a_pre_epoch_fraction_keeps_arrow_timestamp_narrowing(unit: str) -> None:
    spelled = "19691231-23:59:59.999999999"
    expected = (
        pyarrow.array([unix_of(spelled)], pyarrow.int64())
        .cast(pyarrow.timestamp("ns"))
        .cast(pyarrow.timestamp(unit), safe=False)
    )
    assert cast_arrow_fix(pyarrow.array([spelled]), pyarrow.timestamp(unit)).equals(expected)


def test_a_chunked_column_is_read_chunk_by_chunk_and_stays_chunked() -> None:
    read = cast_arrow_fix(pyarrow.chunked_array([["176"], ["12x"]]), pyarrow.int64())
    assert read.num_chunks == 2
    assert read.to_pylist() == [176, None]


def test_an_empty_column_still_comes_back_as_the_type_asked_for() -> None:
    """Zero rows is the boundary every kernel here has to cross without a shape."""
    read = cast_arrow_fix(pyarrow.array([], type=pyarrow.string()), pyarrow.timestamp("ns"))
    assert read.type == pyarrow.timestamp("ns")
    assert len(read) == 0


def test_the_value_reading_is_one_function_under_every_name_it_is_imported_by() -> None:
    """`unix_of` moved into `rekep.fix.fields`; `rekep.market` still hands out that one."""
    assert rekep.market.fix.unix_of is rekep.fix.fields.unix_of
    assert rekep.market.unix_of is rekep.fix.fields.unix_of
    assert unix_of is rekep.fix.fields.unix_of


# -- helpers ------------------------------------------------------------------


def _unreachable(*_arguments: object, **_named: object) -> int:
    """Stands in for the scalar reading where the vectorised one must have answered."""
    raise AssertionError("the scalar reading is the fallback, not the path a good column takes")
