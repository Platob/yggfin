"""The FIX datatype projection, and the deliberately forgiving value readings."""

import pyarrow
import pytest

from rekep.fix import arrow_type_of, cast_arrow_bool, fix_field

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
