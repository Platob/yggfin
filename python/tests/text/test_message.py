"""The protocol-neutral row produced by text files."""

from pathlib import Path

import pyarrow

from rekep import Kwarg, Message, TextFile
from rekep.enums import EventType
from rekep.market import Event


def test_a_message_adds_log_provenance_and_generic_arguments() -> None:
    assert issubclass(Message, Event)
    assert Message.into_field().names == [
        *Event.into_field().names,
        "source_url",
        "source_rownum",
        "thread_name",
        "plugin_code",
        "message",
        "kwargs",
    ]
    assert all(
        not any(key.startswith("fix:") for key in field.metadata)
        for field in Message.into_field().fields
    )


def test_kwarg_is_the_required_ordered_argument_shape() -> None:
    field = Kwarg.into_field()
    assert field.names == ["key", "value"]
    assert field.field("key").nullable is False
    assert field.field("value").nullable is False
    assert Message.into_field().field("kwargs").item.arrow_type == field.arrow_type


def test_a_message_always_has_a_non_null_argument_list() -> None:
    field = Message.into_field().field("kwargs")

    assert field.nullable is False
    assert Message().kwargs == []
    assert Message(kwargs=None).kwargs == []  # type: ignore[arg-type]


def test_generic_arguments_keep_mixed_separators_repeats_and_spelling() -> None:
    parsed = Kwarg.parse_arrow(
        pyarrow.array(
            [
                "8=FIX.4.4|55=A|55=B|10=000|",
                "toBridge #SIDE=1;#SIDE=2",
                "8=FIX.4.4\x0135=D\x0110=000\x01",
                "plain text",
            ]
        )
    ).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed[0]] == [
        ("8", "FIX.4.4"),
        ("55", "A"),
        ("55", "B"),
        ("10", "000"),
    ]
    assert [(entry["key"], entry["value"]) for entry in parsed[1]] == [
        ("#SIDE", "1"),
        ("#SIDE", "2"),
    ]
    assert [entry["key"] for entry in parsed[2]] == ["8", "35", "10"]
    assert parsed[3] == []


def test_generic_arguments_keep_prefixes_whitespace_and_empty_values() -> None:
    parsed = Kwarg.parse_arrow(
        pyarrow.array(
            [
                "prefix ACCOUNT=A CLIENTID=B",
                "A=|B=2|",
            ]
        )
    ).to_pylist()

    assert [[(entry["key"], entry["value"]) for entry in row] for row in parsed] == [
        [("ACCOUNT", "A"), ("CLIENTID", "B")],
        [("A", ""), ("B", "2")],
    ]
    assert all(entry["value"] is not None for row in parsed for entry in row)


def test_an_indexed_group_can_be_the_first_argument() -> None:
    (parsed,) = Kwarg.parse_arrow(
        pyarrow.array(["#NoPartyIDs[0]=PartyID=A\x01PartyRole=1|#B=2"])
    ).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed] == [
        ("#NoPartyIDs[0]", "PartyID=A\x01PartyRole=1"),
        ("#B", "2"),
    ]


def test_a_single_argument_does_not_keep_its_trailing_delimiter() -> None:
    (parsed,) = Kwarg.parse_arrow(pyarrow.array(["Only=one|"])).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed] == [("Only", "one")]


def test_hash_separators_distinguish_wire_delimiters_from_marked_keys() -> None:
    parsed = Kwarg.parse_arrow(pyarrow.array(["8=FIX.4.4#35=D#10=000", "#A=1#B=2"])).to_pylist()

    assert [entry["key"] for entry in parsed[0]] == ["8", "35", "10"]
    assert [entry["key"] for entry in parsed[1]] == ["#A", "#B"]


def test_a_numeric_key_too_wide_for_a_tag_remains_a_key() -> None:
    (parsed,) = Kwarg.parse_arrow(pyarrow.array(["1234567890=wide|Other=kept|"])).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed] == [
        ("1234567890", "wide"),
        ("Other", "kept"),
    ]


def test_generic_arguments_preserve_numeric_dotted_members() -> None:
    (parsed,) = Kwarg.parse_arrow(pyarrow.array(["54.5=x|NoPartyIDs[0].448=A|Side=1"])).to_pylist()

    assert [entry["key"] for entry in parsed] == [
        "54.5",
        "NoPartyIDs[0].448",
        "Side",
    ]


def test_fix_whitespace_around_equals_is_not_a_field_separator() -> None:
    parsed = Kwarg.parse_arrow(
        pyarrow.array(["Side\x0b=1|Price=2", "N[0]=\x0bM=v|Side=1"])
    ).to_pylist()

    assert [[(entry["key"], entry["value"]) for entry in row] for row in parsed] == [
        [("Side", "1"), ("Price", "2")],
        [("N[0]", "M=v"), ("Side", "1")],
    ]


def test_separator_padding_is_ascii_and_does_not_strip_unicode_text() -> None:
    parsed = Kwarg.parse_arrow(
        pyarrow.array(
            [
                "8=FIX.4.4| 54=1|55=X|10=000|",
                "8=FIX.4.4|\t54=1|55=X|10=000|",
                "8=FIX.4.4|\u00a054=1|55=X|10=000|",
                "Sending order to venue#CLORDID=ORD-1|#SIDE=1",
            ]
        )
    ).to_pylist()

    assert [[entry["key"] for entry in row] for row in parsed] == [
        ["8", "54", "55", "10"],
        ["8", "54", "55", "10"],
        ["8", "55", "10"],
        ["#CLORDID", "#SIDE"],
    ]


def test_generic_arguments_do_not_apply_fix_checksum_semantics() -> None:
    (parsed,) = Kwarg.parse_arrow(
        pyarrow.array(["8=FIX.4.4|10=000|55=AFTER-CHECKSUM|"])
    ).to_pylist()
    assert [(entry["key"], entry["value"]) for entry in parsed] == [
        ("8", "FIX.4.4"),
        ("10", "000"),
        ("55", "AFTER-CHECKSUM"),
    ]


def test_raw_identity_is_scoped_to_the_payload_and_its_source() -> None:
    first = Message(message="same", source_url="one.log", source_rownum=2).identify()
    again = Message(message="same", source_url="one.log", source_rownum=2).identify()
    copied = Message(message="same", source_url="two.log", source_rownum=2).identify()

    assert first.hash == again.hash == first.xhash
    assert copied.hash != first.hash


def test_a_text_file_does_not_interpret_fix_fields(tmp_path: Path) -> None:
    path = tmp_path / "capture.log"
    payload = "8=FIX.4.4|35=D|49=XPAR|56=BUY|55=IBM|10=000"
    path.write_text(f"2026-08-14 09:30:00.123 [thread] [bridge] (INFO) {payload}\n")

    with TextFile.from_path(path) as source:
        table = source.read_arrow_table()

    assert table.schema.names == Message.into_field().names
    assert table.column("message").to_pylist() == [payload]
    assert [(entry["key"], entry["value"]) for entry in table.column("kwargs")[0].as_py()] == [
        ("8", "FIX.4.4"),
        ("35", "D"),
        ("49", "XPAR"),
        ("56", "BUY"),
        ("55", "IBM"),
        ("10", "000"),
    ]
    assert table.column("etype").to_pylist() == [int(EventType.UNKNOWN)]
    assert table.column("mic").to_pylist() == [None]
    assert table.column("hash").to_pylist() == table.column("xhash").to_pylist()
    assert table.column("hash")[0].as_py() == Message.hash_of(
        payload, table.column("source_url")[0].as_py(), 1
    )
