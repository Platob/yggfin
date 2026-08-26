"""The protocol-neutral row produced by text files."""

from pathlib import Path

import pyarrow

import rekep.text.kwargs as kwargs_module
from rekep import FixRegistry, Kwarg, Message, TextFile
from rekep.enums import EventType
from rekep.market import Event, hash_bytes


def test_a_message_adds_log_provenance_and_generic_arguments() -> None:
    assert issubclass(Message, Event)
    assert Message.into_field().names == [
        *Event.into_field().names,
        "source_url",
        "source_rownum",
        "thread_name",
        "plugin_code",
        "message",
        "protocol_code",
        "MsgType",
        "kwargs",
    ]
    assert all(
        not any(key.startswith("fix:") for key in field.metadata)
        for field in Message.into_field().fields
    )


def test_kwarg_is_the_required_ordered_argument_shape() -> None:
    field = Kwarg.into_field()
    assert field.names == ["tag", "key", "value", "namespace", "comp"]
    assert field.field("key").nullable is False
    assert field.field("value").nullable is False
    assert Message.into_field().field("kwargs").item.arrow_type == field.arrow_type


def test_a_direct_kwarg_drops_a_leading_marker_and_normalizes_the_required_value() -> None:
    plain = Kwarg(key="#SIDE", value=None)  # type: ignore[arg-type]
    nested = Kwarg(key="#NoPartyIDs[0].PartyID", value="ABC")

    assert (plain.key, plain.value) == ("SIDE", "")
    assert (nested.key, nested.comp) == ("PartyID", "NoPartyIDs[0]")
    message = Message(  # type: ignore[list-item]
        kwargs=[plain, nested, ("#PAIR", "1"), {"key": "#MAP", "value": "2"}]
    )
    assert [(entry.key, entry.value) for entry in message.kwargs] == [
        ("SIDE", ""),
        ("PartyID", "ABC"),
        ("PAIR", "1"),
        ("MAP", "2"),
    ]


def test_a_message_always_has_a_non_null_argument_list() -> None:
    field = Message.into_field().field("kwargs")

    assert field.nullable is False
    assert Message().kwargs == []
    assert Message(kwargs=None).kwargs == []  # type: ignore[arg-type]


def test_a_message_promotes_the_first_message_type_and_removes_every_copy() -> None:
    message = Message(
        kwargs=[
            ("35", "D"),
            ("Symbol", "IBM"),
            ("#MSGTYPE", "G"),
            ("msg_type", "not-the-fix-name"),
        ]
    )

    assert message.MsgType == "D"
    assert [(entry.key, entry.value) for entry in message.kwargs] == [
        ("Symbol", "IBM"),
        ("msg_type", "not-the-fix-name"),
    ]


def test_an_explicit_message_type_still_strips_it_from_generic_arguments() -> None:
    message = Message(MsgType="8", kwargs=[("MsgType", "D"), ("Text", "kept")])

    assert message.MsgType == "8"
    assert [(entry.key, entry.value) for entry in message.kwargs] == [("Text", "kept")]


def test_a_message_without_a_discriminator_is_misc_and_skips_incidental_arguments() -> None:
    message = Message(message="a very long diagnostic with A=1 inside it")

    assert message.etype is EventType.MISC
    assert message.MsgType is None
    assert message.kwargs == []


def test_a_piped_message_without_a_discriminator_keeps_generic_arguments() -> None:
    message = Message(message="toBridge #SYMBOL=TTF|#SIDE=1")

    assert message.etype is EventType.MISC
    assert [(entry.key, entry.value) for entry in message.kwargs] == [
        ("SYMBOL", "TTF"),
        ("SIDE", "1"),
    ]


def test_an_explicit_empty_argument_list_is_authoritative() -> None:
    message = Message(message="35=D|Text=not-parsed|", kwargs=[])

    assert message.MsgType is None
    assert message.kwargs == []


def test_a_user_wrapper_promotes_its_named_message_kind() -> None:
    message = Message(message="8=FIX.4.4|35=UL|#MSGTYPE=D|#SIDE=1|")

    assert message.MsgType == "D"
    assert [(entry.key, entry.value) for entry in message.kwargs] == [
        ("8", "FIX.4.4"),
        ("SIDE", "1"),
    ]


def test_scalar_message_type_uses_the_same_case_and_checksum_boundaries() -> None:
    lower = Message(kwargs=[("35", "uL"), ("MsgType", "D")])
    after_checksum = Message(kwargs=[("10", "000"), ("35", "D")])

    assert lower.MsgType == "uL"
    assert lower.kwargs == []
    assert after_checksum.MsgType is None
    assert [(entry.key, entry.value) for entry in after_checksum.kwargs] == [
        ("10", "000"),
        ("35", "D"),
    ]


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
        ("SIDE", "1"),
        ("SIDE", "2"),
    ]
    assert [entry["key"] for entry in parsed[2]] == ["8", "35", "10"]
    assert parsed[3] == []


def test_pipe_and_soh_fast_paths_equal_complete_separator_inference() -> None:
    messages = pyarrow.array(
        [
            "prefix 8=FIX.4.4|35=D|58=a=b|10=000|",
            "8=FIX.4.4\x0135=D\x0158=a=b\x0110=000\x01",
            "A=x:B=inner|C=outer",
            "#NoPartyIDs[0]=PartyID=A\x01PartyRole=1|#B=2",
        ]
    )

    expected = kwargs_module._parse_generic(messages).to_pylist()

    assert Kwarg.parse_arrow(messages).to_pylist() == expected
    assert kwargs_module._common_separators(messages).to_pylist() == [
        "|",
        "\x01",
        None,
        None,
    ]


def test_a_long_common_separator_row_bypasses_generic_inference(monkeypatch) -> None:
    messages = pyarrow.array(["A=" + "x" * (1 << 18) + "|B=2"])
    expected = kwargs_module._parse_generic(messages).to_pylist()

    def unexpected(_messages):
        raise AssertionError("common separator used generic inference")

    monkeypatch.setattr(kwargs_module, "_parse_generic", unexpected)

    assert Kwarg.parse_arrow(messages).to_pylist() == expected


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
        ("NoPartyIDs[0]", "PartyID=A\x01PartyRole=1"),
        ("B", "2"),
    ]


def test_a_single_argument_does_not_keep_its_trailing_delimiter() -> None:
    (parsed,) = Kwarg.parse_arrow(pyarrow.array(["Only=one|"])).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed] == [("Only", "one")]


def test_hash_separators_distinguish_wire_delimiters_from_marked_keys() -> None:
    parsed = Kwarg.parse_arrow(pyarrow.array(["8=FIX.4.4#35=D#10=000", "#A=1#B=2"])).to_pylist()

    assert [entry["key"] for entry in parsed[0]] == ["8", "35", "10"]
    assert [entry["key"] for entry in parsed[1]] == ["A", "B"]


def test_a_numeric_key_too_wide_for_a_tag_remains_a_key() -> None:
    (parsed,) = Kwarg.parse_arrow(pyarrow.array(["1234567890=wide|Other=kept|"])).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed] == [
        ("1234567890", "wide"),
        ("Other", "kept"),
    ]


def test_generic_arguments_preserve_numeric_dotted_members() -> None:
    (parsed,) = Kwarg.parse_arrow(pyarrow.array(["54.5=x|NoPartyIDs[0].448=A|Side=1"])).to_pylist()

    assert [(entry["namespace"], entry["comp"], entry["key"]) for entry in parsed] == [
        ("54", None, "5"),
        (None, "NoPartyIDs[0]", "448"),
        (None, None, "Side"),
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
        ["CLORDID", "SIDE"],
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


def test_raw_identity_depends_only_on_the_payload() -> None:
    first = Message(message="same", source_url="one.log", source_rownum=2).identify()
    copied = Message(message="same", source_url="two.log", source_rownum=9).identify()
    changed = Message(message="different", source_url="one.log", source_rownum=2).identify()

    assert first.hash == copied.hash == first.xhash == hash_bytes(b"same")
    assert changed.hash != first.hash


def test_a_text_file_promotes_only_message_type_before_fix_parsing(tmp_path: Path) -> None:
    path = tmp_path / "capture.log"
    payload = "8=FIX.4.4|35=D|49=XPAR|56=BUY|55=IBM|10=000"
    path.write_text(f"2026-08-14 09:30:00.123 [thread] [bridge] (INFO) {payload}\n")

    with TextFile.from_path(
        path,
        msg_type_event_types=FixRegistry.from_builtin().msg_type_event_types(),
    ) as source:
        table = source.read_arrow_table()

    assert table.schema.names == Message.into_field().names
    assert table.column("message").to_pylist() == [payload]
    assert table.column("MsgType").to_pylist() == ["D"]
    assert [(entry["key"], entry["value"]) for entry in table.column("kwargs")[0].as_py()] == [
        ("8", "FIX.4.4"),
        ("49", "XPAR"),
        ("56", "BUY"),
        ("55", "IBM"),
        ("10", "000"),
    ]
    assert table.column("etype").to_pylist() == [int(EventType.ORDER)]
    assert table.column("mic").to_pylist() == [None]
    assert table.column("hash").to_pylist() == table.column("xhash").to_pylist()
    assert table.column("hash")[0].as_py() == hash_bytes(payload.encode("utf-8"))
