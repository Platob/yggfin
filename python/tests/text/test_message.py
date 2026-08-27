"""The protocol-neutral row produced by text files."""

from pathlib import Path

import pyarrow

import rekep.text.entries as entries_module
from rekep import Entry, FixRegistry, Message, TextFile
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
        "entries",
        "direction",
    ]
    assert all(
        not any(key.startswith("fix:") for key in field.metadata)
        for field in Message.into_field().fields
    )


def test_entry_is_the_required_ordered_argument_shape() -> None:
    field = Entry.into_field()
    assert field.names == ["tag", "key", "value", "namespace", "comp"]
    assert field.field("key").nullable is False
    assert field.field("value").nullable is False
    assert Message.into_field().field("entries").item.dtype == field.dtype


def test_one_entry_shape_serves_storage_and_reading_alike() -> None:
    """The stored struct and the accessor's view are one class: the read
    views derive lazily from the stored spelling, so nothing renders a key
    to text and re-splits it on the way to an answer."""
    from rekep.fix import Entry as fix_entry
    from rekep.fix.access import Entry as access_entry

    assert Entry is fix_entry is access_entry

    stored = Entry(tag=448, key="PartyID", comp="NoPartyIDs[0]", value="A")
    assert (stored.name, stored.index, stored.lead, stored.entry_lead) == (
        "PartyID",
        None,
        "NoPartyIDs[0]",
        True,
    )
    assert stored.spelling == "NoPartyIDs[0].PartyID"

    indexed = Entry(key="Side[0]", value="1")
    assert (indexed.name, indexed.index, indexed.spelling) == ("Side", 0, "Side[0]")

    namespaced = Entry(key="TECH.CLIENTID", value="x")
    assert (namespaced.namespace, namespaced.name, namespaced.entry_lead) == (
        "TECH",
        "CLIENTID",
        False,
    )

    typed = Entry.of(tag=44, key="44", value=9.5)
    assert typed.value == 9.5, "a ready view keeps its typed value"
    assert (typed.spelling, typed.folded) == ("44", "44")


def test_the_stored_split_answers_before_any_respelling() -> None:
    """The stored members are already the split: `comp` asserts group
    semantics whatever its spelling, a trailing-dot key keeps its stored
    name, and an empty namespace defers to the comp beside it."""
    indexless = Entry(key="PartyID", comp="NoPartyIDs", value="P")
    assert (indexless.lead, indexless.entry_lead) == ("NoPartyIDs", True)

    dotted = Entry(key="A.", value="v")
    assert (dotted.namespace, dotted.name, dotted.lead) == ("A", "A.", "A")

    beside = Entry(key="PartyID", namespace="", comp="NoPartyIDs[0]", value="P")
    assert (beside.spelling, beside.entry_lead) == ("NoPartyIDs[0].PartyID", True)


def test_a_ready_view_normalizes_into_storage_and_caches_reset() -> None:
    """`of` keeps a typed value for reading; storage takes the text -- and a
    mutated stored member re-derives every cached view."""
    typed = Entry.of(tag=44, key="44", value=9.5)
    assert typed.value == 9.5
    assert Entry.from_stored(typed).value == "9.5"

    entry = Entry(key="Symbol", tag=55, value="IBM")
    assert entry.name == "Symbol"
    entry.key = "Side[0]"
    assert (entry.name, entry.index) == ("Side", 0)


def test_a_direct_entry_drops_a_leading_marker_and_normalizes_the_required_value() -> None:
    plain = Entry(key="#SIDE", value=None)  # type: ignore[arg-type]
    nested = Entry(key="#NoPartyIDs[0].PartyID", value="ABC")

    assert (plain.key, plain.value) == ("SIDE", "")
    assert (nested.key, nested.comp) == ("PartyID", "NoPartyIDs[0]")
    message = Message(  # type: ignore[list-item]
        entries=[plain, nested, ("#PAIR", "1"), {"key": "#MAP", "value": "2"}]
    )
    assert [(entry.key, entry.value) for entry in message.entries] == [
        ("SIDE", ""),
        ("PartyID", "ABC"),
        ("PAIR", "1"),
        ("MAP", "2"),
    ]


def test_direction_is_resolved_where_the_raw_line_still_exists() -> None:
    """`parse_fix` reads these rows back with `message` projected out, so the
    message stage is where the verb before the payload has to become the
    stored answer -- for the batch reading and the scalar row alike."""
    lines = [
        "Receiving : 8=FIX.4.4|35=D|11=C1|10=000",
        "Sending : 8=FIX.4.4|35=8|37=O1|39=0|10=000",
        # A rendered line the probe reads as UL but no rule pattern anchors:
        # an unanchored verb answers nothing rather than from anywhere.
        "Sending : ACCOUNT=A1|MSGTYPE=D|PRICE=9.5",
        # The verb inside a payload value is prose, not movement.
        "toBridge #MSGTYPE=8|#CLORDID=C5|#TEXT=order sent to market",
        "just some heartbeat prose",
    ]
    parsed = Message.parse_arrow(pyarrow.array(lines))
    assert parsed["direction"].to_pylist() == [False, True, None, None, None]

    assert Message(message=lines[0]).direction is False
    assert Message(message=lines[1]).direction is True
    assert Message(message=lines[4]).direction is None
    assert Message(message=lines[0], direction=True).direction is True, (
        "an explicitly stored answer is not recomputed"
    )


def test_a_message_always_has_a_non_null_argument_list() -> None:
    field = Message.into_field().field("entries")

    assert field.nullable is False
    assert Message().entries == []
    assert Message(entries=None).entries == []  # type: ignore[arg-type]


def test_a_payload_parses_scalar_like_the_column_path() -> None:
    """`from_text` is the scalar spelling of `parse_arrow`: same promotion,
    same residual arguments -- the raw text kept only when declared."""
    staged = Message.from_text("8=FIX.4.4|35=D|11=C1|10=000", runix=7)
    column = Message(message="8=FIX.4.4\x0135=D\x0111=C1\x0110=000\x01")

    assert staged.MsgType == column.MsgType == "D"
    assert staged.runix == 7
    assert staged.message == ""
    assert (
        [(entry.tag, entry.value) for entry in staged.entries]
        == [(entry.tag, entry.value) for entry in column.entries]
        == [(8, "FIX.4.4"), (11, "C1"), (10, "000")]
    )


def test_a_message_promotes_the_first_message_type_and_removes_every_copy() -> None:
    message = Message(
        entries=[
            ("35", "D"),
            ("Symbol", "IBM"),
            ("#MSGTYPE", "G"),
            ("msg_type", "not-the-fix-name"),
        ]
    )

    assert message.MsgType == "D"
    assert [(entry.key, entry.value) for entry in message.entries] == [
        ("Symbol", "IBM"),
        ("msg_type", "not-the-fix-name"),
    ]


def test_an_explicit_message_type_still_strips_it_from_generic_arguments() -> None:
    message = Message(MsgType="8", entries=[("MsgType", "D"), ("Text", "kept")])

    assert message.MsgType == "8"
    assert [(entry.key, entry.value) for entry in message.entries] == [("Text", "kept")]


def test_a_message_without_a_discriminator_is_misc_and_skips_incidental_arguments() -> None:
    message = Message(message="a very long diagnostic with A=1 inside it")

    assert message.etype is EventType.MISC
    assert message.MsgType is None
    assert message.entries == []


def test_a_piped_message_without_a_discriminator_keeps_generic_arguments() -> None:
    message = Message(message="toBridge #SYMBOL=TTF|#SIDE=1")

    assert message.etype is EventType.MISC
    assert [(entry.key, entry.value) for entry in message.entries] == [
        ("SYMBOL", "TTF"),
        ("SIDE", "1"),
    ]


def test_an_explicit_empty_argument_list_is_authoritative() -> None:
    message = Message(message="35=D|Text=not-parsed|", entries=[])

    assert message.MsgType is None
    assert message.entries == []


def test_a_user_wrapper_promotes_its_named_message_kind() -> None:
    message = Message(message="8=FIX.4.4|35=UL|#MSGTYPE=D|#SIDE=1|")

    assert message.MsgType == "D"
    assert [(entry.key, entry.value) for entry in message.entries] == [
        ("8", "FIX.4.4"),
        ("SIDE", "1"),
    ]


def test_scalar_message_type_uses_the_same_case_and_checksum_boundaries() -> None:
    lower = Message(entries=[("35", "uL"), ("MsgType", "D")])
    after_checksum = Message(entries=[("10", "000"), ("35", "D")])

    assert lower.MsgType == "uL"
    assert lower.entries == []
    assert after_checksum.MsgType is None
    assert [(entry.key, entry.value) for entry in after_checksum.entries] == [
        ("10", "000"),
        ("35", "D"),
    ]


def test_generic_arguments_keep_mixed_separators_repeats_and_spelling() -> None:
    parsed = Entry.parse_arrow(
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

    expected = entries_module._parse_generic(messages).to_pylist()

    assert Entry.parse_arrow(messages).to_pylist() == expected
    assert entries_module._common_separators(messages).to_pylist() == [
        "|",
        "\x01",
        None,
        None,
    ]


def test_a_long_common_separator_row_bypasses_generic_inference(monkeypatch) -> None:
    messages = pyarrow.array(["A=" + "x" * (1 << 18) + "|B=2"])
    expected = entries_module._parse_generic(messages).to_pylist()

    def unexpected(_messages):
        raise AssertionError("common separator used generic inference")

    monkeypatch.setattr(entries_module, "_parse_generic", unexpected)

    assert Entry.parse_arrow(messages).to_pylist() == expected


def test_generic_arguments_keep_prefixes_whitespace_and_empty_values() -> None:
    parsed = Entry.parse_arrow(
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
    (parsed,) = Entry.parse_arrow(
        pyarrow.array(["#NoPartyIDs[0]=PartyID=A\x01PartyRole=1|#B=2"])
    ).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed] == [
        ("NoPartyIDs[0]", "PartyID=A\x01PartyRole=1"),
        ("B", "2"),
    ]


def test_a_single_argument_does_not_keep_its_trailing_delimiter() -> None:
    (parsed,) = Entry.parse_arrow(pyarrow.array(["Only=one|"])).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed] == [("Only", "one")]


def test_hash_separators_distinguish_wire_delimiters_from_marked_keys() -> None:
    parsed = Entry.parse_arrow(pyarrow.array(["8=FIX.4.4#35=D#10=000", "#A=1#B=2"])).to_pylist()

    assert [entry["key"] for entry in parsed[0]] == ["8", "35", "10"]
    assert [entry["key"] for entry in parsed[1]] == ["A", "B"]


def test_a_numeric_key_too_wide_for_a_tag_remains_a_key() -> None:
    (parsed,) = Entry.parse_arrow(pyarrow.array(["1234567890=wide|Other=kept|"])).to_pylist()

    assert [(entry["key"], entry["value"]) for entry in parsed] == [
        ("1234567890", "wide"),
        ("Other", "kept"),
    ]


def test_generic_arguments_preserve_numeric_dotted_members() -> None:
    (parsed,) = Entry.parse_arrow(pyarrow.array(["54.5=x|NoPartyIDs[0].448=A|Side=1"])).to_pylist()

    assert [(entry["namespace"], entry["comp"], entry["key"]) for entry in parsed] == [
        ("54", None, "5"),
        (None, "NoPartyIDs[0]", "448"),
        (None, None, "Side"),
    ]


def test_fix_whitespace_around_equals_is_not_a_field_separator() -> None:
    parsed = Entry.parse_arrow(
        pyarrow.array(["Side\x0b=1|Price=2", "N[0]=\x0bM=v|Side=1"])
    ).to_pylist()

    assert [[(entry["key"], entry["value"]) for entry in row] for row in parsed] == [
        [("Side", "1"), ("Price", "2")],
        [("N[0]", "M=v"), ("Side", "1")],
    ]


def test_separator_padding_is_ascii_and_does_not_strip_unicode_text() -> None:
    parsed = Entry.parse_arrow(
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
    (parsed,) = Entry.parse_arrow(
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
    assert [(entry["key"], entry["value"]) for entry in table.column("entries")[0].as_py()] == [
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
