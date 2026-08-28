"""The protocol-neutral row produced by text files."""

from pathlib import Path

import pyarrow

import rekep.text.entries as entries_module
from rekep import Entry, FixRegistry, Message, TextFile
from rekep.enums import EventType
from rekep.fields import DISPLAY
from rekep.market import Event, hash_bytes
from rekep.market.identity import hash_int_of

#: The standard header this stage lifts out of `entries` into columns of its
#: own, in the order `Message` declares them, with the FIX tag each answers to.
#: Spelled out rather than imported from `rekep.text.message.SESSION_FIELDS`, so
#: a field quietly leaving that tuple cannot move both sides of an assertion
#: together.
LIFTED_HEADER = {
    "beginstring": "8",
    "bodylength": "9",
    "msgtype": "35",
    "sendercompid": "49",
    "sendersubid": "50",
    "senderlocationid": "142",
    "targetcompid": "56",
    "targetsubid": "57",
    "targetlocationid": "143",
    "onbehalfofcompid": "115",
    "onbehalfofsubid": "116",
    "onbehalfoflocationid": "144",
    "delivertocompid": "128",
    "delivertosubid": "129",
    "delivertolocationid": "145",
    "msgseqnum": "34",
    "lastmsgseqnumprocessed": "369",
    "possdupflag": "43",
    "possresend": "97",
    "sendingtime": "52",
    "origsendingtime": "122",
    "onbehalfofsendingtime": "370",
    "applverid": "1128",
    "cstmapplverid": "1129",
    "applextid": "1156",
    "messageencoding": "347",
    "xmldatalen": "212",
    "xmldata": "213",
    "securedatalen": "90",
    "securedata": "91",
    "signaturelength": "93",
    "signature": "89",
}

#: `CheckSum <10>` is the boundary every lift is measured against -- a field is
#: eligible only where it stands in front of it -- so it is deliberately not
#: among the lifted and stays in `entries` for the FIX stage to read.
UNLIFTED_TRAILER = "10"


def test_a_message_adds_log_provenance_and_generic_arguments() -> None:
    assert issubclass(Message, Event)
    assert Message.into_field().names == [
        *Event.into_field().names,
        "sourceurl",
        "sourcerownum",
        "threadname",
        "plugincode",
        "message",
        "protocolcode",
        *LIFTED_HEADER,
        "entries",
        "direction",
    ]
    assert UNLIFTED_TRAILER not in LIFTED_HEADER.values()
    assert "CheckSum" not in Message.into_field().names, (
        "the boundary the lift is measured against is not one of the lifted"
    )
    # Protocol-neutral columns: this stage reads no numbers and names no zone,
    # so each of the seven keeps the text the payload spelled, and none of them
    # carries the `fix:` metadata a dictionary-typed column would -- beyond the
    # display every column carries, which says what it is called, not how it
    # reads.
    for name in LIFTED_HEADER:
        field = Message.into_field().field(name)
        assert field.dtype == pyarrow.string(), name
        assert field.nullable is True, name
    assert all(
        not any(key.startswith("fix:") for key in field.metadata if key != DISPLAY)
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

    assert staged.msgtype == column.msgtype == "D"
    assert staged.beginstring == column.beginstring == "FIX.4.4"
    assert staged.runix == 7
    assert staged.message == ""
    # `8` and `35` are standard header and leave for columns of their own; `11`
    # is body and `10` is the boundary, so both stay exactly where they were.
    assert (
        [(entry.tag, entry.value) for entry in staged.entries]
        == [(entry.tag, entry.value) for entry in column.entries]
        == [(11, "C1"), (10, "000")]
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

    assert message.msgtype == "D"
    assert [(entry.key, entry.value) for entry in message.entries] == [
        ("Symbol", "IBM"),
        ("msg_type", "not-the-fix-name"),
    ]


def test_an_explicit_message_type_still_strips_it_from_generic_arguments() -> None:
    message = Message(msgtype="8", entries=[("MsgType", "D"), ("Text", "kept")])

    assert message.msgtype == "8"
    assert [(entry.key, entry.value) for entry in message.entries] == [("Text", "kept")]


def test_a_message_without_a_discriminator_is_misc_and_skips_incidental_arguments() -> None:
    message = Message(message="a very long diagnostic with A=1 inside it")

    assert message.etype is EventType.MISC
    assert message.msgtype is None
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

    assert message.msgtype is None
    assert message.entries == []


def test_a_user_wrapper_promotes_its_named_message_kind() -> None:
    message = Message(message="8=FIX.4.4|35=UL|#MSGTYPE=D|#SIDE=1|")

    assert message.msgtype == "D"
    assert message.beginstring == "FIX.4.4"
    # Both spellings of the discriminator leave together: the wrapper `35=UL`
    # is what the rendered name was read to correct, so neither is left behind
    # to be read again.
    assert [(entry.key, entry.value) for entry in message.entries] == [("SIDE", "1")]


def test_scalar_message_type_uses_the_same_case_and_checksum_boundaries() -> None:
    lower = Message(entries=[("35", "uL"), ("MsgType", "D")])
    after_checksum = Message(entries=[("10", "000"), ("35", "D")])

    assert lower.msgtype == "uL"
    assert lower.entries == []
    assert after_checksum.msgtype is None
    assert [(entry.key, entry.value) for entry in after_checksum.entries] == [
        ("10", "000"),
        ("35", "D"),
    ]


def test_the_standard_header_lifts_into_columns_of_its_own() -> None:
    """A column each; `entries` keeps the body and the boundary.

    Every one of them is the text the payload spelled -- `9=176` is the three
    characters and `43=Y` is the letter -- because this stage reads no
    dictionary. What the payload does not state stays null: the whole header
    is declared, and a message states the part of it that it states.
    """
    message = Message(
        message="8=FIX.4.4|9=176|35=D|34=1092|49=BUYSIDE|50=DESK|56=XPAR|115=ORIG|"
        "43=Y|52=20260814-09:30:00.000|55=IBM|10=000"
    )

    stated = {
        name: found for name in LIFTED_HEADER if (found := getattr(message, name)) is not None
    }
    assert stated == {
        "beginstring": "FIX.4.4",
        "bodylength": "176",
        "msgtype": "D",
        "sendercompid": "BUYSIDE",
        "sendersubid": "DESK",
        "targetcompid": "XPAR",
        "onbehalfofcompid": "ORIG",
        "msgseqnum": "1092",
        "possdupflag": "Y",
        "sendingtime": "20260814-09:30:00.000",
    }
    assert [(entry.key, entry.value) for entry in message.entries] == [
        ("55", "IBM"),
        (UNLIFTED_TRAILER, "000"),
    ]
    assert Message.into_field_metadata() == {"version": "1"}


def test_a_header_field_stated_twice_two_ways_is_lifted_by_neither() -> None:
    """Two readings of one fact is not one statement of it.

    A bridge writing one field twice on purpose is telling the reader
    something a first-wins pop would throw away, so both stay in `entries` and
    the column says nothing -- while the fields beside it are lifted as usual.
    """
    torn = Message(message="8=FIX.4.4|49=A|49=B|55=IBM|10=000")

    assert torn.sendercompid is None
    assert [(entry.key, entry.value) for entry in torn.entries] == [
        ("49", "A"),
        ("49", "B"),
        ("55", "IBM"),
        ("10", "000"),
    ]

    repeated = Message(message="8=FIX.4.4|49=A|49=A|55=IBM|10=000")

    assert repeated.sendercompid == "A", "one fact stated twice is still stated once"
    assert [(entry.key, entry.value) for entry in repeated.entries] == [
        ("55", "IBM"),
        ("10", "000"),
    ], "and every occurrence of it leaves"
    assert repeated.beginstring == torn.beginstring == "FIX.4.4", (
        "one torn field is not the six beside it"
    )

    scalar = Message.from_text("8=FIX.4.4|49=A|49=B|55=IBM|10=000")

    assert scalar.sendercompid is None
    assert [(entry.key, entry.value) for entry in scalar.entries] == [
        (entry.key, entry.value) for entry in torn.entries
    ], "the rule is the tokenizer's, not the column kernel's"


def test_the_header_lift_stops_at_the_checksum() -> None:
    """`CheckSum` is the boundary, under the tag or under its rendered name."""
    tagged = Message(entries=[("8", "FIX.4.4"), ("10", "000"), ("49", "AFTER"), ("52", "LATE")])
    rendered = Message(entries=[("CheckSum", "000"), ("52", "LATE")])

    assert tagged.beginstring == "FIX.4.4"
    assert (tagged.sendercompid, tagged.sendingtime) == (None, None)
    assert [(entry.key, entry.value) for entry in tagged.entries] == [
        ("10", "000"),
        ("49", "AFTER"),
        ("52", "LATE"),
    ], "the trailer stays, and so does everything the row wrote behind it"
    assert rendered.sendingtime is None
    assert [(entry.key, entry.value) for entry in rendered.entries] == [
        ("CheckSum", "000"),
        ("52", "LATE"),
    ]


def test_only_the_discriminator_value_is_constrained_to_letters_and_digits() -> None:
    """The standard constrains `MsgType` alone; the rest carry punctuation."""
    message = Message(entries=[("8", "FIX.4.4"), ("35", "D-1"), ("52", "20260814-09:30:00.000")])

    assert message.msgtype is None
    assert (message.beginstring, message.sendingtime) == (
        "FIX.4.4",
        "20260814-09:30:00.000",
    )
    assert [(entry.key, entry.value) for entry in message.entries] == [("35", "D-1")], (
        "a value no discriminator can be is left where a reader can see it"
    )


def test_only_the_discriminator_answers_to_a_rendered_name() -> None:
    """A bridge that renders its header writes its own names, and this stage
    keeps them: which name a feed uses is data. `MsgType` is the exception it
    has always been -- a `35=U1` wrapper naming its real type beside it is the
    whole reason the rendered spelling is read at all."""
    message = Message(
        message="#BeginString=FIX.4.4|#SendingTime=20260814-09:30:00.000|#MsgType=D|#Side=1"
    )

    assert message.msgtype == "D"
    assert (message.beginstring, message.sendingtime) == (None, None)
    assert [(entry.key, entry.value) for entry in message.entries] == [
        ("BeginString", "FIX.4.4"),
        ("SendingTime", "20260814-09:30:00.000"),
        ("Side", "1"),
    ]


def test_an_explicit_header_column_still_strips_its_entry() -> None:
    """What a caller declares stands, and the payload's own copy still leaves --
    the same rule the discriminator has always followed."""
    message = Message(beginstring="FIX.4.2", entries=[("8", "FIX.4.4"), ("Text", "kept")])

    assert message.beginstring == "FIX.4.2"
    assert [(entry.key, entry.value) for entry in message.entries] == [("Text", "kept")]


def test_the_column_path_and_the_scalar_row_lift_the_same_header() -> None:
    """`parse_arrow` and `__post_init__` are one rule spelled twice: same seven
    columns, same residual arguments, row for row."""
    lines = [
        "8=FIX.4.4|9=176|35=D|34=1092|49=BUYSIDE|56=XPAR|52=20260814-09:30:00.000|55=IBM|10=000",
        "8=FIX.4.4|49=A|49=B|10=000",
        "8=FIX.4.4|35=UL|#MSGTYPE=D|#SIDE=1|",
        "#BeginString=FIX.4.4|#MsgType=D|#Side=1",
        "8=FIX.4.4|35=D|10=000|49=AFTER",
        "just some heartbeat prose",
    ]
    parsed = Message.parse_arrow(pyarrow.array(lines))

    assert all(parsed[name].type == pyarrow.string() for name in LIFTED_HEADER)
    for index, line in enumerate(lines):
        row = Message(message=line)
        assert {name: parsed[name][index].as_py() for name in LIFTED_HEADER} == {
            name: getattr(row, name) for name in LIFTED_HEADER
        }, line
        assert [(entry["key"], entry["value"]) for entry in parsed["entries"][index].as_py()] == [
            (entry.key, entry.value) for entry in row.entries
        ], line
    assert {name: parsed[name][-1].as_py() for name in LIFTED_HEADER} == dict.fromkeys(
        LIFTED_HEADER
    ), "a prose row states no header, and every column says so"


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
    first = Message(message="same", sourceurl="one.log", sourcerownum=2).identify()
    copied = Message(message="same", sourceurl="two.log", sourcerownum=9).identify()
    changed = Message(message="different", sourceurl="one.log", sourcerownum=2).identify()

    assert first.hash == copied.hash == first.xhash == hash_bytes(b"same")
    assert changed.hash != first.hash


def test_a_text_file_promotes_the_standard_header_before_fix_parsing(tmp_path: Path) -> None:
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
    stated = {
        name: found for name in LIFTED_HEADER if (found := table.column(name).to_pylist()) != [None]
    }
    assert stated == {
        "beginstring": ["FIX.4.4"],
        "msgtype": ["D"],
        "sendercompid": ["XPAR"],
        "targetcompid": ["BUY"],
    }, "the four this payload states; every other header column is null"
    assert [(entry["key"], entry["value"]) for entry in table.column("entries")[0].as_py()] == [
        ("55", "IBM"),
        ("10", "000"),
    ], "the body and the boundary, and nothing the header already answers"
    assert table.column("etype").to_pylist() == [int(EventType.ORDER)]
    assert table.column("mic").to_pylist() == [None]
    assert table.column("hash").to_pylist() == table.column("xhash").to_pylist()
    assert hash_int_of(table.column("hash")[0].as_py()) == hash_bytes(payload.encode("utf-8"))


def test_the_log_s_own_prose_does_not_decide_the_payload_s_separator() -> None:
    """A capture writes `seq=1092 sending >>` in front of the message.

    That prefix is an assignment, so a separator inferred from the whole line
    read the `>` of `>>` as the delimiter and returned the whole message as
    one entry, with the rest of it stored as `BeginString`.
    """
    line = "seq=1092 sending >> 8=FIX.4.2|9=176|35=D|34=1092|49=A|56=B|55=TTF|10=203"

    parsed = Message.parse_arrow(pyarrow.array([line]))

    assert parsed["beginstring"][0].as_py() == "FIX.4.2"
    assert parsed["msgtype"][0].as_py() == "D"
    assert [(entry["key"], entry["value"]) for entry in parsed["entries"][0].as_py()] == [
        ("55", "TTF"),
        ("10", "203"),
    ]


def test_a_line_that_names_no_message_keeps_all_of_itself() -> None:
    """Which is what a generic argument list is."""
    parsed = Message.parse_arrow(pyarrow.array(["a=1;b=2;c=3"]))

    assert [(entry["key"], entry["value"]) for entry in parsed["entries"][0].as_py()] == [
        ("a", "1"),
        ("b", "2"),
        ("c", "3"),
    ]


def test_the_raw_stage_reads_every_separator_the_fix_parsers_declare() -> None:
    """EOT/ETX is one of them, and the raw stage saw none of such a payload."""
    line = "8=FIX.4.2\x04\x0335=D\x04\x0349=SEND\x04\x0356=TARG\x04\x0355=IBM\x04\x0310=001"

    parsed = Message.parse_arrow(pyarrow.array([line]))

    assert parsed["protocolcode"][0].as_py() == "FIX"
    assert parsed["msgtype"][0].as_py() == "D"
    assert parsed["sendercompid"][0].as_py() == "SEND"
    assert Message.msg_types_arrow(pyarrow.array([line]))[0].as_py() == "D"
    assert [(entry["key"], entry["value"]) for entry in parsed["entries"][0].as_py()] == [
        ("55", "IBM"),
        ("10", "001"),
    ]


def test_a_begin_string_needs_no_dotted_version_to_be_fix() -> None:
    """`8=FIX4` is what this repository's own fixture writes.

    The syntax probe demanded `FIX.<major>.<minor>` where the shipped
    classification rule asks only for `8=FIX`, so such a row reached the store
    as OTHER -- and lost `direction` with it, which is keyed on the protocol.
    """
    parsed = Message.parse_arrow(pyarrow.array(["sending >> 8=FIX4|9=61|34=1|49=A|10=1"]))

    assert parsed["protocolcode"][0].as_py() == "FIX"
    assert parsed["direction"][0].as_py() is True
    assert parsed["beginstring"][0].as_py() == "FIX4"


def test_prose_that_merely_contains_fix_is_not_a_message() -> None:
    """The BeginString value stops at a separator, and prose has none there."""
    parsed = Message.parse_arrow(pyarrow.array(["the 8=FIXTURE cost 12"]))

    assert parsed["protocolcode"][0].as_py() == "OTHER"


def test_a_row_carrying_its_text_answers_the_syntax_columns_either_way() -> None:
    """Whoever tokenized its arguments -- `from_text` passes its own in."""
    line = "8=FIX.4.2|9=176|35=D|34=1092|49=BUYSIDE|56=XPAR|11=ORD-1|10=203"

    assert Message.from_text(line, message=line).protocolcode == "FIX"
    assert Message(message=line).protocolcode == "FIX"
    # Without the text there is nothing to read a syntax column off.
    assert Message.from_text(line).protocolcode == "OTHER"


def test_the_discriminator_agrees_with_itself_before_it_is_lifted() -> None:
    """One spelling stating two values is torn, exactly like the six beside it.

    The second `35=` used to be claimed and dropped, so a re-wrapped line came
    out of the parser without the reading it disagreed on.
    """
    soh = chr(1)
    torn = f"8=FIX.4.4{soh}35=D{soh}35=8{soh}55=A{soh}10=001{soh}"
    agreed = f"8=FIX.4.4{soh}35=D{soh}35=D{soh}55=A{soh}10=001{soh}"

    assert [(e.key, e.value) for e in Message.from_text(torn).entries] == [
        ("35", "D"),
        ("35", "8"),
        ("55", "A"),
        ("10", "001"),
    ]
    assert Message.from_text(torn).msgtype is None
    assert [(e.key, e.value) for e in Message.from_text(agreed).entries] == [
        ("55", "A"),
        ("10", "001"),
    ]
    assert Message.from_text(agreed).msgtype == "D"

    # The column path keeps both readings too; its column then falls back to
    # the raw line's own first discriminator, which the scalar row has no text
    # to read.
    found = Message.parse_arrow(pyarrow.array([torn, agreed]))
    assert found["msgtype"].to_pylist() == ["D", "D"]
    assert [[entry["key"] for entry in row] for row in found["entries"].to_pylist()] == [
        ["35", "35", "55", "10"],
        ["55", "10"],
    ]


def test_the_two_discriminator_spellings_still_have_their_own_rule() -> None:
    """A `U`-prefixed wire type defers to a rendered name beside it."""
    message = Message.from_text("8=FIX.4.4|35=U1|#MSGTYPE=D|55=A|10=1")

    assert message.msgtype == "D"
    assert [(entry.key, entry.value) for entry in message.entries] == [("55", "A"), ("10", "1")]
