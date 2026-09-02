"""The protocol-neutral row produced by text files."""

from pathlib import Path

import pyarrow
import pytest

import rekep.text.entries as entries_module
import rekep.text.message as message_module
from rekep import Entry, FixRegistry, Message, TextFile, txhash
from rekep.enums import Direction, EventType, Plugin, Protocol
from rekep.market import Event, hash_bytes

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
        "level",
        "body",
        "protocol",
        *LIFTED_HEADER,
        "entries",
        "direction",
    ]
    assert UNLIFTED_TRAILER not in LIFTED_HEADER.values()
    assert "CheckSum" not in Message.into_field().names, (
        "the boundary the lift is measured against is not one of the lifted"
    )
    body = Message.into_field().field("body")
    assert body.dtype == pyarrow.binary() and not body.nullable
    plugin = Message.into_field().field("plugin")
    assert plugin.dtype == Plugin.into_storage_type() and not plugin.nullable
    # Protocol-neutral columns keep the text the payload spelled. The inherited
    # venue is the one exception: its LastMkt identity has to survive when the
    # raw body is projected away before FIX transcription.
    for name in LIFTED_HEADER:
        field = Message.into_field().field(name)
        assert field.dtype == pyarrow.string(), name
        assert field.nullable is True, name
    typed = {
        field.name
        for field in Message.into_field().fields
        if any(key.startswith("fix:") for key in field.metadata if key != "fix:name")
    }
    assert typed == {"lastmkt"}
    assert Message.into_field().field("lastmkt").fix.tag == 30
    direction = Message.into_field().field("direction")
    assert not direction.nullable and direction.dtype == pyarrow.int32()
    assert direction.metadata["enum:name"] == "Direction"


def test_entry_is_the_required_ordered_argument_shape() -> None:
    field = Entry.into_field()
    assert field.names == ["tag", "key", "value", "comp"]
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
    assert (namespaced.key, namespaced.name, namespaced.lead, namespaced.entry_lead) == (
        "TECH.CLIENTID",
        "CLIENTID",
        "TECH",
        False,
    )

    typed = Entry.of(tag=44, key="44", value=9.5)
    assert typed.value == 9.5, "a ready view keeps its typed value"
    assert (typed.spelling, typed.folded) == ("44", "44")


def test_the_stored_split_answers_before_any_respelling() -> None:
    """A trailing-dot key stays whole and an indexed comp stays beside its key."""
    dotted = Entry(key="A.", value="v")
    assert (dotted.key, dotted.name, dotted.lead) == ("A.", "A.", "A")

    beside = Entry(key="PartyID", comp="NoPartyIDs[0]", value="P")
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
    """`parse_fix_*` reads these rows back with `body` projected out, so the
    message stage is where the verb before the payload has to become the
    stored answer -- for the batch reading and the scalar row alike."""
    lines = [
        "Receiving : 8=FIX.4.4|35=D|11=C1|10=000",
        "Sending : 8=FIX.4.4|35=8|37=O1|39=0|10=000",
        # A named document has an anchor of its own, so a verb in front of one
        # answers exactly as it does in front of a frame.
        "Sending : ACCOUNT=A1|MSGTYPE=D|PRICE=9.5",
        # The verb inside a payload value is prose, not movement.
        "toBridge #MSGTYPE=8|#CLORDID=C5|#TEXT=order sent to market",
        "just some heartbeat prose",
    ]
    parsed = Message.parse_arrow(pyarrow.array(lines))
    assert parsed["direction"].to_pylist() == [
        int(Direction.RECV),
        int(Direction.SENT),
        int(Direction.SENT),
        int(Direction.UNKNOWN),
        int(Direction.UNKNOWN),
    ]

    assert Message(body=lines[0]).direction is Direction.RECV
    assert Message(body=lines[1]).direction is Direction.SENT
    assert Message(body=lines[4]).direction is Direction.UNKNOWN
    assert Message(body=lines[0], direction=Direction.SENT).direction is Direction.SENT, (
        "an explicitly stored answer is not recomputed"
    )


def test_a_message_always_has_a_non_null_argument_list() -> None:
    field = Message.into_field().field("entries")

    assert field.nullable is False
    assert Message().entries == []
    assert Message(entries=None).entries == []  # type: ignore[arg-type]


def test_a_payload_parses_scalar_like_the_column_path() -> None:
    """`from_text` is the scalar spelling of `parse_arrow`."""
    staged = Message.from_text("8=FIX.4.4|35=D|11=C1|10=000", recunix=7)
    column = Message(body="8=FIX.4.4\x0135=D\x0111=C1\x0110=000\x01")

    assert staged.msgtype == column.msgtype == "D"
    assert staged.beginstring == column.beginstring == "FIX.4.4"
    assert staged.recunix == 7
    assert staged.body == b"8=FIX.4.4|35=D|11=C1|10=000"
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
    message = Message(body="a very long diagnostic with A=1 inside it")

    assert message.eventtype is EventType.MISC
    assert message.msgtype is None
    assert message.entries == []


def test_a_piped_message_without_a_discriminator_keeps_generic_arguments() -> None:
    message = Message(body="toBridge #SYMBOL=TTF|#SIDE=1")

    assert message.eventtype is EventType.MISC
    assert [(entry.key, entry.value) for entry in message.entries] == [
        ("SYMBOL", "TTF"),
        ("SIDE", "1"),
    ]


def test_an_explicit_empty_argument_list_is_authoritative() -> None:
    message = Message(body="35=D|Text=not-parsed|", entries=[])

    assert message.msgtype is None
    assert message.entries == []


def test_a_user_wrapper_promotes_its_named_message_kind() -> None:
    message = Message(body="8=FIX.4.4|35=UL|#MSGTYPE=D|#SIDE=1|")

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


@pytest.mark.parametrize(
    ("message_name", "checksum_name"),
    [("Msg_Type", "Check_Sum"), ("Msg Type", "Check Sum"), ("M!sg@Type", "Check/Sum")],
)
def test_rendered_header_names_use_the_column_fold_in_scalar_and_arrow(
    message_name: str,
    checksum_name: str,
) -> None:
    line = f"#{message_name}=D|#{checksum_name}=000|#Side=1"
    assert Message(body=line).msgtype == "D"
    assert Message.parse_arrow(pyarrow.array([line]))["msgtype"].to_pylist() == ["D"]

    after = f"#{checksum_name}=000|#{message_name}=D"
    assert Message(body=after).msgtype is None
    assert Message.parse_arrow(pyarrow.array([after]))["msgtype"].to_pylist() == [None]


def test_one_batch_mixes_lifted_and_probed_message_types_row_by_row() -> None:
    """The probe reads the rows that lifted nothing, and answers on their own rows.

    A row spelling `35=` twice keeps both entries and lifts no column, so the
    probe is what names it -- beside rows that lifted their own, rows that
    carry no discriminator at all, and a row that carries no payload.
    """
    lines = [
        "8=FIX.4.2|35=D|49=A|56=B|10=203",
        "8=FIX.4.2|35=8|35=A|49=A|10=203",
        "After Enrichment -> ACCOUNT=ACCT-000001",
        "8=FIX.4.4|35=U1|#MSGTYPE=AB|10=1",
        None,
    ]

    parsed = Message.parse_arrow(pyarrow.array(lines))

    assert parsed["msgtype"].to_pylist() == ["D", "8", None, "AB", None]
    assert Message.msg_types_arrow(pyarrow.array(lines)).to_pylist() == [
        "D",
        "8",
        None,
        "AB",
        None,
    ]


def test_the_standard_header_lifts_into_columns_of_its_own() -> None:
    """A column each; `entries` keeps the body and the boundary.

    Every one of them is the text the payload spelled -- `9=176` is the three
    characters and `43=Y` is the letter -- because this stage reads no
    dictionary. What the payload does not state stays null: the whole header
    is declared, and a message states the part of it that it states.
    """
    message = Message(
        body="8=FIX.4.4|9=176|35=D|34=1092|49=BUYSIDE|50=DESK|56=XPAR|115=ORIG|"
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
    assert Message.into_field_metadata() == {"version": "2"}


def test_a_header_field_stated_twice_two_ways_is_lifted_by_neither() -> None:
    """Two readings of one fact is not one statement of it.

    A bridge writing one field twice on purpose is telling the reader
    something a first-wins pop would throw away, so both stay in `entries` and
    the column says nothing -- while the fields beside it are lifted as usual.
    """
    torn = Message(body="8=FIX.4.4|49=A|49=B|55=IBM|10=000")

    assert torn.sendercompid is None
    assert [(entry.key, entry.value) for entry in torn.entries] == [
        ("49", "A"),
        ("49", "B"),
        ("55", "IBM"),
        ("10", "000"),
    ]

    repeated = Message(body="8=FIX.4.4|49=A|49=A|55=IBM|10=000")

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
        body="#BeginString=FIX.4.4|#SendingTime=20260814-09:30:00.000|#MsgType=D|#Side=1"
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
        row = Message(body=line)
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
        "|",
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


def test_payload_token_diagnostics_are_row_local_and_best_effort() -> None:
    found = Message.parse_arrow(pyarrow.array(["A=1|broken token=lost|B=2|"]))

    assert [entry["key"] for entry in found["entries"][0].as_py()] == ["A", "B"]
    assert found["parseerror"].to_pylist() == ["FIX parse skipped unmatched tokens: 1"]


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
    (parsed,) = Entry.parse_arrow(
        pyarrow.array(["54.5=x|NoPartyIDs[0].448=A|TECH.CLIENTID=42|TECH.NoPartyIDs[0].PartyID=P"])
    ).to_pylist()

    assert [(entry["tag"], entry["comp"], entry["key"]) for entry in parsed] == [
        (0, None, "54.5"),
        (448, "NoPartyIDs[0]", "448"),
        (0, None, "TECH.CLIENTID"),
        (0, "TECH.NoPartyIDs[0]", "PartyID"),
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
    first = Message(body="same", sourceurl="one.log", sourcerownum=2).identify()
    copied = Message(body="same", sourceurl="two.log", sourcerownum=9).identify()
    changed = Message(body="different", sourceurl="one.log", sourcerownum=2).identify()

    expected = hash_bytes(b"same")
    assert first.vhash == copied.vhash == expected
    assert first.xhash == copied.xhash == 0, "an unnamed raw line has no lifecycle"
    assert first.hash == copied.hash == txhash.couple128(0, expected)
    assert changed.vhash != first.vhash


def test_raw_identity_drops_its_own_exact_hash_from_scalar_and_arrow_links() -> None:
    first = Message(unix=1_000, body="same").identify()
    scalar = Message(
        unix=first.unix,
        body=first.body,
        linkhashes=[first.hash, -1],
    ).identify()
    assert scalar.hash == first.hash
    assert scalar.linkhashes == [-1]

    pending = Message(
        unix=first.unix,
        body=first.body,
        linkhashes=[first.hash, -1],
    )
    source = Message.into_arrow_batch([pending])
    columns = {name: source.column(name) for name in source.schema.names}
    arrow = Message.identified(columns, source.schema, 1)
    assert arrow.column("hash").to_pylist() == [first.into_row()["hash"]]
    assert arrow.column("linkhashes").to_pylist() == [[txhash.wide_bytes(-1)]]


def test_an_overwide_plugin_becomes_non_null_unknown_at_the_arrow_boundary() -> None:
    source = Message.into_arrow_batch([Message(unix=1, body="payload")])
    columns = {name: source.column(name) for name in source.schema.names}
    columns["plugin"] = pyarrow.array(["ModuleMarketDataManager"])

    identified = Message.identified(columns, source.schema, 1)

    assert identified.column("plugin").null_count == 0
    assert identified.column("plugin").to_pylist() == [Plugin.UNKNOWN.into_stored()]


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
    assert table.column("body").cast(pyarrow.string()).to_pylist() == [payload]
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
    assert table.column("eventtype").to_pylist() == [int(EventType.ORDER)]
    assert table.column("lastmkt").to_pylist() == [None]
    expected = hash_bytes(payload.encode("utf-8"))
    assert table.column("vhash").to_pylist() == [expected]
    assert table.column("xhash").to_pylist() == [txhash.wide_bytes(0)]
    assert table.column("altids").to_pylist() == [[]]
    assert txhash.vhash_of(table.column("hash")[0].as_py()) == expected


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

    assert Protocol.from_int(parsed["protocol"][0].as_py()) is Protocol.from_str("FIX4.2")
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

    assert Protocol.from_int(parsed["protocol"][0].as_py()) is Protocol.FIX
    assert parsed["direction"][0].as_py() == int(Direction.SENT)
    assert parsed["beginstring"][0].as_py() == "FIX4"


def test_prose_that_merely_contains_fix_is_not_a_message() -> None:
    """The BeginString value stops at a separator, and prose has none there."""
    parsed = Message.parse_arrow(pyarrow.array(["the 8=FIXTURE cost 12"]))

    assert Protocol.from_int(parsed["protocol"][0].as_py()) is Protocol.OTHER


def test_a_row_carrying_its_body_answers_the_syntax_columns_either_way() -> None:
    """Whoever tokenized its arguments -- `from_text` retains the body."""
    line = "8=FIX.4.2|9=176|35=D|34=1092|49=BUYSIDE|56=XPAR|11=ORD-1|10=203"

    assert Message.from_text(line).protocol is Protocol.from_str("FIX4.2")
    assert Message(body=line).protocol is Protocol.from_str("FIX4.2")
    # The sentinel is the member, so a caller spelling the default still gets
    # the reading rather than keeping the word it passed in.
    assert Message(body=line, protocol="other").protocol is Protocol.from_str("FIX4.2")
    assert Message(body=line, protocol="ul").protocol is Protocol.from_str("UL4.2")


def test_xmlapi_body_keeps_ordered_attributes_and_nested_components() -> None:
    body = (
        b"Receiving XmlApi: <?xml version='1.0'?><Order ClOrdID='A1' Side='1'>"
        b"<Instrument SecurityID='IBM'><Symbol>IBM</Symbol></Instrument>"
        b"<Leg Currency='USD'/><Leg Currency='EUR'/></Order>"
    )

    row = Message(body=body)

    assert row.body == body
    assert row.protocol is Protocol.XML
    assert row.direction is Direction.RECV
    assert [(entry.comp, entry.key, entry.value) for entry in row.entries] == [
        ("Order[0]", "ClOrdID", "A1"),
        ("Order[0]", "Side", "1"),
        ("Order[0].Instrument[0]", "SecurityID", "IBM"),
        ("Order[0].Instrument[0]", "Symbol", "IBM"),
        ("Order[0].Leg[0]", "Currency", "USD"),
        ("Order[0].Leg[1]", "Currency", "EUR"),
    ]


def test_from_text_xml_uses_the_ordered_structured_parser() -> None:
    xml = b'<Order ID="C1"><Leg Symbol="AAPL"/><Leg Symbol="MSFT"/></Order>'

    row = Message.from_text(xml)

    assert row.protocol is Protocol.XML
    assert [(entry.comp, entry.key, entry.value) for entry in row.entries] == [
        ("Order[0]", "ID", "C1"),
        ("Order[0].Leg[0]", "Symbol", "AAPL"),
        ("Order[0].Leg[1]", "Symbol", "MSFT"),
    ]


def test_malformed_xml_isolated_to_its_row() -> None:
    bodies = pyarrow.array(
        [
            b"<Order ClOrdID='A1'/>",
            b"XmlApi: <Order ClOrdID='broken'>",
            b"8=FIX.4.4|35=D|11=A2|10=000|",
        ],
        pyarrow.binary(),
    )

    parsed = Message.parse_arrow(bodies)

    assert [Protocol.from_int(code) for code in parsed["protocol"].to_pylist()] == [
        Protocol.XML,
        Protocol.XML,
        Protocol.from_str("FIX4.4"),
    ]
    assert parsed["parseerror"].to_pylist()[0] is None
    assert parsed["parseerror"].to_pylist()[1].startswith("XML parse failed: ParseError:")
    assert parsed["parseerror"].to_pylist()[2] is None
    assert parsed["entries"].to_pylist()[1] == []
    assert Message(body=bodies[1].as_py()).reason.startswith("XML parse failed: ParseError:")


def test_referential_uses_depth_aware_headers_and_canonical_tick_members() -> None:
    body = (
        "Receiving: Referential(XLON|equity|dbi;GB00BN7SWP63_XLON_GBX|["
        "quantity-type=, tick-size-scale-id=PRIMARY|[[0|0.01], [100|0.05]], "
        "vendor-note=[inside|the, value]])"
    )

    row = Message.from_text(body)

    assert row.protocol is Protocol.REFERENTIAL
    assert row.direction is Direction.RECV
    assert row.eventtype is EventType.INSTRUMENT
    assert row.msgtype is None
    assert [(entry.comp, entry.key, entry.value) for entry in row.entries] == [
        ("Referential", "Venue", "XLON"),
        ("Referential", "AssetClass", "equity"),
        ("Referential", "InstrumentKey", "dbi;GB00BN7SWP63_XLON_GBX"),
        ("Referential", "TickSizeScaleID", "PRIMARY"),
        ("TickRules[0]", "StartTickPriceRange", "0"),
        ("TickRules[0]", "TickIncrement", "0.01"),
        ("TickRules[1]", "StartTickPriceRange", "100"),
        ("TickRules[1]", "TickIncrement", "0.05"),
        ("Referential", "vendor-note", "[inside|the, value]"),
    ]
    assert all(entry.key != "QuantityType" for entry in row.entries), (
        "an empty source value is absence, not a null Entry value"
    )


def test_malformed_referential_isolated_to_its_row() -> None:
    bodies = pyarrow.array(
        [
            "Referential(XLON|equity|dbi;GB00BN7SWP63_XLON_GBX|[quantity-type=1])",
            "Referential(XLON|equity|dbi;GB00BN7SWP63_XLON_GBX|[unclosed=1)",
            "ACCOUNT=A1|MSGTYPE=D|SIDE=1",
        ]
    )

    parsed = Message.parse_arrow(bodies)

    assert [Protocol.from_int(code) for code in parsed["protocol"].to_pylist()] == [
        Protocol.REFERENTIAL,
        Protocol.REFERENTIAL,
        Protocol.from_str("UL5SP2"),
    ]
    assert parsed["parseerror"].to_pylist()[0] is None
    assert parsed["parseerror"].to_pylist()[1].startswith("Referential parse failed: ValueError:")
    assert parsed["parseerror"].to_pylist()[2] is None
    assert parsed["entries"].to_pylist()[1] == []
    assert Message(body=bodies[1].as_py()).reason.startswith(
        "Referential parse failed: ValueError:"
    )


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


#: Bytes no UTF-8 decoder accepts, one per rule that rejects them: a lone
#: continuation, an overlong encoding, a truncated sequence, a surrogate, and a
#: code point past U+10FFFF.
INVALID_UTF8 = (b"\x80", b"\xc0\x80", b"\xe2\x82", b"\xed\xa0\x80", b"\xf4\x90\x80\x80")


@pytest.mark.parametrize("position", [0, 1, 63, 64, 65, 127, 128, 4095])
def test_one_invalid_body_repairs_without_decoding_its_neighbours(position: int) -> None:
    """A dirty row is decoded; the rows beside it stay inside Arrow.

    Pinned at the boundaries of the halving leaf, because a run that validates
    is cast whole and only the leaf holding the bad row reaches Python -- so
    the row's offset inside that leaf is the one thing that can go wrong.
    """
    bodies: list[bytes] = [b"8=FIX.4.2|35=D|55=TTF|10=203|"] * 4096
    bodies[position] = b"\xff" + bodies[position]
    found = message_module._body_text_arrow(pyarrow.array(bodies, pyarrow.binary()))

    assert found.to_pylist() == [body.decode("utf-8", "replace") for body in bodies]


def test_every_invalid_shape_reads_as_the_replacement_decoder_reads_it() -> None:
    """The repair is `errors="replace"`, whatever made the bytes invalid."""
    bodies = [*INVALID_UTF8, b"caf\xc3\xa9", b"", b"\xff\xfe\xfd"]
    found = message_module._body_text_arrow(pyarrow.array(bodies, pyarrow.binary()))

    assert found.to_pylist() == [body.decode("utf-8", "replace") for body in bodies]


def test_a_null_body_reads_as_empty_text_whether_or_not_the_batch_is_dirty() -> None:
    """`body` is never null downstream, and a repair does not change that."""
    clean = pyarrow.array([b"35=D", None], pyarrow.binary())
    dirty = pyarrow.array([b"\xff", None], pyarrow.binary())

    assert message_module._body_text_arrow(clean).to_pylist() == ["35=D", ""]
    assert message_module._body_text_arrow(dirty).to_pylist() == ["�", ""]


def test_a_wholly_invalid_batch_still_returns_every_row() -> None:
    """Halving reaches a leaf on every path, so no row is dropped."""
    bodies = [b"\xff\xfe"] * 300
    found = message_module._body_text_arrow(pyarrow.array(bodies, pyarrow.binary()))

    assert found.to_pylist() == [b"\xff\xfe".decode("utf-8", "replace")] * 300
