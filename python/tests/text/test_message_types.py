"""Registry-driven event typing at the streamed message boundary."""

from __future__ import annotations

import pyarrow
import pytest

import rekep.text.entries as entries
from rekep.enums import EventType, Protocol
from rekep.text import Entry, Message


def _protocols(found: dict) -> list[str]:
    """The parsed `protocol` column spelled out, registered names included."""
    return [Protocol.from_int(code).code for code in found["protocol"].to_pylist()]


EVENT_TYPES = {
    "8": EventType.EXECUTION,
    "D": EventType.ORDER,
    "W": EventType.BOOK,
}

#: The standard header this stage lifts out of `entries` into columns of its
#: own, spelled out rather than imported from `rekep.text.message` so a field
#: quietly added to or dropped from the lift is a failure here.
SESSION_COLUMNS = (
    "beginstring",
    "bodylength",
    "msgtype",
    "sendercompid",
    "sendersubid",
    "senderlocationid",
    "targetcompid",
    "targetsubid",
    "targetlocationid",
    "onbehalfofcompid",
    "onbehalfofsubid",
    "onbehalfoflocationid",
    "delivertocompid",
    "delivertosubid",
    "delivertolocationid",
    "msgseqnum",
    "lastmsgseqnumprocessed",
    "possdupflag",
    "possresend",
    "sendingtime",
    "origsendingtime",
    "onbehalfofsendingtime",
    "applverid",
    "cstmapplverid",
    "applextid",
    "messageencoding",
    "securedatalen",
    "securedata",
    "signaturelength",
    "signature",
)


def parsed(*messages: str | None) -> dict[str, object]:
    """Parse one test column with its small configurable registry projection."""
    return Message.parse_arrow(pyarrow.array(messages, pyarrow.string()), EVENT_TYPES)


def test_a_mapped_message_type_assigns_its_registry_event_type() -> None:
    found = parsed("8=FIX.4.4|35=D|11=one|")

    assert found["msgtype"].to_pylist() == ["D"]
    assert found["eventtype"].to_pylist() == [int(EventType.ORDER)]


def test_the_configured_spelling_may_be_a_name_a_mnemonic_or_a_code() -> None:
    """A config written as the member's name, its mnemonic, or its stored code
    all land in the column as that code."""
    for spelled in ("ORDER", "order", int(EventType.ORDER), str(int(EventType.ORDER))):
        found = Message.parse_arrow(
            pyarrow.array(["8=FIX.4.4|35=D|11=one|"], pyarrow.string()), {"D": spelled}
        )
        assert found["eventtype"].to_pylist() == [int(EventType.ORDER)], spelled


def test_a_spelling_no_event_kind_answers_to_is_refused() -> None:
    """Malformed open codes and unknown stored integers are refused."""
    column = pyarrow.array(["8=FIX.4.4|35=D|11=one|"], pyarrow.string())
    with pytest.raises(ValueError, match="EventType"):
        Message.parse_arrow(column, {"D": "TOO-LONG-CODE"})
    with pytest.raises(ValueError, match="EventType"):
        Message.parse_arrow(column, {"D": 999})


def test_a_wire_discriminator_without_begin_string_is_fix() -> None:
    found = parsed("35=D|11=one|")

    assert found["msgtype"].to_pylist() == ["D"]
    assert _protocols(found) == ["FIX"]


def test_no_message_type_is_misc_and_keeps_no_incidental_assignments() -> None:
    found = parsed(
        "diagnostic A=1",
        "observed 35=D in prose",
        "observed MsgType=0 in prose",
        None,
        "",
    )

    assert found["msgtype"].to_pylist() == [None, None, None, None, None]
    assert found["eventtype"].to_pylist() == [int(EventType.MISC)] * 5
    assert found["entries"].to_pylist() == [[], [], [], [], []]


def test_an_unregistered_message_type_remains_unknown() -> None:
    found = parsed("8=FIX.4.4|35=ZZ|Text=future|")

    assert found["msgtype"].to_pylist() == ["ZZ"]
    assert found["eventtype"].to_pylist() == [int(EventType.UNKNOWN)]
    entry = found["entries"].to_pylist()[0][-1]
    assert (entry["key"], entry["value"]) == ("Text", "future")


def test_named_message_types_use_the_same_registry_mapping() -> None:
    found = parsed("MsgType=8|Text=rendered|", "#MSGTYPE=W|#Text=marked|")

    assert found["msgtype"].to_pylist() == ["8", "W"]
    assert _protocols(found) == ["UL", "UL"]
    assert found["eventtype"].to_pylist() == [
        int(EventType.EXECUTION),
        int(EventType.BOOK),
    ]


def test_user_defined_wire_wrapper_falls_back_to_named_kind() -> None:
    found = parsed("8=FIX.4.4|35=UL|#MSGTYPE=D|#SIDE=1|")

    assert found["msgtype"].to_pylist() == ["D"]
    assert _protocols(found) == ["FIXML"], "tags and names together"
    assert found["beginstring"].to_pylist() == ["FIX.4.4"]
    residual = found["entries"].to_pylist()[0]
    assert [entry["key"] for entry in residual] == ["SIDE"]


def test_a_regular_wire_kind_stays_authoritative_over_named_noise() -> None:
    found = parsed("8=FIX.4.4|35=8|58=quoted #MSGTYPE=D|10=000|")

    assert found["msgtype"].to_pylist() == ["8"]
    assert found["eventtype"].to_pylist() == [int(EventType.EXECUTION)]


def test_only_an_uppercase_user_wire_kind_falls_back_to_the_named_kind() -> None:
    found = parsed("35=UL|MsgType=D|", "35=uL|MsgType=D|")

    assert found["msgtype"].to_pylist() == ["D", "uL"]
    assert found["eventtype"].to_pylist() == [
        int(EventType.ORDER),
        int(EventType.UNKNOWN),
    ]


def test_message_type_is_read_from_tokens_not_prose_values_or_the_trailer() -> None:
    found = parsed(
        "diagnostic says 35=D| in prose",
        "diagnostic ends with 35=D|",
        "Text=quoted 35=D|Other=1",
        "8=FIX.4.4|10=000|35=D|",
        "8=FIX.4.4|10=000|MsgType=D|",
    )

    assert found["msgtype"].to_pylist() == [None] * 5
    assert found["eventtype"].to_pylist() == [int(EventType.MISC)] * 5
    # Every header field is measured against the same boundary: `8` precedes
    # the checksum in the last two rows and so is lifted, while the
    # discriminator written behind it is not.
    assert found["beginstring"].to_pylist() == [None, None, None, "FIX.4.4", "FIX.4.4"]
    assert [[entry["key"] for entry in row] for row in found["entries"].to_pylist()] == [
        [],
        [],
        ["Text", "Other"],
        ["10", "35"],
        ["10", "MsgType"],
    ]


def test_a_prefixed_marked_checksum_ends_discriminator_promotion() -> None:
    found = Message.parse_arrow(
        pyarrow.array(["wrapper #10=000|#MSGTYPE=0|#Text=after|"]),
        EVENT_TYPES,
    )

    assert found["msgtype"].to_pylist() == [None]
    assert found["eventtype"].to_pylist() == [int(EventType.MISC)]
    assert [entry["key"] for entry in found["entries"].to_pylist()[0]] == [
        "10",
        "MSGTYPE",
        "Text",
    ]


def test_a_bridge_renders_a_tag_it_has_no_name_for_as_a_digit_key() -> None:
    """`#35=D` is a marked key, so it is part of the message and not its prefix.

    A rule that took only a letter-initial name put such a token in the prose
    every reader of a bridge line cuts away, so the field was lost with it.
    """
    found = Message.parse_arrow(pyarrow.array(["toBridge #35=D|#55=IBM|#54=1|"]), EVENT_TYPES)

    assert found["msgtype"].to_pylist() == ["D"]
    assert found["eventtype"].to_pylist() == [int(EventType.ORDER)]
    assert [entry["key"] for entry in found["entries"].to_pylist()[0]] == ["55", "54"]


def test_only_a_valid_fix_begin_string_qualifies_a_single_assignment() -> None:
    found = parsed(
        "diagnostic 8=FIXTURE " + "x" * 10_000,
        "8=FIX.4.4|",
        "8=FIXT.1.1^",
    )

    assert found["msgtype"].to_pylist() == [None, None, None]
    # The qualifying rows are the ones that reached the splitter at all, and
    # the begin string they spelled is now the column rather than an entry.
    assert found["beginstring"].to_pylist() == [None, "FIX.4.4", "FIXT.1.1"]
    assert [
        [(entry["key"], entry["value"]) for entry in row] for row in found["entries"].to_pylist()
    ] == [
        [],
        [],
        [],
    ]


def test_bare_caret_delimited_fields_are_tokens() -> None:
    found = parsed("35=D^Text=kept^")

    assert found["msgtype"].to_pylist() == ["D"]
    assert [(entry["key"], entry["value"]) for entry in found["entries"].to_pylist()[0]] == [
        ("Text", "kept")
    ]


def test_chunk_boundaries_keep_types_and_arguments_aligned() -> None:
    messages = pyarrow.chunked_array(
        [
            pyarrow.array(["plain", "35=D|49=BUYSIDE|A=1"], pyarrow.large_string()),
            pyarrow.array(["#MSGTYPE=W|#B=2"], pyarrow.large_string()),
        ]
    )

    found = Message.parse_arrow(messages, EVENT_TYPES)

    assert found["eventtype"].to_pylist() == [
        int(EventType.MISC),
        int(EventType.ORDER),
        int(EventType.BOOK),
    ]
    assert found["msgtype"].to_pylist() == [None, "D", "W"]
    assert found["sendercompid"].to_pylist() == [None, "BUYSIDE", None]
    assert [[entry["key"] for entry in row] for row in found["entries"].to_pylist()] == [
        [],
        ["A"],
        ["B"],
    ]


def test_unstructured_long_rows_never_enter_the_key_value_splitter(monkeypatch) -> None:
    def unexpected(messages):
        raise AssertionError(f"split {len(messages)} rows")

    monkeypatch.setattr(entries, "parse_arrow", unexpected)
    found = Message.parse_arrow(pyarrow.array(["x" * 1_000_000, "diagnostic A=1"]), EVENT_TYPES)

    assert found["eventtype"].to_pylist() == [int(EventType.MISC), int(EventType.MISC)]
    assert found["entries"].to_pylist() == [[], []]


def test_plugin_codes_do_not_define_message_types() -> None:
    found = Message.parse_arrow(
        pyarrow.array(["8=FIX.4.4|35=D|11=one|"]),
        {"D": EventType.ORDER},
        plugins=pyarrow.array(["Jolokia"]),
    )

    assert found["eventtype"].to_pylist() == [int(EventType.ORDER)]
    assert found["beginstring"].to_pylist() == ["FIX.4.4"]
    assert [(entry["key"], entry["value"]) for entry in found["entries"][0].as_py()] == [
        ("11", "one"),
    ]


def test_plugin_key_names_and_null_values_are_applied_before_promotion() -> None:
    found = Message.parse_arrow(
        pyarrow.array(
            [
                "TYPE=D|CLIENT=A-1|EMPTY= NONE |BLANK=|",
                "TYPE=D|CLIENT=A-2|EMPTY=kept|",
            ]
        ),
        {"D": EventType.ORDER},
        plugins=pyarrow.array(["Bridge", "Other"]),
        plugin_keys={"bridge": {"TYPE": "MsgType", "CLIENT": "ClOrdID"}},
        null_values=["none", ""],
    )

    assert found["msgtype"].to_pylist() == ["D", None]
    assert found["eventtype"].to_pylist() == [int(EventType.ORDER), int(EventType.MISC)]
    assert [(entry["key"], entry["value"]) for entry in found["entries"][0].as_py()] == [
        ("ClOrdID", "A-1")
    ]
    assert [(entry["key"], entry["value"]) for entry in found["entries"][1].as_py()] == [
        ("TYPE", "D"),
        ("CLIENT", "A-2"),
        ("EMPTY", "kept"),
    ]


def test_null_values_do_not_define_the_retained_protocol_shape() -> None:
    found = Message.parse_arrow(
        pyarrow.array(["8=FIX.4.4|35=D|11=A-1|VENDOR=NONE|10=000|"]),
        null_values=["none"],
    )

    assert _protocols(found) == ["FIX"]
    assert [(entry["key"], entry["value"]) for entry in found["entries"][0].as_py()] == [
        ("11", "A-1"),
        ("10", "000"),
    ]


def test_entry_normalization_rejects_ambiguous_configuration() -> None:
    body = pyarrow.array(["TYPE=D|CLIENT=A-1|"])
    with pytest.raises(TypeError, match="null_values must be a sequence"):
        Message.parse_arrow(body, null_values="none")
    with pytest.raises(ValueError, match="same number of rows"):
        Message.parse_arrow(body, plugins=pyarrow.array([], pyarrow.string()))
    with pytest.raises(ValueError, match="two key maps"):
        Message.parse_arrow(
            body,
            plugins=pyarrow.array(["bridge"]),
            plugin_keys={"Bridge": {"TYPE": "MsgType"}, "bridge": {"TYPE": "EventType"}},
        )


@pytest.mark.parametrize(
    "prefix",
    [
        "Gateway <debug> payload -> ",
        "<debug>transport</debug> -> ",
    ],
)
def test_complete_event_xml_after_an_unknown_transport_prefix_is_xml(prefix: str) -> None:
    body = prefix + '<event id="one"><action type="update"/></event>'
    found = Message.parse_arrow(pyarrow.array([body]))

    assert _protocols(found) == ["XML"]
    assert [
        (entry["comp"], entry["key"], entry["value"]) for entry in found["entries"][0].as_py()
    ] == [
        ("event[0]", "id", "one"),
        ("event[0].action[0]", "type", "update"),
    ]


def test_complete_event_xml_inside_fix_text_stays_with_its_envelope() -> None:
    body = "8=FIX.4.4|35=0|58=<event id='one'></event>|10=000|"
    found = Message.parse_arrow(pyarrow.array([body]))

    assert _protocols(found) == ["FIX"]


def test_custom_protocol_classifier_reads_every_retained_row() -> None:
    heartbeat = "toBridge #MSGTYPE=0|#Text=" + "A=1|" * 1000
    market = "8=FIX.4.4|35=D|11=one|"

    class Classifier:
        seen: list[str] = []

        def into_arrow_protocol_array(self, messages, plugins, entries):
            self.seen.extend(messages.to_pylist())
            assert plugins.to_pylist() == ["bridge", "fix"]
            assert len(entries) == len(messages)
            return pyarrow.array([int(Protocol.FIX)] * len(messages), pyarrow.int64())

        def into_arrow_direction_array(self, messages, protocols):
            return pyarrow.repeat(pyarrow.scalar(0, pyarrow.int32()), len(messages))

    classifier = Classifier()
    found = Message.parse_arrow(
        pyarrow.array([heartbeat, market]),
        EVENT_TYPES,
        pyarrow.array(["bridge", "fix"]),
        protocol_rules=classifier,
    )

    assert classifier.seen == [heartbeat, market]
    assert _protocols(found) == ["FIX", "FIX"]


def test_a_stored_technical_message_keeps_empty_arguments(monkeypatch) -> None:
    stored = Message(
        body="8=FIX.4.4|35=0|58=" + "A=1|" * 1000,
        protocol=Protocol.MISC,
        msgtype="0",
        eventtype=EventType.MISC,
        entries=[],
    ).into_dict()

    def unexpected(cls, messages):
        raise AssertionError(f"reparsed {len(messages)} stored technical rows")

    monkeypatch.setattr(Entry, "parse_arrow", classmethod(unexpected))
    restored = Message.from_dict(stored)

    assert restored.msgtype == "0"
    assert restored.beginstring is None, "a stored row's header is what it stored"
    assert restored.protocol is Protocol.MISC
    assert restored.eventtype == EventType.MISC
    assert restored.entries == []


def test_empty_input_keeps_the_declared_column_types() -> None:
    found = Message.parse_arrow(pyarrow.array([], pyarrow.string()), EVENT_TYPES)

    assert found["eventtype"].type == pyarrow.int64()
    assert found["protocol"].type == Protocol.into_arrow_type().index_type
    for name in SESSION_COLUMNS:
        assert found[name].type == pyarrow.string(), name
    assert found["entries"].type == Message.into_field().field("entries").dtype
