"""Registry-driven event typing at the streamed message boundary."""

from __future__ import annotations

import pyarrow

from rekep.enums import EventType
from rekep.text import Kwarg, Message

EVENT_TYPES = {
    "8": EventType.EXECUTION,
    "D": EventType.ORDER,
    "W": EventType.BOOK,
}


def parsed(*messages: str | None) -> dict[str, object]:
    """Parse one test column with its small configurable registry projection."""
    return Message.parse_arrow(pyarrow.array(messages, pyarrow.string()), EVENT_TYPES)


def test_a_mapped_message_type_assigns_its_registry_event_type() -> None:
    found = parsed("8=FIX.4.4|35=D|11=one|")

    assert found["MsgType"].to_pylist() == ["D"]
    assert found["etype"].to_pylist() == [int(EventType.ORDER)]


def test_a_wire_discriminator_without_begin_string_is_fix() -> None:
    found = parsed("35=D|11=one|")

    assert found["MsgType"].to_pylist() == ["D"]
    assert found["protocol_code"].to_pylist() == ["FIX"]


def test_no_message_type_is_misc_and_keeps_no_incidental_assignments() -> None:
    found = parsed(
        "diagnostic A=1",
        "observed 35=D in prose",
        "observed MsgType=0 in prose",
        None,
        "",
    )

    assert found["MsgType"].to_pylist() == [None, None, None, None, None]
    assert found["etype"].to_pylist() == [int(EventType.MISC)] * 5
    assert found["kwargs"].to_pylist() == [[], [], [], [], []]


def test_an_unregistered_message_type_remains_unknown() -> None:
    found = parsed("8=FIX.4.4|35=ZZ|Text=future|")

    assert found["MsgType"].to_pylist() == ["ZZ"]
    assert found["etype"].to_pylist() == [int(EventType.UNKNOWN)]
    entry = found["kwargs"].to_pylist()[0][-1]
    assert (entry["key"], entry["value"]) == ("Text", "future")


def test_named_message_types_use_the_same_registry_mapping() -> None:
    found = parsed("MsgType=8|Text=rendered|", "#MSGTYPE=W|#Text=marked|")

    assert found["MsgType"].to_pylist() == ["8", "W"]
    assert found["etype"].to_pylist() == [
        int(EventType.EXECUTION),
        int(EventType.BOOK),
    ]


def test_user_defined_wire_wrapper_falls_back_to_named_kind() -> None:
    found = parsed("8=FIX.4.4|35=UL|#MSGTYPE=D|#SIDE=1|")

    assert found["MsgType"].to_pylist() == ["D"]
    residual = found["kwargs"].to_pylist()[0]
    assert [entry["key"] for entry in residual] == ["8", "SIDE"]


def test_a_regular_wire_kind_stays_authoritative_over_named_noise() -> None:
    found = parsed("8=FIX.4.4|35=8|58=quoted #MSGTYPE=D|10=000|")

    assert found["MsgType"].to_pylist() == ["8"]
    assert found["etype"].to_pylist() == [int(EventType.EXECUTION)]


def test_only_an_uppercase_user_wire_kind_falls_back_to_the_named_kind() -> None:
    found = parsed("35=UL|MsgType=D|", "35=uL|MsgType=D|")

    assert found["MsgType"].to_pylist() == ["D", "uL"]
    assert found["etype"].to_pylist() == [
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

    assert found["MsgType"].to_pylist() == [None] * 5
    assert found["etype"].to_pylist() == [int(EventType.MISC)] * 5
    assert [[entry["key"] for entry in row] for row in found["kwargs"].to_pylist()] == [
        [],
        [],
        ["Text", "Other"],
        ["8", "10", "35"],
        ["8", "10", "MsgType"],
    ]


def test_a_prefixed_marked_checksum_ends_discriminator_promotion() -> None:
    found = Message.parse_arrow(
        pyarrow.array(["wrapper #10=000|#MSGTYPE=0|#Text=after|"]),
        EVENT_TYPES,
    )

    assert found["MsgType"].to_pylist() == [None]
    assert found["etype"].to_pylist() == [int(EventType.MISC)]
    assert [entry["key"] for entry in found["kwargs"].to_pylist()[0]] == [
        "10",
        "MSGTYPE",
        "Text",
    ]


def test_only_a_valid_fix_begin_string_qualifies_a_single_assignment() -> None:
    found = parsed(
        "diagnostic 8=FIXTURE " + "x" * 10_000,
        "8=FIX.4.4|",
        "8=FIXT.1.1^",
    )

    assert found["MsgType"].to_pylist() == [None, None, None]
    assert [
        [(entry["key"], entry["value"]) for entry in row] for row in found["kwargs"].to_pylist()
    ] == [
        [],
        [("8", "FIX.4.4")],
        [("8", "FIXT.1.1")],
    ]


def test_bare_caret_delimited_fields_are_tokens() -> None:
    found = parsed("35=D^Text=kept^")

    assert found["MsgType"].to_pylist() == ["D"]
    assert [(entry["key"], entry["value"]) for entry in found["kwargs"].to_pylist()[0]] == [
        ("Text", "kept")
    ]


def test_chunk_boundaries_keep_types_and_arguments_aligned() -> None:
    messages = pyarrow.chunked_array(
        [
            pyarrow.array(["plain", "35=D|A=1"], pyarrow.large_string()),
            pyarrow.array(["#MSGTYPE=W|#B=2"], pyarrow.large_string()),
        ]
    )

    found = Message.parse_arrow(messages, EVENT_TYPES)

    assert found["etype"].to_pylist() == [10, 110, 320]
    assert found["MsgType"].to_pylist() == [None, "D", "W"]
    assert [[entry["key"] for entry in row] for row in found["kwargs"].to_pylist()] == [
        [],
        ["A"],
        ["B"],
    ]


def test_unstructured_long_rows_never_enter_the_key_value_splitter(monkeypatch) -> None:
    def unexpected(cls, messages):
        raise AssertionError(f"split {len(messages)} rows")

    monkeypatch.setattr(Kwarg, "parse_arrow", classmethod(unexpected))
    found = Message.parse_arrow(pyarrow.array(["x" * 1_000_000, "diagnostic A=1"]), EVENT_TYPES)

    assert found["etype"].to_pylist() == [10, 10]
    assert found["kwargs"].to_pylist() == [[], []]


def test_plugin_codes_do_not_define_message_types() -> None:
    found = Message.parse_arrow(
        pyarrow.array(["8=FIX.4.4|35=D|11=one|"]),
        {"D": EventType.ORDER},
        plugins=pyarrow.array(["Jolokia"]),
    )

    assert found["etype"].to_pylist() == [int(EventType.ORDER)]
    assert [(entry["key"], entry["value"]) for entry in found["kwargs"][0].as_py()] == [
        ("8", "FIX.4.4"),
        ("11", "one"),
    ]


def test_custom_protocol_classifier_reads_every_retained_row() -> None:
    heartbeat = "toBridge #MSGTYPE=0|#Text=" + "A=1|" * 1000
    market = "8=FIX.4.4|35=D|11=one|"

    class Classifier:
        seen: list[str] = []

        def into_arrow_protocol_array(self, messages, plugins):
            self.seen.extend(messages.to_pylist())
            assert plugins.to_pylist() == ["bridge", "fix"]
            return pyarrow.array(["FIX"] * len(messages))

    classifier = Classifier()
    found = Message.parse_arrow(
        pyarrow.array([heartbeat, market]),
        EVENT_TYPES,
        pyarrow.array(["bridge", "fix"]),
        protocol_rules=classifier,
    )

    assert classifier.seen == [heartbeat, market]
    assert found["protocol_code"].to_pylist() == ["FIX", "FIX"]


def test_a_stored_technical_message_keeps_empty_arguments(monkeypatch) -> None:
    stored = Message(
        message="8=FIX.4.4|35=0|58=" + "A=1|" * 1000,
        protocol_code="MISC",
        MsgType="0",
        etype=EventType.MISC,
        kwargs=[],
    ).into_dict()

    def unexpected(cls, messages):
        raise AssertionError(f"reparsed {len(messages)} stored technical rows")

    monkeypatch.setattr(Kwarg, "parse_arrow", classmethod(unexpected))
    restored = Message.from_dict(stored)

    assert restored.MsgType == "0"
    assert restored.protocol_code == "MISC"
    assert restored.etype == EventType.MISC
    assert restored.kwargs == []


def test_empty_input_keeps_the_declared_column_types() -> None:
    found = Message.parse_arrow(pyarrow.array([], pyarrow.string()), EVENT_TYPES)

    assert found["etype"].type == pyarrow.int32()
    assert found["MsgType"].type == pyarrow.string()
    assert found["kwargs"].type == Message.into_field().field("kwargs").arrow_type
