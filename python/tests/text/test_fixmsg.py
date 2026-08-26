"""`FixMsg`'s own contract; the parser that fills it is tested beside it."""

import datetime
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, FixCodec, FixMsg, Message
from rekep.fix import KWARGS, NO_PROTOCOL, FixRegistry, Party
from rekep.fix.columns import COLUMNS, COMMON, DECLARATIONS, FLAT, SESSION, STAMPS, _physical_type
from rekep.market import MIC, BookIterator, Event, EventType
from rekep.market.event import HOUR, SECOND
from rekep.text import Kwarg

#: The dictionary this repository publishes, beside `python/`, read offline:
#: a contract that only holds while the site answers is not a contract.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: The envelope every event carries, then log-only source ordering and content.
ENVELOPE = [
    "unix",
    "unix_partition",
    "etype",
    "cunix",
    "runix",
    "eunix",
    "sunix",
    "hash",
    "xhash",
    "linked_events",
    "version",
    "state",
    "code",
    "codes",
    "prev_unix",
    "parent_hash",
    "mic",
    "reason",
]
SOURCE: list[str] = []
LINE = [
    "source_url",
    "source_rownum",
    "thread_name",
    "plugin_code",
    "message",
    "protocol_code",
    "MsgType",
    "kwargs",
]
MESSAGE = [
    "unix_source",
    "protocol_version",
    "protocol_version_source",
    "MsgSeqNum",
    "Parties",
    "TrdRegTimestamps",
    "SideTrdRegTS",
    "ISINCODE",
]

#: Raw FIX names stay distinct from the protocol-neutral event envelope.
RAW_TAGS = {55: "Symbol", 34: "MsgSeqNum"}

#: The flattened message layer, derived from the module that names it and
#: pinned below -- so a column renamed in one file and not in the other fails
#: here, rather than moving both sides of every comparison together.
FLAT_COLUMNS = [column for _, column in FLAT]
ADDED_COLUMNS = [
    column for column in FLAT_COLUMNS if column not in set(ENVELOPE + SOURCE + LINE + MESSAGE)
]
EXPECTED_SESSION_COLUMNS = 33
EXPECTED_COMMON_COLUMNS = 26
EXPECTED_FLAT_COLUMNS = 77
EXPECTED_LOG_COLUMNS = 109


@pytest.fixture(scope="module")
def registry() -> FixRegistry:
    """The published dictionary. Offline, because this must not test the site."""
    return FixRegistry(cache_dir=DATA, offline=True)


def test_a_log_line_is_an_event() -> None:
    """Which is what lets a parsed log be read beside the orders it describes."""
    assert issubclass(FixMsg, Event)
    assert (
        FixMsg.into_field().into_arrow_schema().names
        == ENVELOPE + SOURCE + LINE + MESSAGE + ADDED_COLUMNS
    )


def test_a_logs_cached_contract_metadata_is_immutable() -> None:
    assert FixMsg.into_field_metadata() == {"version": "1"}
    with pytest.raises(TypeError):
        FixMsg.into_field_metadata()["version"] = "2"


def test_the_envelope_is_the_same_one_every_other_event_carries() -> None:
    """Position included: a reader of the envelope must not need to know the shape."""
    assert Event.into_field().names == ENVELOPE


def test_every_column_a_line_adds_is_required_except_the_payload() -> None:
    """A line always has a file, a thread and a plugin, even an empty one.

    `message` is the exception, and deliberately: on `fix.market`
    `kwargs` carries every field the line held, so the raw string is dropped
    rather than stored a second time. An all-null column costs nothing on
    disk, which is what makes one stored shape across the three tables
    affordable.
    """
    field = FixMsg.into_field()
    for name in LINE:
        if name in {"message", "MsgType", "kwargs"}:
            assert field.field(name).nullable, f"a row may leave {name} null"
            continue
        assert not field.field(name).nullable, name


def test_only_fixmsg_adds_registry_metadata_to_the_promoted_message_type() -> None:
    raw = Message.into_field().field("MsgType")
    parsed = FixMsg.into_field().field("MsgType")

    assert raw.nullable and "fix:tag" not in raw.metadata
    assert parsed.nullable and parsed.fix["tag"] == "35"


def test_a_line_always_says_which_protocol_it_carries() -> None:
    """`OTHER` is an answer and not a missing one -- it is most of a capture --
    so the column is NOT NULL and the fall-through is what a line starts as."""
    assert not FixMsg.into_field().field("protocol_code").nullable
    assert FixMsg.into_field().field("protocol_code").arrow_type == pyarrow.string()
    assert FixMsg().protocol_code == NO_PROTOCOL


def test_a_line_carrying_no_message_has_no_pairs_at_all() -> None:
    """Null is not an empty list: a bridge that sent an empty payload and a stack
    trace that never was a message have to stay tellable apart."""
    assert FixMsg.into_field().field("kwargs").nullable
    assert FixMsg().kwargs is None


def test_an_explicit_empty_parsed_argument_list_is_not_reparsed() -> None:
    parsed = FixMsg(message="8=FIX.4.4|35=D|10=000|", kwargs=[])

    assert parsed.kwargs == []


def test_a_stored_field_always_says_what_it_is() -> None:
    """`tag` and `key` are how a consumer addresses a field, so neither is null:
    a field the dictionary did not resolve is `tag` `0` and not a missing tag."""
    member = FixMsg.into_field().field("kwargs")
    assert pyarrow.types.is_list(member.arrow_type)
    assert member.item.nullable is False
    assert member.item.field("tag").nullable is False
    assert member.item.field("key").nullable is False
    assert member.item.field("value").nullable is False
    for name in ("namespace", "comp"):
        assert member.item.field(name).nullable is True, name
        assert member.item.field(name).arrow_type == pyarrow.string(), name
    assert all(isinstance(entry, Kwarg) for entry in FixMsg(kwargs=[(55, "IBM")]).kwargs or ())


def test_stored_fields_keep_repeats_across_python_and_arrow_entry_shapes() -> None:
    reader = FixMsg(
        MsgType="D",
        kwargs=[
            (55, "A"),
            [55, "B"],
            {"tag": 55, "key": "Symbol", "value": "C"},
            ("VenueOwnThing", "x"),
            ["VenueOwnThing", "y"],
            {"tag": 0, "key": "VenueOwnThing", "value": "z"},
            {"tag": 0, "key": "CLIENTID", "value": "ACCT-TEST-01", "namespace": "TECH"},
            {"tag": 448, "key": "PartyID", "value": "PARTY-TEST-A", "comp": "NoPartyIDs[0]"},
        ],
    ).into_fix_events()

    assert reader.message.into_fix_pairs() == [
        ("35", "D"),
        ("55", "A"),
        ("55", "B"),
        ("55", "C"),
        ("VenueOwnThing", "x"),
        ("VenueOwnThing", "y"),
        ("VenueOwnThing", "z"),
        ("TECH.CLIENTID", "ACCT-TEST-01"),
        ("NoPartyIDs[0].PartyID", "PARTY-TEST-A"),
    ], "a resolved field keeps the component position that is part of its identity"


def test_market_translation_uses_the_parsed_row_it_was_given() -> None:
    row = FixMsg.from_pairs([("8", "FIX.4.4"), ("35", "D"), ("11", "C1")])

    assert row.into_fix_events().message is row


def test_scalar_parsing_resolves_the_fix_version_once_on_the_message() -> None:
    row = FixMsg.from_text("8=FIX.4.4|35=D|11=C1|10=000")

    assert row.protocol_code == "FIX"
    assert row.protocol_version == "4.4"
    assert row.protocol_version_source == "begin_string"


def test_scalar_hybrid_projection_prefers_named_registry_identity() -> None:
    row = FixMsg.from_("8=FIX.4.4|35=U1|55=wire|#MSGTYPE=D|#SYMBOL=named|11=C1|10=000|")

    assert row.MsgType == "D"
    assert row.into_fix_events(fix_version="4.4").by_tag["55"] == "named"


def test_named_group_members_do_not_shadow_numeric_repetitions() -> None:
    row = FixMsg.from_pairs([("448", "wire"), ("NoPartyIDs[1].PartyID", "named")])
    reader = row.into_fix_events(fix_version="4.4")

    assert [value for tag, value in row.into_fix_pairs(reader.access) if tag == "448"] == [
        "wire",
        "named",
    ]


def test_arrow_named_group_members_remain_repetitions(registry: FixRegistry) -> None:
    source = pyarrow.array(
        [
            [
                {"tag": 448, "key": "448", "value": "wire"},
                {
                    "tag": 0,
                    "key": "PARTYID",
                    "value": "named",
                    "comp": "NoPartyIDs[1]",
                },
            ]
        ],
        type=KWARGS,
    )
    resolved = FixCodec(registry=registry).complete_kwargs(source, "4.4")

    kept = FixMsg._prefer_named_kwargs(source, resolved)

    assert [entry["value"] for entry in kept[0].as_py()] == ["wire", "named"]


def test_hybrid_flat_names_do_not_erase_numeric_repeating_groups(
    registry: FixRegistry,
) -> None:
    party_line = (
        "8=FIX.4.4|35=UL|#MSGTYPE=D|#PARTYID=HEADER|453=1|"
        "448=GROUP|447=D|452=1|11=C1|55=AAPL|54=1|38=1|"
        "60=20260821-10:00:00|10=000"
    )
    party_batch = FixMsg.from_message_arrow_batch(
        _raw_batch(Message(message=party_line)), FixCodec(registry=registry)
    )
    party = FixMsg.from_dict(party_batch.to_pylist()[0])

    assert party_batch.column("Parties")[0].as_py()[0]["PartyID"] == "GROUP"
    assert party.group(453) == [[("448", "GROUP"), ("447", "D"), ("452", "1")]]

    depth_line = (
        "8=FIX.4.4|35=UL|#MSGTYPE=X|#SYMBOL=HEADER|268=1|279=0|269=0|55=ENTRY|270=100|271=1|10=000"
    )
    depth_batch = FixMsg.from_message_arrow_batch(
        _raw_batch(Message(message=depth_line)), FixCodec(registry=registry)
    )
    depth = FixMsg.from_dict(depth_batch.to_pylist()[0])

    assert depth.group(268) == [
        [("279", "0"), ("269", "0"), ("55", "ENTRY"), ("270", "100"), ("271", "1")]
    ]
    assert [event.symbol for event in depth.into_market_events(fix_version="4.4")] == ["ENTRY"]


def test_typed_timestamps_keep_direct_and_stored_book_outputs_equal(
    registry: FixRegistry,
) -> None:
    line = (
        "8=FIX.4.4|35=8|37=O1|11=C1|17=E1|55=AAPL|54=1|39=1|150=1|"
        "38=5|31=100|32=2|14=2|151=3|768=1|769=20260821-09:59:00|770=1|"
        "60=20260821-10:01:00|10=000"
    )
    direct = FixMsg.from_text(line)
    stored_batch = FixMsg.from_message_arrow_batch(
        _raw_batch(Message(message=line)), FixCodec(registry=registry)
    )
    stored = FixMsg.from_dict(stored_batch.to_pylist()[0])

    assert direct.get(769).raw == "20260821-09:59:00"
    assert [value for tag, value in direct.pairs if tag == "769"] == ["20260821-09:59:00.000000"]
    assert [value for tag, value in stored.pairs if tag == "769"] == ["20260821-09:59:00.000000"]
    assert list(BookIterator(logs=[stored], snapshot_every=0)) == list(
        BookIterator(logs=[direct], snapshot_every=0)
    )


def test_market_projection_keeps_typed_fix_timestamp_spelling() -> None:
    row = FixMsg(SendingTime=datetime.datetime(2026, 8, 25, 9, 30, 1, 250000))

    assert ("52", "20260825-09:30:01.250000") in row.into_fix_pairs()


def test_malformed_stored_field_entries_are_not_silently_dropped() -> None:
    with pytest.raises(KeyError, match="key"):
        FixMsg(kwargs=[{"value": "x"}])
    with pytest.raises(ValueError, match="not enough values"):
        FixMsg(kwargs=[["OnlyKey"]])


def test_parties_keep_exact_registry_fields_and_a_flexible_buffer(
    registry: FixRegistry,
) -> None:
    parties = FixMsg.into_field().field("Parties")
    assert parties.nullable and not parties.item.nullable
    assert parties.metadata["fix:component"] == "Parties"
    for tag, name in ((448, "PartyID"), (447, "PartyIDSource"), (452, "PartyRole")):
        expected = registry.scalar(tag)
        actual = Party.into_field().field(name)
        assert actual.fix["name"] == expected.name
        assert actual.arrow_type == expected.arrow_type
        assert actual.metadata == expected.metadata
        assert actual.description == expected.description
    assert Party.into_field().field("buffer").arrow_type == pyarrow.map_(
        pyarrow.string(), pyarrow.field("value", pyarrow.string(), nullable=False)
    )


def test_every_column_is_documented() -> None:
    for member in FixMsg.into_field().fields:
        assert member.description, f"{member.name} has no description"
        assert "\n" not in member.description, f"{member.name} description is not one line"


def test_the_key_is_the_moment_and_the_line() -> None:
    """Two columns: a hash identifies the line, the time is what an engine prunes on."""
    assert FixMsg.into_field().primary_keys() == ["unix", "hash"]


def test_the_partition_is_the_hour_the_line_falls_in() -> None:
    """An identity partition on an integer, so every engine below reads it alike."""
    assert FixMsg.into_field().partition_keys() == {"unix_partition": "identity"}
    assert FixMsg.into_field().field("unix_partition").arrow_type == pyarrow.int32()


def test_every_unix_column_declares_its_unit() -> None:
    for name in ("unix", "cunix", "runix", "eunix", "sunix", "prev_unix"):
        metadata = FixMsg.into_field().field(name).metadata
        assert metadata["unit"] == "ns", name
        assert metadata["epoch"] == "1970-01-01", name
    partition_metadata = FixMsg.into_field().field("unix_partition").metadata
    assert partition_metadata["unit"] == "second"
    assert partition_metadata["epoch"] == "1970-01-01"


def test_the_line_digest_is_an_int64_like_every_other_identifier() -> None:
    """The one column every engine below Arrow reads the same way, and the key
    is `(unix, hash)` -- so two digests only meet if they also share a
    nanosecond."""
    for name in ("hash", "xhash"):
        assert FixMsg.into_field().field(name).arrow_type == pyarrow.int64(), name
    assert FixMsg.into_field().field("unix").arrow_type == pyarrow.int64()


def test_a_line_is_unclassified_until_something_classifies_it() -> None:
    """The fallback the rules fall back to, on the class rather than in the parser."""
    assert FixMsg.into_event_type() is EventType.UNKNOWN
    assert FixMsg().etype is EventType.UNKNOWN


def test_the_hour_is_derived_from_the_instant() -> None:
    built = FixMsg(unix=3 * HOUR + 5)
    hour_seconds = HOUR // SECOND
    assert built.unix_partition == 3 * hour_seconds
    assert FixMsg(unix=-1).unix_partition == -hour_seconds, (
        "and it floors, either side of the epoch"
    )


def test_the_schema_says_which_class_it_came_from() -> None:
    schema = FixMsg.into_field().into_arrow_schema()
    assert schema.metadata[b"name"] == b"FixMsg"
    assert Field.from_arrow_schema(schema) == FixMsg.into_field()


def _stored(tag: int, key: str, value: str) -> dict[str, object]:
    """One stored field in the whole spelling `FixMsg.into_dict` writes."""
    return {"tag": tag, "key": key, "value": value, "namespace": None, "comp": None}


def test_a_row_round_trips_as_a_document() -> None:
    """The message layer preserves checksums and repeated ordered pairs."""
    row = FixMsg(
        source_url="a.txt",
        unix=2,
        hash=3,
        xhash=3,
        etype=EventType.ORDER,
        thread_name="t",
        plugin_code="d",
        message="m",
        protocol_code="FIX",
        kwargs=[_stored(11, "ClOrdID", one) for one in ("ORD-1", "ORD-1-again")]
        + [_stored(0, "ISINCODE", one) for one in ("FAKE-ISIN-0001", "FAKE-ISIN-0002")],
        code="TTF",
        MsgSeqNum=7,
        Symbol="TTF",
        SendingTime=datetime.datetime.fromtimestamp(1_755_163_800.123, tz=datetime.UTC),
        PossDupFlag=True,
        CheckSum="010",
        mic=MIC.from_str("XPAR"),
        reason="test reject",
    )
    assert FixMsg.from_json(row.into_json()) == row


def test_typed_components_share_stored_access_and_group_projection() -> None:
    row = FixMsg(
        Parties=[Party(PartyID="P1", PartyIDSource="D", PartyRole=1)],
        kwargs=[],
    )
    restored = FixMsg.from_json(row.into_json())

    assert restored.pairs == [
        ("453", "1"),
        ("448", "P1"),
        ("447", "D"),
        ("452", "1"),
    ]
    assert restored.get("PartyID").raw == "P1"
    assert restored.group(453) == [[("448", "P1"), ("447", "D"), ("452", "1")]]


def test_mic_is_a_lossless_optional_int32_code() -> None:
    member = FixMsg.into_field().field("mic")
    assert member.nullable and member.arrow_type == pyarrow.int32()
    assert member.metadata["enum:encoding"] == "ascii-big-endian"
    assert member.metadata["enum:pattern"] == "[A-Z0-9]{4}"
    assert "enum:dynamic" not in member.metadata


def test_reason_is_generic_optional_text_on_every_event() -> None:
    member = Event.into_field().field("reason")
    assert member.nullable and member.arrow_type == pyarrow.string()
    assert "fix:tag" not in member.metadata


# -- the raw-message boundary -------------------------------------------------


def _raw_batch(*messages: Message) -> pyarrow.RecordBatch:
    """One raw-message batch, including the zero-row shape."""
    if not messages:
        return pyarrow.RecordBatch.from_pylist([], schema=Message.into_field().into_arrow_schema())
    return next(iter(Message.into_arrow_reader(messages)))


def test_fixmsg_conversion_is_the_layer_that_parses_fix(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(
            etype=EventType.ORDER,
            message=(
                "8=FIX.4.4|35=D|34=7|41=ROOT|55=IBM|461=EXXXXX|6=12.5|"
                "453=1|448=BUYSIDE|447=D|452=1|10=000|"
            ),
            source_url="capture.log",
            source_rownum=1,
            plugin_code="fix",
        ),
        Message(
            etype=EventType.EXECUTION,
            message=(
                "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=8|#ORIGCLORDID=OLD|#ISINCODE=XX0000084733|"
            ),
            source_url="capture.log",
            source_rownum=2,
            plugin_code="ULBridge",
        ),
        Message(
            message="plain text",
            source_url="capture.log",
            source_rownum=3,
            plugin_code="misc",
        ),
    )

    assert raw.schema.names == Message.into_field().names
    assert raw.column("MsgType").to_pylist() == ["D", "8", None]
    assert raw.column("protocol_code").to_pylist() == ["FIX", "UL", "OTHER"]
    assert "OrigClOrdID" not in raw.schema.names

    parsed = FixMsg.from_message_arrow_batch(raw, FixCodec(registry=registry))

    assert parsed.column("etype").to_pylist() == [
        int(EventType.ORDER),
        int(EventType.EXECUTION),
        int(EventType.MISC),
    ]
    assert parsed.column("protocol_code").to_pylist() == ["FIX", "UL", "OTHER"]
    assert parsed.column("protocol_version").to_pylist() == ["4.4", "4.4", None]
    assert parsed.column("MsgType").to_pylist() == ["D", "8", None]
    assert parsed.column("MsgSeqNum").to_pylist() == [7, None, None]
    assert parsed.column("OrigClOrdID").to_pylist() == ["ROOT", "OLD", None]
    assert parsed.column("CFICode").to_pylist() == ["EXXXXX", None, None]
    assert parsed.column("AvgPx").to_pylist() == [12.5, None, None]
    assert parsed.column("ISINCODE").to_pylist() == [None, "XX0000084733", None]
    assert parsed.column("Parties").to_pylist()[0] == [
        {
            "PartyID": "BUYSIDE",
            "PartyIDSource": "D",
            "PartyRole": 1,
            "buffer": None,
        }
    ]
    assert parsed.column("codes").to_pylist()[0] == [("orig_cl_ord_id", "ROOT")]


def test_fixmsg_preserves_the_message_stage_type_and_event_code(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(
            message="8=FIX.4.4|35=D|11=A|10=000|ExecutionReport",
            MsgType="D",
            etype=EventType.QUOTE,
        )
    )

    parsed = FixMsg.from_message_arrow_batch(raw, FixCodec(registry=registry))

    assert parsed.column("MsgType").to_pylist() == ["D"]
    assert parsed.column("etype").to_pylist() == [int(EventType.QUOTE)]


def test_fixmsg_projection_does_not_need_the_raw_message(registry: FixRegistry) -> None:
    raw = _raw_batch(
        Message(message="8=FIX.4.4|35=D|11=A|VendorField=x|10=000|"),
        Message(message="8=FIX.4.4|35=UL|#MSGTYPE=D|#CLORDID=B|10=000|"),
    )
    codec = FixCodec(registry=registry)

    whole = FixMsg.from_message_arrow_batch(raw, codec)
    projected = FixMsg.from_message_arrow_batch(
        raw.select([name for name in raw.schema.names if name != "message"]), codec
    )

    assert whole.column("protocol_code").to_pylist() == ["FIX", "UL"]
    assert projected.column("protocol_code").equals(whole.column("protocol_code"))
    assert projected.column("kwargs").equals(whole.column("kwargs"))
    assert projected.column("hash").equals(whole.column("hash"))
    assert projected.column("message").null_count == projected.num_rows


def test_staged_protocol_matching_the_codec_survives_projection(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(message="8=FIX.4.4|35=UL|#MSGTYPE=D|#CLORDID=A|10=000|"))
    codec = FixCodec(registry=registry)
    at = raw.schema.get_field_index("protocol_code")
    staged = codec.categorise(raw.column("message"), raw.column("plugin_code"))
    raw = raw.set_column(at, raw.schema.field(at), staged)

    whole = FixMsg.from_message_arrow_batch(raw, codec)
    projected = FixMsg.from_message_arrow_batch(raw.drop_columns(["message"]), codec)

    assert whole.column("protocol_code").to_pylist() == ["UL"]
    assert whole.column("ClOrdID").to_pylist() == ["A"]
    assert projected.column("protocol_code").equals(whole.column("protocol_code"))
    assert projected.column("ClOrdID").equals(whole.column("ClOrdID"))
    assert projected.column("kwargs").equals(whole.column("kwargs"))
    assert projected.column("hash").equals(whole.column("hash"))


def test_wire_discriminator_without_begin_string_survives_projection(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(message="35=D|11=A|"))
    codec = FixCodec(registry=registry)

    whole = FixMsg.from_message_arrow_batch(raw, codec)
    projected = FixMsg.from_message_arrow_batch(raw.drop_columns(["message"]), codec)

    assert raw.column("protocol_code").to_pylist() == ["FIX"]
    assert [(entry["key"], entry["value"]) for entry in whole.column("kwargs")[0].as_py()] == [
        ("11", "A")
    ]
    assert projected.column("kwargs").equals(whole.column("kwargs"))
    assert projected.column("hash").equals(whole.column("hash"))


def test_unread_message_identity_survives_raw_message_projection(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(message="alpha prose", source_url="capture.log", source_rownum=1).identify(),
        Message(message="beta prose", source_url="capture.log", source_rownum=1).identify(),
    )
    codec = FixCodec(registry=registry)

    whole = FixMsg.from_message_arrow_batch(raw, codec)
    projected = FixMsg.from_message_arrow_batch(raw.drop_columns(["message"]), codec)

    assert whole.column("kwargs").null_count == 2
    assert whole.column("hash").equals(projected.column("hash"))
    assert len(set(whole.column("hash").to_pylist())) == 2


def test_fixmsg_projection_preserves_the_configured_message_mic(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(message="8=FIX.4.4|35=D|11=A|10=000|", mic=MIC.from_str("XPAR")))
    codec = FixCodec(registry=registry)

    whole = FixMsg.from_message_arrow_batch(raw, codec)
    projected = FixMsg.from_message_arrow_batch(raw.drop_columns(["message"]), codec)

    assert whole.column("mic").to_pylist() == [int(MIC.from_str("XPAR"))]
    assert projected.column("mic").equals(whole.column("mic"))


def test_numeric_flat_fixmsg_arrow_matches_the_registry_reference(
    registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rekep.text.fixmsg_arrow as fixmsg_arrow

    lines = (
        "8=FIX.4.4|35=D|34=1|11=C1|55=AAPL|54=Buy|38=10|40=2|44=100.5|"
        "60=20260825-09:30:00.123456789|9998=audit|10=000|",
        "8=FIX.4.4|35=F|34=2|41=C1|11=C2|55=AAPL|54=1|38=10|60=20260825-09:30:01|10=000|",
        "8=FIX.4.4|35=G|34=3|41=C1|11=C3|55=AAPL|54=1|38=12|44=99.5|60=20260825-09:30:02|10=000|",
        "8=FIX.4.4|35=8|34=4|37=O1|11=C3|17=E1|55=AAPL|54=1|39=1|150=F|"
        "38=12|31=99.25|32=2|14=2|151=10|6=99.25|43=Y|"
        "60=20260825-09:30:03.5|10=000|",
        "8=FIX.4.4|35=AE|34=5|17=E2|55=AAPL|54=2|31=99.75|32=3|60=20260825-09:30:04|10=000|",
    )
    source = _raw_batch(*(Message(message=line) for line in lines)).drop_columns(["message"])
    codec = FixCodec(registry=registry)
    original = fixmsg_arrow.into_flat_fixmsg_batch
    activated: list[bool] = []

    def observed(*args, **kwargs):
        translated = original(*args, **kwargs)
        activated.append(translated is not None)
        return translated

    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", observed)
    translated = FixMsg.from_message_arrow_batch(source, codec)
    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", lambda *args, **kwargs: None)
    reference = FixMsg.from_message_arrow_batch(source, codec)

    assert activated == [True]
    assert translated.equals(reference, check_metadata=True)
    assert translated.column("Side").to_pylist()[0] == "1"
    assert translated.column("mic").null_count == 5


def test_lifted_numeric_keeps_only_a_raw_spelling_typing_cannot_reproduce(
    registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rekep.text.fixmsg_arrow as fixmsg_arrow

    source = _raw_batch(
        Message(message="8=FIX.4.4|35=8|6=0010.5000|10=000|"),
        Message(message="8=FIX.4.4|35=8|6=10.5|10=000|"),
    ).drop_columns(["message"])
    codec = FixCodec(registry=registry)
    fast = FixMsg.from_message_arrow_batch(source, codec)
    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", lambda *args, **kwargs: None)
    reference = FixMsg.from_message_arrow_batch(source, codec)

    assert fast.equals(reference, check_metadata=True)
    assert fast.column("AvgPx").to_pylist() == [10.5, 10.5]
    assert fast.column("hash")[0].as_py() != fast.column("hash")[1].as_py()
    assert [
        [(entry["tag"], entry["value"]) for entry in row if entry["tag"] == 6]
        for row in fast.column("kwargs").to_pylist()
    ] == [[(6, "0010.5000")], []]


def test_numeric_fixmsg_arrow_falls_back_when_one_row_has_no_version(
    registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rekep.text.fixmsg_arrow as fixmsg_arrow

    source = _raw_batch(
        Message(message="8=FIX.4.4|35=D|11=A|55=IBM|10=000|"),
        Message(message="35=D|11=B|55=MSFT|10=000|"),
    ).drop_columns(["message"])
    original = fixmsg_arrow.into_flat_fixmsg_batch
    activated: list[bool] = []

    def observed(*args, **kwargs):
        translated = original(*args, **kwargs)
        activated.append(translated is not None)
        return translated

    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", observed)
    translated = FixMsg.from_message_arrow_batch(source, FixCodec(registry=registry))

    assert activated == [False, True]
    assert translated.column("ClOrdID").to_pylist() == ["A", None]
    assert translated.column("Symbol").to_pylist() == ["IBM", None]
    assert [entry["tag"] for entry in translated.column("kwargs")[1].as_py()] == [11, 55, 10]


def test_mixed_fixmsg_batch_keeps_flat_rows_fast_and_scatters_exactly(
    registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rekep.text.fixmsg_arrow as fixmsg_arrow

    lines = (
        "8=FIX.4.4|35=D|11=A|55=IBM|10=000|",
        "8=FIX.4.4|35=UL|#MSGTYPE=D|#CLORDID=UL-1|10=000|",
        "8=FIX.4.4|35=F|41=A|11=B|55=IBM|10=000|",
        "8=FIX.4.4|35=D|453=1|448=P1|447=D|452=1|11=C|10=000|",
        "8=FIX.4.4|35=G|41=B|11=D|55=IBM|10=000|",
        "35=D|11=NO-VERSION|55=MSFT|10=000|",
        "8=FIX.4.4|35=AE|17=E1|55=IBM|31=10|32=1|10=000|",
        "8=FIX.4.4|35=D|11=VENDOR|VendorField=x|10=000|",
    )
    source = _raw_batch(*(Message(message=line) for line in lines)).drop_columns(["message"])
    codec = FixCodec(registry=registry)
    original = fixmsg_arrow.into_flat_fixmsg_batch
    activated: list[bool] = []

    def observed(*args, **kwargs):
        translated = original(*args, **kwargs)
        activated.append(translated is not None)
        return translated

    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", observed)
    translated = FixMsg.from_message_arrow_batch(source, codec)
    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", lambda *args, **kwargs: None)
    reference = FixMsg.from_message_arrow_batch(source, codec)

    assert activated == [False, True]
    assert translated.equals(reference, check_metadata=True)
    assert translated.column("ClOrdID").to_pylist() == [
        "A",
        "UL-1",
        "B",
        "C",
        "D",
        None,
        None,
        "VENDOR",
    ]


def test_raw_direction_words_do_not_change_projected_mic(registry: FixRegistry) -> None:
    raw = _raw_batch(Message(message="received 8=FIX.4.4|35=D|49=XPAR|56=XNAS|11=A|10=000|"))
    codec = FixCodec(registry=registry)

    whole = FixMsg.from_message_arrow_batch(raw, codec)
    projected = FixMsg.from_message_arrow_batch(raw.drop_columns(["message"]), codec)

    assert whole.column("mic").to_pylist() == [int(MIC.from_str("XNAS"))]
    assert projected.column("mic").equals(whole.column("mic"))


def test_fixmsg_conversion_preserves_static_extra_columns(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(message="8=FIX.4.4|35=D|11=A|10=000|"))
    static = pyarrow.field(
        "capture_id",
        pyarrow.string(),
        nullable=False,
        metadata={b"source": b"static"},
    )
    raw = raw.append_column(static, pyarrow.array(["day-1"]))

    parsed = FixMsg.from_message_arrow_batch(raw, FixCodec(registry=registry))

    assert parsed.schema.names == [*FixMsg.into_field().names, "capture_id"]
    assert parsed.schema.field("capture_id") == static
    assert parsed.column("capture_id").to_pylist() == ["day-1"]


def test_fixmsg_applies_checksum_semantics_to_the_stored_arguments(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(message="8=FIX.4.4|35=D|10=000|55=AFTER-CHECKSUM|"))
    assert raw.column("kwargs")[0].as_py()[-1]["value"] == "AFTER-CHECKSUM"

    parsed = FixMsg.from_message_arrow_batch(raw, FixCodec(registry=registry))

    assert parsed.column("Symbol").to_pylist() == [None]
    assert all(entry["value"] != "AFTER-CHECKSUM" for entry in parsed.column("kwargs")[0].as_py())


def test_fixmsg_consumes_a_hash_delimited_wire_message(registry: FixRegistry) -> None:
    raw = _raw_batch(Message(message="8=FIX.4.4#35=D#55=TTF#10=000"))

    parsed = FixMsg.from_message_arrow_batch(raw, FixCodec(registry=registry))

    assert parsed.column("MsgType").to_pylist() == ["D"]
    assert parsed.column("Symbol").to_pylist() == ["TTF"]


def test_hybrid_named_fields_shadow_numeric_copies(registry: FixRegistry) -> None:
    raw = _raw_batch(
        Message(
            message=(
                "8=FIX.4.4|35=UL|9998=before|55=wire|#MSGTYPE=D|"
                "#VENDOR.OWN=x|#SYMBOL=named|9999=after|10=000|"
            )
        )
    )

    parsed = FixMsg.from_message_arrow_batch(raw, FixCodec(registry=registry))
    residual = parsed.column("kwargs")[0].as_py()

    assert parsed.column("MsgType").to_pylist() == ["D"]
    assert parsed.column("Symbol").to_pylist() == ["named"]
    assert [entry["value"] for entry in residual if entry["tag"] in {9998, 9999}] == [
        "before",
        "after",
    ]
    assert all(entry["tag"] != 55 for entry in residual)


def test_staged_groups_preserve_malformed_continuations(registry: FixRegistry) -> None:
    line = "toBridge #NOPARTYIDS[0]=PARTYID=x\x01garbage|#SIDE=1"
    raw = _raw_batch(Message(message=line))
    codec = FixCodec(registry=registry)

    staged = codec.into_pairs_from_kwargs(raw.column("kwargs"), "UL")
    direct = codec.into_pairs(pyarrow.array([line]), "UL")

    assert (
        staged.to_pylist()
        == direct.to_pylist()
        == [
            [
                ("NOPARTYIDS[0].PARTYID", "x"),
                ("NOPARTYIDS[0]", "garbage"),
                ("SIDE", "1"),
            ]
        ]
    )


def test_staged_wire_conversion_drops_message_markers_before_fix_rules(
    registry: FixRegistry,
) -> None:
    lines = [
        "8=FIX.4.2|SIDE=1|55=TTF|10=000|",
        "8=FIX.4.2|#54=2|55=IBM|10=000|",
    ]
    raw = _raw_batch(*(Message(message=line) for line in lines))
    codec = FixCodec(registry=registry)
    _, expected_columns = codec.into_fixmsg_columns(
        codec.into_pairs(pyarrow.array(lines), "FIX"), "4.2"
    )

    parsed = FixMsg.from_message_arrow_batch(raw, codec)

    assert [entry["key"] for entry in raw.column("kwargs")[0].as_py()] == [
        "8",
        "SIDE",
        "55",
        "10",
    ]
    assert [entry["key"] for entry in raw.column("kwargs")[1].as_py()] == [
        "8",
        "54",
        "55",
        "10",
    ]
    assert expected_columns["Side"].to_pylist() == [None, None]
    assert parsed.column("Side").to_pylist() == [None, "2"]
    assert parsed.column("Symbol").to_pylist() == expected_columns["Symbol"].to_pylist()
    assert parsed.column("kwargs").to_pylist() == [[], []]


def test_an_extra_column_cannot_shadow_a_fix_only_field(registry: FixRegistry) -> None:
    raw = _raw_batch(Message(message="plain text")).append_column(
        "OrigClOrdID", pyarrow.array(["caller-value"])
    )

    with pytest.raises(ValueError, match="collide.*OrigClOrdID"):
        FixMsg.from_message_arrow_batch(raw, FixCodec(registry=registry))


def test_fixmsg_conversion_keeps_the_empty_contract(
    registry: FixRegistry,
) -> None:
    parsed = FixMsg.from_message_arrow_batch(
        _raw_batch(),
        FixCodec(registry=registry),
    )

    assert parsed.num_rows == 0
    assert parsed.schema == FixMsg.into_field().into_arrow_schema()


# -- the message layer, flattened ---------------------------------------------


def test_the_flat_layer_is_the_session_layer_and_what_a_trading_log_is_made_of() -> None:
    """Derived from `rekep.fix.columns` and pinned, so a tag dropped from either
    tuple cannot quietly shrink every check that walks it. One tag, one column,
    both ways: a repeat would silently overwrite whatever it landed on."""
    assert len(SESSION) == EXPECTED_SESSION_COLUMNS
    assert len(COMMON) == EXPECTED_COMMON_COLUMNS
    assert len(FLAT) == len(COLUMNS) == EXPECTED_FLAT_COLUMNS
    assert len(set(FLAT_COLUMNS)) == EXPECTED_FLAT_COLUMNS
    assert len(FixMsg.into_field().fields) == EXPECTED_LOG_COLUMNS


def test_fix_names_do_not_alias_the_generic_event_envelope() -> None:
    assert {tag: COLUMNS[tag] for tag in RAW_TAGS} == RAW_TAGS
    assert set(RAW_TAGS.values()).isdisjoint(Event.into_field().names)
    assert {"code", *RAW_TAGS.values()} <= set(FixMsg.into_field().names)
    assert FixMsg.into_field().field("MsgSeqNum").fix["tag"] == "34"


def test_every_promoted_name_is_the_registrys_exact_spelling() -> None:
    names = [field.name for field in DECLARATIONS.values()]
    assert len(names) == 92
    assert all(field.name == field.fix["name"] for field in DECLARATIONS.values())
    assert {tag: COLUMNS[tag] for tag in (6, 35, 41, 461)} == {
        6: "AvgPx",
        35: "MsgType",
        41: "OrigClOrdID",
        461: "CFICode",
    }
    assert {tag: DECLARATIONS[tag].name for tag in (453, 802)} == {
        453: "NoPartyIDs",
        802: "NoPartySubIDs",
    }
    assert Party.into_field().names == ["PartyID", "PartyIDSource", "PartyRole", "buffer"]


def test_no_other_lifted_column_lands_on_one_the_line_already_had() -> None:
    """Raw protocol fields and the generic envelope have separate names."""
    assert set(FLAT_COLUMNS) & set(ENVELOPE + LINE + MESSAGE) == {
        "MsgType",
        "MsgSeqNum",
    }


def test_every_flat_column_is_the_type_the_dictionary_gives_its_tag(
    registry: FixRegistry,
) -> None:
    """The one check that keeps the names (`rekep.fix.columns`) and the types
    (here) from drifting apart. A column stands for a tag, and what a tag holds
    is the dictionary's to say -- not this package's, and not a reading of the
    fixture that happens to parse."""
    for tag, column in FLAT:
        if tag in STAMPS:
            continue
        assert FixMsg.into_field().field(column).arrow_type == registry.field(tag).arrow_type, (
            column
        )


def test_a_lifted_stamp_is_a_microsecond_utc_timestamp(
    registry: FixRegistry,
) -> None:
    """Promoted FIX clocks use Iceberg's width and their documented UTC zone."""
    dictated = {
        tag for tag, _ in FLAT if pyarrow.types.is_timestamp(registry.field(tag).arrow_type)
    }
    assert dictated == set(STAMPS)
    for tag in STAMPS:
        assert FixMsg.into_field().field(COLUMNS[tag]).arrow_type == pyarrow.timestamp(
            "us", tz="UTC"
        ), tag


def test_timestamp_projection_is_naive_until_the_fix_documentation_says_utc() -> None:
    local = Field(
        name="LocalStamp",
        arrow_type=pyarrow.timestamp("ns"),
        metadata={"fix:type": "Time"},
    )
    utc = Field(
        name="UtcStamp",
        arrow_type=pyarrow.timestamp("ns"),
        metadata={"fix:type": "UTCTimestamp"},
    )
    assert _physical_type(local) == pyarrow.timestamp("us")
    assert _physical_type(utc) == pyarrow.timestamp("us", tz="UTC")


def test_every_flat_column_admits_absence() -> None:
    """Whether a FIX field is required belongs to the message that carries it."""
    blank = FixMsg()
    for column in FLAT_COLUMNS:
        assert FixMsg.into_field().field(column).nullable, column
        assert getattr(blank, column) is None, column


def test_every_flat_column_keeps_the_registry_name_metadata_and_description(
    registry: FixRegistry,
) -> None:
    for tag, column in FLAT:
        expected = registry.scalar(tag)
        actual = FixMsg.into_field().field(column)
        assert actual.fix["name"] == expected.name, column
        assert actual.metadata == expected.metadata, column
        assert actual.description == expected.description, column


def test_rendered_isincode_keeps_its_source_identity() -> None:
    field = FixMsg.into_field().field("ISINCODE")
    assert field.arrow_type == pyarrow.string()
    assert field.fix == {"name": "ISINCODE", "type": "String"}
