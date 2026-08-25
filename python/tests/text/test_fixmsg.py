"""`FixMsg`'s own contract; the parser that fills it is tested beside it."""

import datetime
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, FixCodec, FixMsg, Message
from rekep.fix import NO_PROTOCOL, FixRegistry, Party
from rekep.fix.columns import COLUMNS, COMMON, DECLARATIONS, FLAT, SESSION, STAMPS, _physical_type
from rekep.market import MIC, Event, EventType
from rekep.market.event import HOUR, SECOND

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
LINE = ["source_url", "source_rownum", "thread_name", "plugin_code", "message"]
MESSAGE = [
    "protocol_code",
    "unix_source",
    "protocol_version",
    "protocol_version_source",
    "MsgSeqNum",
    "kwargs",
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
    assert FixMsg.into_field_metadata() == {"version": "3"}
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
        if name == "message":
            assert field.field(name).nullable, "a market row leaves the raw line null"
            continue
        assert not field.field(name).nullable, name


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


def test_a_stored_field_always_says_what_it_is() -> None:
    """`tag` and `key` are how a consumer addresses a field, so neither is null:
    a field the dictionary did not resolve is `tag` `0` and not a missing tag."""
    member = FixMsg.into_field().field("kwargs")
    assert pyarrow.types.is_list(member.arrow_type)
    assert member.item.nullable is False
    assert member.item.field("tag").nullable is False
    assert member.item.field("key").nullable is False
    for name in ("value", "namespace", "comp"):
        assert member.item.field(name).nullable is True, name
        assert member.item.field(name).arrow_type == pyarrow.string(), name


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

    assert reader.message.pairs == [
        ("35", "D"),
        ("55", "A"),
        ("55", "B"),
        ("55", "C"),
        ("VenueOwnThing", "x"),
        ("VenueOwnThing", "y"),
        ("VenueOwnThing", "z"),
        ("TECH.CLIENTID", "ACCT-TEST-01"),
        ("448", "PARTY-TEST-A"),
    ], "a resolved field is addressed by its tag and an unresolved one by its spelling"


def test_malformed_stored_field_entries_are_not_silently_dropped() -> None:
    with pytest.raises(KeyError, match="key"):
        FixMsg(kwargs=[{"value": "x"}]).into_fix_events()
    with pytest.raises(ValueError, match="not enough values"):
        FixMsg(kwargs=[["OnlyKey"]]).into_fix_events()


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
        assert metadata["unit"] == "nanosecond", name
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
            source_url="capture.log",
            source_rownum=1,
            plugin_code="fix",
            message=(
                "8=FIX.4.4|35=D|34=7|41=ROOT|55=IBM|461=EXXXXX|6=12.5|"
                "453=1|448=BUYSIDE|447=D|452=1|10=000|"
            ),
        ),
        Message(
            source_url="capture.log",
            source_rownum=2,
            plugin_code="ULBridge",
            message=(
                "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=8|#ORIGCLORDID=OLD|#ISINCODE=XX0000084733|"
            ),
        ),
        Message(
            source_url="capture.log",
            source_rownum=3,
            plugin_code="misc",
            message="plain text",
        ),
    )

    assert raw.schema.names == Message.into_field().names
    assert not {"protocol_code", "MsgType", "OrigClOrdID"} & set(raw.schema.names)

    parsed = FixMsg.from_message_arrow_batch(
        raw,
        FixCodec(registry=registry),
        FixMsg.into_message_rules(),
    )

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
    assert parsed.column("codes").to_pylist()[0] == [
        ("orig_cl_ord_id", "ROOT"),
        ("symbol", "IBM"),
    ]


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

    parsed = FixMsg.from_message_arrow_batch(
        raw,
        FixCodec(registry=registry),
        FixMsg.into_message_rules(),
    )

    assert parsed.schema.names == [*FixMsg.into_field().names, "capture_id"]
    assert parsed.schema.field("capture_id") == static
    assert parsed.column("capture_id").to_pylist() == ["day-1"]


def test_a_static_column_cannot_shadow_a_fix_field(registry: FixRegistry) -> None:
    raw = _raw_batch(Message(message="plain text")).append_column(
        "MsgType", pyarrow.array(["caller-value"])
    )

    with pytest.raises(ValueError, match="collide.*MsgType"):
        FixMsg.from_message_arrow_batch(
            raw, FixCodec(registry=registry), FixMsg.into_message_rules()
        )


def test_fixmsg_conversion_keeps_the_empty_contract(
    registry: FixRegistry,
) -> None:
    parsed = FixMsg.from_message_arrow_batch(
        _raw_batch(),
        FixCodec(registry=registry),
        FixMsg.into_message_rules(),
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
    assert set(FLAT_COLUMNS) & set(ENVELOPE + LINE + MESSAGE) == {"MsgSeqNum"}


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
