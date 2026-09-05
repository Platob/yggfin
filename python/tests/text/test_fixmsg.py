"""`FixMsg`'s own contract; the parser that fills it is tested beside it."""

import datetime
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, FixCodec, FixMsg, Message, txhash
from rekep.enums import Direction, Plugin, Protocol, SecurityIDSource
from rekep.fields import column_name
from rekep.fix import ENTRIES, FixRegistry, Party
from rekep.fix.columns import (
    _COLUMN_METADATA,
    COLUMNS,
    COMMON,
    DECLARATIONS,
    FLAT,
    SESSION,
    STAMPS,
    column_metadata,
    physical_type,
)
from rekep.fix.components import SecurityAltID, SideTrdRegTimestamp
from rekep.fix.fields import fix_field, unix_of
from rekep.fix.rules import Rules
from rekep.market import (
    HASH,
    MIC,
    AssetKind,
    BookIterator,
    Currency,
    Event,
    EventType,
    Instrument,
    InstUpdate,
    OptionKind,
    Side,
    hash_of,
)
from rekep.market.event import HOUR, SECOND
from rekep.market.fix import FixEvents
from rekep.text import Entry
from rekep.text.fixmsg import _ERROR_LENGTH, _UNDIGESTED, _digest_text, _merge_error_columns

#: The dictionary this repository publishes, beside `python/`, read offline:
#: a contract that only holds while the site answers is not a contract.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: The envelope every event carries, then log-only source ordering and content.
ENVELOPE = [
    "unix",
    "unixpartition",
    "eventtype",
    "plugin",
    "creaunix",
    "recunix",
    "expunix",
    "snapunix",
    "hash",
    "vhash",
    "xhash",
    "linkhashes",
    "version",
    "state",
    "code",
    "altids",
    "prevunix",
    "prevhash",
    "parenthash",
    "lastmkt",
    "reason",
]
SOURCE: list[str] = []


def _protocols(batch: pyarrow.RecordBatch | pyarrow.Table) -> list[str]:
    """The `protocol` column spelled out, registered names included."""
    return [Protocol.from_int(code).code for code in batch.column("protocol").to_pylist()]


def _instrument_column(
    batch: pyarrow.RecordBatch | pyarrow.Table, name: str
) -> pyarrow.ChunkedArray | pyarrow.Array:
    """One member of the nested instrument component."""
    return pyarrow.compute.struct_field(batch.column("instrument"), name)


def _message_at(body: str | bytes, recunix: int, **raw) -> Message:
    """One raw row whose captured timestamp stores the requested nanosecond."""
    seconds, nanos = divmod(recunix, SECOND)
    stamp = datetime.datetime.fromtimestamp(seconds, datetime.UTC)
    return Message(
        body=body,
        timestamp=f"{stamp:%Y%m%d-%H:%M:%S}.{nanos:09d}",
        **raw,
    )


def _instruments(*messages: FixMsg) -> list[Instrument]:
    """Components carried by parsed messages through the class-owned API."""
    return [update.instrument for update in InstUpdate.from_fixmsgs(messages)]


LINE = [
    "sourceurl",
    "sourcerownum",
    "threadname",
    "level",
    "protocol",
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
    "entries",
    "direction",
]
MESSAGE = [
    "unixsource",
    "unmap",
    "parties",
    "trdregtimestamps",
    "sidetrdregts",
    "parentclordid",
    "parentorderid",
]
#: Structured components declared after every flat column: Iceberg collects
#: bounds for leaf columns in declaration order, and this contract crosses
#: that cutoff -- a list declared earlier would push flat columns past it.
TRAILING_COMPONENTS = [
    "securityaltid",
    "instrument",
    "omsorders",
]

#: Raw FIX names stay distinct from the protocol-neutral event envelope.
RAW_TAGS = {55: "symbol", 34: "msgseqnum"}

#: The standard header parsed into FixMsg columns, and the tag each is lifted
#: by. Only the discriminator answers to a
#: rendered name as well: a bridge's own `#BEGINSTRING=` spelling stays an
#: argument, because which name a feed writes is data. Spelled out rather than
#: imported from the parser internals, so a field quietly leaving
#: that tuple cannot move both sides of an assertion together.
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

#: Session fields whose parsed columns use a non-text storage type.
RETYPED_HEADER = {
    "bodylength": pyarrow.int64(),
    "msgseqnum": pyarrow.int64(),
    "lastmsgseqnumprocessed": pyarrow.int64(),
    "possdupflag": pyarrow.bool_(),
    "possresend": pyarrow.bool_(),
    "sendingtime": pyarrow.timestamp("us", tz="UTC"),
    "origsendingtime": pyarrow.timestamp("us", tz="UTC"),
    "onbehalfofsendingtime": pyarrow.timestamp("us", tz="UTC"),
    "applextid": pyarrow.int32(),
    "securedatalen": pyarrow.int64(),
    "securedata": pyarrow.binary(),
    "signaturelength": pyarrow.int64(),
    "signature": pyarrow.binary(),
}

#: The flattened message layer, derived from the module that names it and
#: pinned below -- so a column renamed in one file and not in the other fails
#: here, rather than moving both sides of every comparison together.
FLAT_COLUMNS = [column for _, column in FLAT]
_FIX_ADDED_COLUMNS = [
    column for column in FLAT_COLUMNS if column not in set(ENVELOPE + SOURCE + LINE + MESSAGE)
]
_INSTRUMENT_COLUMNS = {
    "symbol",
    "securityid",
    "securityidsource",
    "securitytype",
    "cficode",
    "securityexchange",
    "currency",
    "maturitydate",
    "maturitymonthyear",
    "strikeprice",
    "putorcall",
    "contractmultiplier",
    "minpriceincrement",
    "roundlot",
    "securitydesc",
}
ADDED_COLUMNS = [column for column in _FIX_ADDED_COLUMNS if column not in _INSTRUMENT_COLUMNS]
# Price provenance has no wire tag, so it sits beside the slots it qualifies.
ADDED_COLUMNS.insert(ADDED_COLUMNS.index("offerpx") + 1, "priceinferred")
EXPECTED_SESSION_COLUMNS = 33
EXPECTED_COMMON_COLUMNS = 49
EXPECTED_FLAT_COLUMNS = 100
EXPECTED_LOG_COLUMNS = 124


@pytest.fixture(scope="module")
def registry() -> FixRegistry:
    """The published dictionary. Offline, because this must not test the site."""
    return FixRegistry(cache_dir=DATA)


@pytest.fixture(scope="module")
def codec(registry: FixRegistry) -> FixCodec:
    """One codec for the file: it snapshots the dictionary and is never mutated.

    Module-scoped for the reason the registry above is, and for a bigger one:
    the first lift of a version plans every column that version can carry, and
    that plan memoizes on the codec instance rather than on the dictionary.
    """
    return FixCodec(registry=registry)


def test_a_log_line_is_an_event() -> None:
    """Which is what lets a parsed log be read beside the orders it describes."""
    assert issubclass(FixMsg, Event)
    assert (
        FixMsg.into_field().into_arrow_schema().names
        == ENVELOPE + SOURCE + LINE + MESSAGE + ADDED_COLUMNS + ["error"] + TRAILING_COMPONENTS
    )


def test_a_logs_cached_contract_metadata_is_immutable() -> None:
    assert FixMsg.into_field_metadata() == {"version": "1"}
    with pytest.raises(TypeError):
        FixMsg.into_field_metadata()["version"] = "9"


def test_the_envelope_is_the_same_one_every_other_event_carries() -> None:
    """Position included: a reader of the envelope must not need to know the shape."""
    assert Event.into_field().names == ENVELOPE


def test_every_column_a_line_adds_is_required_except_parsed_payload_fields() -> None:
    """The parsed shape consumes the body and retains nullable parsed fields."""
    field = FixMsg.into_field()
    assert "body" not in field.names
    for name in (
        "sourceurl",
        "sourcerownum",
        "threadname",
        "direction",
        "protocol",
        "unixsource",
    ):
        assert not field.field(name).nullable, name
    for name in (
        "level",
        "msgseqnum",
        "entries",
        "unmap",
        "parties",
        "trdregtimestamps",
        "sidetrdregts",
        "parentclordid",
        "parentorderid",
    ):
        assert field.field(name).nullable, name


def test_raw_messages_have_only_physical_text_record_fields() -> None:
    raw = Message.into_field()
    parsed = FixMsg.into_field()
    assert raw.names == [
        "sourceurl",
        "sourcerownum",
        "timestamp",
        "threadname",
        "plugin",
        "level",
        "body",
    ]
    assert raw.primary_keys() == ["sourceurl", "sourcerownum"]
    assert raw.field("body").dtype == pyarrow.binary()
    assert not set(raw.names) & {"entries", "protocol", "msgtype", "eventtype", "recunix"}
    for name, tag in LIFTED_HEADER.items():
        assert parsed.field(name).fix["tag"] == tag, name
        assert parsed.field(name).dtype == RETYPED_HEADER.get(name, pyarrow.string()), name
    assert parsed.field("CheckSum").fix["tag"] == "10"


def test_a_line_always_says_which_protocol_it_carries() -> None:
    """`OTHER` is an answer and not a missing one -- it is most of a capture --
    so the column is NOT NULL and the fall-through is what a line starts as."""
    member = FixMsg.into_field().field("protocol")
    assert not member.nullable and member.dtype == pyarrow.binary(16)
    assert member.metadata["enum:name"] == "Protocol"
    assert member.metadata["enum:encoding"] == "ascii-big-endian"
    assert member.metadata["enum:byte_width"] == "16"
    assert member.metadata["enum:pattern"] == "[A-Z0-9._-]{1,16}"
    assert FixMsg().protocol is Protocol.OTHER


def test_a_parsed_row_takes_its_packed_codes_off_whatever_spelled_them() -> None:
    """Parsed enum columns normalize either stored codes or readable names."""
    row = FixMsg(unix=1, hash=1, xhash=1, protocol="fix", direction="sent")

    assert row.protocol is Protocol.FIX
    assert row.direction is Direction.SENT
    assert FixMsg(protocol=int(Protocol.UL)).protocol is Protocol.UL
    # The open code retains a venue spelling within its 16-byte persisted bound.
    assert FixMsg(protocol="VENUEBRIDGE").protocol.code == "VENUEBRIDGE"
    assert FixMsg(protocol="VENUEBRIDGE-OVERWIDE").protocol is Protocol.UNKNOWN
    built = FixMsg.into_arrow_array([row])
    assert built.field("direction").to_pylist() == [int(Direction.SENT)]
    assert built.field("protocol").to_pylist() == [Protocol.FIX.into_stored()]


def test_a_line_carrying_no_message_has_no_pairs_at_all() -> None:
    """Null is not an empty list: a bridge that sent an empty payload and a stack
    trace that never was a message have to stay tellable apart."""
    assert FixMsg.into_field().field("entries").nullable
    assert FixMsg().entries is None
    assert FixMsg.into_field().field("unmap").nullable
    assert FixMsg().unmap is None


def test_an_explicit_empty_parsed_argument_list_stays_empty() -> None:
    parsed = FixMsg(entries=[])

    assert parsed.entries == []


def test_a_stored_field_always_says_what_it_is() -> None:
    """`tag` and `key` remain non-null even when the registry knows no identity."""
    entries = FixMsg.into_field().field("entries")
    unmap = FixMsg.into_field().field("unmap")
    assert entries.dtype == unmap.dtype == ENTRIES
    assert pyarrow.types.is_list(entries.dtype)
    assert "fix:display" not in entries.item.metadata
    assert "fix:display" not in unmap.item.metadata
    assert entries.item.nullable is False
    assert entries.item.field("tag").nullable is False
    assert entries.item.field("key").nullable is False
    assert entries.item.field("value").nullable is False
    assert entries.item.field("comp").nullable is True
    assert entries.item.field("comp").dtype == pyarrow.string()
    assert all(isinstance(entry, Entry) for entry in FixMsg(entries=[(55, "IBM")]).entries or ())


def test_stored_fields_keep_repeats_across_python_and_arrow_entry_shapes() -> None:
    reader = FixMsg(
        msgtype="D",
        entries=[
            (55, "A"),
            [55, "B"],
            {"tag": 55, "key": "Symbol", "value": "C"},
            ("VenueOwnThing", "x"),
            ["VenueOwnThing", "y"],
            {"tag": 0, "key": "VenueOwnThing", "value": "z"},
            {"tag": 0, "key": "TECH.CLIENTID", "value": "ACCT-TEST-01"},
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


def test_named_text_version_evidence_keeps_the_bridge_protocol() -> None:
    row = FixMsg.from_text("BEGINSTRING=FIX.4.4|MSGTYPE=D|CLORDID=C1")

    assert row.protocol.code == "UL4.4"
    assert len(list(row.into_market_events())) == 1


def test_a_parsed_row_is_the_raw_row_transcribed() -> None:
    """The raw row's provenance and capture clock carry over whole."""
    line = "8=FIX.4.4|35=D|11=C1|54=1|10=000"
    staged = _message_at(line, 9, sourceurl="s3://x/y.log", sourcerownum=4)
    row = FixMsg.from_message(staged)

    assert (row.sourceurl, row.sourcerownum, row.recunix) == ("s3://x/y.log", 4, 9)
    assert row.eventtype == FixRegistry.from_builtin().msg_type_event_types()["D"]
    assert row.protocol.code == "FIX4.4"
    assert row.resolved_version() == "4.4"
    assert FixMsg.from_(staged) == row, "the generic builder reaches the same seam"


def test_scalar_and_arrow_identification_share_the_registry_projection() -> None:
    line = "8=FIX.4.4|35=D|11=C1|54=1|10=000"
    raw = _message_at(
        line,
        1_700_000_000_000_000_000,
        sourceurl="capture.log",
        sourcerownum=1,
    )
    scalar = FixMsg.from_message(raw)
    arrow = FixMsg.from_message_batch([raw])

    assert scalar.code == arrow.column("code")[0].as_py() == "C1"
    assert scalar.altids == dict(arrow.column("altids")[0].as_py())
    assert scalar.altids["clordid"] == scalar.altids["code"] == "C1"
    assert scalar.vhash == arrow.column("vhash")[0].as_py()
    assert scalar.into_row()["xhash"] == arrow.column("xhash")[0].as_py()
    assert scalar.xhash == Event.xhash_of(scalar.code)
    assert scalar.into_row()["hash"] == arrow.column("hash")[0].as_py()


def test_a_precomputed_named_row_still_fills_its_lifecycle_identity() -> None:
    clock = 1_700_000_000_123_456_789
    vhash = hash_of("parsed value")
    event_hash = txhash.couple128(clock // 1_000, vhash)
    row = FixMsg(unix=clock, creaunix=clock - 10_000, code="C1", hash=event_hash, vhash=vhash)

    row.identify()

    assert (row.hash, row.vhash) == (event_hash, vhash)
    assert row.xhash == Event.xhash_of("C1")
    assert row.altids == {"code": "C1"}


@pytest.mark.parametrize(
    "line",
    (
        (
            "8=FIX.4.4|35=D|11=C1|55=AAPL|60=20260821-10:00:00|"
            "42=20260821-09:59:57|122=20260821-09:59:58|"
            "52=20260821-09:59:59|10=000|"
        ),
        (
            "BEGINSTRING=FIX.4.4|MSGTYPE=D|CLORDID=C1|SYMBOL=AAPL|"
            "TRANSACTTIME=20260821-10:00:00|ORIGTIME=20260821-09:59:57|"
            "ORIGSENDINGTIME=20260821-09:59:58|SENDINGTIME=20260821-09:59:59|"
        ),
    ),
    ids=("fix", "ul"),
)
def test_standard_fix_clocks_fill_distinct_generic_times(line: str, codec: FixCodec) -> None:
    """Descriptions decide the mapping: transaction, origination, then capture."""
    recorded = unix_of("20260821-10:00:02")
    assert recorded is not None
    source = _raw_batch(_message_at(line, recorded))

    row = FixMsg.from_message_batch(source, codec).to_pylist()[0]
    assert row["unix"] == unix_of("20260821-10:00:00")
    assert row["unixsource"] == "TransactTime"
    assert row["creaunix"] == unix_of("20260821-09:59:57")
    assert row["recunix"] == recorded
    assert row["origtime"] == datetime.datetime(2026, 8, 21, 9, 59, 57, tzinfo=datetime.UTC)


@pytest.mark.parametrize(
    ("fields", "recorded", "expected"),
    (
        (
            "52=20260821-00:00:00|272=20260821|273=10:30:00|",
            "20260822-01:00:00",
            "20260821-10:30:00",
        ),
        (
            "52=20260821-00:00:00|273=10:30:00|",
            "20260822-01:00:00",
            "20260821-10:30:00",
        ),
        (
            "273=10:30:00|",
            "20260822-01:00:00",
            "20260822-10:30:00",
        ),
        (
            "52=19691231-12:00:00|273=23:30:00|",
            "19700102-01:00:00",
            "19691231-23:30:00",
        ),
        (
            "52=19691231-12:00:00|273=19691230-23:30:00|",
            "19700102-01:00:00",
            "19691231-23:30:00",
        ),
    ),
    ids=(
        "date-and-time",
        "sending-day",
        "recording-day",
        "pre-epoch-day",
        "pre-epoch-clock-spelling",
    ),
)
def test_residual_mdentry_clocks_match_scalar_resolution(
    fields: str, recorded: str, expected: str
) -> None:
    """A time-only market stamp uses the nearest known day, never 1970 by casting."""
    line = f"8=FIX.4.4|35=D|11=C1|55=AAPL|54=1|{fields}10=000|"
    recunix = unix_of(recorded)
    assert recunix is not None
    parsed = FixMsg.from_message_batch([_message_at(line, recunix)])
    row = parsed.to_pylist()[0]
    scalar = FixEvents.from_text(line, recunix=recunix, fix_version="4.4").transacted

    assert (row["unix"], row["unixsource"]) == (unix_of(expected), "MDEntry")
    assert (scalar.unix, scalar.source) == (row["unix"], row["unixsource"])
    assert [entry["tag"] for entry in row["entries"]] == (
        [272, 273] if "272=" in fields else [273]
    ), "clock projection must not consume the residual facts"


def test_count_free_indexed_mdentries_do_not_set_the_enclosing_message_clock() -> None:
    """A component path is group evidence even where a bridge omits tag 268."""
    line = (
        "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=X|#SYMBOL=AAPL|"
        "#SENDINGTIME=20260821-10:30:00.250|"
        "#NOMDENTRIES[0].MDUPDATEACTION=0|#NOMDENTRIES[0].MDENTRYTYPE=0|"
        "#NOMDENTRIES[0].MDENTRYPX=100|#NOMDENTRIES[0].MDENTRYSIZE=5|"
        "#NOMDENTRIES[0].MDENTRYDATE=20260821|"
        "#NOMDENTRIES[0].MDENTRYTIME=10:29:59.100|"
        "#NOMDENTRIES[1].MDUPDATEACTION=0|#NOMDENTRIES[1].MDENTRYTYPE=1|"
        "#NOMDENTRIES[1].MDENTRYPX=100.5|#NOMDENTRIES[1].MDENTRYSIZE=7|"
        "#NOMDENTRIES[1].MDENTRYDATE=20260821|"
        "#NOMDENTRIES[1].MDENTRYTIME=10:29:59.200"
    )

    parsed = FixMsg.from_message_batch([Message(body=line)])
    row = parsed.to_pylist()[0]
    events = list(FixMsg.from_dict(row).into_market_events())

    assert (row["unix"], row["unixsource"]) == (
        unix_of("20260821-10:30:00.250"),
        "SendingTime",
    )
    assert {entry["comp"] for entry in row["entries"] if entry["comp"] is not None} == {
        "NOMDENTRIES[0]",
        "NOMDENTRIES[1]",
    }
    assert [event.unix for event in events] == [
        unix_of("20260821-10:29:59.100"),
        unix_of("20260821-10:29:59.200"),
    ]


def test_scalar_from_message_resolves_capture_and_fix_clocks_like_arrow(
    codec: FixCodec,
) -> None:
    """The raw timestamp is capture time; FIX owns the event clocks."""
    line = "8=FIX.4.4|35=D|11=C1|55=AAPL|54=1|52=20260821-10:30:00|10=000|"
    raw = _message_at(line, 997)
    fresh = FixMsg.from_message(raw, registry=codec.registry)
    parsed = FixMsg.from_message_batch([raw], codec)
    row = parsed.to_pylist()[0]

    scalar_event = next(fresh.into_market_events())
    arrow_event = next(FixMsg.from_dict(row).into_market_events())
    expected = unix_of("20260821-10:30:00")

    assert (scalar_event.unix, scalar_event.creaunix, scalar_event.recunix) == (
        expected,
        expected,
        997,
    )
    assert (arrow_event.unix, arrow_event.creaunix, arrow_event.recunix) == (
        scalar_event.unix,
        scalar_event.creaunix,
        scalar_event.recunix,
    )
    identified = FixMsg.from_message(raw, registry=codec.registry).identify()
    assert (identified.unix, identified.creaunix, identified.recunix) == (
        row["unix"],
        row["creaunix"],
        row["recunix"],
    )


def test_creation_uses_only_origination_and_transmission_evidence(codec: FixCodec) -> None:
    """A transaction time alone does not claim when its lifecycle was created."""
    prefix = "8=FIX.4.4|35=D|11=C1|55=AAPL|60=20260821-10:00:00|"
    lines = (
        prefix + "42=20260821-09:59:57|122=20260821-09:59:58|52=20260821-09:59:59|10=000|",
        prefix + "122=20260821-09:59:58|370=20260821-09:59:58.5|52=20260821-09:59:59|10=000|",
        prefix + "370=20260821-09:59:58.5|52=20260821-09:59:59|10=000|",
        prefix + "52=20260821-09:59:59|10=000|",
        prefix + "10=000|",
    )

    parsed = FixMsg.from_message_batch(_raw_batch(*(Message(body=line) for line in lines)), codec)

    assert parsed.column("creaunix").to_pylist() == [
        unix_of("20260821-09:59:57"),
        unix_of("20260821-09:59:58"),
        unix_of("20260821-09:59:58.5"),
        unix_of("20260821-09:59:59"),
        0,
    ]


def test_stated_clocks_override_inference_but_not_the_local_recording_clock(
    codec: FixCodec,
) -> None:
    line = (
        "BEGINSTRING=FIX.4.4|MSGTYPE=D|CLORDID=C1|SYMBOL=AAPL|"
        "UNIX=100|CREAUNIX=90|RECUNIX=110|"
        "TRANSACTTIME=20260821-10:00:00|SENDINGTIME=20260821-09:59:59|"
    )
    parsed = FixMsg.from_message_batch(
        _raw_batch(Message(body=line), _message_at(line, 120)), codec
    )

    assert parsed.column("unix").to_pylist() == [100, 100]
    assert parsed.column("unixsource").to_pylist() == ["Unix", "Unix"]
    assert parsed.column("creaunix").to_pylist() == [90, 90]
    assert parsed.column("recunix").to_pylist() == [110, 120]
    assert parsed.column("unmap").to_pylist() == [None, None]

    scalar = FixMsg.from_text(line, registry=codec.registry).identify()
    assert (scalar.unix, scalar.creaunix, scalar.recunix, scalar.unixsource) == (
        100,
        90,
        110,
        "Unix",
    )


def test_the_digest_excludes_clocks_identities_and_recorder_provenance() -> None:
    """Stated by exclusion, so a column added to the shape is in the digest the
    day it lands rather than the day someone remembers to name it."""
    named = set(FixMsg.into_digest_columns())

    assert named == set(FixMsg.into_field().names) - _UNDIGESTED
    assert {"clordid", "price", "side", "orderqty", "parties", "instrument"} <= named, (
        "a lifted field is content"
    )
    assert not named & {
        "unix",
        "recunix",
        "hash",
        "vhash",
        "xhash",
        "body",
        "error",
        "plugin",
    }


@pytest.mark.parametrize(
    "column",
    [
        pyarrow.nulls(3, pyarrow.string()),
        pyarrow.nulls(3, pyarrow.int64()),
        pyarrow.nulls(3, pyarrow.list_(pyarrow.string())),
        pyarrow.nulls(3, pyarrow.map_(pyarrow.string(), pyarrow.string())),
    ],
    ids=("string", "integer", "list", "map"),
)
def test_all_null_digest_columns_broadcast_their_empty_text(column: pyarrow.Array) -> None:
    assert _digest_text(column, len(column)).equals(pyarrow.repeat("", len(column)))
    assert len(_digest_text(column.slice(0, 0), 0)) == 0


def test_an_all_null_digest_struct_keeps_its_member_boundaries() -> None:
    column = pyarrow.nulls(
        2, pyarrow.struct([("name", pyarrow.string()), ("value", pyarrow.int64())])
    )
    assert _digest_text(column, len(column)).to_pylist() == ["\x1d", "\x1d"]


def test_the_recorder_plugin_survives_without_changing_content_identity(
    codec: FixCodec,
) -> None:
    line = "8=FIX.4.4|35=D|11=ORD-1|55=IBM|54=1|38=1|44=10|10=000|"
    parsed = FixMsg.from_message_batch(
        _raw_batch(Message(body=line, plugin="one"), Message(body=line, plugin="two")),
        codec,
    )

    assert parsed.column("plugin").to_pylist() == [
        Plugin.from_str("one").into_stored(),
        Plugin.from_str("two").into_stored(),
    ]
    assert parsed.column("vhash")[0].as_py() == parsed.column("vhash")[1].as_py()


def test_two_orders_differing_only_in_lifted_fields_are_two_rows(codec: FixCodec) -> None:
    """The registry projection promotes every parsed field *out* of `entries`,
    so a digest that named a handful of columns met an empty list and gave two
    unrelated orders one `hash` -- which is half the primary key."""
    one = "8=FIX.4.4|9=1|35=D|34=1|49=A|56=B|11=ORD-1|55=TTF|54=1|38=100|44=41.25|10=000|"
    other = one.replace("11=ORD-1", "11=ORD-2").replace("55=TTF", "55=IBM")

    batch = FixMsg.from_message_batch(_raw_batch(Message(body=one), Message(body=other)), codec)

    assert batch.column("entries").to_pylist() == [[], []], "nothing was left to hash there"
    assert batch.column("vhash")[0].as_py() != batch.column("vhash")[1].as_py()
    assert batch.column("hash")[0].as_py() != batch.column("hash")[1].as_py()


def test_a_reformatted_message_keeps_its_digest(codec: FixCodec) -> None:
    """The other half of taking a digest over parsed values: the separator
    moved and the row did not, so the identity must not move either."""
    piped = "8=FIX.4.4|9=1|35=D|34=1|49=A|56=B|11=ORD-1|55=TTF|54=1|38=100|44=41.25|10=000|"
    soh = piped.replace("|", "\x01").rstrip("\x01")

    batch = FixMsg.from_message_batch(_raw_batch(Message(body=piped), Message(body=soh)), codec)

    assert batch.column("vhash")[0].as_py() == batch.column("vhash")[1].as_py()


def test_fixmsg_value_hash_excludes_the_event_clock() -> None:
    line = "8=FIX.4.4|35=D|11=C1|54=1|10=000"
    parsed = FixMsg.from_message_batch(
        [
            _message_at(line, 1_000),
            _message_at(line, 2_000),
        ]
    )

    assert parsed.column("vhash")[0].as_py() == parsed.column("vhash")[1].as_py()
    assert parsed.column("hash")[0].as_py() != parsed.column("hash")[1].as_py()


def test_message_batches_transcribe_from_rows_and_arrow_alike() -> None:
    """`from_message_batch` is one boundary: scalar rows and a raw RecordBatch
    land as the same parsed batch, under the packaged default codec."""
    rows = [
        _message_at("8=FIX.4.4\x0135=D\x0111=C1\x0154=1\x0138=5\x0110=000\x01", 1),
        _message_at("plain prose", 2),
    ]
    from_rows = FixMsg.from_message_batch(rows)
    raw = Message.into_arrow_batch(rows)

    assert from_rows.equals(FixMsg.from_message_batch(raw))
    assert from_rows.equals(FixMsg.from_message_batch(rows, FixRegistry.from_builtin())), (
        "a registry is all the conversion needs; the codec derives from it"
    )
    assert from_rows.column("clordid").to_pylist() == ["C1", None]
    assert _protocols(from_rows) == ["FIX4.4", "OTHER"]

    empty = FixMsg.from_message_batch([])
    assert empty.num_rows == 0
    assert empty.schema.names == FixMsg.into_field().into_arrow_schema().names

    with pytest.raises(TypeError, match="Message rows"):
        FixMsg.from_message_batch(["8=FIX.4.4|35=D|10=000"])


def test_malformed_typed_values_report_the_row_without_stopping_its_batch(
    codec: FixCodec,
) -> None:
    """Typed nulls remain best effort, and now retain why they became null."""
    lines = [
        "8=FIX.4.4|9=12|35=D|34=1|52=20260821-10:00:00|43=N|11=GOOD-1|55=AAPL|44=10.5|54=1|10=000|",
        "8=FIX.4.4|9=12x|35=D|34=9223372036854775808|"
        "52=20260230-25:61:00|43=perhaps|11=BAD|55=AAPL|44=abc|54=1|10=000|",
        "8=FIX.4.4|9=12|35=D|34=3|52=20260821-10:00:02|43=N|11=GOOD-2|55=AAPL|44=11.5|54=1|10=000|",
    ]
    rows = [_message_at(line, index + 1) for index, line in enumerate(lines)]

    raw = _raw_batch(*rows)
    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.slice(0, 1).equals(FixMsg.from_message_batch(_raw_batch(rows[0]), codec))
    assert parsed.slice(2, 1).equals(FixMsg.from_message_batch(_raw_batch(rows[2]), codec))
    assert parsed.column("clordid").to_pylist() == ["GOOD-1", "BAD", "GOOD-2"]
    assert _instrument_column(parsed, "symbol").to_pylist() == ["AAPL"] * 3
    assert parsed.column("price").to_pylist() == [10.5, None, 11.5]
    assert parsed.column("reason").to_pylist() == [None, None, None]
    assert parsed.column("error").to_pylist() == [
        None,
        "BodyLength <9>: invalid 12x; "
        "MsgSeqNum <34>: invalid 9223372036854775808; "
        "PossDupFlag <43>: invalid perhaps; "
        "SendingTime <52>: invalid 20260230-25:61:00; "
        "Price <44>: invalid abc",
        None,
    ]
    assert parsed.column("entries")[1].as_py() == [
        {"tag": 44, "key": "Price", "value": "abc", "comp": None}
    ]


@pytest.mark.parametrize(
    ("tag", "name", "column", "malformed"),
    [
        (30029, "MarketMarker", "marketmarker", "perhaps"),
        (30031, "CreationTime", "creationtime", "not-a-time"),
    ],
)
@pytest.mark.parametrize("rendered", [False, True], ids=["numeric", "UL"])
def test_one_diagnostic_owns_each_malformed_typed_package_field(
    codec: FixCodec,
    tag: int,
    name: str,
    column: str,
    malformed: str,
    *,
    rendered: bool,
) -> None:
    key = f"#{name.upper()}" if rendered else str(tag)
    header = "35=UL|#MSGTYPE=D|#CLORDID=BAD" if rendered else "35=D|11=BAD"
    raw = _raw_batch(Message(body=f"8=FIX.4.4|{header}|{key}={malformed}|10=000|"))

    parsed = FixMsg.from_message_batch(raw, codec)
    assert parsed.column(column).to_pylist() == [None]
    assert parsed.column("error").to_pylist() == [f"{name} <{tag}>: invalid {malformed}"]


@pytest.mark.parametrize(
    "line",
    (
        "8=FIX.4.4|35=8|17=E-1|120=ZZ|10=000|",
        "bridge #BEGINSTRING=FIX.4.4|#MSGTYPE=8|#EXECID=E-1|#SETTLCURRENCY=ZZ",
    ),
    ids=("numeric", "UL"),
)
def test_malformed_settlement_currency_is_one_best_effort_diagnostic(
    line: str, codec: FixCodec
) -> None:
    parsed = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert parsed.column("settlcurrency").to_pylist() == [None]
    assert parsed.column("error").to_pylist() == ["SettlCurrency <120>: invalid ZZ"]


def test_configured_null_session_spellings_are_absent_not_errors(codec: FixCodec) -> None:
    rows = [
        Message(body="8=FIX.4.4|9=null|35=D|49=null|11=A|10=000|"),
        Message(body="8=FIX.4.4|35=D|34= <NULL> |56= <NULL> |11=B|10=000|"),
        Message(body="8=FIX.4.4|35=D|43= N/A |115= N/A |11=C|10=000|"),
    ]

    raw = _raw_batch(*rows)
    parsed = FixMsg.from_message_batch(raw, codec)
    assert parsed.column("bodylength").to_pylist() == [None, None, None]
    assert parsed.column("msgseqnum").to_pylist() == [None, None, None]
    assert parsed.column("possdupflag").to_pylist() == [None, None, None]
    assert parsed.column("sendercompid").to_pylist() == [None, None, None]
    assert parsed.column("targetcompid").to_pylist() == [None, None, None]
    assert parsed.column("onbehalfofcompid").to_pylist() == [None, None, None]
    assert parsed.column("error").to_pylist() == [None, None, None]

    mixed = FixMsg.from_message_batch(
        _raw_batch(
            rows[0],
            Message(body="toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=D|#CLORDID=NAMED|"),
        ),
        codec,
    )
    assert mixed.column("sendercompid").to_pylist() == [None, None]


def test_one_throwing_transcription_isolated_between_valid_rows(
    registry: FixRegistry,
) -> None:
    """The vector path bisects only after failure and preserves input order."""

    class FailingCodec(FixCodec):
        def into_lifted_columns(
            self, entries: object, version: str | None = None
        ) -> tuple[dict[str, object], object]:
            values = pyarrow.compute.struct_field(pyarrow.compute.list_flatten(entries), "value")
            if pyarrow.compute.any(pyarrow.compute.equal(values, "FAIL"), min_count=0).as_py():
                raise ValueError("synthetic transcription failure")
            return super().into_lifted_columns(entries, version)

    codec = FailingCodec(registry=registry)
    rows = [
        _message_at(
            f"8=FIX.4.4|35=D|11={clordid}|55=AAPL|54=1|38=5|40=2|44=10|10=000|",
            index,
            sourceurl="capture.log",
            sourcerownum=index,
        )
        for index, clordid in enumerate(("GOOD-1", "FAIL", "GOOD-2"), 1)
    ]

    raw = _raw_batch(*rows)
    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.slice(0, 1).equals(FixMsg.from_message_batch(_raw_batch(rows[0]), codec))
    assert parsed.slice(2, 1).equals(FixMsg.from_message_batch(_raw_batch(rows[2]), codec))
    assert parsed.column("clordid").to_pylist() == ["GOOD-1", None, "GOOD-2"]
    assert parsed.column("sourcerownum").to_pylist() == [1, 2, 3]
    assert parsed.column("reason").to_pylist() == [None, None, None]
    assert parsed.column("error").to_pylist() == [
        None,
        "FIX transcription failed: ValueError: synthetic transcription failure",
        None,
    ]
    assert parsed.column("entries")[1].as_py() is None

    messages = list(FixMsg.from_arrow_reader([parsed]))
    assert list(messages[1].into_market_events(registry=registry)) == []
    assert list(InstUpdate.from_fixmsgs((messages[1],), registry=registry)) == []
    translated = list(FixMsg.into_market_arrow_batches(parsed, registry=registry))
    assert sum(batch.num_rows for _, batch in translated) == 2


def test_failed_enrichment_fallback_does_not_repeat_the_failure(
    codec: FixCodec,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing(*args, **kwargs):
        del args, kwargs
        raise pyarrow.ArrowInvalid("synthetic enrichment failure")

    monkeypatch.setattr(Instrument, "from_fix_arrow", failing)
    parsed = FixMsg.from_message_batch(
        _raw_batch(Message(body="8=FIX.4.4|35=D|11=SYNTH|10=000|")),
        codec,
    )

    assert parsed.num_rows == 1
    assert parsed.column("error").to_pylist() == [
        "FIX transcription failed: ArrowInvalid: synthetic enrichment failure"
    ]
    assert parsed.column("entries").to_pylist() == [None]


def _widened(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    """The batch as an Iceberg scan hands it back: every string 64-bit wide."""

    def wide(dtype: pyarrow.DataType) -> pyarrow.DataType:
        if pyarrow.types.is_string(dtype):
            return pyarrow.large_string()
        if pyarrow.types.is_binary(dtype):
            return pyarrow.large_binary()
        if pyarrow.types.is_list(dtype):
            return pyarrow.large_list(pyarrow.field(dtype.field(0).name, wide(dtype.field(0).type)))
        if pyarrow.types.is_struct(dtype):
            return pyarrow.struct([member.with_type(wide(member.type)) for member in dtype])
        return dtype

    schema = pyarrow.schema([member.with_type(wide(member.type)) for member in batch.schema])
    return pyarrow.RecordBatch.from_arrays(
        [column.cast(member.type) for column, member in zip(batch.columns, schema, strict=True)],
        schema=schema,
    )


def test_a_batch_scanned_back_out_of_storage_transcribes_the_same() -> None:
    """Each `parse_fix_*` stage reads its rows back through a scan, which
    hands `large_string` back where the raw contract says `string` -- and the
    kernels this path builds its own constants for refuse the mix."""
    rows = [
        _message_at("8=FIX.4.4\x0135=D\x0111=C1\x0154=1\x0138=5\x0110=000\x01", 1),
        _message_at("plain prose", 2),
    ]
    fresh = FixMsg.from_message_batch(rows)
    stored = _widened(Message.into_arrow_batch(rows))
    assert stored.column("body").type == pyarrow.large_binary(), "the fixture is the wide one"
    assert FixMsg.from_message_batch(stored).equals(fresh)


def test_a_raw_projection_without_body_is_a_batch_contract_error() -> None:
    raw = _raw_batch(Message(body="8=FIX.4.4|35=D|11=C1|10=000|"))
    projected = raw.drop_columns(["body"])

    with pytest.raises(ValueError, match="raw Message batch needs body"):
        FixMsg.from_message_batch(projected)


def test_a_stored_field_reads_through_its_own_structure() -> None:
    """A stored argument's tag, key and component path are the read's."""
    row = FixMsg(
        msgtype="D",
        entries=[
            {"tag": 55, "key": "Symbol", "value": "IBM"},
            {"tag": 0, "key": "TECH.CLIENTID", "value": "A1"},
        ],
    )

    assert (row.get(55).raw, row.get(55).key) == ("IBM", "Symbol")
    assert row.get("TECH.CLIENTID").raw == "A1"
    assert row.pairs == [("35", "D"), ("55", "IBM"), ("TECH.CLIENTID", "A1")]


def test_fix_fields_render_without_the_raw_message_envelope() -> None:
    row = FixMsg.from_message(
        Message.from_text(
            b"ParentClOrdID=PARENT|ISINCODE=US0378331005",
            plugin="capture",
            sourceurl="capture.log",
        )
    )

    assert row.plugin.code == "CAPTURE"
    assert "body" not in FixMsg.into_field().names
    assert ("ParentClOrdID", "PARENT") in row.pairs
    assert ("ISINCODE", "US0378331005") in row.pairs
    assert row.entries == []
    assert not row.unmap, "registry-declared rendered names are retained as known fields"


def test_an_exotic_stored_spelling_renders_verbatim() -> None:
    """Whole dotted keys and indexed component paths render byte for byte."""
    row = FixMsg(
        msgtype="D",
        entries=[
            {"key": "Side[03]", "value": "1"},
            {"key": "X.a.b[0]", "value": "2"},
            {"key": "TECH.NoPartyIDs[0].PartyID", "value": "P"},
        ],
    )

    assert row.pairs == [
        ("35", "D"),
        ("Side[03]", "1"),
        ("X.a.b[0]", "2"),
        ("TECH.NoPartyIDs[0].PartyID", "P"),
    ]
    assert row.get("X.a.b[0]").raw == "2"
    assert row.get("PartyID").raw == "P"
    trailing = FixMsg(entries=[("A.", "v1")])
    assert trailing.get("A.").raw == "v1", "a trailing-dot key stays readable"
    assert [reading.raw for reading in row.readings("Side")] == ["1"]


def test_a_component_restores_only_the_members_it_declares() -> None:
    """A component column round trips its own members and nothing else: what
    it never projected stayed in `entries`, which is where the round trip
    picks it up rather than from a second residual on the entry."""
    row = FixMsg(parties=[{"partyid": "A"}], entries=[Entry.from_pair("007", "x")])

    assert row.pairs == [("453", "1"), ("448", "A"), ("7", "x")]


def test_an_unlinked_row_reads_through_the_packaged_dictionary() -> None:
    assert FixMsg().registry is FixRegistry.from_builtin()
    assert FixMsg.from_text("8=FIX.4.4|35=D|10=000").registry is FixRegistry.from_builtin()


def test_a_row_privately_links_the_dictionary_it_was_parsed_under(
    registry: FixRegistry,
) -> None:
    """The link steers every read on the row and travels into translation --
    but it is reader state, never a column: the stored contract is unchanged."""
    row = FixMsg.from_text("8=FIX.4.4|35=D|11=C1|10=000", registry=registry)

    assert row.registry is registry
    assert row.into_fix_events().registry is registry
    assert "_FixMsg__registry" not in FixMsg.into_field().names
    assert "_FixMsg__registry" not in row.into_dict()


def test_the_translator_links_its_dictionary_onto_its_message(registry: FixRegistry) -> None:
    """One dictionary per translation: a message handed to `FixEvents` under a
    registry answers its own `get` under that same registry afterwards."""
    row = FixMsg.from_text("8=FIX.4.4|35=D|11=C1|10=000")
    row.into_fix_events(registry=registry)

    assert row.registry is registry


def test_scalar_parsing_resolves_the_fix_version_once_on_the_message() -> None:
    row = FixMsg.from_text("8=FIX.4.4|35=D|11=C1|10=000")

    assert row.protocol.code == "FIX4.4"
    assert row.resolved_version() == "4.4"


def test_fixt_dict_reconstruction_keeps_the_persisted_application_version() -> None:
    row = FixMsg.from_dict(
        {
            "protocol": Protocol.from_str("FIX5SP2").into_stored(),
            "beginstring": "FIXT.1.1",
            "entries": [],
        }
    )

    assert row.protocol.code == "FIX5SP2"
    assert row.resolved_version() == "5.0.SP2"


#: The four ways two diagnostic columns can meet, and a fifth that crosses the
#: bound. Merging is skipped where one side says nothing, so each has to answer
#: what joining them says.
_DIAGNOSTICS: tuple[tuple[list[str | None], list[str | None]], ...] = (
    ([None, None], [None, None]),
    (["torn", None], [None, None]),
    ([None, None], [None, "torn"]),
    (["torn", None], [None, "late"]),
    (["x" * _ERROR_LENGTH, "torn"], ["late", "y" * _ERROR_LENGTH]),
)


@pytest.mark.parametrize(("left", "right"), _DIAGNOSTICS)
def test_merged_diagnostics_read_as_the_join_of_both_sides(
    left: list[str | None], right: list[str | None]
) -> None:
    """The reference is the join; an empty side only makes it cheaper."""
    compute = pyarrow.compute
    columns = (pyarrow.array(left, pyarrow.string()), pyarrow.array(right, pyarrow.string()))
    stated = [compute.fill_null(column, "") for column in columns]
    separator = compute.if_else(
        compute.and_(*(compute.not_equal(one, "") for one in stated)), "; ", ""
    )
    joined = compute.utf8_slice_codeunits(
        compute.binary_join_element_wise(stated[0], separator, stated[1], ""),
        start=0,
        stop=_ERROR_LENGTH,
    )
    expected = compute.if_else(compute.equal(joined, ""), None, joined)

    assert _merge_error_columns(*columns).to_pylist() == expected.to_pylist()


def test_merged_diagnostics_refuse_two_widths() -> None:
    """A shortcut takes one width; two remain the join's to refuse."""
    with pytest.raises(pyarrow.ArrowInvalid):
        _merge_error_columns(pyarrow.nulls(2, pyarrow.string()), pyarrow.array(["torn", None, ""]))


def test_named_group_members_do_not_shadow_numeric_repetitions() -> None:
    row = FixMsg.from_pairs([("448", "wire"), ("NoPartyIDs[1].PartyID", "named")])
    reader = row.into_fix_events(fix_version="4.4")

    assert [value for tag, value in row.into_fix_pairs(reader.access) if tag == "448"] == [
        "wire",
        "named",
    ]


def test_arrow_named_group_members_remain_repetitions(
    codec: FixCodec, registry: FixRegistry
) -> None:
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
        type=ENTRIES,
    )
    resolved = codec.complete_entries(source, "4.4")

    kept = FixMsg._prefer_named_entries(source, resolved)

    assert [entry["value"] for entry in kept[0].as_py()] == ["wire", "named"]


def test_hybrid_flat_names_do_not_erase_numeric_repeating_groups(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    party_line = (
        "8=FIX.4.4|35=UL|#MSGTYPE=D|#PARTYID=HEADER|453=1|"
        "448=GROUP|447=D|452=1|11=C1|55=AAPL|54=1|38=1|"
        "60=20260821-10:00:00|10=000"
    )
    party_batch = FixMsg.from_message_batch(_raw_batch(Message(body=party_line)), codec)
    party = FixMsg.from_dict(party_batch.to_pylist()[0])

    assert party_batch.column("parties")[0].as_py()[0]["partyid"] == "GROUP"
    assert party.group(453) == [[("448", "GROUP"), ("447", "D"), ("452", "1")]]

    depth_line = (
        "8=FIX.4.4|35=UL|#MSGTYPE=X|#SYMBOL=HEADER|268=1|279=0|269=0|55=ENTRY|270=100|271=1|10=000"
    )
    depth_batch = FixMsg.from_message_batch(_raw_batch(Message(body=depth_line)), codec)
    depth = FixMsg.from_dict(depth_batch.to_pylist()[0])

    assert depth.group(268) == [
        [("279", "0"), ("269", "0"), ("55", "ENTRY"), ("270", "100"), ("271", "1")]
    ]
    assert [event.symbolticker for event in depth.into_market_events(fix_version="4.4")] == [
        "ENTRY"
    ]


def test_instrument_groups_resolve_into_their_structured_columns(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """Alt-ids and legs land typed, leave `entries`, and read back identically."""
    line = (
        "8=FIX.4.4|35=d|55=SPREAD|48=XS123|22=4|"
        "454=2|455=US0378331005|456=4|455=037833100|456=1|"
        "555=2|600=AAPL|624=1|623=1|611=20270115|612=150.5|"
        "600=MSFT|624=2|623=2|556=USD|687=9|10=000"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    altids = batch.column("securityaltid")[0].as_py()
    assert [(entry["securityaltid"], entry["securityaltidsource"]) for entry in altids] == [
        ("US0378331005", "4"),
        ("037833100", "1"),
    ]
    legs = _instrument_column(batch, "legs")[0].as_py()
    assert [(entry["symbol"], entry["side"], entry["ratio"]) for entry in legs] == [
        ("AAPL", int(Side.BUY), 1.0),
        ("MSFT", int(Side.SELL), 2.0),
    ]
    assert legs[0]["maturitydate"] == datetime.datetime(2027, 1, 15)
    assert [(entry["key"], entry["value"]) for entry in batch.column("entries")[0].as_py()] == [
        ("LegQty", "9")
    ], "a member no column projects stays in `entries`, and nothing is stored twice"

    stored = FixMsg.from_dict(batch.to_pylist()[0])
    assert stored.group(454, (455, 456)) == [
        [("455", "US0378331005"), ("456", "4")],
        [("455", "037833100"), ("456", "1")],
    ]
    assert [dict(entry).get("600") for entry in map(dict, stored.group(555))] == ["AAPL", "MSFT"]

    (instrument,) = _instruments(stored)
    (direct,) = _instruments(FixMsg.from_text(line, "|"))
    assert instrument.isincode == "XS123", "the primary ISIN outranks the alternative"
    assert [(leg.symbol, leg.side.name, leg.ratio) for leg in instrument.legs] == [
        ("AAPL", "BUY", 1.0),
        ("MSFT", "SELL", 2.0),
    ]
    assert instrument == direct, "the resolved columns and the pair walk agree"


def test_instrument_component_decimals_accept_fix_exponents(codec: FixCodec) -> None:
    """Component gates and their casts accept the same valid numeric spellings."""
    line = "8=FIX.4.4|35=d|55=SPREAD|555=1|600=AAPL|612=1e3|623=2.5E-1|624=1|10=000|"

    parsed = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)
    (leg,) = _instrument_column(parsed, "legs")[0].as_py()

    assert leg["strikeprice"] == 1_000.0
    assert leg["ratio"] == 0.25
    assert parsed.column("error").to_pylist() == [None]
    assert 612 not in {entry["tag"] for entry in parsed.column("entries")[0].as_py()}
    assert 623 not in {entry["tag"] for entry in parsed.column("entries")[0].as_py()}


def test_nested_instrument_does_not_absorb_lifecycle_altids() -> None:
    message = FixMsg(instrument=Instrument(symbol="AAPL"), altids={"clordid": "C1"})

    assert message.instrument.symbolticker == "AAPL"
    assert message.altids == {"clordid": "C1", "symbolticker": "AAPL"}
    assert "altids" not in Instrument.into_field().names


def test_instrument_projection_prefers_promoted_values_and_fills_from_entries() -> None:
    message = FixMsg(
        unix=23,
        protocol="FIX4.4",
        instrument=Instrument(symbol="PROMOTED"),
        entries=[(55, "RESIDUAL"), (107, "reference facts")],
    )
    (update,) = InstUpdate.from_fixmsgs([message])
    instrument = update.instrument

    assert (update.unix, instrument.symbol, instrument.securitydesc) == (
        23,
        "PROMOTED",
        "reference facts",
    )
    assert (update.creaunix, update.recunix) == (0, 0)


def test_rendered_indexed_instrument_groups_resolve_the_same_way(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """The bridge's `NOLEGS[i]=...` spelling reaches the same typed columns."""
    member = "\x04\x03"
    line = (
        "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=d|#SYMBOL=SPREAD|"
        f"#NOSECURITYALTID=1|#NOSECURITYALTID[0]=SECURITYALTID=US0378331005{member}"
        f"SECURITYALTIDSOURCE=4{member}|"
        f"#NOLEGS=2|#NOLEGS[0]=LEGSYMBOL=AAPL{member}LEGSIDE=1{member}LEGRATIOQTY=1{member}|"
        f"#NOLEGS[1]=LEGSYMBOL=MSFT{member}LEGSIDE=2{member}LEGRATIOQTY=2{member}"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert Protocol.from_int(batch.column("protocol")[0].as_py()).code == "UL4.4"
    altids = batch.column("securityaltid")[0].as_py()
    assert [(entry["securityaltid"], entry["securityaltidsource"]) for entry in altids] == [
        ("US0378331005", "4"),
    ]
    legs = _instrument_column(batch, "legs")[0].as_py()
    assert [(entry["symbol"], entry["side"], entry["ratio"]) for entry in legs] == [
        ("AAPL", int(Side.BUY), 1.0),
        ("MSFT", int(Side.SELL), 2.0),
    ]

    (instrument,) = _instruments(FixMsg.from_dict(batch.to_pylist()[0]))
    assert instrument.isincode == "US0378331005"
    assert [(leg.symbol, leg.side.name, leg.ratio) for leg in instrument.legs] == [
        ("AAPL", "BUY", 1.0),
        ("MSFT", "SELL", 2.0),
    ]

    scalar = FixMsg.from_text(line, registry=registry)
    assert [(leg.symbol, leg.side.name, leg.ratio) for leg in scalar.instrument.legs or ()] == [
        ("AAPL", "BUY", 1.0),
        ("MSFT", "SELL", 2.0),
    ]
    scalar_pairs = scalar.into_fix_pairs()
    assert sum(key == "600" for key, _ in scalar_pairs) == 2
    assert sum(key == "555" for key, _ in scalar_pairs) == 1


def test_nested_source_component_is_not_rendered_again_at_the_message_root() -> None:
    stamp = datetime.datetime(2026, 8, 29, 22, 5, 0, 41190)
    row = FixMsg(
        protocol=Protocol.with_version(Protocol.FIX, "4.4"),
        sidetrdregts=[SideTrdRegTimestamp(sidetrdregtimestamp=stamp)],
        entries=[
            Entry(
                tag=1012,
                key="SideTrdRegTimestamp",
                value="20260829-22:05:00.041190",
                comp="NoSides[0].NoSideTrdRegTS[0]",
            )
        ],
    )

    pairs = row.into_fix_pairs()

    assert (
        "NoSides[0].NoSideTrdRegTS[0].SideTrdRegTimestamp",
        "20260829-22:05:00.041190",
    ) in pairs
    assert not any(key in {"1016", "1012"} for key, _ in pairs)


def test_nested_scoped_component_does_not_hide_a_distinct_root_group() -> None:
    row = FixMsg(
        protocol=Protocol.with_version(Protocol.FIX, "4.4"),
        securityaltid=[
            SecurityAltID(securityaltid="ROOT", securityaltidsource="4"),
        ],
        entries=[
            Entry(
                tag=455,
                key="SecurityAltID",
                value="NESTED",
                comp="NoMDEntries[0].NoSecurityAltID[0]",
            )
        ],
    )

    pairs = row.into_fix_pairs()

    assert ("454", "1") in pairs
    assert ("455", "ROOT") in pairs
    assert ("456", "4") in pairs
    assert ("NoMDEntries[0].NoSecurityAltID[0].SecurityAltID", "NESTED") in pairs

    prefixed = FixMsg(
        protocol=Protocol.with_version(Protocol.FIX, "4.4"),
        securityaltid=[SecurityAltID(securityaltid="ROOT", securityaltidsource="4")],
        entries=[
            Entry(
                tag=455,
                key="SecurityAltID",
                value="ROOT",
                comp="Instrument.NoSecurityAltID[0]",
            ),
            Entry(
                tag=456,
                key="SecurityAltIDSource",
                value="4",
                comp="Instrument.NoSecurityAltID[0]",
            ),
        ],
    )
    prefixed_pairs = prefixed.into_fix_pairs()
    assert ("Instrument.NoSecurityAltID[0].SecurityAltID", "ROOT") in prefixed_pairs
    assert ("Instrument.NoSecurityAltID[0].SecurityAltIDSource", "4") in prefixed_pairs
    assert not any(key in {"454", "455", "456"} for key, _ in prefixed_pairs)


def test_residual_group_member_does_not_hide_typed_members_after_storage(
    codec: FixCodec,
) -> None:
    separator = "\x04\x03"
    lines = [
        (f"#NOPARTYIDS=1|#NOPARTYIDS[0]=PARTYID=P-1{separator}VENUEFLAG=kept|"),
        f"#NOLEGS=1|#NOLEGS[0]=LEGSYMBOL=A{separator}LEGQTY=9|",
        (f"#NOPARTYIDS=2|#NOPARTYIDS[0]=PARTYID=P-2{separator}VENUEFLAG=kept|"),
    ]
    batch = FixMsg.from_message_batch(
        _raw_batch(*(Message(body=f"toBridge #MSGTYPE=D|{line}") for line in lines)),
        codec,
    )
    parties, legs, mismatched = [
        FixMsg.from_dict(row).into_fix_pairs() for row in batch.to_pylist()
    ]

    assert ("453", "1") in parties
    assert ("448", "P-1") in parties
    assert ("NOPARTYIDS[0].VENUEFLAG", "kept") in parties
    assert ("555", "1") in legs
    assert ("600", "A") in legs
    assert ("NOLEGS[0].LegQty", "9") in legs
    assert ("453", "2") in mismatched
    assert ("448", "P-2") in mismatched
    assert sum(key == "453" for key, _ in mismatched) == 1
    assert mismatched.index(("453", "2")) < mismatched.index(("448", "P-2"))


def test_control_separated_party_group_leaves_absent_instrument_values_null() -> None:
    separator = "\x04\x03"
    payload = (
        "#NOPARTYIDS=1|#NOPARTYIDS[0]=PARTYID=SYNTH"
        f"{separator}PARTYIDSOURCE=shortcodeid"
        f"{separator}PARTYROLE=executingsystem"
        f"{separator}PARTYROLEQUALIFIER=exchangeordersubmitter|"
    )

    parsed = FixMsg.from_message_batch([Message.from_text(payload)])

    assert parsed.column("parties").to_pylist() == [
        [
            {
                "partyid": "SYNTH",
                "partyidsource": "P",
                "partyrole": 16,
                "partyrolequalifier": 30,
            }
        ]
    ]
    assert _instrument_column(parsed, "quantitytype").to_pylist() == [None]
    assert parsed.column("error").to_pylist() == [None]


def test_an_entry_scoped_alt_id_group_stays_with_its_entry(
    codec: FixCodec, registry: FixRegistry
) -> None:
    """A group inside one market-data entry is that entry's, not the message's.

    The scoped extractors leave it in `entries`, so the per-entry instrument
    readers find it exactly where the scalar walk does -- hoisting it into the
    message-level column would have filed the identifier under whichever
    instrument the header names.
    """
    line = (
        "8=FIX.4.4|35=X|52=20260814-00:05:01.149|268=2|"
        "279=0|269=0|55=BTC-USD|270=100.0|271=5|"
        "279=0|269=0|55=ETH-USD|48=ETH-ID|454=1|455=US0378331005|456=4|270=99.0|271=1|10=000"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert batch.column("securityaltid")[0].as_py() is None
    assert 454 in [entry["tag"] for entry in batch.column("entries")[0].as_py()]

    stored = FixMsg.from_dict(batch.to_pylist()[0])
    direct = FixMsg.from_text(line, "|")
    found = [(one.symbol, one.isincode) for one in _instruments(stored)]
    assert found == [(one.symbol, one.isincode) for one in _instruments(direct)]
    assert found == [("BTC-USD", None), ("ETH-USD", "US0378331005")]


def test_a_quote_entry_scoped_alt_id_group_stays_with_its_entry(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """The same ownership, one nesting deeper: quote sets scope quote entries."""
    line = (
        "8=FIX.4.4|35=i|117=Q1|296=1|302=S1|295=2|"
        "299=E1|55=AAA|132=1|133=2|"
        "299=E2|55=BBB|454=1|455=037833100|456=1|132=3|133=4|10=000"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert batch.column("securityaltid")[0].as_py() is None

    stored = FixMsg.from_dict(batch.to_pylist()[0])
    direct = FixMsg.from_text(line, "|")
    found = [(one.symbol, one.securityid) for one in _instruments(stored)]
    assert found == [(one.symbol, one.securityid) for one in _instruments(direct)]
    assert found == [("AAA", None), ("BBB", None)]


def test_a_4_3_row_answers_from_the_column_and_from_entries_at_once(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """4.3 declares `SecAltIDGrp` and the legs group, so one stored row reads
    both off resolved columns rather than walking the pairs for either.

    `InstrmtLegGrp` -- the block that wraps the legs -- arrives in 4.4, and
    while a group had to be found through such a wrapper this row answered
    nothing for `legs` in 4.3. The group itself is 4.3's: `NoLegs <555>` is
    declared there, and it is now stored beside the `InstrumentLeg` block one
    entry of it is, so the version answers what its own dictionary says.
    """
    line = (
        "8=FIX.4.3|35=d|55=SPREAD|454=1|455=US0378331005|456=4|"
        "555=2|600=AAPL|624=1|623=1|600=MSFT|624=2|623=2|10=000"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert [entry["securityaltid"] for entry in batch.column("securityaltid")[0].as_py()] == [
        "US0378331005"
    ]
    legs = _instrument_column(batch, "legs")[0].as_py()
    assert [leg["symbol"] for leg in legs] == ["AAPL", "MSFT"], "4.3 declares NoLegs <555>"
    assert 555 not in [entry["tag"] for entry in batch.column("entries")[0].as_py()], (
        "a tag a column answers for is not also left unresolved beside it"
    )

    (instrument,) = _instruments(FixMsg.from_dict(batch.to_pylist()[0]))
    assert instrument.isincode == "US0378331005"
    assert [(leg.symbol, leg.side.name) for leg in instrument.legs] == [
        ("AAPL", "BUY"),
        ("MSFT", "SELL"),
    ]


def test_typed_timestamps_keep_direct_and_stored_book_outputs_equal(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    line = (
        "8=FIX.4.4|35=8|37=O1|11=C1|17=E1|55=AAPL|54=1|39=1|150=1|"
        "38=5|31=100|32=2|14=2|151=3|768=1|769=20260821-09:59:00|770=1|"
        "60=20260821-10:01:00|10=000"
    )
    direct = FixMsg.from_text(line)
    stored_batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)
    stored = FixMsg.from_dict(stored_batch.to_pylist()[0])

    assert direct.get(769).raw == datetime.datetime(2026, 8, 21, 9, 59, tzinfo=datetime.UTC)
    assert [value for tag, value in direct.pairs if tag == "769"] == ["20260821-09:59:00.000000"]
    assert [value for tag, value in stored.pairs if tag == "769"] == ["20260821-09:59:00.000000"]
    assert list(BookIterator(logs=[stored], snapshot_every=0)) == list(
        BookIterator(logs=[direct], snapshot_every=0)
    )


def test_market_projection_keeps_typed_fix_timestamp_spelling() -> None:
    row = FixMsg(sendingtime=datetime.datetime(2026, 8, 25, 9, 30, 1, 250000))

    assert ("52", "20260825-09:30:01.250000") in row.into_fix_pairs()


def test_malformed_stored_field_entries_are_not_silently_dropped() -> None:
    with pytest.raises(KeyError, match="key"):
        FixMsg(entries=[{"value": "x"}])
    with pytest.raises(ValueError, match="not enough values"):
        FixMsg(entries=[["OnlyKey"]])


def test_parties_keep_exactly_the_registry_fields_they_declare(
    registry: FixRegistry,
) -> None:
    """The registry's own reading of each member, narrowed to what a column
    says: which field it is, what it is called, and its FIX type. The version
    list, the messages that carry it and the enumeration it declares stay in
    the registry rather than being copied onto every row of every table."""
    parties = FixMsg.into_field().field("Parties")
    assert parties.nullable and not parties.item.nullable
    assert parties.metadata["fix:component"] == "Parties"
    for tag, name in ((448, "PartyID"), (447, "PartyIDSource"), (452, "PartyRole")):
        expected = registry.scalar(tag)
        actual = Party.into_field().field(name)
        assert actual.name == column_name(expected.name)
        assert actual.fix["name"] == expected.name
        assert actual.fix.canonical == expected.name, "folded to store, spelled to read"
        assert actual.dtype == expected.dtype
        assert actual.metadata == column_metadata({**expected.metadata, "fix:name": expected.name})
        assert set(actual.metadata) <= {"description", *_COLUMN_METADATA}
        assert actual.description == expected.description


def test_every_column_is_documented() -> None:
    for member in FixMsg.into_field().fields:
        assert member.description, f"{member.name} has no description"
        assert "\n" not in member.description, f"{member.name} description is not one line"


def test_the_key_is_the_moment_and_the_line() -> None:
    """Two columns: a hash identifies the line, the time is what an engine prunes on."""
    assert FixMsg.into_field().primary_keys() == ["unix", "hash"]


def test_the_partition_is_the_hour_the_line_falls_in() -> None:
    """An identity partition on an integer, so every engine below reads it alike."""
    assert FixMsg.into_field().partition_keys() == {"unixpartition": "identity"}
    assert FixMsg.into_field().field("unixpartition").dtype == pyarrow.int32()


def test_every_unix_column_declares_its_unit() -> None:
    for name in ("unix", "creaunix", "recunix", "expunix", "snapunix", "prevunix"):
        metadata = FixMsg.into_field().field(name).metadata
        assert metadata["unit"] == "ns", name
        assert metadata["epoch"] == "1970-01-01", name
    partition_metadata = FixMsg.into_field().field("unixpartition").metadata
    assert partition_metadata["unit"] == "second"
    assert partition_metadata["epoch"] == "1970-01-01"


def test_hash_widths_match_their_roles() -> None:
    """Exact versions and lifecycles are wide; value identity is int64."""
    for name in ("hash", "prevhash", "xhash"):
        assert FixMsg.into_field().field(name).dtype == HASH, name
    assert FixMsg.into_field().field("vhash").dtype == pyarrow.int64()
    assert FixMsg.into_field().field("linkhashes").dtype.value_type == HASH
    assert FixMsg.into_field().field("unix").dtype == pyarrow.int64()


def test_a_line_is_unclassified_until_something_classifies_it() -> None:
    """The fallback the rules fall back to, on the class rather than in the parser."""
    assert FixMsg.into_event_type() is EventType.UNKNOWN
    assert FixMsg().eventtype is EventType.UNKNOWN


def test_the_hour_is_derived_from_the_instant() -> None:
    built = FixMsg(unix=3 * HOUR + 5)
    hour_seconds = HOUR // SECOND
    assert built.unixpartition == 3 * hour_seconds
    assert FixMsg(unix=-1).unixpartition == -hour_seconds, "and it floors, either side of the epoch"


def test_the_schema_says_which_class_it_came_from() -> None:
    schema = FixMsg.into_field().into_arrow_schema()
    assert schema.metadata[b"name"] == b"FixMsg"
    assert Field.from_arrow_schema(schema) == FixMsg.into_field()


def _stored(tag: int, key: str, value: str) -> dict[str, object]:
    """One stored field in the whole spelling `FixMsg.into_dict` writes."""
    return {"tag": tag, "key": key, "value": value, "comp": None}


def test_a_row_round_trips_as_a_document() -> None:
    """The message layer preserves checksums and repeated ordered pairs."""
    row = FixMsg(
        sourceurl="a.txt",
        unix=2,
        hash=3,
        xhash=3,
        eventtype=EventType.ORDER,
        threadname="t",
        plugin="d",
        protocol=Protocol.FIX,
        entries=[_stored(11, "ClOrdID", one) for one in ("ORD-1", "ORD-1-again")]
        + [_stored(0, "ISINCODE", one) for one in ("FAKE-ISIN-0001", "FAKE-ISIN-0002")],
        code="TTF",
        msgseqnum=7,
        instrument=Instrument(symbol="TTF"),
        sendingtime=datetime.datetime.fromtimestamp(1_755_163_800.123, tz=datetime.UTC),
        possdupflag=True,
        checksum="010",
        lastmkt=MIC.from_str("XPAR"),
        reason="test reject",
    )
    assert FixMsg.from_json(row.into_json()) == row


def test_typed_components_share_stored_access_and_group_projection() -> None:
    row = FixMsg(
        parties=[Party(partyid="P1", partyidsource="D", partyrole=1)],
        entries=[],
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


def test_lastmkt_uses_the_standard_fix_field_with_the_mic_enum_type() -> None:
    member = FixMsg.into_field().field("lastmkt")
    assert member.nullable and member.dtype == pyarrow.int32()
    assert member.fix.tag == 30
    assert member.fix.canonical == "LastMkt"
    assert member.metadata["enum:encoding"] == "ascii-big-endian"
    assert member.metadata["enum:pattern"] == "[A-Z0-9]{4}"
    assert "enum:dynamic" not in member.metadata


def test_currency_and_order_context_keep_their_declared_fix_identities() -> None:
    member = FixMsg.into_field().field("settlcurrency")
    assert member.nullable and member.dtype == pyarrow.int32()
    assert (member.fix.tag, member.fix.type, member.fix.canonical) == (
        120,
        "Currency",
        "SettlCurrency",
    )
    assert member.metadata["enum:name"] == "Currency"
    assert member.metadata["enum:encoding"] == "ascii-big-endian"
    assert member.metadata["enum:pattern"] == "[A-Z]{3}"
    assert {
        name: FixMsg.into_field().field(name).fix.tag
        for name in ("orderflags", "orderoriginatorid", "conversationid", "bloombergcode")
    } == {
        "orderflags": 30035,
        "orderoriginatorid": 30036,
        "conversationid": 30037,
        "bloombergcode": 30038,
    }
    legcurrency = FixMsg.into_field().field("instrument").field("legs").item.field("currency")
    assert legcurrency.dtype == pyarrow.int32()
    assert legcurrency.metadata["enum:name"] == "Currency"


def test_reason_is_generic_optional_text_on_every_event() -> None:
    member = Event.into_field().field("reason")
    assert member.nullable and member.dtype == pyarrow.string()
    assert "fix:tag" not in member.metadata


# -- the raw-message boundary -------------------------------------------------


def _raw_batch(*messages: Message) -> pyarrow.RecordBatch:
    """One raw-message batch, including the zero-row shape."""
    if not messages:
        return pyarrow.RecordBatch.from_pylist([], schema=Message.into_field().into_arrow_schema())
    return Message.into_arrow_batch(messages)


def test_xml_transcription_is_best_effort_and_never_persists_body(
    codec: FixCodec,
) -> None:
    raw = _raw_batch(
        Message(body=b"XmlApi: <Order ClOrdID='broken'>"),
        Message(body=b"8=FIX.4.4|35=D|11=clean|10=000|"),
    )

    parsed = FixMsg.from_message_batch(raw, codec)

    assert "body" not in parsed.schema.names
    assert _protocols(parsed) == ["XML", "FIX4.4"]
    assert parsed.column("error")[0].as_py().startswith("XML parse failed: ParseError:")
    assert parsed.column("error")[1].as_py() is None
    assert parsed.column("clordid").to_pylist()[1] == "clean"


def test_fixmsg_batch_enriches_uniform_and_side_prices(codec: FixCodec) -> None:
    raw = _raw_batch(
        Message(body=b"8=FIX.4.4|35=D|54=1|44=10|132=9|10=000|"),
        Message(body=b"8=FIX.4.4|35=D|54=2|44=11|10=000|"),
        Message(body=b"8=FIX.4.4|35=X|269=0|270=12.5|10=000|"),
    )

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("lastpx").to_pylist() == [10.0, 11.0, 12.5]
    assert parsed.column("bidpx").to_pylist() == [9.0, None, 12.5]
    assert parsed.column("offerpx").to_pylist() == [None, 11.0, None]


def test_fixmsg_conversion_is_the_layer_that_parses_fix(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(
            body=(
                "8=FIX.4.4|35=D|34=7|41=ROOT|55=IBM|461=EXXXXX|6=12.5|"
                "453=1|448=BUYSIDE|447=D|452=1|10=000|"
            ),
            sourceurl="capture.log",
            sourcerownum=1,
            plugin="fix",
        ),
        Message(
            body=(
                "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=8|#ORIGCLORDID=OLD|#ISINCODE=XX0000084733|"
            ),
            sourceurl="capture.log",
            sourcerownum=2,
            plugin="ULBridge",
        ),
        Message(
            body="plain text",
            sourceurl="capture.log",
            sourcerownum=3,
            plugin="misc",
        ),
    )

    assert raw.schema.names == Message.into_field().names
    assert raw.column("body").to_pylist()[2] == b"plain text"
    assert not set(raw.schema.names) & {"entries", "protocol", "msgtype", "eventtype"}

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("eventtype").to_pylist() == [
        int(EventType.ORDER),
        int(EventType.EXECUTION),
        int(EventType.MISC),
    ]
    assert parsed.column("plugin").to_pylist() == [
        Plugin.from_str("fix").into_stored(),
        Plugin.from_str("ULBridge").into_stored(),
        Plugin.from_str("misc").into_stored(),
    ]
    assert _protocols(parsed) == ["FIX4.4", "UL4.4", "OTHER"]
    assert parsed.column("msgtype").to_pylist() == ["D", "8", None]
    assert parsed.column("msgseqnum").to_pylist() == [7, None, None]
    assert parsed.column("origclordid").to_pylist() == ["ROOT", "OLD", None]
    assert _instrument_column(parsed, "cficode").to_pylist() == ["EXXXXX", None, None]
    assert parsed.column("avgpx").to_pylist() == [12.5, None, None]
    assert _instrument_column(parsed, "isincode").to_pylist() == [None, "XX0000084733", None]
    assert parsed.column("parties").to_pylist()[0] == [
        {
            "partyid": "BUYSIDE",
            "partyidsource": "D",
            "partyrole": 1,
            "partyrolequalifier": None,
        }
    ]
    assert parsed.column("altids").to_pylist()[0] == [
        ("origclordid", "ROOT"),
        ("code", "ROOT"),
        ("symbolticker", "IBM"),
    ]


@pytest.mark.parametrize(
    "line",
    (
        (
            "8=FIX.4.4|35=8|32=2|120=USD|126=20260825-16:30:00|30029=Y|"
            "30030=G-1|30031=20260825-09:29:58.123456|30032=prod|30033=R-1|"
            "30034=RO-1|30035=POST_ONLY|30036=ORIGIN-1|30037=CONV-1|"
            "30038=FAKE-BBG|10=000|"
        ),
        (
            "bridge #BEGINSTRING=FIX.4.4|#LASTSHARES=2|"
            "#MSGTYPE=8|#EXPIRETIME=20260825-16:30:00|#MARKETMARKER=TRUE|"
            "#GLOBALORDERID=G-1|#CREATIONTIME=20260825-09:29:58.123456|#ENV=prod|"
            "#ROOTORDERID=R-1|#ROOTORIGINATORORDERID=RO-1|#ORDERFLAGS=POST_ONLY|"
            "#ORDERORIGINATORID=ORIGIN-1|#CONVERSATIONID=CONV-1|"
            "#BLOOMBERGCODE=FAKE-BBG|#SETTLCURRENCY=USD"
        ),
    ),
    ids=("fix-tags", "ul-names"),
)
def test_bridge_fields_are_typed_and_fill_generic_clocks(line: str, codec: FixCodec) -> None:
    creation = datetime.datetime(2026, 8, 25, 9, 29, 58, 123456, tzinfo=datetime.UTC)
    expiry = datetime.datetime(2026, 8, 25, 16, 30, tzinfo=datetime.UTC)

    parsed = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)
    row = parsed.to_pylist()[0]

    assert row["lastqty"] == 2.0, "`#LASTSHARES` is tag 32 under its pre-4.3 name"
    assert row["marketmarker"] is True
    assert (
        row["globalorderid"],
        row["env"],
        row["rootorderid"],
        row["rootoriginatororderid"],
        row["orderflags"],
        row["orderoriginatorid"],
        row["conversationid"],
        row["bloombergcode"],
    ) == (
        "G-1",
        "prod",
        "R-1",
        "RO-1",
        "POST_ONLY",
        "ORIGIN-1",
        "CONV-1",
        "FAKE-BBG",
    )
    currency = Currency.from_str("USD")
    assert row["settlcurrency"] == int(currency)
    assert row["error"] is None
    assert (row["creationtime"], row["expiretime"]) == (creation, expiry)
    assert (row["creaunix"], row["expunix"]) == (
        unix_of("20260825-09:29:58.123456"),
        unix_of("20260825-16:30:00"),
    )

    scalar = FixMsg.from_text(line, registry=codec.registry)
    assert scalar.get("LastQty").value == 2.0
    assert scalar.get("SettlCurrency").value == int(currency)
    assert scalar.get("BloombergCode").value == "FAKE-BBG"
    scalar.identify()
    assert (scalar.creaunix, scalar.expunix) == (row["creaunix"], row["expunix"])


def test_numeric_flat_fixmsg_arrow_matches_the_registry_reference(
    codec: FixCodec, registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rekep.text.fixmsg_arrow as fixmsg_arrow

    lines = (
        "8=FIX.4.4|35=NewOrderSingle|34=1|11=C1|55=AAPL|54=Buy|38=10|40=2|44=100.5|"
        "60=20260825-09:30:00.123456789|9998=audit|10=000|",
        "8=FIX.4.4|35=F|34=2|41=C1|11=C2|55=AAPL|54=1|38=10|60=20260825-09:30:01|10=000|",
        "8=FIX.4.4|35=G|34=3|41=C1|11=C3|55=AAPL|54=1|38=12|44=99.5|60=20260825-09:30:02|10=000|",
        "8=FIX.4.4|35=8|34=4|37=O1|11=C3|17=E1|55=AAPL|54=1|39=1|150=F|"
        "38=12|31=99.25|32=2|14=2|151=10|6=99.25|43=Y|"
        "60=20260825-09:30:03.5|10=000|",
        "8=FIX.4.4|35=AE|34=5|17=E2|55=AAPL|54=2|31=99.75|32=3|60=20260825-09:30:04|10=000|",
    )
    source = _raw_batch(*(Message(body=line) for line in lines))
    original = fixmsg_arrow.into_flat_fixmsg_batch
    activated: list[bool] = []

    def observed(*args, **kwargs):
        translated = original(*args, **kwargs)
        activated.append(translated is not None)
        return translated

    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", observed)
    translated = FixMsg.from_message_batch(source, codec)
    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", lambda *args, **kwargs: None)
    reference = FixMsg.from_message_batch(source, codec)

    assert activated == [True]
    assert translated.equals(reference, check_metadata=True)
    assert translated.column("msgtype").to_pylist()[0] == "D"
    assert EventType(translated.column("eventtype")[0].as_py()) is EventType.ORDER
    assert translated.column("side").to_pylist()[0] == "1"
    assert translated.column("lastmkt").null_count == 5
    assert translated.column("unmap")[0].as_py() == [
        {"tag": 9998, "key": "9998", "value": "audit", "comp": None}
    ]


def test_unknown_numeric_fields_follow_the_linked_registry(
    codec: FixCodec, tmp_path: Path, registry: FixRegistry
) -> None:
    line = "8=FIX.4.4|35=D|11=C1|9998=audit|10=000|"
    scalar = FixMsg.from_text(line, registry=registry)
    arrow = FixMsg.from_message_batch(
        _raw_batch(
            Message(body=line),
            Message(body=line.replace("audit", "changed")),
        ),
        codec,
    )

    assert arrow.column("unmap")[0].as_py() == [
        {"tag": 9998, "key": "9998", "value": "audit", "comp": None}
    ]
    assert [(entry.tag, entry.value) for entry in scalar.unmap or ()] == [(9998, "audit")]
    assert scalar.get(9998).raw == "audit"
    assert [reading.raw for reading in scalar.readings(9998)] == ["audit"]
    assert ("9998", "audit") in scalar.pairs
    assert "9998=audit" in scalar.into_text("|")
    assert FixMsg.from_dict(scalar.into_dict()).unmap == scalar.unmap
    assert FixMsg.from_dict(scalar.into_row()).unmap == scalar.unmap
    assert scalar.identify().vhash == arrow.column("vhash")[0].as_py()
    assert arrow.column("vhash")[0].as_py() != arrow.column("vhash")[1].as_py()

    custom = FixRegistry(cache_dir=tmp_path / "fix")
    venue_audit = fix_field(
        "VenueAudit", 9998, "String", values={"A": "Audit"}, namespace="standard"
    )
    venue_audit.fix.versions = ("4.4",)
    custom.add_fields((venue_audit,))
    linked = FixMsg.from_text(line, registry=custom)
    assert [(entry.tag, entry.key, entry.value) for entry in linked.entries or ()] == [
        (9998, "VenueAudit", "A")
    ]
    assert [(entry.tag, entry.value) for entry in linked.unmap or ()] == [
        (11, "C1"),
        (10, "000"),
    ]

    relinked = FixMsg(
        protocol="FIX4.4",
        entries=[],
        unmap=[{"tag": 0, "key": "VenueAudit", "value": "Audit"}],
    ).link_registry(custom)
    assert [(entry.tag, entry.key, entry.value) for entry in relinked.entries or ()] == [
        (0, "VenueAudit", "Audit")
    ]
    assert relinked.unmap is None


def test_lifted_numeric_keeps_only_a_raw_spelling_typing_cannot_reproduce(
    codec: FixCodec, registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rekep.text.fixmsg_arrow as fixmsg_arrow

    source = _raw_batch(
        Message(body="8=FIX.4.4|35=8|6=0010.5000|10=000|"),
        Message(body="8=FIX.4.4|35=8|6=10.5|10=000|"),
    )
    fast = FixMsg.from_message_batch(source, codec)
    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", lambda *args, **kwargs: None)
    reference = FixMsg.from_message_batch(source, codec)

    assert fast.equals(reference, check_metadata=True)
    assert fast.column("avgpx").to_pylist() == [10.5, 10.5]
    assert fast.column("hash")[0].as_py() != fast.column("hash")[1].as_py()
    assert [
        [(entry["tag"], entry["value"]) for entry in row if entry["tag"] == 6]
        for row in fast.column("entries").to_pylist()
    ] == [[(6, "0010.5000")], []]


def test_numeric_fixmsg_arrow_falls_back_as_one_mixed_batch(
    codec: FixCodec, registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rekep.text.fixmsg_arrow as fixmsg_arrow

    source = _raw_batch(
        Message(body="8=FIX.4.4|35=D|11=A|55=IBM|10=000|"),
        Message(body="35=D|11=B|55=MSFT|10=000|"),
    )
    original = fixmsg_arrow.into_flat_fixmsg_batch
    activated: list[bool] = []

    def observed(*args, **kwargs):
        translated = original(*args, **kwargs)
        activated.append(translated is not None)
        return translated

    monkeypatch.setattr(fixmsg_arrow, "into_flat_fixmsg_batch", observed)
    translated = FixMsg.from_message_batch(source, codec)

    assert activated == [False]
    assert translated.column("clordid").to_pylist() == ["A", None]
    assert _instrument_column(translated, "symbol").to_pylist() == ["IBM", ""]
    assert [entry["tag"] for entry in translated.column("entries")[1].as_py()] == [11, 55, 10]
    assert translated.column("unmap")[1].as_py() is None


def test_raw_direction_words_do_not_change_projected_mic(
    codec: FixCodec, registry: FixRegistry
) -> None:
    raw = _raw_batch(Message(body="received 8=FIX.4.4|35=D|49=XPAR|56=XNAS|11=A|10=000|"))

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("lastmkt").to_pylist() == [int(MIC.from_str("XNAS"))]


def test_fixmsg_conversion_preserves_static_extra_columns(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(body="8=FIX.4.4|35=D|11=A|10=000|"))
    static = pyarrow.field(
        "capture_id",
        pyarrow.string(),
        nullable=False,
        metadata={b"source": b"static"},
    )
    raw = raw.append_column(static, pyarrow.array(["day-1"]))

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.schema.names == [*FixMsg.into_field().names, "capture_id"]
    assert parsed.schema.field("capture_id") == static
    assert parsed.column("capture_id").to_pylist() == ["day-1"]


def test_the_parsed_header_reaches_fixmsg_columns_not_entries(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """The parsing boundary promotes the standard header exactly once."""
    line = "8=FIX.4.4|9=61|35=D|34=7|49=ME|52=20260814-09:30:00.000|56=YOU|11=C1|55=IBM|10=000|"
    raw = _raw_batch(Message(body=line))

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("beginstring").to_pylist() == ["FIX.4.4"]
    assert parsed.column("bodylength").to_pylist() == [61]
    assert parsed.column("msgseqnum").to_pylist() == [7]
    assert parsed.column("sendercompid").to_pylist() == ["ME"]
    assert parsed.column("targetcompid").to_pylist() == ["YOU"]
    assert parsed.column("sendingtime").to_pylist() == [
        datetime.datetime(2026, 8, 14, 9, 30, tzinfo=datetime.UTC)
    ]
    assert _protocols(parsed) == ["FIX4.4"]
    assert parsed.column("checksum").to_pylist() == ["000"]
    assert parsed.column("entries").to_pylist() == [[]]

    # One row at a time reaches the same values through the same batch parser.
    scalar = FixMsg.from_text(line)
    assert (scalar.beginstring, scalar.bodylength, scalar.msgtype) == ("FIX.4.4", 61, "D")
    assert (scalar.msgseqnum, scalar.sendercompid, scalar.targetcompid) == (7, "ME", "YOU")
    assert scalar.sendingtime == datetime.datetime(2026, 8, 14, 9, 30, tzinfo=datetime.UTC)
    assert scalar.entries == []


def test_a_header_field_spelled_two_ways_stays_where_a_reader_can_see_it(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """Two readings of one fact is not one statement of it: a row spelling a
    header field twice with two values lifts neither reading, and this stage
    does not go looking in `entries` for what the column left null. Spelled
    twice with one value it is one statement, lifted once, and every occurrence
    leaves the list."""
    raw = _raw_batch(
        Message(
            body=(
                "8=FIX.4.4|35=D|52=20260814-09:30:00.000|52=20260814-09:31:00.000|"
                "34=7|34=7|11=C1|10=000|"
            )
        )
    )

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("sendingtime").to_pylist() == [None]
    assert parsed.column("msgseqnum").to_pylist() == [7]
    assert [(entry["tag"], entry["value"]) for entry in parsed.column("entries")[0].as_py()] == [
        (52, "20260814-09:30:00.000"),
        (52, "20260814-09:31:00.000"),
    ]


def test_fixmsg_stops_parsing_at_the_checksum(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(body="8=FIX.4.4|35=D|49=null|10=000|55=AFTER-CHECKSUM|"),
        Message(body="8=FIX.4.4|35=D|10=000|52=20260814-09:30:00.000|"),
    )
    parsed = FixMsg.from_message_batch(raw, codec)

    assert _instrument_column(parsed, "symbol").to_pylist() == ["", ""]
    assert parsed.column("sendercompid").to_pylist() == [None, None]
    assert parsed.column("sendingtime").to_pylist() == [None, None]
    assert parsed.column("error").to_pylist() == [None, None]
    assert parsed.column("entries").to_pylist() == [[], []]


def test_fixmsg_consumes_a_hash_delimited_wire_message(
    codec: FixCodec, registry: FixRegistry
) -> None:
    raw = _raw_batch(Message(body="8=FIX.4.4#35=D#55=TTF#10=000"))

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("msgtype").to_pylist() == ["D"]
    assert _instrument_column(parsed, "symbol").to_pylist() == ["TTF"]


def test_a_hybrid_frame_reads_named_and_numeric_fields_together(codec: FixCodec) -> None:
    """A numeric envelope carrying named bridge fields resolves both spellings.

    `#MSGTYPE` names the message the frame carries and `35=UL` the wire it came
    in on, so both are read. `55` and `#SYMBOL` disagree about the symbol, so
    both readings stay in `entries` and neither becomes the column.
    """
    raw = _raw_batch(
        Message(
            body=(
                "8=FIX.4.4|35=UL|9998=before|55=wire|#MSGTYPE=D|"
                "#VENDOR.OWN=x|#SYMBOL=named|9999=after|10=000|"
            )
        )
    )

    parsed = FixMsg.from_message_batch(raw, codec)
    residual = parsed.column("unmap")[0].as_py()

    assert parsed.column("msgtype").to_pylist() == ["D"]
    assert _instrument_column(parsed, "symbol").to_pylist() == [""]
    assert [(entry["tag"], entry["value"]) for entry in parsed.column("entries")[0].as_py()] == [
        (55, "wire"),
        (55, "named"),
    ]
    assert [entry["value"] for entry in residual if entry["tag"] in {9998, 9999}] == [
        "before",
        "after",
    ]
    assert all(entry["tag"] != 55 for entry in residual)


def test_wire_conversion_drops_message_markers_before_fix_rules(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    lines = [
        "8=FIX.4.2|SIDE=1|55=TTF|10=000|",
        "8=FIX.4.2|#54=2|55=IBM|10=000|",
    ]
    raw = _raw_batch(*(Message(body=line) for line in lines))
    _, expected_columns = codec.into_fixmsg_columns(
        codec.into_pairs(pyarrow.array(lines), "FIX"), "4.2"
    )

    parsed = FixMsg.from_message_batch(raw, codec)

    assert expected_columns["side"].to_pylist() == [None, None], (
        "read as tags alone, the rendered spelling is noise"
    )
    assert parsed.column("side").to_pylist() == ["1", "2"], (
        "read as the mixed message it is, both spellings reach one column"
    )
    assert (
        _instrument_column(parsed, "symbol").to_pylist() == expected_columns["symbol"].to_pylist()
    )
    assert parsed.column("entries").to_pylist() == [[], []]


def test_an_extra_column_cannot_shadow_a_fix_only_field(
    codec: FixCodec, registry: FixRegistry
) -> None:
    raw = _raw_batch(Message(body="plain text")).append_column(
        "OrigClOrdID", pyarrow.array(["caller-value"])
    )

    with pytest.raises(ValueError, match="collide.*OrigClOrdID"):
        FixMsg.from_message_batch(raw, codec)


def test_fixmsg_conversion_keeps_the_empty_contract(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    parsed = FixMsg.from_message_batch(
        _raw_batch(),
        codec,
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
    assert {"code", "msgseqnum"} <= set(FixMsg.into_field().names)
    assert "symbol" in FixMsg.into_field().field("instrument").names
    assert FixMsg.into_field().field("MsgSeqNum").fix["tag"] == "34"


def test_every_promoted_name_is_the_registrys_exact_spelling_folded() -> None:
    """One name, folded to store and spelled to read: the fold is the column and
    the dictionary's own spelling is what the column says it is called."""
    names = [field.name for field in DECLARATIONS.values()]
    assert len(names) == 144
    assert all(field.name == column_name(field.fix["name"]) for field in DECLARATIONS.values())
    assert all(field.fix.canonical == field.fix["name"] for field in DECLARATIONS.values())
    assert {tag: COLUMNS[tag] for tag in (6, 35, 41, 461)} == {
        6: "avgpx",
        35: "msgtype",
        41: "origclordid",
        461: "cficode",
    }
    assert {tag: DECLARATIONS[tag].fix["name"] for tag in (6, 35, 41, 461)} == {
        6: "AvgPx",
        35: "MsgType",
        41: "OrigClOrdID",
        461: "CFICode",
    }
    assert {tag: DECLARATIONS[tag].name for tag in (453, 802)} == {
        453: "nopartyids",
        802: "nopartysubids",
    }
    assert Party.into_field().names == [
        "partyid",
        "partyidsource",
        "partyrole",
        "partyrolequalifier",
    ]


def test_every_flat_column_is_the_type_the_dictionary_gives_its_tag(
    registry: FixRegistry,
) -> None:
    """The one check that keeps the names (`rekep.fix.columns`) and the types
    (here) from drifting apart. A column stands for a tag, and what a tag holds
    is the dictionary's to say -- not this package's, and not a reading of the
    fixture that happens to parse."""
    for tag, column in FLAT:
        if column in _INSTRUMENT_COLUMNS:
            continue  # normalized under the component contract, checked below
        declared = registry.field(tag).dtype
        if pyarrow.types.is_timestamp(declared):
            continue  # a clock's width and zone are the test below
        if column in CODED_COLUMNS:
            continue  # a field read as a code states its own width, below
        assert FixMsg.into_field().field(column).dtype == declared, column


#: Flat columns this package reads as a packed code rather than as the text the
#: standard types them. One so far: `SecurityIDSource <22>` is `String` to the
#: dictionary and thirty-three single-character codes in practice.
CODED_COLUMNS = {"securityidsource": SecurityIDSource}


def test_an_isin_fills_the_identifier_pair_from_either_side(codec: FixCodec) -> None:
    """`SecurityIDSource <22>` calls ISIN `4`, so an ISIN is `SecurityID <48>`
    under that scheme whichever field carried it -- a bridge's own `ISINCODE`
    or the tag pair -- and each fills the other."""
    wire = "8=FIX.4.4|35=D|48=US0378331005|22=4|55=AAPL|10=000|"

    batch = FixMsg.from_message_batch(_raw_batch(Message(body=wire)), codec)

    assert _instrument_column(batch, "securityid").to_pylist() == ["US0378331005"]
    assert _instrument_column(batch, "isincode").to_pylist() == ["US0378331005"], (
        "the pair fills the field"
    )
    assert [
        SecurityIDSource.from_int(code)
        for code in _instrument_column(batch, "securityidsource").to_pylist()
    ] == [SecurityIDSource.ISIN]

    # The other direction, where the bridge rendered the ISIN as its own field
    # and the row carried no tag 48 at all.
    bridged = FixMsg(instrument=Instrument(isincode="XX0000084733", symbol="TTF"))
    assert bridged.instrument.securityid == "XX0000084733"
    assert bridged.instrument.securityidsource is SecurityIDSource.ISIN


def test_a_scheme_the_message_never_stated_stays_absent(codec: FixCodec) -> None:
    """Absent is not `UNKNOWN`: one says the message was silent, the other that
    it named a scheme nothing recognises."""
    batch = FixMsg.from_message_batch(
        _raw_batch(Message(body="8=FIX.4.4|35=D|55=TTF|10=000|")), codec
    )

    assert _instrument_column(batch, "securityidsource").to_pylist() == [None]
    assert _instrument_column(batch, "securityid").to_pylist() == [None]


def test_a_coded_flat_column_states_its_own_width() -> None:
    """The exception the check above steps over, spelled out rather than
    assumed: a coded column is the width its vocabulary declares."""
    for column, declared in CODED_COLUMNS.items():
        member = FixMsg.into_field().field("instrument").field(column)
        assert member.dtype == declared.into_arrow_type().index_type, column
        assert member.nullable, f"{column}: a scheme nobody stated is absent, not UNKNOWN"
        assert member.metadata["enum:name"] == declared.__name__, column


def test_a_lifted_stamp_is_a_microsecond_utc_timestamp(
    registry: FixRegistry,
) -> None:
    """Promoted FIX clocks use Iceberg's width and their documented UTC zone."""
    dictated = {tag for tag, _ in FLAT if pyarrow.types.is_timestamp(registry.field(tag).dtype)}
    assert set(STAMPS) <= dictated
    for tag in STAMPS:
        assert FixMsg.into_field().field(COLUMNS[tag]).dtype == pyarrow.timestamp("us", tz="UTC"), (
            tag
        )
    for tag in dictated - set(STAMPS):
        column = COLUMNS[tag]
        if column in _INSTRUMENT_COLUMNS:
            assert Instrument.into_field().field(column).dtype == pyarrow.timestamp("us")
        else:
            assert FixMsg.into_field().field(column).dtype == pyarrow.timestamp("us"), (
                f"{tag} is a date the dictionary types as an instant, and carries no zone"
            )


def test_timestamp_projection_is_naive_until_the_fix_documentation_says_utc() -> None:
    date = Field(
        name="LocalDate",
        dtype=pyarrow.date32(),
        metadata={"fix:type": "LocalMktDate"},
    )
    local = Field(
        name="LocalStamp",
        dtype=pyarrow.timestamp("ns"),
        metadata={"fix:type": "Time"},
    )
    utc = Field(
        name="UtcStamp",
        dtype=pyarrow.timestamp("ns"),
        metadata={"fix:type": "UTCTimestamp"},
    )
    assert physical_type(date) == pyarrow.timestamp("us")
    assert physical_type(local) == pyarrow.timestamp("us")
    assert physical_type(utc) == pyarrow.timestamp("us", tz="UTC")


def test_every_flat_column_admits_absence() -> None:
    """Whether a FIX field is required belongs to the message that carries it."""
    blank = FixMsg()
    for column in FLAT_COLUMNS:
        if column in _INSTRUMENT_COLUMNS:
            continue
        assert FixMsg.into_field().field(column).nullable, column
        assert getattr(blank, column) is None, column
    component = blank.instrument
    for member in Instrument.into_field().fields:
        value = getattr(component, member.name)
        if member.nullable:
            assert value is None, member.name


def test_every_flat_column_keeps_the_registry_name_metadata_and_description(
    registry: FixRegistry,
) -> None:
    for tag, column in FLAT:
        if column in _INSTRUMENT_COLUMNS:
            continue
        expected = registry.scalar(tag)
        actual = FixMsg.into_field().field(column)
        assert actual.name == column_name(expected.name), column
        assert actual.fix["name"] == expected.name, column
        assert actual.fix.canonical == expected.name, column
        # Minus the `enum:` block: the registry knows `SecurityIDSource <22>`
        # as text, and reading its thirty-three codes as one code is this
        # package's statement about the field, not the dictionary's. And
        # narrowed to what a column says about the field it reads: the rest of
        # the record stays where it is kept, which is the registry.
        stored = {k: v for k, v in actual.metadata.items() if not k.startswith("enum:")}
        assert stored == column_metadata({**expected.metadata, "fix:name": expected.name}), column
        assert actual.description == expected.description, column


def test_a_nested_message_in_xmldata_survives_the_standard_header_lift(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """`XmlData <213>` stays in parser entries because bridges put a `key=value` message in
    it, and `into_payload_pairs` expands that in the place the tag sat. Lifting
    it would have taken the tag out of the list that expansion reads, leaving a
    nested order unread and the column holding the whole payload as bytes.

    The codec's own tests read the expansion off pairs, which is upstream of
    the lift and could not see this -- so it is pinned here, on the pipeline.
    """
    body = "ClOrdID=ORD-TEST-01|Side=1|Account=ACCT-TEST-01"
    line = f"8=FIX.4.4|35=8|212={len(body)}|213={body}|10=000"
    parsed = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert parsed.column("clordid").to_pylist() == ["ORD-TEST-01"]
    assert parsed.column("side").to_pylist() == ["1"]
    assert parsed.column("account").to_pylist() == ["ACCT-TEST-01"]
    assert parsed.column("xmldata").to_pylist() == [None], "expanded, so nothing is left over"


def test_an_xmldata_document_still_lands_in_its_own_column(
    codec: FixCodec, registry: FixRegistry
) -> None:
    """A payload that reads as XML rather than as pairs has nothing to expand,
    so the FIX stage fills the column from `entries` exactly as it always did."""
    document = '<order id="1"/>'
    line = f"8=FIX.4.4|35=8|212={len(document)}|213={document}|10=000"
    parsed = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert parsed.column("xmldata").to_pylist() == [document]
    assert parsed.column("clordid").to_pylist() == [None]


def test_the_parser_leaves_structural_header_fields_whole() -> None:
    """Checksum and XML framing stay in the parser's residual token stream."""
    from rekep.fix.message_parser import SESSION_NAMES

    lifted = {name for name, _ in SESSION_NAMES}
    assert {"CheckSum", "XmlData", "XmlDataLen"}.isdisjoint(lifted)
    assert {"SecureDataLen", "SecureData", "SignatureLength", "Signature"} <= lifted, (
        "the other two length-prefixed pairs lift whole, so they stay adjacent"
    )
    assert not {"xmldata", "xmldatalen", "checksum"} & set(Message.into_field().names)


def test_rendered_isincode_keeps_its_source_identity() -> None:
    field = FixMsg.into_field().field("instrument").field("isincode")
    assert field.dtype == pyarrow.string()
    assert field.fix.canonical == "ISINCODE"
    assert field.metadata["iso"] == "6166"
    assert field.fix.type == "String"


def test_fixml_decodes_a_named_value_inside_its_wire_envelope(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    payload = "EXECTYPE=fill|CURRENCY=NOK|COUNTERAMOUNT=1200"
    line = f"8=FIX.4.2|35=UL|212={len(payload)}|213={payload}|10=000"

    parsed = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert _protocols(parsed) == ["FIXML4.2"]
    assert parsed.column("msgtype").to_pylist() == ["UL"]
    assert parsed.column("exectype").to_pylist() == ["2"]


def test_ul_enum_names_become_their_real_fix_wire_values(codec: FixCodec) -> None:
    line = (
        "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=ExecutionReport|#ORDERID=O-1|"
        "#CLORDID=C-1|#EXECID=E-1|#SYMBOL=AAPL|#SIDE=buy|#ORDSTATUS=canceled|"
        "#EXECTYPE=canceled|#ORDTYPE=limit|#TIMEINFORCE=gtd|#SECURITYTYPE=future|"
        "#PUTORCALL=put|#NOPARTYIDS=1|#NOPARTYIDS[0].PARTYID=P-1|"
        "#NOPARTYIDS[0].PARTYIDSOURCE=proprietary custom code|"
        "#NOPARTYIDS[0].PARTYROLE=client id"
    )

    new_order = line.replace("ExecutionReport", "NewOrderSingle").replace("O-1", "O-2")
    parsed = FixMsg.from_message_batch(
        _raw_batch(Message(body=line), Message(body=new_order)), codec
    )

    assert _protocols(parsed) == ["UL4.4", "UL4.4"]
    assert parsed.column("msgtype").to_pylist() == ["8", "D"]
    assert parsed.column("eventtype").to_pylist() == [
        int(EventType.EXECUTION),
        int(EventType.ORDER),
    ]
    assert {
        name: parsed.column(name).to_pylist()
        for name in ("side", "ordstatus", "exectype", "ordtype", "timeinforce")
    } == {
        "side": ["1", "1"],
        "ordstatus": ["4", "4"],
        "exectype": ["4", "4"],
        "ordtype": ["2", "2"],
        "timeinforce": ["6", "6"],
    }
    assert _instrument_column(parsed, "securitytype").to_pylist() == ["FUT", "FUT"]
    assert all(
        OptionKind(value.as_py()) is OptionKind.PUT
        for value in _instrument_column(parsed, "putorcall")
    )
    assert parsed.column("parties").to_pylist() == [
        [
            {
                "partyid": "P-1",
                "partyidsource": "D",
                "partyrole": 3,
                "partyrolequalifier": None,
            }
        ],
        [
            {
                "partyid": "P-1",
                "partyidsource": "D",
                "partyrole": 3,
                "partyrolequalifier": None,
            }
        ],
    ]
    assert parsed.column("error").to_pylist() == [None, None]


def test_ul_glued_explicit_groups_are_partial_lossless_and_row_isolated(
    codec: FixCodec,
) -> None:
    group = (
        "#NOPARTYIDS[3]=PARTYROLE=client id|"
        "#NOPARTYIDS[3].VENUEFLAG=kept|"
        "#NOPARTYIDS[1]=PARTYID=P-1PARTYIDSOURCE=proprietary custom code"
    )
    clean = Message(body=f"toBridge #MSGTYPE=D|#NOPARTYIDS=2|{group}")
    mismatched = Message(body=f"toBridge #MSGTYPE=D|#NOPARTYIDS=3|{group}")

    parsed = FixMsg.from_message_batch(_raw_batch(clean, mismatched), codec)

    assert _protocols(parsed) == ["UL5SP2", "UL5SP2"]
    expected = [
        {
            "partyid": None,
            "partyidsource": None,
            "partyrole": 3,
            "partyrolequalifier": None,
        },
        {
            "partyid": "P-1",
            "partyidsource": "D",
            "partyrole": None,
            "partyrolequalifier": None,
        },
    ]
    assert parsed.column("parties").to_pylist() == [expected, expected]
    assert parsed.column("error").to_pylist() == [
        None,
        "NoPartyIDs count mismatch: declared 3, found 2 indexed groups",
    ]
    assert parsed.column("unmap").to_pylist() == [None, None]
    assert [
        [(entry["comp"], entry["key"], entry["value"]) for entry in row]
        for row in parsed.column("entries").to_pylist()
    ] == [
        [("NOPARTYIDS[3]", "VENUEFLAG", "kept")],
        [
            (None, "NoPartyIDs", "3"),
            ("NOPARTYIDS[3]", "VENUEFLAG", "kept"),
        ],
    ]
    assert "protocolversion" not in parsed.schema.names


def test_fixmsg_classifies_each_raw_body(codec: FixCodec, registry: FixRegistry) -> None:
    """FIX parsing classifies text only when it builds a `FixMsg`."""
    echo = Message(
        body="RouteMessage : BEGINSTRING=FIX.4.4|ACCOUNT=807768.001"
        "|MSGTYPE=D|CLORDID=PL024819|SIDE=1"
    )
    heartbeat = Message(body="heartbeat emitted seq=7")
    wrapped = Message(body="sending >> 8=FIX.4.2|35=UL|#SYMBOL=TTF|#SIDE=1|10=044|")

    batch = FixMsg.from_message_batch(_raw_batch(echo, heartbeat, wrapped), codec)
    assert _protocols(batch) == ["UL4.4", "MISC", "FIXML4.2"]
    assert batch.column("clordid").to_pylist()[0] == "PL024819", "promoted, not dropped"
    assert batch.column("account").to_pylist()[0] == "807768.001"
    assert batch.column("msgtype").to_pylist()[0] == "D"
    assert batch.column("entries").to_pylist()[1] is None, "operational rows stay unread"

    # Registry latest supplies a reproducible dictionary version where the
    # bridge states none, and the resolved token persists that choice.
    bare = Message(
        body="After Enrichment -> ACCOUNT=59.1|MSGTYPE=NewOrderSingle|CLORDID=PL9|SIDE=2"
    )
    lone = FixMsg.from_message_batch(_raw_batch(bare), codec)
    assert _protocols(lone) == ["UL5SP2"]
    assert "protocolversion" not in lone.schema.names
    assert lone.column("msgtype").to_pylist() == ["D"]
    assert lone.column("eventtype").to_pylist() == [int(EventType.ORDER)]
    assert lone.column("entries").to_pylist() == [[]]
    assert lone.column("unmap").to_pylist() == [None]
    assert lone.column("account").to_pylist() == ["59.1"]
    assert lone.column("clordid").to_pylist() == ["PL9"]
    assert lone.column("side").to_pylist() == ["2"]
    assert lone.column("altids").to_pylist() == [[("clordid", "PL9"), ("code", "PL9")]]


def test_a_custom_empty_rule_set_classifies_raw_text_as_other(
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(body="#MSGTYPE=D|#CLORDID=SYNTH|"))

    parsed = FixMsg.from_message_batch(
        raw,
        FixCodec(registry=registry, rules=Rules(rules=[])),
    )

    assert _protocols(parsed) == ["OTHER"]
    assert parsed.column("entries").to_pylist() == [None]


def test_direction_reads_the_verb_before_the_payload(
    codec: FixCodec, registry: FixRegistry
) -> None:
    """`Receiving`/`Sending` before the payload's first token is the direction;
    the same words inside the payload -- a reject's prose, a bridge value --
    answer nothing, and neither does a line saying both or neither."""
    lines = [
        "Receiving : 8=FIX.4.4|35=D|11=C1|55=AAPL|54=1|38=5|44=10|58=order sent late|10=000",
        "Sending : 8=FIX.4.4|35=8|37=O1|11=C1|17=E1|54=1|39=0|150=0|10=000",
        "Message received: 8=FIX.4.4|35=0|10=000",
        "toBridge Sending #MSGTYPE=D|#CLORDID=C2|#SYMBOL=MSFT|#SIDE=1",
        "RouteMessage : 8=FIX.4.4|35=D|11=C3|55=IBM|54=2|38=1|10=000",
        "Receiving Sending 8=FIX.4.4|35=D|11=C4|10=000",
        # A rendered bridge line anchors on the `FIXML` rule's own vocabulary,
        # a verb only inside its payload answers nothing.
        "toBridge #MSGTYPE=8|#CLORDID=C5|#TEXT=order sent to market",
        "just some heartbeat prose",
    ]
    batch = FixMsg.from_message_batch(_raw_batch(*(Message(body=line) for line in lines)), codec)

    assert batch.column("direction").to_pylist() == [
        int(Direction.RECV),
        int(Direction.SENT),
        int(Direction.RECV),
        int(Direction.SENT),
        int(Direction.UNKNOWN),
        int(Direction.UNKNOWN),
        int(Direction.UNKNOWN),
        int(Direction.UNKNOWN),
    ]

    # A named document has an anchor of its own, so a verb in front of one
    # answers exactly as it does in front of a frame.
    named = Message(body="Sending : ACCOUNT=A1|MSGTYPE=D|PRICE=9.5")
    document = FixMsg.from_message_batch(_raw_batch(named), codec)
    assert _protocols(document) == ["UL5SP2"]
    assert document.column("direction").to_pylist() == [int(Direction.SENT)]


# -- the instrument a message carries, in typed columns ----------------------


#: One instrument written every way one message can write it: the flat
#: contract facts, the alternate identifiers, and two legs.
INSTRUMENT_WIRE = (
    "8=FIX.4.4|35=d|55=AAPL|48=US0378331005|22=4|167=OPT|461=OCASPS|207=XNAS|15=USD"
    "|541=20261218|200=202612|202=150.5|201=1|231=100|561=100|107=Apple Dec26 150 Call"
    "|454=2|455=BBG000B9XRY4|456=A|455=037833100|456=1"
    "|555=2|600=AAPL|602=US0378331005|603=4|624=1|623=1|600=MSFT|624=2|623=2|10=000"
)

#: The same instrument as a bridge renders it: no tags at all, one named key
#: per field. Both spellings resolve through the same registry.
INSTRUMENT_NAMED = (
    "#BEGINSTRING=FIX.4.4|#MSGTYPE=d|#SYMBOL=AAPL|#SECURITYID=US0378331005|#SECURITYIDSOURCE=4|#SECURITYTYPE=OPT"
    "|#CFICODE=OCASPS|#SECURITYEXCHANGE=XNAS|#CURRENCY=USD|#MATURITYDATE=20261218"
    "|#MATURITYMONTHYEAR=202612|#STRIKEPRICE=150.5|#PUTORCALL=1|#CONTRACTMULTIPLIER=100"
    "|#SECURITYDESC=Apple Dec26 150 Call"
)

#: What a typed row must carry for either spelling.
INSTRUMENT_COLUMNS = {
    "symbolticker": "XNAS:AAPL",
    "symbol": "AAPL",
    "kind": int(AssetKind.OPTION),
    "securityid": "US0378331005",
    "securityidsource": int(SecurityIDSource.ISIN),
    "isincode": "US0378331005",
    "securitytype": "OPT",
    "cficode": "OCASPS",
    "securityexchange": "XNAS",
    "currency": int(Currency.USD),
    "maturitydate": datetime.datetime(2026, 12, 18),
    "strikeprice": 150.5,
    "putorcall": int(OptionKind.CALL),
    "contractmultiplier": 100.0,
    "securitydesc": "Apple Dec26 150 Call",
}


@pytest.mark.parametrize("line", (INSTRUMENT_WIRE, INSTRUMENT_NAMED), ids=("tags", "names"))
def test_an_instrument_reaches_typed_columns_however_it_is_spelled(
    line: str, codec: FixCodec
) -> None:
    """Numbered and named spellings resolve to one identity, so one column."""
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)
    for name, expected in INSTRUMENT_COLUMNS.items():
        assert _instrument_column(batch, name)[0].as_py() == expected, name
    assert batch.column("lastmkt")[0].as_py() == int(MIC.from_str("XNAS"))


def test_ul_detailed_cficode_replaces_the_generic_reading(codec: FixCodec) -> None:
    line = "#BEGINSTRING=FIX.4.4|#MSGTYPE=d|#SYMBOL=AAPL|#CFICODE=EXXXXX|#DETAILEDCFICODE=OCASPS"

    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert _protocols(batch) == ["UL4.4"]
    assert _instrument_column(batch, "cficode").to_pylist() == ["OCASPS"]
    assert all(
        column_name(entry["key"]) != "detailedcficode"
        for entry in (batch.column("unmap")[0].as_py() or [])
    )


def test_alternate_identifiers_and_legs_are_structured_entries(codec: FixCodec) -> None:
    """Repeated FIX structures stay repeated: a list of entries, in wire order."""
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=INSTRUMENT_WIRE)), codec)

    assert [
        (entry["securityaltid"], entry["securityaltidsource"])
        for entry in batch.column("securityaltid")[0].as_py()
    ] == [("BBG000B9XRY4", "A"), ("037833100", "1")]
    legs = _instrument_column(batch, "legs")[0].as_py()
    assert [(leg["symbol"], leg["securityidsource"], leg["ratio"]) for leg in legs] == [
        ("AAPL", "4", 1.0),
        ("MSFT", None, 2.0),
    ], "a member the second leg omits is null there and not the first leg's"


def test_a_market_entrys_instrument_stays_in_that_entry(codec: FixCodec) -> None:
    """An alt-id group opened inside `NoMDEntries` belongs to the entry it is in.

    Promoting it to message scope would give the message an identifier one of
    its levels carries, so the group stays in `entries` for the market reader
    that knows which entry it opened in.
    """
    line = (
        "8=FIX.4.4|35=W|55=AAPL|268=1|269=0|270=41.25|271=100|454=1|455=BBG000B9XRY4|456=A|10=000"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert batch.column("securityaltid")[0].as_py() is None
    assert 455 in {entry["tag"] for entry in batch.column("entries")[0].as_py()}
    assert _instrument_column(batch, "symbol")[0].as_py() == "AAPL", (
        "the message's own field still lifts"
    )


def test_every_entry_has_exactly_one_destination(codec: FixCodec) -> None:
    """Promoted, residual or unmapped -- one of the three, and never two.

    `unmap` is null and not `[]` where everything resolved, because a row that
    resolved has no unmapped fields rather than an empty list of them.
    """
    line = INSTRUMENT_WIRE.replace("|10=000", "|9998=audit|VENUEOWNFIELD=kept|10=000")
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    promoted = {
        name
        for name in INSTRUMENT_COLUMNS
        if _instrument_column(batch, name)[0].as_py() is not None
    }
    residual = {entry["key"] for entry in batch.column("entries")[0].as_py()}
    unmapped = {entry["key"] for entry in batch.column("unmap")[0].as_py()}

    assert unmapped == {"9998", "VENUEOWNFIELD"}, "an unknown tag and an unknown name"
    assert not residual & unmapped, "no entry is in both lists"
    assert promoted == set(INSTRUMENT_COLUMNS)
    # A component's members leave `entries` with it; the count stays only where
    # the group was not structured.
    assert not {"455", "456", "600", "602"} & residual

    resolved = FixMsg.from_message_batch(_raw_batch(Message(body=INSTRUMENT_WIRE)), codec)
    assert resolved.column("unmap")[0].as_py() is None


def test_the_shipped_capture_partitions_every_entry_it_carries(codec: FixCodec) -> None:
    """The same three-way split over the capture the pipeline documentation runs.

    A synthetic line proves the rule; a real capture proves it holds for prose,
    a folded stack trace, a numeric frame, a rendered bridge document and the
    unknown vendor fields a feed actually writes.
    """
    lines = (
        (Path(__file__).parent.parent / "data" / "app_messages_sample.txt")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    batch = FixMsg.from_message_batch(_raw_batch(*(Message(body=line) for line in lines)), codec)

    promoted = {name for name in COMMON if name not in ("entries", "unmap")}
    for row in batch.to_pylist():
        entries = row["entries"] or []
        unmap = row["unmap"]
        assert unmap != [], "a row that resolved has no unmapped fields, not an empty list of them"
        keys = [entry["key"] for entry in entries]
        assert len(keys) == len(set(keys)) or all(
            entry["comp"] for entry in entries if keys.count(entry["key"]) > 1
        ), "a key twice in `entries` is a repeating group, never one field copied"
        assert not {entry["key"] for entry in entries} & {
            entry["key"] for entry in (unmap or ())
        }, "no entry is both resolved and unresolved"
        for name in promoted:
            value = row.get(name)
            if value is None or isinstance(value, list):
                continue
            assert not any(
                entry["value"] == value and column_name(entry["key"]) == name
                for entry in (*entries, *(unmap or ()))
            ), f"{name} was promoted and left behind"


def test_a_conflicting_numeric_and_named_copy_stays_visible(codec: FixCodec) -> None:
    """One field written twice is a repetition; written twice differently it is
    a conflict, and which the sender meant is not this stage's to decide."""
    conflict = FixMsg.from_message_batch(
        _raw_batch(Message(body="8=FIX.4.4|35=D|55=AAPL|SYMBOL=MSFT|10=000")), codec
    )

    assert [(entry["tag"], entry["value"]) for entry in conflict.column("entries")[0].as_py()] == [
        (55, "AAPL"),
        (55, "MSFT"),
    ]
    assert _instrument_column(conflict, "symbol")[0].as_py() == "", "and neither is promoted"

    agreed = FixMsg.from_message_batch(
        _raw_batch(Message(body="8=FIX.4.4|35=D|55=AAPL|SYMBOL=AAPL|10=000")), codec
    )

    assert _instrument_column(agreed, "symbol")[0].as_py() == "AAPL"
    assert agreed.column("entries")[0].as_py() == [], "one field, written twice, is one field"


def test_lifecycle_identifiers_are_their_own_columns(codec: FixCodec) -> None:
    """`OrigClOrdID` and the bridge's parent keys are FIX fields, and stay so.

    None of them is `prevhash` or `linkhashes`: those relate exact stored
    versions, where these are what the sender wrote.
    """
    line = (
        "8=FIX.4.4|35=G|11=NEW-1|41=OLD-1|37=ORD-9|55=AAPL"
        "|PARENTCLORDID=ROOT-1|PARENTORDERID=ROOT-ORD|10=000"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert batch.column("clordid")[0].as_py() == "NEW-1"
    assert batch.column("origclordid")[0].as_py() == "OLD-1"
    assert batch.column("orderid")[0].as_py() == "ORD-9"
    assert batch.column("parentclordid")[0].as_py() == "ROOT-1"
    assert batch.column("parentorderid")[0].as_py() == "ROOT-ORD"
    assert batch.column("prevhash")[0].as_py() is None
    assert batch.column("linkhashes")[0].as_py() == []


def test_a_missing_group_count_and_a_malformed_continuation_stay_visible(
    codec: FixCodec,
) -> None:
    """Nothing is invented and nothing is dropped: what cannot be structured
    stays where a reader can see it."""
    countless = "8=FIX.4.4|35=d|55=AAPL|455=BBG000B9XRY4|456=A|10=000"
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=countless)), codec)
    keys = {entry["key"] for entry in batch.column("entries")[0].as_py() or ()}
    keys |= {entry["key"] for entry in batch.column("unmap")[0].as_py() or ()}
    assert {"SecurityAltID", "SecurityAltIDSource"} <= keys or batch.column("securityaltid")[
        0
    ].as_py()

    broken = "8=FIX.4.4|35=d|55=AAPL|454=|455=|10=000"
    survived = FixMsg.from_message_batch(_raw_batch(Message(body=broken)), codec)
    assert _instrument_column(survived, "symbol")[0].as_py() == "AAPL"


def test_invalid_group_counts_are_diagnostic_and_incomplete_groups_stay_raw(
    codec: FixCodec,
) -> None:
    lines = [
        "8=FIX.4.4|35=D|11=A|453=x|448=P|447=D|452=1|10=000|",
        "8=FIX.4.4|35=D|11=B|453=2|448=P|447=D|452=1|10=000|",
        "8=FIX.4.4|35=D|11=C|453=1|448=P|447=D|452=1|10=000|",
        "8=FIX.4.4|35=D|11=D|453=1|448=P|447=D|452=abc|10=000|",
        "8=FIX.4.4|35=D|11=E|768=1|769=bad-time|770=1|10=000|",
        "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=D|#PRICE=bad-price|",
    ]

    parsed = FixMsg.from_message_batch(_raw_batch(*(Message(body=line) for line in lines)), codec)

    assert parsed.column("error").to_pylist() == [
        "NoPartyIDs <453>: invalid x",
        None,
        None,
        "PartyRole <452>: invalid abc",
        "TrdRegTimestamp <769>: invalid bad-time",
        "Price <44>: invalid bad-price",
    ]
    assert parsed.column("parties").to_pylist() == [
        None,
        None,
        [
            {
                "partyid": "P",
                "partyidsource": "D",
                "partyrole": 1,
                "partyrolequalifier": None,
            }
        ],
        [
            {
                "partyid": "P",
                "partyidsource": "D",
                "partyrole": None,
                "partyrolequalifier": None,
            }
        ],
        None,
        None,
    ]
    assert [parsed.column("entries")[row].as_py()[0]["value"] for row in (0, 1)] == [
        "x",
        "2",
    ]
    assert [parsed.column("entries")[row].as_py()[0]["value"] for row in (3, 4, 5)] == [
        "abc",
        "bad-time",
        "bad-price",
    ]


def test_unknown_versions_and_tags_remain_forward_compatible_non_errors(
    codec: FixCodec,
) -> None:
    line = "8=FIX.9.9|35=D|11=C1|9999=future|44=abc|10=000|"

    parsed = FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec)

    assert _protocols(parsed) == ["FIX9.9"]
    assert parsed.column("error").to_pylist() == [None]
    assert parsed.column("clordid").to_pylist() == ["C1"]
    assert parsed.column("checksum").to_pylist() == ["000"]
    retained = [
        entry["value"]
        for name in ("entries", "unmap")
        for entry in (parsed.column(name)[0].as_py() or ())
    ]
    assert set(retained) == {"future", "abc"}


def test_the_scalar_row_and_the_batch_lift_the_same_instrument(codec: FixCodec) -> None:
    """The reference reading and the kernel one, on the same line.

    A scalar row carries its payload as entries until it is identified, and
    reads a field through `get` whichever list holds it; `identify` promotes
    the same columns the batch does, so the two are compared there.
    """
    scalar = FixMsg.from_text(INSTRUMENT_WIRE, registry=codec.registry)
    batch = FixMsg.from_message_batch(_raw_batch(Message(body=INSTRUMENT_WIRE)), codec)

    assert scalar.get("Symbol").raw == "AAPL"
    assert scalar.get("MaturityDate").raw == "20261218"
    identified = scalar.identify()
    for name, expected in INSTRUMENT_COLUMNS.items():
        assert _instrument_column(batch, name)[0].as_py() == expected, name
    assert (
        identified.instrument.symbolticker == _instrument_column(batch, "symbolticker")[0].as_py()
    )
    assert identified.vhash == batch.column("vhash")[0].as_py()


def test_a_stored_row_reads_back_as_the_row_that_was_parsed(codec: FixCodec) -> None:
    """A `Message` written down and read again transcribes identically."""
    direct = FixMsg.from_message_batch(_raw_batch(Message(body=INSTRUMENT_WIRE)), codec)
    stored = Message.from_dict(Message(body=INSTRUMENT_WIRE).into_dict())
    again = FixMsg.from_message_batch(_raw_batch(stored), codec)

    assert again.to_pylist() == direct.to_pylist()


def test_a_mixed_protocol_batch_keeps_every_row_where_it_stood(codec: FixCodec) -> None:
    """Rows are grouped by protocol to parse and scattered back by position."""
    lines = [
        INSTRUMENT_WIRE,
        INSTRUMENT_NAMED,
        "just some prose",
        "8=FIX.4.4|35=D|11=C1|SYMBOL=MSFT|10=000",
        "BEGINSTRING=FIX.4.4|ACCOUNT=A1|MSGTYPE=D|CLORDID=C2",
    ]
    batch = FixMsg.from_message_batch(_raw_batch(*(Message(body=line) for line in lines)), codec)

    assert _protocols(batch) == [
        "FIX4.4",
        "UL4.4",
        "OTHER",
        "FIXML4.4",
        "UL4.4",
    ]
    assert _instrument_column(batch, "symbol").to_pylist() == [
        "AAPL",
        "AAPL",
        "",
        "MSFT",
        "",
    ]
    assert batch.column("clordid").to_pylist() == [None, None, None, "C1", "C2"]
    one_by_one = [
        FixMsg.from_message_batch(_raw_batch(Message(body=line)), codec).to_pylist()[0]
        for line in lines
    ]
    assert batch.to_pylist() == one_by_one
