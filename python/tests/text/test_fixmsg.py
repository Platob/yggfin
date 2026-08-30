"""`FixMsg`'s own contract; the parser that fills it is tested beside it."""

import datetime
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, FixCodec, FixMsg, Message, txhash
from rekep.enums import Direction, Protocol, SecurityIDSource
from rekep.fields import DISPLAY, column_name
from rekep.fix import ENTRIES, FixRegistry, Party
from rekep.fix.columns import (
    _COLUMN_METADATA,
    COLUMNS,
    COMMON,
    DECLARATIONS,
    FLAT,
    SESSION,
    STAMPS,
    _physical_type,
    column_metadata,
)
from rekep.fix.fields import fix_field
from rekep.market import (
    HASH,
    MIC,
    AssetKind,
    BookIterator,
    Currency,
    Event,
    EventType,
    Instrument,
    InstrumentUpdate,
    OptionKind,
    Side,
)
from rekep.market.event import HOUR, SECOND
from rekep.text import Entry
from rekep.text.fixmsg import _UNDIGESTED

#: The dictionary this repository publishes, beside `python/`, read offline:
#: a contract that only holds while the site answers is not a contract.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: The envelope every event carries, then log-only source ordering and content.
ENVELOPE = [
    "unix",
    "unixpartition",
    "eventtype",
    "creaunix",
    "recunix",
    "expunix",
    "snapunix",
    "hash",
    "vhash",
    "xhash",
    "linkedhashes",
    "version",
    "state",
    "code",
    "altids",
    "prevunix",
    "prevhash",
    "parenthash",
    "mic",
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


def _instruments(*messages: FixMsg) -> list[Instrument]:
    """Components carried by parsed messages through the class-owned API."""
    return [update.instrument for update in InstrumentUpdate.from_fixmsgs(messages)]


LINE = [
    "sourceurl",
    "sourcerownum",
    "threadname",
    "plugincode",
    "message",
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
    "protocolversion",
    "protocolversionsource",
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
]

#: Raw FIX names stay distinct from the protocol-neutral event envelope.
RAW_TAGS = {55: "symbol", 34: "msgseqnum"}

#: The standard header the raw stage lifts out of `entries` into columns of its
#: own, and the tag each is lifted by. Only the discriminator answers to a
#: rendered name as well: a bridge's own `#BEGINSTRING=` spelling stays an
#: argument, because which name a feed writes is data. Spelled out rather than
#: imported from `rekep.text.message.SESSION_FIELDS`, so a field quietly leaving
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

#: The three of the seven the two stages hold differently: the raw stage is
#: protocol-neutral and keeps every one of them as the text the payload spelled,
#: and this stage is the one that reads the dictionary. The other four are text
#: on both sides. `CheckSum <10>` is not lifted at all -- it is the boundary the
#: lift is measured against -- so it reaches this stage inside `entries`.
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
EXPECTED_SESSION_COLUMNS = 33
EXPECTED_COMMON_COLUMNS = 34
EXPECTED_FLAT_COLUMNS = 85
EXPECTED_LOG_COLUMNS = 109


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
        == ENVELOPE + SOURCE + LINE + MESSAGE + ADDED_COLUMNS + TRAILING_COMPONENTS
    )


def test_a_logs_cached_contract_metadata_is_immutable() -> None:
    assert FixMsg.into_field_metadata() == {"version": "1"}
    with pytest.raises(TypeError):
        FixMsg.into_field_metadata()["version"] = "9"


def test_the_envelope_is_the_same_one_every_other_event_carries() -> None:
    """Position included: a reader of the envelope must not need to know the shape."""
    assert Event.into_field().names == ENVELOPE


def test_every_column_a_line_adds_is_required_except_the_payload() -> None:
    """A line always has a file, a thread and a plugin, even an empty one.

    `message` is the exception, and deliberately: on `fix.market`
    `entries` carries every field the line held, so the raw string is dropped
    rather than stored a second time. An all-null column costs nothing on
    disk, which is what makes one stored shape across the three tables
    affordable. The seven standard-header columns are payload too -- a line
    that spelled none of them leaves all seven null -- so what stays required
    is the provenance a line always has.
    """
    field = FixMsg.into_field()
    payload = {"message", "entries", *LIFTED_HEADER}
    for name in LINE:
        if name in payload:
            assert field.field(name).nullable, f"a row may leave {name} null"
            continue
        assert not field.field(name).nullable, name


def test_only_fixmsg_adds_registry_metadata_to_the_promoted_message_type() -> None:
    raw = Message.into_field().field("MsgType")
    parsed = FixMsg.into_field().field("MsgType")

    assert raw.nullable and "fix:tag" not in raw.metadata
    assert parsed.nullable and parsed.fix["tag"] == "35"


def test_the_lifted_header_arrives_as_text_and_is_typed_here() -> None:
    """The raw stage lifts seven header fields out of `entries` and keeps every
    one as the text the payload spelled, carrying no dictionary at all. This
    stage is the one that reads the dictionary: the same seven names arrive
    tagged, and three of them stop being text."""
    raw = Message.into_field()
    parsed = FixMsg.into_field()
    for name, tag in LIFTED_HEADER.items():
        assert raw.field(name).nullable and raw.field(name).dtype == pyarrow.string(), name
        assert [key for key in raw.field(name).metadata if key.startswith("fix:")] == [
            "fix:display"
        ], name
        assert parsed.field(name).fix["tag"] == tag, name
        assert parsed.field(name).dtype == RETYPED_HEADER.get(name, pyarrow.string()), name
    assert "checksum" not in raw.names, "the boundary the lift is measured against is not lifted"
    assert parsed.field("CheckSum").fix["tag"] == "10"


def test_a_line_always_says_which_protocol_it_carries() -> None:
    """`OTHER` is an answer and not a missing one -- it is most of a capture --
    so the column is NOT NULL and the fall-through is what a line starts as."""
    member = FixMsg.into_field().field("protocol")
    assert not member.nullable and member.dtype == pyarrow.int64()
    assert member.metadata["enum:name"] == "Protocol"
    assert member.metadata["enum:encoding"] == "ascii-big-endian"
    assert member.metadata["enum:byte_width"] == "8"
    assert member.metadata["enum:pattern"] == "[A-Z0-9._-]{1,8}"
    assert FixMsg().protocol is Protocol.OTHER


def test_a_parsed_row_takes_its_packed_codes_off_whatever_spelled_them() -> None:
    """`FixMsg.__post_init__` reaches `Event`'s and not `Message`'s, so the two
    packed enums the raw row declares are read here or nowhere -- and a column
    that got the word rather than the code cannot be built at all."""
    row = FixMsg(unix=1, hash=1, xhash=1, message="x", protocol="fix", direction="sent")

    assert row.protocol is Protocol.FIX
    assert row.direction is Direction.SENT
    assert FixMsg(protocol=int(Protocol.UL)).protocol is Protocol.UL
    # Tolerant where `Rule` refuses: a declaration is read once and a bad one
    # is a configuration error, while a row path has to survive its input.
    assert FixMsg(protocol="VENUEBRIDGE").protocol is Protocol.UNKNOWN
    built = FixMsg.into_arrow_array([row])
    assert built.field("direction").to_pylist() == [int(Direction.SENT)]
    assert built.field("protocol").to_pylist() == [int(Protocol.FIX)]


def test_a_line_carrying_no_message_has_no_pairs_at_all() -> None:
    """Null is not an empty list: a bridge that sent an empty payload and a stack
    trace that never was a message have to stay tellable apart."""
    assert FixMsg.into_field().field("entries").nullable
    assert FixMsg().entries is None
    assert FixMsg.into_field().field("unmap").nullable
    assert FixMsg().unmap is None


def test_an_explicit_empty_parsed_argument_list_is_not_reparsed() -> None:
    parsed = FixMsg(message="8=FIX.4.4|35=D|10=000|", entries=[])

    assert parsed.entries == []


def test_a_stored_field_always_says_what_it_is() -> None:
    """`tag` and `key` remain non-null even when the registry knows no identity."""
    entries = FixMsg.into_field().field("entries")
    unmap = FixMsg.into_field().field("unmap")
    assert entries.dtype == unmap.dtype == ENTRIES
    assert pyarrow.types.is_list(entries.dtype)
    assert entries.item.fix["display"] == unmap.item.fix["display"] == "Item"
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


def test_a_parsed_row_is_the_raw_row_transcribed() -> None:
    """`from_text` is `from_message` over `Message.from_text`: one seam, and
    the raw row's provenance and envelope carry over whole."""
    line = "8=FIX.4.4|35=D|11=C1|54=1|10=000"
    staged = Message.from_text(line, sourceurl="s3://x/y.log", sourcerownum=4, recunix=9)
    row = FixMsg.from_message(staged)

    assert row == FixMsg.from_text(line, sourceurl="s3://x/y.log", sourcerownum=4, recunix=9)
    assert (row.sourceurl, row.sourcerownum, row.recunix) == ("s3://x/y.log", 4, 9)
    assert row.eventtype == FixRegistry.from_builtin().msg_type_event_types()["D"]
    assert row.protocolversion == "4.4"
    assert FixMsg.from_(staged) == row, "the generic builder reaches the same seam"


def test_transcribing_keeps_a_stored_classification_and_resets_identity() -> None:
    """A raw stage that already classified the row is kept; identity is not --
    a parsed row hashes over its parsed values, never the raw line's digest."""
    staged = Message.from_text("8=FIX.4.4|35=D|11=C1|10=000", eventtype=EventType.MISC)
    staged.hash = staged.vhash = staged.xhash = 12345

    row = FixMsg.from_message(staged)
    assert row.eventtype == EventType.MISC
    assert row.hash == row.vhash == row.xhash == 0

    with pytest.raises(TypeError, match="Message"):
        FixMsg.from_message("8=FIX.4.4|35=D|10=000")


def test_scalar_and_arrow_identification_share_the_registry_projection() -> None:
    line = "8=FIX.4.4|35=D|11=C1|54=1|10=000"
    declared = {
        "unix": 1_700_000_000_000_000_000,
        "recunix": 1_700_000_000_000_000_000,
        "sourceurl": "capture.log",
        "sourcerownum": 1,
    }
    scalar = FixMsg.from_text(line, **declared).identify()
    # `eventtype` is a value the row carries and so a value it hashes: the two
    # sides have to be given the same one. `from_text` reads it through the
    # registry where a bare `Message` leaves it UNKNOWN.
    arrow = FixMsg.from_message_batch(
        [Message(message=line, eventtype=scalar.eventtype, **declared)]
    )

    assert scalar.code == arrow.column("code")[0].as_py() == "C1"
    assert scalar.vhash == arrow.column("vhash")[0].as_py()
    assert scalar.xhash == arrow.column("xhash")[0].as_py()
    assert scalar.into_row()["hash"] == arrow.column("hash")[0].as_py()


def test_the_digest_is_every_column_but_the_clocks_and_the_identities() -> None:
    """Stated by exclusion, so a column added to the shape is in the digest the
    day it lands rather than the day someone remembers to name it."""
    named = set(FixMsg.into_digest_columns())

    assert named == set(FixMsg.into_field().names) - _UNDIGESTED
    assert {"clordid", "price", "side", "orderqty", "parties", "instrument"} <= named, (
        "a lifted field is content"
    )
    assert not named & {"unix", "recunix", "hash", "vhash", "xhash", "message"}


def test_two_orders_differing_only_in_lifted_fields_are_two_rows(codec: FixCodec) -> None:
    """The registry projection promotes every parsed field *out* of `entries`,
    so a digest that named a handful of columns met an empty list and gave two
    unrelated orders one `hash` -- which is half the primary key."""
    one = "8=FIX.4.4|9=1|35=D|34=1|49=A|56=B|11=ORD-1|55=TTF|54=1|38=100|44=41.25|10=000|"
    other = one.replace("11=ORD-1", "11=ORD-2").replace("55=TTF", "55=IBM")

    batch = FixMsg.from_message_batch(
        _raw_batch(Message(message=one), Message(message=other)), codec
    )

    assert batch.column("entries").to_pylist() == [[], []], "nothing was left to hash there"
    assert batch.column("vhash")[0].as_py() != batch.column("vhash")[1].as_py()
    assert batch.column("hash")[0].as_py() != batch.column("hash")[1].as_py()


def test_a_reformatted_message_keeps_its_digest(codec: FixCodec) -> None:
    """The other half of taking a digest over parsed values: the separator
    moved and the row did not, so the identity must not move either."""
    piped = "8=FIX.4.4|9=1|35=D|34=1|49=A|56=B|11=ORD-1|55=TTF|54=1|38=100|44=41.25|10=000|"
    soh = piped.replace("|", "\x01").rstrip("\x01")

    batch = FixMsg.from_message_batch(
        _raw_batch(Message(message=piped), Message(message=soh)), codec
    )

    assert batch.column("vhash")[0].as_py() == batch.column("vhash")[1].as_py()


def test_fixmsg_value_hash_excludes_the_event_clock() -> None:
    line = "8=FIX.4.4|35=D|11=C1|54=1|10=000"
    parsed = FixMsg.from_message_batch(
        [
            Message(message=line, unix=1_000, recunix=1_000),
            Message(message=line, unix=2_000, recunix=2_000),
        ]
    )

    assert parsed.column("vhash")[0].as_py() == parsed.column("vhash")[1].as_py()
    assert parsed.column("hash")[0].as_py() != parsed.column("hash")[1].as_py()


def test_message_batches_transcribe_from_rows_and_arrow_alike() -> None:
    """`from_message_batch` is one boundary: scalar rows and a raw RecordBatch
    land as the same parsed batch, under the packaged default codec."""
    rows = [
        Message(message="8=FIX.4.4\x0135=D\x0111=C1\x0154=1\x0138=5\x0110=000\x01", recunix=1),
        Message(message="plain prose", recunix=2),
    ]
    from_rows = FixMsg.from_message_batch(rows)
    raw = Message.into_arrow_reader(rows).read_all().to_batches()[0]

    assert from_rows.equals(FixMsg.from_message_batch(raw))
    assert from_rows.equals(FixMsg.from_message_batch(rows, FixRegistry.from_builtin())), (
        "a registry is all the conversion needs; the codec derives from it"
    )
    assert from_rows.column("clordid").to_pylist() == ["C1", None]
    assert from_rows.column("protocolversion").to_pylist() == ["4.4", None]

    empty = FixMsg.from_message_batch([])
    assert empty.num_rows == 0
    assert empty.schema.names == FixMsg.into_field().into_arrow_schema().names

    with pytest.raises(TypeError, match="Message rows"):
        FixMsg.from_message_batch(["8=FIX.4.4|35=D|10=000"])


def _widened(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    """The batch as an Iceberg scan hands it back: every string 64-bit wide."""

    def wide(dtype: pyarrow.DataType) -> pyarrow.DataType:
        if pyarrow.types.is_string(dtype):
            return pyarrow.large_string()
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
    """The whole `parse_fix` stage reads its rows back through a scan, which
    hands `large_string` back where the raw contract says `string` -- and the
    kernels this path builds its own constants for refuse the mix."""
    rows = [
        Message(message="8=FIX.4.4\x0135=D\x0111=C1\x0154=1\x0138=5\x0110=000\x01", recunix=1),
        Message(message="plain prose", recunix=2),
    ]
    fresh = FixMsg.from_message_batch(rows)
    stored = _widened(Message.into_arrow_reader(rows).read_all().to_batches()[0])
    assert stored.column("message").type == pyarrow.large_string(), "the fixture is the wide one"
    assert FixMsg.from_message_batch(stored).equals(fresh)


def test_a_projected_batch_keeps_the_column_the_reader_left_behind() -> None:
    """`parse_fix` projects `message` away and parses the stored entries. The
    declaration must not fill it back in: an all-null text column would send
    the classifier down the path that reads text, over rows that have none."""
    rows = [Message(message="8=FIX.4.4\x0135=D\x0111=C1\x0110=000\x01", recunix=1)]
    raw = Message.into_arrow_reader(rows).read_all().to_batches()[0]
    projected = raw.select([name for name in raw.schema.names if name != "message"])
    parsed = FixMsg.from_message_batch(_widened(projected))
    assert "message" not in parsed.schema.names or parsed.column("message").null_count == 1
    assert parsed.column("clordid").to_pylist() == ["C1"], "the stored entries still parse"


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

    assert row.protocol is Protocol.FIX
    assert row.protocolversion == "4.4"
    assert row.protocolversionsource == "begin_string"


def test_scalar_hybrid_projection_prefers_named_registry_identity() -> None:
    row = FixMsg.from_("8=FIX.4.4|35=U1|55=wire|#MSGTYPE=D|#SYMBOL=named|11=C1|10=000|")

    assert row.msgtype == "D"
    assert row.into_fix_events(fix_version="4.4").by_tag["55"] == "named"


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
    party_batch = FixMsg.from_message_batch(_raw_batch(Message(message=party_line)), codec)
    party = FixMsg.from_dict(party_batch.to_pylist()[0])

    assert party_batch.column("parties")[0].as_py()[0]["partyid"] == "GROUP"
    assert party.group(453) == [[("448", "GROUP"), ("447", "D"), ("452", "1")]]

    depth_line = (
        "8=FIX.4.4|35=UL|#MSGTYPE=X|#SYMBOL=HEADER|268=1|279=0|269=0|55=ENTRY|270=100|271=1|10=000"
    )
    depth_batch = FixMsg.from_message_batch(_raw_batch(Message(message=depth_line)), codec)
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
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

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
    assert legs[0]["maturitydate"] == datetime.date(2027, 1, 15)
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


def test_nested_instrument_does_not_absorb_lifecycle_altids() -> None:
    message = FixMsg(instrument=Instrument(symbol="AAPL"), altids={"clordid": "C1"})

    assert message.instrument.symbolticker == "AAPL"
    assert message.altids == {"clordid": "C1"}
    assert "altids" not in Instrument.into_field().names


def test_instrument_projection_prefers_promoted_values_and_fills_from_entries() -> None:
    message = FixMsg(
        unix=23,
        protocolversion="4.4",
        instrument=Instrument(symbol="PROMOTED"),
        entries=[(55, "RESIDUAL"), (107, "reference facts")],
    )
    (update,) = InstrumentUpdate.from_fixmsgs([message])
    instrument = update.instrument

    assert (update.unix, instrument.symbol, instrument.securitydesc) == (
        23,
        "PROMOTED",
        "reference facts",
    )


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
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

    assert Protocol.from_int(batch.column("protocol")[0].as_py()) is Protocol.UL
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
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

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
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

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
    """4.3 declares `SecAltIDGrp` and no legs component, so one stored row must
    read `altids` off the resolved column while `legs` still walk the pairs."""
    line = (
        "8=FIX.4.3|35=d|55=SPREAD|454=1|455=US0378331005|456=4|"
        "555=2|600=AAPL|624=1|623=1|600=MSFT|624=2|623=2|10=000"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

    assert [entry["securityaltid"] for entry in batch.column("securityaltid")[0].as_py()] == [
        "US0378331005"
    ]
    assert _instrument_column(batch, "legs")[0].as_py() is None
    assert 555 in [entry["tag"] for entry in batch.column("entries")[0].as_py()]

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
    stored_batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)
    stored = FixMsg.from_dict(stored_batch.to_pylist()[0])

    assert direct.get(769).raw == "20260821-09:59:00"
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
        assert actual.fix.display == expected.name, "folded to store, spelled to read"
        assert actual.dtype == expected.dtype
        assert actual.metadata == column_metadata({**expected.metadata, DISPLAY: expected.name})
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
    """Version provenance stays wide while value and lifecycle joins are int64."""
    for name in ("hash", "prevhash"):
        assert FixMsg.into_field().field(name).dtype == HASH, name
    for name in ("vhash", "xhash"):
        assert FixMsg.into_field().field(name).dtype == pyarrow.int64(), name
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
        plugincode="d",
        message="m",
        protocol=Protocol.FIX,
        entries=[_stored(11, "ClOrdID", one) for one in ("ORD-1", "ORD-1-again")]
        + [_stored(0, "ISINCODE", one) for one in ("FAKE-ISIN-0001", "FAKE-ISIN-0002")],
        code="TTF",
        msgseqnum=7,
        instrument=Instrument(symbol="TTF"),
        sendingtime=datetime.datetime.fromtimestamp(1_755_163_800.123, tz=datetime.UTC),
        possdupflag=True,
        checksum="010",
        mic=MIC.from_str("XPAR"),
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


def test_mic_is_a_lossless_optional_int32_code() -> None:
    member = FixMsg.into_field().field("mic")
    assert member.nullable and member.dtype == pyarrow.int32()
    assert member.metadata["enum:encoding"] == "ascii-big-endian"
    assert member.metadata["enum:pattern"] == "[A-Z0-9]{4}"
    assert "enum:dynamic" not in member.metadata


def test_reason_is_generic_optional_text_on_every_event() -> None:
    member = Event.into_field().field("reason")
    assert member.nullable and member.dtype == pyarrow.string()
    assert "fix:tag" not in member.metadata


# -- the raw-message boundary -------------------------------------------------


def _raw_batch(*messages: Message) -> pyarrow.RecordBatch:
    """One raw-message batch, including the zero-row shape."""
    if not messages:
        return pyarrow.RecordBatch.from_pylist([], schema=Message.into_field().into_arrow_schema())
    return next(iter(Message.into_arrow_reader(messages)))


def test_fixmsg_conversion_is_the_layer_that_parses_fix(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(
            eventtype=EventType.ORDER,
            message=(
                "8=FIX.4.4|35=D|34=7|41=ROOT|55=IBM|461=EXXXXX|6=12.5|"
                "453=1|448=BUYSIDE|447=D|452=1|10=000|"
            ),
            sourceurl="capture.log",
            sourcerownum=1,
            plugincode="fix",
        ),
        Message(
            eventtype=EventType.EXECUTION,
            message=(
                "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=8|#ORIGCLORDID=OLD|#ISINCODE=XX0000084733|"
            ),
            sourceurl="capture.log",
            sourcerownum=2,
            plugincode="ULBridge",
        ),
        Message(
            message="plain text",
            sourceurl="capture.log",
            sourcerownum=3,
            plugincode="misc",
        ),
    )

    assert raw.schema.names == Message.into_field().names
    assert raw.column("msgtype").to_pylist() == ["D", "8", None]
    assert raw.column("beginstring").to_pylist() == ["FIX.4.4", None, None], (
        "the tag is lifted; the bridge's rendered `#BEGINSTRING=` stays an argument"
    )
    assert [entry["key"] for entry in raw.column("entries")[1].as_py()] == [
        "BEGINSTRING",
        "ORIGCLORDID",
        "ISINCODE",
    ], "only the discriminator answers to a rendered header name"
    assert raw.column("msgseqnum").to_pylist() == ["7", None, None], "text, until this stage"
    assert _protocols(raw) == ["FIX", "UL", "OTHER"]
    assert "OrigClOrdID" not in raw.schema.names

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("eventtype").to_pylist() == [
        int(EventType.ORDER),
        int(EventType.EXECUTION),
        int(EventType.MISC),
    ]
    assert _protocols(parsed) == ["FIX", "UL", "OTHER"]
    assert parsed.column("protocolversion").to_pylist() == ["4.4", "4.4", None]
    assert parsed.column("msgtype").to_pylist() == ["D", "8", None]
    assert parsed.column("msgseqnum").to_pylist() == [7, None, None]
    assert parsed.column("origclordid").to_pylist() == ["ROOT", "OLD", None]
    assert _instrument_column(parsed, "cficode").to_pylist() == ["EXXXXX", None, None]
    assert parsed.column("avgpx").to_pylist() == [12.5, None, None]
    assert _instrument_column(parsed, "isincode").to_pylist() == [None, "XX0000084733", None]
    assert parsed.column("parties").to_pylist()[0] == [
        {"partyid": "BUYSIDE", "partyidsource": "D", "partyrole": 1}
    ]
    assert parsed.column("altids").to_pylist()[0] == [("origclordid", "ROOT")]


def test_fixmsg_preserves_the_message_stage_type_and_event_code(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(
            message="8=FIX.4.4|35=D|11=A|10=000|ExecutionReport",
            msgtype="D",
            eventtype=EventType.QUOTE,
        )
    )

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("msgtype").to_pylist() == ["D"]
    assert parsed.column("eventtype").to_pylist() == [int(EventType.QUOTE)]


def test_fixmsg_projection_does_not_need_the_raw_message(
    codec: FixCodec, registry: FixRegistry
) -> None:
    raw = _raw_batch(
        Message(message="Sending : 8=FIX.4.4|35=D|11=A|VendorField=x|10=000|"),
        Message(message="8=FIX.4.4|35=UL|#MSGTYPE=D|#CLORDID=B|10=000|"),
    )

    whole = FixMsg.from_message_batch(raw, codec)
    projected = FixMsg.from_message_batch(
        raw.select([name for name in raw.schema.names if name != "message"]), codec
    )

    assert _protocols(whole) == ["FIXML", "FIXML"]
    assert projected.column("protocol").equals(whole.column("protocol"))
    assert projected.column("entries").equals(whole.column("entries"))
    assert projected.column("hash").equals(whole.column("hash"))
    assert projected.column("message").null_count == projected.num_rows
    # The production shape: `parse_fix` reads with `message` projected out,
    # so the direction the message stage stored is the one the parsed row
    # carries -- identical to what the text would have answered.
    assert whole.column("direction").to_pylist() == [
        int(Direction.SENT),
        int(Direction.UNKNOWN),
    ]
    assert projected.column("direction").equals(whole.column("direction"))


def test_staged_protocol_matching_the_codec_survives_projection(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(message="8=FIX.4.4|35=UL|#MSGTYPE=D|#CLORDID=A|10=000|"))
    at = raw.schema.get_field_index("protocol")
    staged = codec.rules.into_arrow_protocol_array(raw.column("message"), raw.column("plugincode"))
    raw = raw.set_column(at, raw.schema.field(at), staged)

    whole = FixMsg.from_message_batch(raw, codec)
    projected = FixMsg.from_message_batch(raw.drop_columns(["message"]), codec)

    assert _protocols(whole) == ["FIXML"]
    assert whole.column("clordid").to_pylist() == ["A"]
    assert projected.column("protocol").equals(whole.column("protocol"))
    assert projected.column("clordid").equals(whole.column("clordid"))
    assert projected.column("entries").equals(whole.column("entries"))
    assert projected.column("hash").equals(whole.column("hash"))


def test_wire_discriminator_without_begin_string_survives_projection(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(message="35=D|11=A|"))

    whole = FixMsg.from_message_batch(raw, codec)
    projected = FixMsg.from_message_batch(raw.drop_columns(["message"]), codec)

    assert _protocols(raw) == ["FIX"]
    assert [(entry["key"], entry["value"]) for entry in whole.column("entries")[0].as_py()] == [
        ("11", "A")
    ]
    assert whole.column("unmap")[0].as_py() is None
    assert projected.column("entries").equals(whole.column("entries"))
    assert projected.column("unmap").equals(whole.column("unmap"))
    assert projected.column("hash").equals(whole.column("hash"))


def test_unread_message_identity_survives_raw_message_projection(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(message="alpha prose", sourceurl="capture.log", sourcerownum=1).identify(),
        Message(message="beta prose", sourceurl="capture.log", sourcerownum=1).identify(),
    )

    whole = FixMsg.from_message_batch(raw, codec)
    projected = FixMsg.from_message_batch(raw.drop_columns(["message"]), codec)

    assert whole.column("entries").null_count == 2
    assert whole.column("vhash").equals(projected.column("vhash"))
    assert whole.column("xhash").equals(projected.column("xhash"))
    assert whole.column("hash").equals(projected.column("hash"))
    assert [txhash.vhash_of(one) for one in whole.column("hash").to_pylist()] == whole.column(
        "vhash"
    ).to_pylist()
    assert len(set(whole.column("hash").to_pylist())) == 2


def test_fixmsg_projection_preserves_the_configured_message_mic(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(Message(message="8=FIX.4.4|35=D|11=A|10=000|", mic=MIC.from_str("XPAR")))

    whole = FixMsg.from_message_batch(raw, codec)
    projected = FixMsg.from_message_batch(raw.drop_columns(["message"]), codec)

    assert whole.column("mic").to_pylist() == [int(MIC.from_str("XPAR"))]
    assert projected.column("mic").equals(whole.column("mic"))


def test_numeric_flat_fixmsg_arrow_matches_the_registry_reference(
    codec: FixCodec, registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
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
    assert translated.column("side").to_pylist()[0] == "1"
    assert translated.column("mic").null_count == 5
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
            Message(message=line, eventtype=scalar.eventtype),
            Message(message=line.replace("audit", "changed"), eventtype=scalar.eventtype),
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
    venue_audit = fix_field("VenueAudit", 9998, "String", values={"A": "Audit"})
    venue_audit.fix.versions = ("4.4",)
    custom.add_field(venue_audit)
    linked = FixMsg.from_text(line, registry=custom)
    assert [(entry.tag, entry.key, entry.value) for entry in linked.entries or ()] == [
        (9998, "9998", "audit")
    ]
    assert [(entry.tag, entry.value) for entry in linked.unmap or ()] == [
        (11, "C1"),
        (10, "000"),
    ]

    relinked = FixMsg(
        protocolversion="4.4",
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
        Message(message="8=FIX.4.4|35=8|6=0010.5000|10=000|"),
        Message(message="8=FIX.4.4|35=8|6=10.5|10=000|"),
    ).drop_columns(["message"])
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


def test_numeric_fixmsg_arrow_falls_back_when_one_row_has_no_version(
    codec: FixCodec, registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
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
    translated = FixMsg.from_message_batch(source, codec)

    assert activated == [False, True]
    assert translated.column("clordid").to_pylist() == ["A", None]
    assert _instrument_column(translated, "symbol").to_pylist() == ["IBM", ""]
    assert [entry["tag"] for entry in translated.column("entries")[1].as_py()] == [11, 55, 10]
    assert translated.column("unmap")[1].as_py() is None


def test_mixed_fixmsg_batch_keeps_flat_rows_fast_and_scatters_exactly(
    codec: FixCodec, registry: FixRegistry, monkeypatch: pytest.MonkeyPatch
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

    assert activated == [False, True]
    assert translated.equals(reference, check_metadata=True)
    assert translated.column("clordid").to_pylist() == [
        "A",
        "UL-1",
        "B",
        "C",
        "D",
        None,
        None,
        "VENDOR",
    ]


def test_raw_direction_words_do_not_change_projected_mic(
    codec: FixCodec, registry: FixRegistry
) -> None:
    raw = _raw_batch(Message(message="received 8=FIX.4.4|35=D|49=XPAR|56=XNAS|11=A|10=000|"))

    whole = FixMsg.from_message_batch(raw, codec)
    projected = FixMsg.from_message_batch(raw.drop_columns(["message"]), codec)

    assert whole.column("mic").to_pylist() == [int(MIC.from_str("XNAS"))]
    assert projected.column("mic").equals(whole.column("mic"))


def test_fixmsg_conversion_preserves_static_extra_columns(
    codec: FixCodec,
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

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.schema.names == [*FixMsg.into_field().names, "capture_id"]
    assert parsed.schema.field("capture_id") == static
    assert parsed.column("capture_id").to_pylist() == ["day-1"]


def test_the_lifted_header_reaches_this_stage_as_columns_not_as_entries(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """The standard header leaves `entries` upstream, and this stage reads it
    off the columns rather than walking the list again for facts already in
    hand. `CheckSum <10>` is the boundary they are lifted before, so it is not
    one of them and still arrives as an argument like any other."""
    line = "8=FIX.4.4|9=61|35=D|34=7|49=ME|52=20260814-09:30:00.000|56=YOU|11=C1|55=IBM|10=000|"
    raw = _raw_batch(Message(message=line))

    assert [entry["key"] for entry in raw.column("entries")[0].as_py()] == ["11", "55", "10"]
    stated = {
        name: found
        for name in LIFTED_HEADER
        if (found := raw.column(name).to_pylist()[0]) is not None
    }
    assert stated == {
        "beginstring": "FIX.4.4",
        "bodylength": "61",
        "msgseqnum": "7",
        "msgtype": "D",
        "sendercompid": "ME",
        "sendingtime": "20260814-09:30:00.000",
        "targetcompid": "YOU",
    }, "what the line states; every other header column is null"

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("beginstring").to_pylist() == ["FIX.4.4"]
    assert parsed.column("bodylength").to_pylist() == [61]
    assert parsed.column("msgseqnum").to_pylist() == [7]
    assert parsed.column("sendercompid").to_pylist() == ["ME"]
    assert parsed.column("targetcompid").to_pylist() == ["YOU"]
    assert parsed.column("sendingtime").to_pylist() == [
        datetime.datetime(2026, 8, 14, 9, 30, tzinfo=datetime.UTC)
    ]
    assert parsed.column("protocolversion").to_pylist() == ["4.4"]
    assert parsed.column("protocolversionsource").to_pylist() == ["begin_string"]
    assert parsed.column("checksum").to_pylist() == ["000"], "the trailer is read, not lifted"

    # One row at a time reaches the same seven, cast the same way: the scalar
    # path lifts what the vectorized one lifts, and leaves what it leaves.
    scalar = FixMsg.from_text(line)
    assert (scalar.beginstring, scalar.bodylength, scalar.msgtype) == ("FIX.4.4", 61, "D")
    assert (scalar.msgseqnum, scalar.sendercompid, scalar.targetcompid) == (7, "ME", "YOU")
    assert scalar.sendingtime == datetime.datetime(2026, 8, 14, 9, 30), (
        "the instant the column holds, read one value at a time and left naive"
    )
    assert [(entry.tag, entry.value) for entry in scalar.entries or ()] == [
        (11, "C1"),
        (55, "IBM"),
        (10, "000"),
    ]


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
            message=(
                "8=FIX.4.4|35=D|52=20260814-09:30:00.000|52=20260814-09:31:00.000|"
                "34=7|34=7|11=C1|10=000|"
            )
        )
    )

    assert raw.column("sendingtime").to_pylist() == [None]
    assert raw.column("msgseqnum").to_pylist() == ["7"]
    assert [entry["key"] for entry in raw.column("entries")[0].as_py()] == [
        "52",
        "52",
        "11",
        "10",
    ]

    parsed = FixMsg.from_message_batch(raw, codec)

    assert parsed.column("sendingtime").to_pylist() == [None]
    assert parsed.column("msgseqnum").to_pylist() == [7]
    assert [(entry["tag"], entry["value"]) for entry in parsed.column("entries")[0].as_py()] == [
        (52, "20260814-09:30:00.000"),
        (52, "20260814-09:31:00.000"),
    ]


def test_a_header_column_that_is_empty_is_read_back_out_of_the_entries(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """One rule, not a fallback for an older shape: a lifted column is filled
    from the list it was lifted out of wherever it is empty, and a column that
    is not in the batch at all is empty in exactly that sense. So a projection
    that dropped it and a row that never carried it read the same."""
    stored = Message(message="8=FIX.4.4|35=D|11=C1|10=000|").into_row()
    stored["message"] = None
    stored["entries"] = [
        {"tag": int(tag), "key": tag, "value": value}
        for tag, value in (
            ("8", "FIX.4.4"),
            ("35", "D"),
            ("34", "7"),
            ("49", "ME"),
            ("52", "20260814-09:30:00.000"),
            ("56", "YOU"),
            ("11", "C1"),
            ("10", "000"),
        )
    ]
    for name in LIFTED_HEADER:
        stored[name] = None
    legacy = pyarrow.RecordBatch.from_pylist(
        [stored], schema=Message.into_field().into_arrow_schema()
    )

    read = FixMsg.from_message_batch(legacy, codec)
    absent = FixMsg.from_message_batch(
        legacy.drop_columns([name for name in LIFTED_HEADER if name != "msgtype"]), codec
    )

    for batch in (read, absent):
        assert batch.column("msgtype").to_pylist() == ["D"]
        assert batch.column("msgseqnum").to_pylist() == [7]
        assert batch.column("sendercompid").to_pylist() == ["ME"]
        assert batch.column("targetcompid").to_pylist() == ["YOU"]
        assert batch.column("sendingtime").to_pylist() == [
            datetime.datetime(2026, 8, 14, 9, 30, tzinfo=datetime.UTC)
        ]
        assert batch.column("protocolversion").to_pylist() == ["4.4"]
        assert batch.column("protocolversionsource").to_pylist() == ["begin_string"]
        assert batch.column("clordid").to_pylist() == ["C1"]


def test_fixmsg_applies_checksum_semantics_to_the_stored_arguments(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    raw = _raw_batch(
        Message(message="8=FIX.4.4|35=D|10=000|55=AFTER-CHECKSUM|"),
        Message(message="8=FIX.4.4|35=D|10=000|52=20260814-09:30:00.000|"),
    )
    assert raw.column("entries")[0].as_py()[-1]["value"] == "AFTER-CHECKSUM"
    assert raw.column("entries")[1].as_py()[-1]["key"] == "52"
    assert raw.column("sendingtime").to_pylist() == [None, None], (
        "the header is lifted before the checksum, so a field behind it is not lifted either"
    )

    parsed = FixMsg.from_message_batch(raw, codec)

    assert _instrument_column(parsed, "symbol").to_pylist() == ["", ""]
    assert parsed.column("sendingtime").to_pylist() == [None, None]
    assert all(entry["value"] != "AFTER-CHECKSUM" for entry in parsed.column("entries")[0].as_py())


def test_fixmsg_consumes_a_hash_delimited_wire_message(
    codec: FixCodec, registry: FixRegistry
) -> None:
    raw = _raw_batch(Message(message="8=FIX.4.4#35=D#55=TTF#10=000"))

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
            message=(
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


def test_staged_groups_preserve_malformed_continuations(
    codec: FixCodec, registry: FixRegistry
) -> None:
    line = "toBridge #NOPARTYIDS[0]=PARTYID=x\x01garbage|#SIDE=1"
    raw = _raw_batch(Message(message=line))

    staged = codec.into_pairs_from_entries(raw.column("entries"), "FIXML")
    direct = codec.into_pairs(pyarrow.array([line]), "FIXML")

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
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    lines = [
        "8=FIX.4.2|SIDE=1|55=TTF|10=000|",
        "8=FIX.4.2|#54=2|55=IBM|10=000|",
    ]
    raw = _raw_batch(*(Message(message=line) for line in lines))
    _, expected_columns = codec.into_fixmsg_columns(
        codec.into_pairs(pyarrow.array(lines), "FIX"), "4.2"
    )

    parsed = FixMsg.from_message_batch(raw, codec)

    assert raw.column("beginstring").to_pylist() == ["FIX.4.2", "FIX.4.2"], (
        "the envelope left `entries` for its own column, and is not gone"
    )
    assert [entry["key"] for entry in raw.column("entries")[0].as_py()] == [
        "SIDE",
        "55",
        "10",
    ]
    assert [entry["key"] for entry in raw.column("entries")[1].as_py()] == [
        "54",
        "55",
        "10",
    ]
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
    raw = _raw_batch(Message(message="plain text")).append_column(
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
    assert len(names) == 118
    assert all(field.name == column_name(field.fix["name"]) for field in DECLARATIONS.values())
    assert all(field.fix.display == field.fix["name"] for field in DECLARATIONS.values())
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
    assert Party.into_field().names == ["partyid", "partyidsource", "partyrole"]


def test_no_other_lifted_column_lands_on_one_the_line_already_had() -> None:
    """Raw protocol fields and the generic envelope have separate names.

    The only names the two layers share are the standard header the raw stage
    lifts, and they are shared on purpose: one field, one column, read as text
    upstream and as the dictionary types it here.
    """
    assert set(FLAT_COLUMNS) & set(ENVELOPE + LINE + MESSAGE) == set(LIFTED_HEADER)


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

    batch = FixMsg.from_message_batch(_raw_batch(Message(message=wire)), codec)

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
        _raw_batch(Message(message="8=FIX.4.4|35=D|55=TTF|10=000|")), codec
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
            assert Instrument.into_field().field(column).dtype == pyarrow.date32()
        else:
            assert FixMsg.into_field().field(column).dtype == pyarrow.timestamp("us"), (
                f"{tag} is a date the dictionary types as an instant, and carries no zone"
            )


def test_timestamp_projection_is_naive_until_the_fix_documentation_says_utc() -> None:
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
    assert _physical_type(local) == pyarrow.timestamp("us")
    assert _physical_type(utc) == pyarrow.timestamp("us", tz="UTC")


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
        assert actual.fix.display == expected.name, column
        # Minus the `enum:` block: the registry knows `SecurityIDSource <22>`
        # as text, and reading its thirty-three codes as one code is this
        # package's statement about the field, not the dictionary's. And
        # narrowed to what a column says about the field it reads: the rest of
        # the record stays where it is kept, which is the registry.
        stored = {k: v for k, v in actual.metadata.items() if not k.startswith("enum:")}
        assert stored == column_metadata({**expected.metadata, DISPLAY: expected.name}), column
        assert actual.description == expected.description, column


def test_a_nested_message_in_xmldata_survives_the_standard_header_lift(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    """`XmlData <213>` is the one field of the standard header the raw stage
    leaves in `entries`, and this is why: bridges put a `key=value` message in
    it, and `into_payload_pairs` expands that in the place the tag sat. Lifting
    it would have taken the tag out of the list that expansion reads, leaving a
    nested order unread and the column holding the whole payload as bytes.

    The codec's own tests read the expansion off pairs, which is upstream of
    the lift and could not see this -- so it is pinned here, on the pipeline.
    """
    body = "ClOrdID=ORD-TEST-01|Side=1|Account=ACCT-TEST-01"
    line = f"8=FIX.4.4|35=8|212={len(body)}|213={body}|10=000"
    parsed = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

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
    parsed = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

    assert parsed.column("xmldata").to_pylist() == [document.encode()]
    assert parsed.column("clordid").to_pylist() == [None]


def test_the_two_fields_the_standard_header_lift_leaves_alone() -> None:
    """Both are structural. `CheckSum <10>` is the boundary eligibility is
    measured against; `XmlData <213>` carries a message the FIX stage expands,
    and `XmlDataLen <212>` is the length that says where it ends, so the two
    are one token and neither half may be lifted without the other."""
    from rekep.text.message import SESSION_NAMES

    lifted = {name for name, _ in SESSION_NAMES}
    assert {"CheckSum", "XmlData", "XmlDataLen"}.isdisjoint(lifted)
    assert {"SecureDataLen", "SecureData", "SignatureLength", "Signature"} <= lifted, (
        "the other two length-prefixed pairs lift whole, so they stay adjacent"
    )
    assert not {"xmldata", "xmldatalen", "checksum"} & set(Message.into_field().names)


def test_rendered_isincode_keeps_its_source_identity() -> None:
    field = FixMsg.into_field().field("instrument").field("isincode")
    assert field.dtype == pyarrow.string()
    assert field.fix.display == "ISINCode"
    assert field.metadata["iso"] == "6166"
    assert not field.fix.tag, "the value is derived from either FIX identifier location"


def test_fixml_decodes_a_named_value_inside_its_wire_envelope(
    codec: FixCodec,
    registry: FixRegistry,
) -> None:
    payload = "EXECTYPE=fill|CURRENCY=NOK|COUNTERAMOUNT=1200"
    line = f"8=FIX.4.2|35=UL|212={len(payload)}|213={payload}|10=000"

    parsed = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

    assert _protocols(parsed) == ["FIXML"]
    assert parsed.column("msgtype").to_pylist() == ["UL"]
    assert parsed.column("protocolversion").to_pylist() == ["4.2"]
    assert parsed.column("exectype").to_pylist() == ["2"]


def test_the_two_stages_classify_a_row_the_same_way(codec: FixCodec, registry: FixRegistry) -> None:
    """One classifier, so the FIX stage's reading of a row it still has the text
    for is the message stage's reading. An enrichment echo writing real bridge
    fields without `#` markers is a named document; a `35=UL` frame with marked
    keys beside its tags is mixed; operational vocabulary is neither."""
    echo = Message(
        message="RouteMessage : BEGINSTRING=FIX.4.4|ACCOUNT=807768.001"
        "|MSGTYPE=D|CLORDID=PL024819|SIDE=1"
    )
    assert echo.protocol is Protocol.UL
    heartbeat = Message(message="heartbeat emitted seq=7")
    assert heartbeat.protocol is Protocol.MISC
    wrapped = Message(message="sending >> 8=FIX.4.2|35=UL|#SYMBOL=TTF|#SIDE=1|10=044|")
    assert wrapped.protocol is Protocol.FIXML

    batch = FixMsg.from_message_batch(_raw_batch(echo, heartbeat, wrapped), codec)
    assert _protocols(batch) == ["UL", "MISC", "FIXML"]
    assert batch.column("clordid").to_pylist()[0] == "PL024819", "promoted, not dropped"
    assert batch.column("account").to_pylist()[0] == "807768.001"
    assert batch.column("msgtype").to_pylist()[0] == "D"
    assert batch.column("protocolversion").to_pylist()[0] == "4.4"
    assert batch.column("entries").to_pylist()[1] is None, "operational rows stay unread"

    # Without a version the registry cannot resolve the spellings, but the
    # rescued row still keeps its arguments and its identities -- both were
    # simply null while the row read as OTHER.
    bare = Message(message="After Enrichment -> ACCOUNT=59.1|MSGTYPE=D|CLORDID=PL9|SIDE=2")
    assert bare.protocol is Protocol.UL
    lone = FixMsg.from_message_batch(_raw_batch(bare), codec)
    assert _protocols(lone) == ["UL"]
    assert lone.column("entries").to_pylist() == [[]]
    assert [(entry["key"], entry["value"]) for entry in lone.column("unmap").to_pylist()[0]] == [
        ("ACCOUNT", "59.1"),
        ("CLORDID", "PL9"),
        ("SIDE", "2"),
    ]
    assert lone.column("altids").to_pylist() == [[]]


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
    batch = FixMsg.from_message_batch(_raw_batch(*(Message(message=line) for line in lines)), codec)

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

    # A projection that omits the derived column computes it through the split
    # path too: the wire row is flat-translated, the rendered row falls back,
    # and both slices must carry the answer.
    raw = _raw_batch(*(Message(message=line) for line in lines[:2] + [lines[3]]))
    projected = raw.remove_column(raw.schema.get_field_index("direction"))
    resolved = FixMsg.from_message_batch(projected, codec)
    assert resolved.column("direction").to_pylist() == [
        int(Direction.RECV),
        int(Direction.SENT),
        int(Direction.SENT),
    ]

    # A named document has an anchor of its own, so a verb in front of one
    # answers exactly as it does in front of a frame.
    named = Message(message="Sending : ACCOUNT=A1|MSGTYPE=D|PRICE=9.5")
    assert named.protocol is Protocol.UL
    document = FixMsg.from_message_batch(_raw_batch(named), codec)
    assert _protocols(document) == ["UL"]
    assert document.column("direction").to_pylist() == [int(Direction.SENT)]

    # A projected row reparsed without its raw message keeps the resolved
    # answer: direction is the message stage's fact, and nothing recomputes
    # it where the text that carried the verb is gone.
    projected = Message(message="", protocol=Protocol.FIX, direction=Direction.SENT).into_row()
    projected["message"] = None
    projected["entries"] = [{"tag": 8, "key": "8", "value": "FIX.4.4"}]
    again = FixMsg.from_message_batch(
        pyarrow.RecordBatch.from_pylist(
            [projected], schema=Message.into_field().into_arrow_schema()
        ),
        codec,
    )
    assert again.column("direction").to_pylist() == [int(Direction.SENT)]


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
    "maturitydate": datetime.date(2026, 12, 18),
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
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)
    for name, expected in INSTRUMENT_COLUMNS.items():
        assert _instrument_column(batch, name)[0].as_py() == expected, name
    assert batch.column("mic")[0].as_py() == int(MIC.from_str("XNAS"))


def test_alternate_identifiers_and_legs_are_structured_entries(codec: FixCodec) -> None:
    """Repeated FIX structures stay repeated: a list of entries, in wire order."""
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=INSTRUMENT_WIRE)), codec)

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
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

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
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

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

    resolved = FixMsg.from_message_batch(_raw_batch(Message(message=INSTRUMENT_WIRE)), codec)
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
    batch = FixMsg.from_message_batch(_raw_batch(*(Message(message=line) for line in lines)), codec)

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
        _raw_batch(Message(message="8=FIX.4.4|35=D|55=AAPL|SYMBOL=MSFT|10=000")), codec
    )

    assert [(entry["tag"], entry["value"]) for entry in conflict.column("entries")[0].as_py()] == [
        (55, "AAPL"),
        (55, "MSFT"),
    ]
    assert _instrument_column(conflict, "symbol")[0].as_py() == "", "and neither is promoted"

    agreed = FixMsg.from_message_batch(
        _raw_batch(Message(message="8=FIX.4.4|35=D|55=AAPL|SYMBOL=AAPL|10=000")), codec
    )

    assert _instrument_column(agreed, "symbol")[0].as_py() == "AAPL"
    assert agreed.column("entries")[0].as_py() == [], "one field, written twice, is one field"


def test_lifecycle_identifiers_are_their_own_columns(codec: FixCodec) -> None:
    """`OrigClOrdID` and the bridge's parent keys are FIX fields, and stay so.

    None of them is `prevhash` or `linkedhashes`: those relate stored versions
    and lifecycles, where these are what the sender wrote.
    """
    line = (
        "8=FIX.4.4|35=G|11=NEW-1|41=OLD-1|37=ORD-9|55=AAPL"
        "|PARENTCLORDID=ROOT-1|PARENTORDERID=ROOT-ORD|10=000"
    )
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec)

    assert batch.column("clordid")[0].as_py() == "NEW-1"
    assert batch.column("origclordid")[0].as_py() == "OLD-1"
    assert batch.column("orderid")[0].as_py() == "ORD-9"
    assert batch.column("parentclordid")[0].as_py() == "ROOT-1"
    assert batch.column("parentorderid")[0].as_py() == "ROOT-ORD"
    assert batch.column("prevhash")[0].as_py() is None
    assert batch.column("linkedhashes")[0].as_py() == []


def test_a_missing_group_count_and_a_malformed_continuation_stay_visible(
    codec: FixCodec,
) -> None:
    """Nothing is invented and nothing is dropped: what cannot be structured
    stays where a reader can see it."""
    countless = "8=FIX.4.4|35=d|55=AAPL|455=BBG000B9XRY4|456=A|10=000"
    batch = FixMsg.from_message_batch(_raw_batch(Message(message=countless)), codec)
    keys = {entry["key"] for entry in batch.column("entries")[0].as_py() or ()}
    keys |= {entry["key"] for entry in batch.column("unmap")[0].as_py() or ()}
    assert {"SecurityAltID", "SecurityAltIDSource"} <= keys or batch.column("securityaltid")[
        0
    ].as_py()

    broken = "8=FIX.4.4|35=d|55=AAPL|454=|455=|10=000"
    survived = FixMsg.from_message_batch(_raw_batch(Message(message=broken)), codec)
    assert _instrument_column(survived, "symbol")[0].as_py() == "AAPL"


def test_the_scalar_row_and_the_batch_lift_the_same_instrument(codec: FixCodec) -> None:
    """The reference reading and the kernel one, on the same line.

    A scalar row carries its payload as entries until it is identified, and
    reads a field through `get` whichever list holds it; `identify` promotes
    the same columns the batch does, so the two are compared there.
    """
    scalar = FixMsg.from_text(INSTRUMENT_WIRE, registry=codec.registry)
    batch = FixMsg.from_message_batch(
        _raw_batch(Message(message=INSTRUMENT_WIRE, eventtype=scalar.eventtype)), codec
    )

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
    direct = FixMsg.from_message_batch(_raw_batch(Message(message=INSTRUMENT_WIRE)), codec)
    stored = Message.from_dict(Message(message=INSTRUMENT_WIRE).into_row())
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
    batch = FixMsg.from_message_batch(_raw_batch(*(Message(message=line) for line in lines)), codec)

    assert _protocols(batch) == ["FIX", "UL", "OTHER", "FIXML", "UL"]
    assert _instrument_column(batch, "symbol").to_pylist() == [
        "AAPL",
        "AAPL",
        "",
        "MSFT",
        "",
    ]
    assert batch.column("clordid").to_pylist() == [None, None, None, "C1", "C2"]
    one_by_one = [
        FixMsg.from_message_batch(_raw_batch(Message(message=line)), codec).to_pylist()[0]
        for line in lines
    ]
    assert batch.to_pylist() == one_by_one
