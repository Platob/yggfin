"""`Log`'s own contract; the parser that fills it is tested beside it."""

import datetime
import re
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, Log
from rekep.fix import NO_PROTOCOL, FixRegistry, Party
from rekep.fix.columns import COLUMNS, COMMON, DECLARATIONS, FLAT, SESSION, STAMPS, _physical_type
from rekep.market import MIC, Event, EventType
from rekep.market.event import HOUR

#: The dictionary this repository publishes, beside `python/`, read offline:
#: a contract that only holds while the site answers is not a contract.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: The envelope every event carries, then log-only source ordering and content.
ENVELOPE = [
    "unix",
    "unix_hour",
    "etype",
    "cunix",
    "runix",
    "eunix",
    "sunix",
    "hash",
    "xhash",
    "linked_xhash",
    "version",
    "state",
    "code",
    "xcode",
    "seq",
    "prev_hash",
    "prev_state",
    "prev_unix",
    "parent_hash",
    "mic",
    "error",
]
SOURCE: list[str] = []
LINE = ["url", "thread_name", "driver_name", "message"]
MESSAGE = ["protocol", "fix_tags", "fix_miss_tags", "keyval", "parties", "isincode"]

#: Raw FIX names stay distinct from the protocol-neutral event envelope.
RAW_TAGS = {55: "symbol", 34: "seq"}

#: The flattened message layer, derived from the module that names it and
#: pinned below -- so a column renamed in one file and not in the other fails
#: here, rather than moving both sides of every comparison together.
FLAT_COLUMNS = [column for _, column in FLAT]
ADDED_COLUMNS = [column for column in FLAT_COLUMNS if column not in set(ENVELOPE)]
EXPECTED_SESSION_COLUMNS = 33
EXPECTED_COMMON_COLUMNS = 26
EXPECTED_FLAT_COLUMNS = 77
EXPECTED_LOG_COLUMNS = 107


@pytest.fixture(scope="module")
def registry() -> FixRegistry:
    """The published dictionary. Offline, because this must not test the site."""
    return FixRegistry(cache_dir=DATA, offline=True)


def test_a_log_line_is_an_event() -> None:
    """Which is what lets a parsed log be read beside the orders it describes."""
    assert issubclass(Log, Event)
    assert (
        Log.into_field().into_arrow_schema().names
        == ENVELOPE + SOURCE + LINE + MESSAGE + ADDED_COLUMNS
    )


def test_a_logs_cached_contract_metadata_is_immutable() -> None:
    with pytest.raises(TypeError):
        Log.into_field_metadata()["version"] = "2"


def test_the_envelope_is_the_same_one_every_other_event_carries() -> None:
    """Position included: a reader of the envelope must not need to know the shape."""
    assert Event.into_field().names == ENVELOPE


def test_every_column_a_line_adds_is_required() -> None:
    """A line always has a file, a thread, a driver and a payload, even an empty one."""
    for name in LINE:
        assert not Log.into_field().field(name).nullable, name


def test_a_line_always_says_which_protocol_it_carries() -> None:
    """`OTHER` is an answer and not a missing one -- it is most of a capture --
    so the column is NOT NULL and the fall-through is what a line starts as."""
    assert not Log.into_field().field("protocol").nullable
    assert Log.into_field().field("protocol").arrow_type == pyarrow.string()
    assert Log().protocol == NO_PROTOCOL


def test_a_line_carrying_no_message_has_no_pairs_at_all() -> None:
    """Null is not an empty list: a bridge that sent an empty payload and a stack
    trace that never was a message have to stay tellable apart."""
    for name in ("fix_tags", "fix_miss_tags", "keyval"):
        assert Log.into_field().field(name).nullable, name
        assert getattr(Log(), name) is None, name


def test_a_pair_whose_value_is_missing_is_not_a_pair() -> None:
    """Both lists declare the value NOT NULL, so a consumer has two states to
    handle and not three; `FixCodec.drop_null_values` is what keeps it true."""
    for name in ("fix_tags", "keyval"):
        member = Log.into_field().field(name)
        assert pyarrow.types.is_list(member.arrow_type), name
        assert member.item.nullable is False, name
        assert member.item.field("key").nullable is False, name
        assert member.item.field("value").nullable is False, name


def test_pair_lists_keep_repeats_across_python_and_arrow_entry_shapes() -> None:
    reader = Log(
        msg_type="D",
        fix_tags=[(55, "A"), [55, "B"], {"key": 55, "value": "C"}],
        keyval=[
            ("VenueOwnThing", "x"),
            ["VenueOwnThing", "y"],
            {"key": "VenueOwnThing", "value": "z"},
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
    ]


def test_malformed_stored_pair_entries_are_not_silently_dropped() -> None:
    with pytest.raises(KeyError, match="key"):
        Log(fix_tags=[{"value": "x"}]).into_fix_events()
    with pytest.raises(ValueError, match="not enough values"):
        Log(keyval=[["OnlyKey"]]).into_fix_events()


def test_registry_miss_keys_are_an_ordered_non_null_text_list() -> None:
    member = Log.into_field().field("fix_miss_tags")
    assert pyarrow.types.is_list(member.arrow_type)
    assert member.item.arrow_type == pyarrow.string()
    assert member.item.nullable is False


def test_parties_keep_exact_registry_fields_and_a_flexible_buffer(
    registry: FixRegistry,
) -> None:
    parties = Log.into_field().field("parties")
    assert parties.nullable and not parties.item.nullable
    assert parties.metadata["fix:component"] == "Parties"
    for tag, name in ((448, "party_id"), (447, "party_id_source"), (452, "party_role")):
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
    for member in Log.into_field().fields:
        assert member.description, f"{member.name} has no description"
        assert "\n" not in member.description, f"{member.name} description is not one line"


def test_the_key_is_the_moment_and_the_line() -> None:
    """Two columns: a hash identifies the line, the time is what an engine prunes on."""
    assert Log.into_field().primary_keys() == ["unix", "hash"]


def test_the_partition_is_the_hour_the_line_falls_in() -> None:
    """An identity partition on an integer, so every engine below reads it alike."""
    assert Log.into_field().partition_keys() == {"unix_hour": "identity"}
    assert Log.into_field().field("unix_hour").arrow_type == pyarrow.int64()


def test_every_unix_column_declares_its_unit() -> None:
    for name in ("unix", "unix_hour", "cunix", "runix", "eunix", "sunix", "prev_unix"):
        metadata = Log.into_field().field(name).metadata
        assert metadata["unit"] == "nanosecond", name
        assert metadata["epoch"] == "1970-01-01", name


def test_the_line_digest_is_an_int64_like_every_other_identifier() -> None:
    """The one column every engine below Arrow reads the same way, and the key
    is `(unix, hash)` -- so two digests only meet if they also share a
    nanosecond."""
    for name in ("hash", "xhash"):
        assert Log.into_field().field(name).arrow_type == pyarrow.int64(), name
    assert Log.into_field().field("unix").arrow_type == pyarrow.int64()


def test_a_line_is_unclassified_until_something_classifies_it() -> None:
    """The fallback the rules fall back to, on the class rather than in the parser."""
    assert Log.into_event_type() is EventType.UNKNOWN
    assert Log().etype is EventType.UNKNOWN


def test_the_hour_is_derived_from_the_instant() -> None:
    built = Log(unix=3 * HOUR + 5)
    assert built.unix_hour == 3 * HOUR
    assert Log(unix=-1).unix_hour == -HOUR, "and it floors, either side of the epoch"


def test_the_schema_says_which_class_it_came_from() -> None:
    schema = Log.into_field().into_arrow_schema()
    assert schema.metadata[b"name"] == b"Log"
    assert Field.from_arrow_schema(schema) == Log.into_field()


def test_a_row_round_trips_as_a_document() -> None:
    """The message layer preserves checksums and repeated ordered pairs."""
    row = Log(
        url="a.txt",
        unix=2,
        hash=3,
        xhash=3,
        etype=EventType.ORDER,
        thread_name="t",
        driver_name="d",
        message="m",
        protocol="FIX",
        fix_tags=[(11, "ORD-1"), (11, "ORD-1-again")],
        fix_miss_tags=["ISINCODE"],
        keyval=[("ISINCODE", "XX0000084733"), ("ISINCODE", "XX0000084734")],
        code="TTF",
        seq=7,
        symbol="TTF",
        sending_time=datetime.datetime.fromtimestamp(1_755_163_800.123, tz=datetime.UTC),
        poss_dup_flag=True,
        check_sum="010",
        mic=MIC.from_str("XPAR"),
        error="test reject",
    )
    assert Log.from_json(row.into_json()) == row


def test_mic_is_a_lossless_optional_int32_code() -> None:
    member = Log.into_field().field("mic")
    assert member.nullable and member.arrow_type == pyarrow.int32()
    assert member.metadata["enum:encoding"] == "ascii-big-endian"
    assert member.metadata["enum:pattern"] == "[A-Z0-9]{4}"
    assert "enum:dynamic" not in member.metadata


def test_error_is_optional_text_on_every_event() -> None:
    member = Event.into_field().field("error")
    assert member.nullable and member.arrow_type == pyarrow.string()


# -- the message layer, flattened ---------------------------------------------


def test_the_flat_layer_is_the_session_layer_and_what_a_trading_log_is_made_of() -> None:
    """Derived from `rekep.fix.columns` and pinned, so a tag dropped from either
    tuple cannot quietly shrink every check that walks it. One tag, one column,
    both ways: a repeat would silently overwrite whatever it landed on."""
    assert len(SESSION) == EXPECTED_SESSION_COLUMNS
    assert len(COMMON) == EXPECTED_COMMON_COLUMNS
    assert len(FLAT) == len(COLUMNS) == EXPECTED_FLAT_COLUMNS
    assert len(set(FLAT_COLUMNS)) == EXPECTED_FLAT_COLUMNS
    assert len(Log.into_field().fields) == EXPECTED_LOG_COLUMNS


def test_snake_fix_names_do_not_alias_the_generic_event_envelope() -> None:
    assert {tag: COLUMNS[tag] for tag in RAW_TAGS} == RAW_TAGS
    assert set(RAW_TAGS.values()) & set(Event.into_field().names) == {"seq"}
    assert {"code", "seq", *RAW_TAGS.values()} <= set(Log.into_field().names)


def test_every_promoted_name_is_snake_case_with_acronyms_split() -> None:
    names = [field.name for field in DECLARATIONS.values()]
    assert len(names) == 84
    assert all(re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", name) for name in names)
    assert {tag: COLUMNS[tag] for tag in (35, 41, 461)} == {
        35: "msg_type",
        41: "orig_cl_ord_id",
        461: "cfi_code",
    }
    assert {tag: DECLARATIONS[tag].name for tag in (453, 802)} == {
        453: "no_party_ids",
        802: "no_party_sub_ids",
    }
    assert Party.into_field().names == ["party_id", "party_id_source", "party_role", "buffer"]


def test_no_other_lifted_column_lands_on_one_the_line_already_had() -> None:
    """Raw protocol fields and the generic envelope have separate names."""
    assert set(FLAT_COLUMNS) & set(ENVELOPE + LINE + MESSAGE) == {"seq"}


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
        assert Log.into_field().field(column).arrow_type == registry.field(tag).arrow_type, column


def test_a_lifted_stamp_is_a_microsecond_utc_timestamp(
    registry: FixRegistry,
) -> None:
    """Promoted FIX clocks use Iceberg's width and their documented UTC zone."""
    dictated = {
        tag for tag, _ in FLAT if pyarrow.types.is_timestamp(registry.field(tag).arrow_type)
    }
    assert dictated == set(STAMPS)
    for tag in STAMPS:
        assert Log.into_field().field(COLUMNS[tag]).arrow_type == pyarrow.timestamp(
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
    blank = Log()
    for column in FLAT_COLUMNS:
        assert Log.into_field().field(column).nullable, column
        assert getattr(blank, column) is None, column


def test_every_flat_column_keeps_the_registry_name_metadata_and_description(
    registry: FixRegistry,
) -> None:
    for tag, column in FLAT:
        expected = registry.scalar(tag)
        actual = Log.into_field().field(column)
        assert actual.fix["name"] == expected.name, column
        assert actual.metadata == expected.metadata, column
        assert actual.description == expected.description, column


def test_rendered_isincode_keeps_its_source_identity() -> None:
    field = Log.into_field().field("isincode")
    assert field.arrow_type == pyarrow.string()
    assert field.fix == {"name": "ISINCODE", "type": "String"}
