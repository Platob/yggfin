"""`Log`'s own contract; the parser that fills it is tested beside it."""

from pathlib import Path

import pyarrow
import pytest

from rekep import Field, Log
from rekep.fix import NO_PROTOCOL, FixRegistry
from rekep.fix.columns import COLUMNS, COMMON, FLAT, SESSION, STAMPS
from rekep.market import Event, EventType
from rekep.market.event import HOUR

#: The dictionary this repository publishes, beside `python/`, read offline:
#: a contract that only holds while the site answers is not a contract.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: The envelope every event carries, then the four columns a log line adds and
#: the three the message it carries adds after them.
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
    "version",
    "state",
    "symbol",
    "seq",
    "prev_hash",
    "prev_state",
    "prev_unix",
    "parent_hash",
]
LINE = ["url", "thread_name", "driver_name", "message"]
MESSAGE = ["protocol", "fix_tags", "keyval"]

#: The two tags whose column `Event` already declares: a parsed log line and a
#: market event answer "which instrument" and "which sequence" with the same
#: column, not with two that can disagree.
ENVELOPE_TAGS = {55: "symbol", 34: "seq"}

#: The flattened message layer, derived from the module that names it and
#: pinned below -- so a column renamed in one file and not in the other fails
#: here, rather than moving both sides of every comparison together.
FLAT_COLUMNS = [column for _, column in FLAT]
ADDED_COLUMNS = [column for column in FLAT_COLUMNS if column not in set(ENVELOPE)]
EXPECTED_SESSION_COLUMNS = 33
EXPECTED_COMMON_COLUMNS = 26
EXPECTED_FLAT_COLUMNS = 59
EXPECTED_LOG_COLUMNS = 81


@pytest.fixture(scope="module")
def registry() -> FixRegistry:
    """The published dictionary. Offline, because this must not test the site."""
    return FixRegistry(cache_dir=DATA, offline=True)


def test_a_log_line_is_an_event() -> None:
    """Which is what lets a parsed log be read beside the orders it describes."""
    assert issubclass(Log, Event)
    assert Log.FIELD.into_arrow_schema().names == ENVELOPE + LINE + MESSAGE + ADDED_COLUMNS


def test_the_envelope_is_the_same_one_every_other_event_carries() -> None:
    """Position included: a reader of the envelope must not need to know the shape."""
    assert Event.FIELD.names == ENVELOPE


def test_every_column_a_line_adds_is_required() -> None:
    """A line always has a file, a thread, a driver and a payload, even an empty one."""
    for name in LINE:
        assert not Log.FIELD.field(name).nullable, name


def test_a_line_always_says_which_protocol_it_carries() -> None:
    """`OTHER` is an answer and not a missing one -- it is most of a capture --
    so the column is NOT NULL and the fall-through is what a line starts as."""
    assert not Log.FIELD.field("protocol").nullable
    assert Log.FIELD.field("protocol").arrow_type == pyarrow.string()
    assert Log().protocol == NO_PROTOCOL


def test_a_line_carrying_no_message_has_no_pairs_at_all() -> None:
    """Null is not an empty map: a bridge that sent an empty payload and a stack
    trace that never was a message have to stay tellable apart."""
    for name in ("fix_tags", "keyval"):
        assert Log.FIELD.field(name).nullable, name
        assert getattr(Log(), name) is None, name


def test_a_pair_whose_value_is_missing_is_not_a_pair() -> None:
    """Both maps declare the value NOT NULL, so a consumer has two states to
    handle and not three; `FixCodec.drop_null_values` is what keeps it true."""
    for name in ("fix_tags", "keyval"):
        member = Log.FIELD.field(name)
        assert member.arrow_type.item_field.nullable is False, name
        assert member.value.nullable is False, name


def test_every_column_is_documented() -> None:
    for member in Log.FIELD.fields:
        assert member.description, f"{member.name} has no description"
        assert "\n" not in member.description, f"{member.name} description is not one line"


def test_the_key_is_the_moment_and_the_line() -> None:
    """Two columns: a hash identifies the line, the time is what an engine prunes on."""
    assert Log.FIELD.primary_keys() == ["unix", "hash"]


def test_the_partition_is_the_hour_the_line_falls_in() -> None:
    """An identity partition on an integer, so every engine below reads it alike."""
    assert Log.FIELD.partition_keys() == {"unix_hour": "identity"}
    assert Log.FIELD.field("unix_hour").arrow_type == pyarrow.int64()


def test_every_unix_column_declares_its_unit() -> None:
    for name in ("unix", "unix_hour", "cunix", "runix", "eunix", "sunix", "prev_unix"):
        metadata = Log.FIELD.field(name).metadata
        assert metadata["unit"] == "nanosecond", name
        assert metadata["epoch"] == "1970-01-01", name


def test_the_line_digest_is_an_int64_like_every_other_identifier() -> None:
    """The one column every engine below Arrow reads the same way, and the key
    is `(unix, hash)` -- so two digests only meet if they also share a
    nanosecond."""
    for name in ("hash", "xhash"):
        assert Log.FIELD.field(name).arrow_type == pyarrow.int64(), name
    assert Log.FIELD.field("unix").arrow_type == pyarrow.int64()


def test_a_line_is_unclassified_until_something_classifies_it() -> None:
    """The fallback the rules fall back to, on the class rather than in the parser."""
    assert Log.EVENT_TYPE is EventType.UNKNOWN
    assert Log().etype is EventType.UNKNOWN


def test_the_hour_is_derived_from_the_instant() -> None:
    built = Log(unix=3 * HOUR + 5)
    assert built.unix_hour == 3 * HOUR
    assert Log(unix=-1).unix_hour == -HOUR, "and it floors, either side of the epoch"


def test_the_schema_says_which_class_it_came_from() -> None:
    schema = Log.FIELD.into_arrow_schema()
    assert schema.metadata[b"name"] == b"Log"
    assert Field.from_arrow_schema(schema) == Log.FIELD


def test_a_row_round_trips_as_a_document() -> None:
    """The message layer included: `010` that came back `10` is a checksum that
    no longer verifies, and a map that came back a list is not the same row."""
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
        fix_tags={11: "ORD-1"},
        keyval={"ISINCODE": "XX0000084733"},
        symbol="TTF",
        seq=7,
        sending_unix=1_755_163_800_123_000_000,
        poss_dup_flag=True,
        check_sum="010",
    )
    assert Log.from_json(row.into_json()) == row


# -- the message layer, flattened ---------------------------------------------


def test_the_flat_layer_is_the_session_layer_and_what_a_trading_log_is_made_of() -> None:
    """Derived from `rekep.fix.columns` and pinned, so a tag dropped from either
    tuple cannot quietly shrink every check that walks it. One tag, one column,
    both ways: a repeat would silently overwrite whatever it landed on."""
    assert len(SESSION) == EXPECTED_SESSION_COLUMNS
    assert len(COMMON) == EXPECTED_COMMON_COLUMNS
    assert len(FLAT) == len(COLUMNS) == EXPECTED_FLAT_COLUMNS
    assert len(set(FLAT_COLUMNS)) == EXPECTED_FLAT_COLUMNS
    assert len(Log.FIELD.fields) == EXPECTED_LOG_COLUMNS


def test_the_two_tags_the_envelope_already_answers_land_on_its_own_columns() -> None:
    """`Symbol <55>` and `MsgSeqNum <34>` are declared on `Event` as those very
    tags, so lifting them fills the column the shape already has rather than a
    second one beside it -- there is no `msg_seq_num`, and a reader joining a
    parsed line to an order compares one `symbol` with one `symbol`."""
    assert {tag: COLUMNS[tag] for tag in ENVELOPE_TAGS} == ENVELOPE_TAGS
    for tag, column in ENVELOPE_TAGS.items():
        assert column in Event.FIELD.names, column
        assert Log.FIELD.field(column).metadata["fix:tag"] == str(tag), column
    assert "msg_seq_num" not in Log.FIELD.into_arrow_schema().names


def test_no_other_lifted_column_lands_on_one_the_line_already_had() -> None:
    """A collision would shadow a column rather than fail: a reader filtering on
    `version` would be handed the FIX one, and the schema would still build. The
    two above are the deliberate exception -- the same fact, not another one."""
    assert set(FLAT_COLUMNS) & set(ENVELOPE + LINE + MESSAGE) == set(ENVELOPE_TAGS.values())


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
        assert Log.FIELD.field(column).arrow_type == registry.field(tag).arrow_type, f"{column}"


def test_a_lifted_stamp_is_int64_nanoseconds_like_every_other_instant(
    registry: FixRegistry,
) -> None:
    """The one departure from the dictionary, pinned rather than excused:
    pyiceberg refuses `timestamp[ns]` outright and `timestamp[us]` would
    truncate a stamp just lifted out of the map. Nanoseconds since the epoch is
    what every other instant here is, so `unix - sending_unix` is a subtraction.
    `STAMPS` has to be *exactly* the lifted tags the dictionary calls
    timestamps, or the next one added lands as an integer of something else."""
    dictated = {
        tag for tag, _ in FLAT if pyarrow.types.is_timestamp(registry.field(tag).arrow_type)
    }
    assert dictated == set(STAMPS)
    for tag in STAMPS:
        assert Log.FIELD.field(COLUMNS[tag]).arrow_type == pyarrow.int64(), tag


def test_every_flat_column_admits_absence() -> None:
    """Whether a field is required is a property of the message that carries it,
    and most lines carry no message at all -- so none of these may be NOT NULL.
    `symbol` is the exception `Event` imposes: it is NOT NULL there, and a line
    whose message never said one keeps the empty default `TextFile._batch`
    coalesces onto."""
    blank = Log()
    assert not Log.FIELD.field("symbol").nullable
    assert blank.symbol == ""
    for column in FLAT_COLUMNS:
        if column == "symbol":
            continue
        assert Log.FIELD.field(column).nullable, column
        assert getattr(blank, column) is None, column


def test_every_flat_column_says_which_tag_it_is() -> None:
    """The comment a reader of the table sees is the other half of the mapping,
    and a column moved to another tag while still describing the old one is a
    lie the types cannot catch: `<49>` and `<56>` are both strings. The two
    `Event` declares say it in `fix:tag` instead, which is checked above."""
    for tag, column in FLAT:
        if tag in ENVELOPE_TAGS:
            continue
        assert f"<{tag}>" in Log.FIELD.field(column).description, column
