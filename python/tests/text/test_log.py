"""`Log`'s own contract; the parser that fills it is tested beside it."""

import pyarrow

from rekep import Field, Log
from rekep.market import Event, EventType
from rekep.market.event import HOUR

#: The envelope every event carries, then the four columns a log line adds.
ENVELOPE = [
    "unix",
    "hunix",
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
MESSAGE = ["category_id", "category_name", "fix_tags", "keyval"]


def test_a_log_line_is_an_event() -> None:
    """Which is what lets a parsed log be read beside the orders it describes."""
    assert issubclass(Log, Event)
    assert Log.FIELD.into_arrow_schema().names == ENVELOPE + LINE + MESSAGE


def test_the_envelope_is_the_same_one_every_other_event_carries() -> None:
    """Position included: a reader of the envelope must not need to know the shape."""
    assert Event.FIELD.names == ENVELOPE


def test_every_column_a_line_adds_is_required() -> None:
    """A line always has a file, a thread, a driver and a payload, even an empty one."""
    for name in LINE:
        assert not Log.FIELD.field(name).nullable, name


def test_every_column_is_documented() -> None:
    for member in Log.FIELD.fields:
        assert member.description, f"{member.name} has no description"
        assert "\n" not in member.description, f"{member.name} description is not one line"


def test_the_key_is_the_moment_and_the_line() -> None:
    """Two columns: a hash identifies the line, the time is what an engine prunes on."""
    assert Log.FIELD.primary_keys() == ["unix", "hash"]


def test_the_partition_is_the_hour_the_line_falls_in() -> None:
    """An identity partition on an integer, so every engine below reads it alike."""
    assert Log.FIELD.partition_keys() == {"hunix": "identity"}
    assert Log.FIELD.field("hunix").arrow_type == pyarrow.int64()


def test_every_unix_column_declares_its_unit() -> None:
    for name in ("unix", "hunix", "cunix", "runix", "eunix", "sunix", "prev_unix"):
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
    assert built.hunix == 3 * HOUR
    assert Log(unix=-1).hunix == -HOUR, "and it floors, either side of the epoch"


def test_the_schema_says_which_class_it_came_from() -> None:
    schema = Log.FIELD.into_arrow_schema()
    assert schema.metadata[b"name"] == b"Log"
    assert Field.from_arrow_schema(schema) == Log.FIELD


def test_a_row_round_trips_as_a_document() -> None:
    row = Log(
        url="a.txt",
        unix=2,
        hash=3,
        xhash=3,
        etype=EventType.ORDER,
        thread_name="t",
        driver_name="d",
        message="m",
    )
    assert Log.from_json(row.into_json()) == row
