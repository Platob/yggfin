"""`unix_of`, `datetime_of` and `window_of`: one reading of an instant, and of a window."""

from __future__ import annotations

import datetime
import os
import re
from pathlib import Path

import pyarrow
import pytest
from yggdryl import IOBase
from yggdryl.fix import ULBRIDGE_ROWHEADER as CORE_ROWHEADER

from rekep.text import Message
from rekep.times import (
    EPOCH,
    ULBRIDGE_ROWHEADER,
    UTC,
    WINDOW,
    datetime_of,
    unix_of,
    where_within,
    window_of,
    within,
)

#: What this package calls two of the bracket's parts that the core's own
#: expression names after the bracket rather than after the column: the clock
#: the read consumes into `currunix`, and the level `logs.messages` keeps. A
#: capture reaches a column by being called what the column is called, so
#: these two are renames and the rest is the core's text -- the core's own
#: header dates nothing, because it names its clock `timestamp`.
RENAMED = {"timestamp": "mtime", "level": "loglevel"}

#: The one part of the bracket this package reads wider than the core's own
#: expression does, as `core: ours`. The core requires the bridge's
#: millisecond fraction, under a point, and admits the grouped micros; this
#: package makes the fraction optional and reads it under a comma too, which
#: is what `times.ISO` already declares a fraction may be spelled with. Stated
#: here so the rest of the bracket is still compared character for character.
WIDENED = {r"\d{2}:\d{2}:\d{2}\.\d{3}(?:_\d{3})?": r"\d{2}:\d{2}:\d{2}(?:[.,]\d{3}(?:_\d{3})?)?"}

#: 2026-08-14 09:30:00.123456 UTC, in the nanoseconds every `*unix` holds.
STAMP = 1_786_699_800_123_456_000
MIDNIGHT = 1_786_665_600_000_000_000
NEXT_MIDNIGHT = MIDNIGHT + 86_400_000_000_000


@pytest.mark.parametrize(
    "spelled",
    [
        "20260814-09:30:00.123456",
        "2026-08-14T09:30:00.123456",
        "2026-08-14 09:30:00.123456",
        "2026-08-14T09:30:00.123456Z",
        "2026-08-14T11:30:00.123456+02:00",
        "20260814093000123456",
        b"2026-08-14T09:30:00.123456",
    ],
)
def test_every_spelling_of_one_instant_reads_as_that_instant(spelled: str | bytes) -> None:
    """FIX writes one, ISO writes four, and a window may be configured in any."""
    assert unix_of(spelled) == STAMP


#: One instant, in every short spelling ISO 8601 allows and `fromisoformat`
#: only learned at 3.11: a basic date, a basic clock, a clock stopping at the
#: hour or the minute, a fraction of any width, a military `Z`, an offset of
#: whole hours, an offset with no colon in it, and any separator at all.
SHORT: tuple[tuple[str, str], ...] = (
    ("20260814T093000", "2026-08-14T09:30:00+00:00"),
    ("20260814T0930", "2026-08-14T09:30:00+00:00"),
    ("20260814T09", "2026-08-14T09:00:00+00:00"),
    ("2026-08-14T09:30:00Z", "2026-08-14T09:30:00+00:00"),
    ("2026-08-14T09:30:00z", "2026-08-14T09:30:00+00:00"),
    ("2026-08-14T11:30:00+02", "2026-08-14T09:30:00+00:00"),
    ("2026-08-14T11:30:00+0200", "2026-08-14T09:30:00+00:00"),
    ("2026-08-14T11:30+0200", "2026-08-14T09:30:00+00:00"),
    ("2026-08-14 11+02", "2026-08-14T09:00:00+00:00"),
    ("2026-08-14x09:30:00", "2026-08-14T09:30:00+00:00"),
    ("2026-08-14T09:30:00.1Z", "2026-08-14T09:30:00.100000+00:00"),
    ("2026-08-14T09:30:00.1234Z", "2026-08-14T09:30:00.123400+00:00"),
    ("2026-08-14T09:30:00.123456789Z", "2026-08-14T09:30:00.123456+00:00"),
    ("2026-08-14T09:30:00,1234+02:00", "2026-08-14T07:30:00.123400+00:00"),
    ("2026-W33", "2026-08-10T00:00:00+00:00"),
    ("2026W335", "2026-08-14T00:00:00+00:00"),
    ("2026-W33-5", "2026-08-14T00:00:00+00:00"),
)


@pytest.mark.parametrize(("spelled", "means"), SHORT, ids=[held for held, _ in SHORT])
def test_the_reading_is_the_module_s_and_not_the_interpreter_s(spelled: str, means: str) -> None:
    """`fromisoformat` learned every one of these at 3.11, and a window cannot
    mean one thing on one interpreter and another on the next -- so the module
    expands them itself rather than asking whichever parser it was imported on.

    The instant is asserted and not merely that one was read: a compact clock
    handed to `strptime` splits greedily, and `20260814T0930` read that way is
    nine minutes past nine rather than half past.
    """
    assert datetime_of(spelled) == datetime.datetime.fromisoformat(means)


@pytest.mark.parametrize("spelled", ["2026-08-14", "20260814", "2026/08/14", "14 Aug 2026"])
def test_a_day_is_its_own_midnight(spelled: str) -> None:
    assert unix_of(spelled) == MIDNIGHT


@pytest.mark.parametrize("spelled", ["2026-08-14", "20260814", datetime.date(2026, 8, 14)])
def test_upper_makes_a_whole_day_the_exclusive_end_of_it(spelled: object) -> None:
    """`end: 2026-08-14` has to mean all of the 14th, not nothing of it."""
    assert unix_of(spelled, upper=True) == NEXT_MIDNIGHT


def test_upper_leaves_an_instant_alone() -> None:
    """It says which day the bound ends, not that every bound moves a day."""
    assert unix_of("2026-08-14T09:30:00.123456", upper=True) == STAMP


def test_compact_fraction_uses_its_own_scale_and_microsecond_width() -> None:
    assert datetime_of("20260828135029258000") == datetime.datetime(
        2026, 8, 28, 13, 50, 29, 258000, tzinfo=UTC
    )
    assert datetime_of("20260828135029258123456") == datetime.datetime(
        2026, 8, 28, 13, 50, 29, 258123, tzinfo=UTC
    )


def test_a_naive_instant_is_read_as_utc_and_an_aware_one_is_converted() -> None:
    naive = datetime.datetime(2026, 8, 14, 9, 30, 0, 123456)
    aware = naive.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    assert unix_of(naive) == STAMP
    assert unix_of(aware) == STAMP - 2 * 3_600_000_000_000


def test_an_integer_is_already_the_nanoseconds_a_unix_column_holds() -> None:
    """One unit, stated: a magnitude test would read `time.time()` as 1970."""
    assert unix_of(STAMP) == STAMP
    assert datetime_of(STAMP) == datetime.datetime(2026, 8, 14, 9, 30, 0, 123456, tzinfo=UTC)


@pytest.mark.parametrize("named", ["now", "utcnow", "today", "yesterday", "tomorrow", "epoch"])
def test_a_configuration_may_name_an_instant_instead_of_spelling_one(named: str) -> None:
    found = datetime_of(named)
    assert found is not None and found.tzinfo is UTC
    assert datetime_of(named.upper()) is not None, "named instants fold case too"


def test_a_named_instant_is_read_when_it_is_read_and_not_when_it_was_written() -> None:
    """A schedule that says `utcnow` means the run, not the first one."""
    assert datetime_of("epoch") == datetime.datetime(1970, 1, 1, tzinfo=UTC)
    midnight = datetime_of("today")
    assert (midnight.hour, midnight.minute, midnight.second) == (0, 0, 0)
    assert datetime_of("tomorrow") - datetime_of("yesterday") == datetime.timedelta(days=2)


@pytest.mark.parametrize("value", [None, "", "   ", "garbage", "2026-13-45", True, object()])
def test_what_names_no_instant_is_none_rather_than_a_guess(value: object) -> None:
    """A window that cannot be read is not read; it is never read as the epoch."""
    assert unix_of(value) is None
    assert datetime_of(value) is None


def test_a_day_first_date_is_refused_rather_than_guessed_at() -> None:
    """`03/04/2026` is two dates, and picking one silently moves a window a month."""
    assert unix_of("03/04/2026") is None


def test_a_wrapped_value_is_asked_what_it_holds() -> None:
    """So a bound read out of a batch does not have to be unwrapped at the call site."""
    assert unix_of(pyarrow.scalar("2026-08-14")) == MIDNIGHT
    assert unix_of(pyarrow.scalar(STAMP, pyarrow.timestamp("ns"))) == STAMP


def test_the_bridge_row_header_is_the_layout_the_core_states() -> None:
    """The bracket this reads is the bracket the installed core reads, part
    for part, and every difference is declared -- what two of those parts are
    called, because a capture reaches its column by being called what the
    column is called, and how wide the clock reads. Compared against the
    constant the extension exports, so the check runs wherever the tests do."""
    held = CORE_ROWHEADER
    for core, ours in WIDENED.items():
        assert held.count(core) == 1, f"the core no longer spells {core}"
        held = held.replace(core, ours)
    renamed = re.sub(
        r"\(\?P<([A-Za-z]+)>",
        lambda found: f"(?P<{RENAMED.get(found[1], found[1])}>",
        held,
    )
    assert ULBRIDGE_ROWHEADER == renamed
    # A rename nobody made is a rename nobody needs: each one has to be a
    # capture the core actually states, or this table is stale.
    assert set(RENAMED) <= set(re.findall(r"\(\?P<([A-Za-z]+)>", CORE_ROWHEADER))


def test_every_bridge_capture_is_named_for_the_column_it_fills() -> None:
    """Spelled out here so a rename is a failing test and not a null column."""
    assert re.findall(r"\(\?P<([A-Za-z]+)>", ULBRIDGE_ROWHEADER) == [
        "mtime",
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    ]


@pytest.mark.parametrize(
    ("fraction", "settled"),
    [
        # What the bridge this package was written against writes.
        (".147", ".147000"),
        # The same clock under a comma locale, which the core's instant parser
        # reads and the old expression dropped.
        (",148", ".148000"),
        # A bridge that states seconds and stops.
        ("", ""),
        # The grouped micros some of this bridge's loggers append, which the
        # core reads as a digit group inside the fraction: fifteen lines of
        # the bundled capture spell their clock this way.
        (".524_315", ".524315"),
    ],
)
def test_the_bridge_clock_reads_every_fraction_the_core_parses(
    tmp_path, fraction: str, settled: str
) -> None:
    """A bracket the expression refuses is a line the header does not date: it
    takes its object's modification time with every capture empty, and reaches
    the walk with no session, context or sequence to fold on -- so the clock
    reads every fraction this bridge writes: absent, under a comma, and with
    the micros grouped under a `_`."""
    line = f"2026-08-14 00:05:01{fraction} [250] [ULBridge] (INFO) body\n"
    source = tmp_path / "bridge.log"
    source.write_bytes(line.encode())

    reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
    try:
        row = reader.read_all().to_pylist()[0]
    finally:
        reader.close()

    assert row["currunix"] == datetime.datetime.fromisoformat(f"2026-08-14 00:05:01{settled}+00:00")
    # A dated line is a matched bracket, so every capture beside it is filled.
    assert (row["msgthreadid"], row["msgpluginid"], row["loglevel"]) == (250, "ULBridge", "INFO")


@pytest.mark.parametrize("fraction", ["_147", ".", ";147", ".147258"])
def test_a_fraction_this_header_does_not_read_leaves_the_line_unmatched(
    tmp_path, fraction: str
) -> None:
    """What this header does not read, and the one outcome.

    `_` groups a fraction's digits in the core and never opens one, so
    matching `01_147` would hand the instant parser a value it refuses -- and
    a located refusal fails the whole batch the line arrived in, which is
    worse than not reading the clock. A fraction this bridge does not write --
    six digits straight on, a lone point, a stray separator -- is left to a
    header of its own, which is what `Message.text_options` takes one for.

    Unmatched, the line is not lost: it keeps its whole text as its body,
    states every capture null, and is dated by the object it was read from --
    its modification time, the one clock the read has left for it."""
    source = tmp_path / "bridge.log"
    source.write_bytes(f"2026-08-14 00:05:01{fraction} [250] [ULBridge] (INFO) body\n".encode())
    written = datetime.datetime(2026, 8, 14, 18, 0, tzinfo=UTC)
    os.utime(source, (written.timestamp(), written.timestamp()))

    reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
    try:
        row = reader.read_all().to_pylist()[0]
    finally:
        reader.close()

    assert row["currunix"] == written
    assert row["msgpluginid"] is None
    assert row["body"] == f"2026-08-14 00:05:01{fraction} [250] [ULBridge] (INFO) body"


# -- the window a run covers -------------------------------------------------


def test_a_window_named_nowhere_is_the_last_day_ending_now() -> None:
    before = datetime.datetime.now(UTC)
    lower, upper = window_of()
    after = datetime.datetime.now(UTC)

    assert WINDOW == datetime.timedelta(days=1)
    assert before <= upper <= after, "the end is the instant the window was read"
    assert upper - lower == WINDOW


def test_a_window_named_in_full_is_exactly_what_it_names() -> None:
    lower, upper = window_of("2026-08-14T09:30:00", "2026-08-14T10:30:00Z")
    assert lower == datetime.datetime(2026, 8, 14, 9, 30, tzinfo=UTC)
    assert upper == datetime.datetime(2026, 8, 14, 10, 30, tzinfo=UTC)


def test_a_whole_day_end_is_the_exclusive_end_of_that_day() -> None:
    """`end: 2026-08-14` covers all of the 14th, and a one-day default before it."""
    lower, upper = window_of(None, "2026-08-14")
    assert upper == datetime.datetime(2026, 8, 15, tzinfo=UTC)
    assert lower == datetime.datetime(2026, 8, 14, tzinfo=UTC)


def test_a_start_alone_reaches_forward_to_now() -> None:
    lower, upper = window_of("2026-08-14")
    assert lower == datetime.datetime(2026, 8, 14, tzinfo=UTC)
    assert upper > lower and upper.tzinfo is UTC


@pytest.mark.parametrize(
    ("start", "end", "said"),
    [
        ("garbage", None, "start='garbage' is not an instant"),
        (None, "2026-13-45", "end='2026-13-45' is not an instant"),
        ("2026-08-15", "2026-08-14", "is empty"),
        ("2026-08-14T10:00", "2026-08-14T10:00", "is empty"),
    ],
)
def test_a_window_that_names_no_interval_is_refused(start: str, end: str, said: str) -> None:
    """A run that would read nothing by construction is a configuration mistake."""
    with pytest.raises(ValueError, match=said):
        window_of(start, end)


def test_within_covers_the_half_open_interval_and_every_row_with_no_clock() -> None:
    # `end: 2026-08-14` is the exclusive end of the 14th, so this is the 14th alone.
    window = window_of("2026-08-14", "2026-08-14")
    assert window == (
        datetime.datetime(2026, 8, 14, tzinfo=UTC),
        datetime.datetime(2026, 8, 15, tzinfo=UTC),
    )
    values = pyarrow.array(
        [
            datetime.datetime(2026, 8, 13, 23, 59, 59, tzinfo=UTC),
            datetime.datetime(2026, 8, 14, tzinfo=UTC),
            datetime.datetime(2026, 8, 14, 12, tzinfo=UTC),
            datetime.datetime(2026, 8, 15, tzinfo=UTC),
            # The pin a read settles a record at where its handle has no
            # clock -- or a message that stated no sending clock -- and a
            # null for a column that admits one: neither states an instant,
            # so no window can place either and every window holds both.
            EPOCH,
            None,
        ],
        pyarrow.timestamp("us", tz="UTC"),
    )

    assert within(values, window).to_pylist() == [False, True, True, False, True, True]
    assert within(pyarrow.chunked_array([values]), window).to_pylist() == [
        False,
        True,
        True,
        False,
        True,
        True,
    ]


# -- the window as the read's own where -------------------------------------


#: The bundled capture, every line of which the shipped header dates.
FIXTURE = Path(__file__).resolve().parent / "data" / "ulbridge.log"


def test_the_window_is_the_reads_own_where_and_not_a_mask_over_its_answer() -> None:
    """The pushdown, shown to have happened rather than assumed: over one
    window, the read handed the clause answers fewer rows than the read
    handed none, and exactly the rows between the two bounds -- so a task
    reads the window's lines and never the rest. The decode itself still
    cuts every line: the clause is the record surface's."""
    window = window_of("2026-08-14", "2026-08-14T14:46:40")
    clause = where_within("currunix", window)
    assert str(clause) == (
        "currunix >= '2026-08-14T00:00:00+00:00' and currunix < '2026-08-14T14:46:40+00:00'"
    )
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        every = handle.read_arrow_reader(options=Message.text_options()).read_all()
        pushed = Message.text_options()
        pushed.filter = clause
        answered = handle.read_arrow_reader(options=pushed).read_all()
        cut = sum(1 for _ in handle.read_text_lines(options=pushed))
    finally:
        handle.close()

    clock = every.column("currunix")
    lower, upper = (pyarrow.scalar(bound, clock.type) for bound in window)
    between = every.filter(
        pyarrow.compute.and_(
            pyarrow.compute.greater_equal(clock, lower), pyarrow.compute.less(clock, upper)
        )
    )
    assert every.num_rows == cut == 144, "the decode yields every line either way"
    assert 0 < answered.num_rows < every.num_rows, "the read answered the window alone"
    assert answered.equals(between), "and exactly the rows between the two bounds"


def test_a_line_no_clock_dates_is_outside_every_window_but_the_epochs() -> None:
    """The clause names the two bounds and nothing else. A handle with no
    modification time -- a buffer nothing addressed -- dates a line the
    header did not match at the epoch, and a window that does not cover 1970
    reads it as what it is: a line that happened outside the window."""
    pushed = Message.text_options()
    pushed.filter = where_within("currunix", window_of("2026-08-14", "2026-08-14"))
    handle = IOBase.from_bytes(b"one physical line\n")
    try:
        every = handle.read_arrow_reader(options=Message.text_options()).read_all()
        answered = handle.read_arrow_reader(options=pushed).read_all()
    finally:
        handle.close()

    assert every.column("currunix").to_pylist() == [EPOCH]
    assert answered.num_rows == 0
    assert answered.schema.equals(every.schema, check_metadata=True)
