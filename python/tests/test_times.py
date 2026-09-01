"""`unix_of` and `datetime_of`: one reading of "an instant", whatever spelled it."""

from __future__ import annotations

import datetime

import pyarrow
import pytest

from rekep.times import UTC, datetime_of, unix_of

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
