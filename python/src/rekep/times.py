"""One reading of "an instant", whatever spelled it.

Every job takes a window, and every job was reading it for itself: five
notebooks each carried the same twelve lines turning `start` and `end` into
nanoseconds, and each accepted a slightly different set of spellings. This is
that reading, once -- so a window written `2026-08-14`, `20260814-09:30:00.123`
or `utcnow` means the same instant wherever it is configured.
"""

from __future__ import annotations

import datetime
import re
from typing import Any

UTC = datetime.UTC
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)

#: Instants a configuration may name instead of spelling. Read when the value
#: is read, not when the document is: a schedule that says `utcnow` means the
#: run, and a document parsed at import would freeze the first one forever.
NAMED: dict[str, Any] = {
    "now": lambda: datetime.datetime.now(UTC),
    "utcnow": lambda: datetime.datetime.now(UTC),
    "today": lambda: datetime.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0),
    "utctoday": lambda: datetime.datetime.now(UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    ),
    "yesterday": lambda: NAMED["today"]() - datetime.timedelta(days=1),
    "tomorrow": lambda: NAMED["today"]() + datetime.timedelta(days=1),
    "epoch": lambda: EPOCH,
}

#: Spellings `datetime.fromisoformat` does not read, in the order they are
#: tried. FIX's own `YYYYMMDD-HH:MM:SS[.ffffff]` leads because it is what a
#: capture carries; the rest are what schedulers and consoles print. Nothing
#: day-first is here on purpose: `03/04/2026` is two dates and guessing which
#: is how a window silently moves a month.
FORMATS: tuple[str, ...] = (
    "%Y%m%d-%H:%M:%S.%f",
    "%Y%m%d-%H:%M:%S",
    "%Y%m%d-%H:%M",
    "%Y%m%d%H%M%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%d %b %Y",
    "%d-%b-%Y",
    "%b %d %Y",
)

#: A value that names a whole day and no time within it. `upper` rolls one of
#: these to the next midnight, which is what makes `end: 2026-08-14` mean "all
#: of the 14th" rather than "nothing of it".
_DATE_ONLY = re.compile(r"^\d{4}([-/]?)\d{2}\1\d{2}$", re.ASCII)


def datetime_of(value: Any, *, upper: bool = False) -> datetime.datetime | None:
    """`value` as one aware UTC instant, or None when it names none.

    Reads a `datetime`, a `date`, a named instant, a FIX or ISO string, and
    anything that hands back one of those (`pyarrow` and `numpy` scalars,
    `pandas` stamps). A naive instant is read as UTC rather than as local time,
    because a pipeline that means local time says so and one that says nothing
    means the clock its data is stored in.

    `upper=True` treats a value naming a whole day as the exclusive end of it.
    """
    found = _instant(value)
    if found is None:
        return None
    if upper and _whole_day(value):
        found += datetime.timedelta(days=1)
    return found


def unix_of(value: Any, *, upper: bool = False) -> int | None:
    """`datetime_of`, in the whole nanoseconds since the epoch a `*unix` holds."""
    found = datetime_of(value, upper=upper)
    if found is None:
        return None
    return (found - EPOCH) // datetime.timedelta(microseconds=1) * 1_000


def _instant(value: Any) -> datetime.datetime | None:
    """`value` as an aware UTC instant, before any `upper` adjustment."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime.datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, (int, float)):
        # Nanoseconds, like every `*unix` column here. One unit, stated, rather
        # than a magnitude test that reads a seconds-since-epoch float as 1970.
        return EPOCH + datetime.timedelta(microseconds=int(value) / 1_000)
    if isinstance(value, (bytes, bytearray)):
        return _parsed(bytes(value).decode("utf-8", "replace"))
    if isinstance(value, str):
        return _parsed(value)
    unwrapped = _unwrapped(value)
    return None if unwrapped is None else _instant(unwrapped)


def _unwrapped(value: Any) -> Any:
    """What a wrapper hands back: a pyarrow scalar, a numpy or pandas stamp."""
    for method in ("as_py", "to_pydatetime", "item"):
        unwrap = getattr(value, method, None)
        if callable(unwrap):
            try:
                found = unwrap()
            except (TypeError, ValueError):
                continue
            if found is not value:
                return found
    return None


def _parsed(text: str) -> datetime.datetime | None:
    """One string as an instant: a named one, then ISO, then the spellings above."""
    stripped = text.strip()
    if not stripped:
        return None
    named = NAMED.get(stripped.lower())
    if named is not None:
        return named()
    try:
        return _aware(datetime.datetime.fromisoformat(stripped))
    except ValueError:
        pass
    for spelling in FORMATS:
        try:
            return _aware(datetime.datetime.strptime(stripped, spelling))
        except ValueError:
            continue
    return None


def _aware(found: datetime.datetime) -> datetime.datetime:
    """A parsed instant in UTC, reading a naive one as already being in it."""
    return found.replace(tzinfo=UTC) if found.tzinfo is None else found.astimezone(UTC)


def _whole_day(value: Any) -> bool:
    """Whether `value` names a day rather than an instant inside one."""
    if isinstance(value, datetime.datetime):
        return False
    if isinstance(value, datetime.date):
        return True
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", "replace")
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return bool(_DATE_ONLY.match(stripped)) or stripped.lower() in {
        "today",
        "utctoday",
        "yesterday",
        "tomorrow",
    }
