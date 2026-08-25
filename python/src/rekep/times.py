"""One reading of "an instant", whatever spelled it.

Every job takes a window, and every job was reading it for itself: five
notebooks each carried the same twelve lines turning `start` and `end` into
nanoseconds, and each accepted a slightly different set of spellings. This is
that reading, once -- so a window written `2026-08-14`, `20260814-09:30:00.123`
or `utcnow` means the same instant wherever it is configured.
"""

from __future__ import annotations

import dataclasses
import datetime
import functools
import re
from typing import Any

UTC = datetime.UTC

#: The epoch, in the three shapes a caller needs it in -- and only here. It
#: had been declared in four modules in four types, which is four places for
#: one of them to be wrong: an aware instant to subtract from, the day a
#: `*unix` of zero falls on, and the proleptic Gregorian ordinal of that day,
#: which is what a date arithmetic that avoids `datetime` counts from.
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)
EPOCH_DATE = EPOCH.date()
EPOCH_ORDINAL = EPOCH_DATE.toordinal()

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


#: How an instant is spelled with a date, a clock and an optional fraction --
#: the three shapes a capture writes, declared once for both readings of them.
#:
#: One declaration because the set of accepted spellings is *one behavior*
#: even where the execution is two: this module reads a configuration value
#: with `strptime`, once per job, and `rekep.text.text_file` reads a column of
#: log-line stamps in Arrow kernels, once per line. The fast path cannot use
#: `strptime`, but it must not decide for itself what a stamp looks like, so
#: it derives its widths and its slicing offsets from these.
@dataclasses.dataclass(frozen=True)
class Stamp:
    """One accepted spelling of an instant, and where its parts sit in it."""

    name: str
    """What the shape is called where it is discussed."""

    #: The date and clock, without the fraction: anchored on both sides by
    #: whoever uses it, and bounded everywhere -- no nested quantifier, so
    #: neither engine backtracks over it.
    head: str

    #: `strptime` spelling of `head`, for the scalar reading.
    format: str

    #: `(start, stop)` of year, month, day, hour, minute and second, as
    #: character offsets into a stamp of this shape. Fixed for the shape
    #: whatever its fraction is, which is what makes slicing sound.
    offsets: tuple[tuple[int, int], ...]

    #: Where the fraction's first digit sits. The separator, where the shape
    #: writes one, is the character before it.
    fraction_at: int

    #: Which characters may separate the fraction from the seconds, empty
    #: where a shape runs the digits straight on. A class, not a spelling:
    #: one capture writes `01.147` and `01,147` in the same file. Whichever
    #: it is, it is one character.
    fraction_separator: str = ""

    #: Whether a separator may also sit *inside* the fraction. One capture
    #: writes `01.147_250`, because one capture is written by several loggers
    #: and they do not agree.
    split_fraction: bool = False

    #: Where this shape already writes `YYYY-MM-DD`, and where it already
    #: writes `HH:MM:SS` -- None where it spells them some other way and a
    #: reader has to put the separators in. A shape that writes them straight
    #: is copied rather than taken apart and rebuilt, which is two slices
    #: instead of six and a join.
    date_at: tuple[int, int] | None = None
    clock_at: tuple[int, int] | None = None

    @property
    def pattern(self) -> str:
        """The whole shape, fraction included, as one unanchored expression.

        A single optional fraction group of one to nine digits -- and for a
        shape that admits one, a separator inside it -- rather than a list of
        widths tried in turn.
        """
        digits = r"[0-9]{1,9}"
        fraction = rf"{digits}(?:[._,]{digits})?" if self.split_fraction else digits
        if not self.fraction_separator:
            return rf"{self.head}(?:{fraction})?"
        return rf"{self.head}(?:[{self.fraction_separator}]{fraction})?"

    @property
    def widths(self) -> tuple[int, ...]:
        """Every width this shape can be sliced at, shortest first.

        The fraction widths a slicing path can read from a fixed offset: none,
        millis, micros, nanos, and -- where the shape admits it -- millis and
        micros with a separator between them. A stamp of any other width is
        read rather than sliced, because the offsets after its fraction would
        be a guess.
        """
        found = [self.fraction_at - (1 if self.fraction_separator else 0)]
        found += [self.fraction_at + digits for digits in (3, 6, 9)]
        if self.split_fraction:
            found.append(self.fraction_at + 7)
        return tuple(sorted(found))

    def fraction_slices(self, width: int) -> tuple[tuple[int, int], ...]:
        """Where a stamp of `width` keeps its fraction digits, in order."""
        digits = width - self.fraction_at
        if digits <= 0:
            return ()
        if self.split_fraction and digits == 7:
            return ((self.fraction_at, self.fraction_at + 3), (self.fraction_at + 4, width))
        return ((self.fraction_at, width),)

    def micro_slices(self, width: int) -> tuple[tuple[tuple[int, int], ...], int]:
        """The first six fraction digits of a stamp of `width`, and the pad after them.

        Six because that is what a microsecond column stores: a logger that
        wrote nanoseconds has its last three dropped here rather than in a
        cast, and one that wrote milliseconds is padded to the same width. The
        pad is a count so a caller can append it as one more literal instead
        of building a second column.
        """
        kept: list[tuple[int, int]] = []
        digits = 0
        for start, stop in self.fraction_slices(width):
            if digits >= 6:
                break
            take = min(stop - start, 6 - digits)
            kept.append((start, start + take))
            digits += take
        return tuple(kept), 6 - digits

    @functools.cached_property
    def matcher(self) -> re.Pattern[str]:
        """This shape alone, anchored -- the scalar half of one rule."""
        return re.compile(rf"^{self.pattern}$", re.ASCII)

    def read(self, text: str) -> datetime.datetime | None:
        """One stamp of this shape as an aware UTC instant, or None if it is not one.

        Read off the same offsets the vectorized path slices at rather than
        through `strptime`, so the two agree by construction and neither has
        an opinion the other lacks. `strptime` also cannot read a compact
        stamp's fraction at all -- there is no separator in front of it to
        anchor `%f`.
        """
        if self.matcher.match(text) is None:
            return None
        year, month, day, hour, minute, second = (
            int(text[start:stop]) for start, stop in self.offsets
        )
        digits = _DIGITS.sub("", text[self.fraction_at :])
        # A fraction's scale is its own width: `.5` is half a second and
        # `.000001` is one microsecond. Padding to six and reading it as an
        # integer is that -- and finer than a microsecond is dropped, which is
        # what the microsecond column downstream stores.
        micros = int(digits.ljust(9, "0")[:6]) if digits else 0
        try:
            return datetime.datetime(year, month, day, hour, minute, second, micros, tzinfo=UTC)
        except ValueError:
            # A shape that parses but is not a date: `20260230`, `99:99:99`.
            return None


#: `2026-08-14 00:05:01.167`, and the same with micros written `.167250` or
#: `.167_250`. What a rendered trading log prints.
ISO = Stamp(
    name="iso",
    head=r"[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}",
    format="%Y-%m-%d %H:%M:%S",
    offsets=((0, 4), (5, 7), (8, 10), (11, 13), (14, 16), (17, 19)),
    fraction_at=20,
    fraction_separator=".,",
    split_fraction=True,
    date_at=(0, 10),
    clock_at=(11, 19),
)

#: `20260824-10:00:01.123`: the spelling the FIX standard fixes for
#: `UTCTimestamp`, which is what a bridge writes when it stamps a line the way
#: it stamps a field.
FIX = Stamp(
    name="fix",
    head=r"[0-9]{8}-[0-9]{2}:[0-9]{2}:[0-9]{2}",
    format="%Y%m%d-%H:%M:%S",
    offsets=((0, 4), (4, 6), (6, 8), (9, 11), (12, 14), (15, 17)),
    fraction_at=18,
    fraction_separator=".",
    clock_at=(9, 17),
)

#: `20260824100001123`: no separators at all, which is what a logger writes
#: when it was told to keep lines short.
COMPACT = Stamp(
    name="compact",
    head=r"[0-9]{14}",
    format="%Y%m%d%H%M%S",
    offsets=((0, 4), (4, 6), (6, 8), (8, 10), (10, 12), (12, 14)),
    fraction_at=14,
)

#: Every shape an instant may be spelled in, in the order a reader tries
#: them. Most separated first, so the
#: one that commits to the most characters is matched before the one that
#: commits to fewest: a compact stamp and a FIX one share a width, and only
#: where the separators sit tells them apart.
SHAPES: tuple[Stamp, ...] = (ISO, FIX, COMPACT)

#: Spellings `datetime.fromisoformat` does not read, in the order they are
#: tried. The three shapes above lead, because they are what a capture
#: carries; the rest are what schedulers and consoles print. Nothing day-first
#: is here on purpose: `03/04/2026` is two dates and guessing which is how a
#: window silently moves a month.
FORMATS: tuple[str, ...] = (
    *(f"{stamp.format}.%f" for stamp in SHAPES),
    *(stamp.format for stamp in SHAPES),
    "%Y%m%d-%H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%d %b %Y",
    "%d-%b-%Y",
    "%b %d %Y",
)

#: Everything that is not a digit, for reading a fraction whatever a logger
#: wrote inside it.
_DIGITS = re.compile(r"[^0-9]", re.ASCII)

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
    for stamp in SHAPES:
        found = stamp.read(stripped)
        if found is not None:
            return found
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
