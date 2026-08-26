"""The FIX datatype projection: what a FIX field's values are in Arrow."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import functools
import json
import re
from collections.abc import Mapping
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, scalar
from rekep.fields.field import arrow_type_for
from rekep.times import EPOCH_ORDINAL as _EPOCH_ORDINAL

#: FIX datatype -> Arrow type, keyed lowercase because the spellings drift
#: across versions (`Boolean`/`boolean`, `MultipleValueString` before FIX 4.4,
#: `MultipleStringValue` after). Two deliberate widenings:
#:
#: - **`char` is a string, not one character.** The standard caps it at one,
#:   real feeds do not -- a value that outgrew its type is still a value, and
#:   a fixed width would truncate it silently. A string holds both.
#: - FIX `int` is its 32-bit protocol scalar. Lengths, counts and sequence
#:   numbers remain `int64`; Python's `int` annotation also remains `int64`.
#: - An unparameterized `array` stays text: no item type was declared, so a
#:   list projection would invent structure the wire did not promise.
#:
#: The time-zoned types (`TZTimestamp`, `TZTimeOnly`) stay strings: their
#: offset is part of the value, and a naive Arrow type would drop it.
FIX_SCALARS: dict[str, pyarrow.DataType] = {
    "int": pyarrow.int32(),
    "integer": pyarrow.int32(),
    "int32": pyarrow.int32(),
    "bigint": pyarrow.int64(),
    "long": pyarrow.int64(),
    "int64": pyarrow.int64(),
    "length": pyarrow.int64(),
    "tagnum": pyarrow.int64(),
    "seqnum": pyarrow.int64(),
    "numingroup": pyarrow.int64(),
    "dayofmonth": pyarrow.int64(),
    "float": pyarrow.float64(),
    "double": pyarrow.float64(),
    "real": pyarrow.float64(),
    "number": pyarrow.float64(),
    "qty": pyarrow.float64(),
    "price": pyarrow.float64(),
    "priceoffset": pyarrow.float64(),
    "amt": pyarrow.float64(),
    "percentage": pyarrow.float64(),
    "char": pyarrow.string(),
    "boolean": pyarrow.bool_(),
    "string": pyarrow.string(),
    "array": pyarrow.string(),
    "list": pyarrow.string(),
    "text": pyarrow.string(),
    "varchar": pyarrow.string(),
    "nvarchar": pyarrow.string(),
    "json": pyarrow.string(),
    "uuid": pyarrow.string(),
    "multiplevaluestring": pyarrow.string(),
    "multiplestringvalue": pyarrow.string(),
    "multiplecharvalue": pyarrow.string(),
    "country": pyarrow.string(),
    "currency": pyarrow.string(),
    "exchange": pyarrow.string(),
    "language": pyarrow.string(),
    "monthyear": pyarrow.string(),
    "tenor": pyarrow.string(),
    "pattern": pyarrow.string(),
    "xmldata": pyarrow.string(),
    # FIX 5.0 SP2 extension packs number XML identifiers and the references
    # between them; both are names, and a name is text.
    "xid": pyarrow.string(),
    "xidref": pyarrow.string(),
    "data": pyarrow.binary(),
    "binary": pyarrow.binary(),
    "bytes": pyarrow.binary(),
    "utctimestamp": pyarrow.timestamp("ns"),
    "datetime": pyarrow.timestamp("ns"),
    "timestamp": pyarrow.timestamp("ns"),
    "time": pyarrow.timestamp("ns"),
    "utcdateonly": pyarrow.date32(),
    "utcdate": pyarrow.date32(),
    "date": pyarrow.date32(),
    "localmktdate": pyarrow.date32(),
    "utctimeonly": pyarrow.time64("ns"),
    "localmkttime": pyarrow.time64("ns"),
    "tztimestamp": pyarrow.string(),
    "tztimeonly": pyarrow.string(),
    # The dictionary's own slips, which a scrape still meets on the older
    # versions' pages even though a record keeps the newest spelling. They are
    # here because the fallback is wrong for them -- a quantity read as text, a
    # day of month read as text, a date read as text -- and its other slips
    # (`Stirng`, `month`) land on a string either way.
    "quantity": pyarrow.float64(),  # RatioQty, in 4.2 and 4.3
    "day": pyarrow.int64(),  # MaturityDay, in 4.1
    "localmmktdate": pyarrow.date32(),  # LegFutSettDate, in 4.3
}

#: What a FIX Boolean accepts, beyond the `Y`/`N` the standard writes: real
#: feeds and the tooling around them print flags in whatever their locale and
#: logger felt like, so the reading is deliberately generous -- both cases,
#: the common English words, digits, and the yes/no of the locales seen in
#: trading logs. Anything in neither set is not a boolean and reads as null
#: rather than as a guess.
TRUE_WORDS = frozenset({"y", "yes", "true", "t", "1", "on", "oui", "si", "ja", "da"})
FALSE_WORDS = frozenset({"n", "no", "false", "f", "0", "off", "non", "nein", "nej"})


def arrow_type_of(datatype: str | None) -> pyarrow.DataType:
    """The Arrow type a FIX datatype stores as; an unknown one is a string.

    A string, because that is what the wire carries: an unknown datatype --
    a new EP, a vendor extension, a typo in a dictionary -- must not make a
    field unrepresentable when every FIX value is representable as text.
    """
    if not datatype:
        return pyarrow.string()
    return FIX_SCALARS.get(datatype.strip().lower(), pyarrow.string())


def declared_arrow_type(text: str) -> pyarrow.DataType | None:
    """The type `text` names, in either spelling a document may use; None for neither.

    Arrow's own first -- `date32[day]`, `timestamp[us, tz=UTC]` -- because
    that is what a dumped schema writes and so what a reader of one reaches
    for, and it says the unit and the zone where a FIX datatype only implies
    them. A FIX datatype (`UTCDateOnly`) is the alias it is.
    """
    if not text:
        return None
    try:
        return arrow_type_for(text)
    except (KeyError, ValueError):
        return FIX_SCALARS.get(text.strip().lower())


def fix_field(
    name: str,
    tag: int | str,
    datatype: str | None = None,
    *,
    description: str | None = None,
    version: str | None = None,
    values: Mapping[str, str] | None = None,
    metadata: Mapping[str, str] | None = None,
) -> Field:
    """One FIX field as a generic `Field`, its FIX identity under `fix:` keys.

    Nullable on purpose: whether a FIX field is required is a property of each
    *message* that carries it, not of the field, so the field itself must
    admit absence. The tag, the FIX datatype, the version it was read from and
    its enumerated values all land in the `fix` protocol's metadata, where
    `field.fix["tag"]` reads them back without the prefix.
    """
    built = Field(name=name, arrow_type=arrow_type_of(datatype), nullable=True, metadata=metadata)
    if description:
        built.description = description
    fix = built.fix
    fix["tag"] = str(int(tag))
    if datatype:
        fix["type"] = datatype
    if version:
        fix["version"] = version
    if values:
        fix["values"] = json.dumps(dict(values), separators=(",", ":"))
    return built


@scalar
class FieldRule(Convertible):
    """How one field's values read, whatever a dictionary says about it.

    A job declares these; nothing here is compiled in. One rule reaches every
    reading of the field it names, because every one of them goes through
    `FixCodec.tag_field` and `cast_arrow_fix`.
    """

    field: str = ""
    """The field: a tag (`60`), a canonical name, or a rendered key."""

    type: str = ""
    """Arrow type its column stores, as Arrow spells one -- `date32[day]`,
    `timestamp[us, tz=UTC]`. A FIX datatype (`UTCDateOnly`) is accepted and
    normalizes to what it projects to, so a dumped rule always states its unit
    and its zone. Empty leaves the dictionary's type alone."""

    values: dict[str, str] = dataclasses.field(default_factory=dict)
    """`{what a feed writes: what it means}`, folded like the dictionary's own."""

    def __post_init__(self) -> None:
        """Refuse a rule that names nothing, and normalize the type it names."""
        if not self.field:
            raise ValueError("a field rule names no field")
        if not self.type:
            return
        found = declared_arrow_type(self.type)
        if found is None:
            raise ValueError(f"{self.type!r} is neither an Arrow type nor a FIX datatype")
        # Held as Arrow spells it, so a rule read back says the unit and the
        # zone whichever spelling wrote it: `date` dumps as `date32[day]`.
        self.type = str(found)

    @functools.cached_property
    def arrow_type(self) -> pyarrow.DataType | None:
        """`type` as Arrow holds it; None where the rule only translates values."""
        return declared_arrow_type(self.type)

    @functools.cached_property
    def folded(self) -> str:
        """`field` as a spelling is matched: case and nothing else."""
        return self.field.strip().lower()

    @property
    def tag(self) -> int | None:
        """The tag `field` spells outright, or None where it spells a name."""
        text = self.field.strip()
        return int(text) if text.isascii() and text.isdigit() else None

    def applied(self, declared: Field | None, name: str) -> Field | None:
        """`declared` read this rule's way, or a field of its own where none is."""
        arrow_type = self.arrow_type
        if arrow_type is None:
            return declared
        if declared is None:
            return Field(name=name, arrow_type=arrow_type, nullable=True)
        if declared.arrow_type.equals(arrow_type):
            return declared
        return dataclasses.replace(declared, arrow_type=arrow_type)


@scalar
class FieldRules(Convertible):
    """The field readings a job declares, resolved against its own dictionary."""

    rules: list[FieldRule] = dataclasses.field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.rules)

    def __iter__(self) -> Any:
        return iter(self.rules)


def cast_arrow_bool(array: Any) -> Any:
    """A column of FIX/log flag spellings as Arrow booleans, in kernels.

    One lowercase pass and two hash lookups per batch: a value in
    `TRUE_WORDS` is true, one in `FALSE_WORDS` is false, and anything else --
    including empty text -- is null, because inventing a truth value for
    `"maybe"` is how a flag column starts lying. A column that is already
    boolean comes back untouched.
    """
    if isinstance(array, pyarrow.ChunkedArray):
        return pyarrow.chunked_array([cast_arrow_bool(chunk) for chunk in array.chunks])
    if pyarrow.types.is_boolean(array.type):
        return array
    compute = pyarrow.compute
    lowered = compute.utf8_lower(
        compute.utf8_trim_whitespace(array.cast(pyarrow.string(), safe=False))
    )
    truthy = compute.is_in(lowered, value_set=pyarrow.array(sorted(TRUE_WORDS)))
    falsy = compute.is_in(lowered, value_set=pyarrow.array(sorted(FALSE_WORDS)))
    return compute.if_else(
        truthy,
        pyarrow.scalar(True),
        compute.if_else(falsy, pyarrow.scalar(False), pyarrow.scalar(None, pyarrow.bool_())),
    )


# -- reading a value ----------------------------------------------------------

#: A FIX timestamp, date or time-of-day, in one pattern. The standard fixes
#: `UTCTimestamp` as `YYYYMMDD-HH:MM:SS[.sss...]`, `UTCDateOnly` as `YYYYMMDD`
#: and `UTCTimeOnly` as `HH:MM:SS[.sss...]`; `T` and a space are admitted
#: because logs rewrite the separator, and a trailing `Z` because feeds add
#: one. Both halves optional, so one pattern reads all three.
#:
#: Two bridge spellings are admitted beside the standard's. A colon-free
#: clock (`094510`, `20260814-094510.250`) lands in `compact`, whole -- six
#: digits or nothing, so a bare `0945` stays unreadable rather than becoming
#: a guess. A trailing zone offset (`-0400`, `+02:00`, a bridge's `-0400s`)
#: lands in the `z*` groups and is *applied to a clock*, where `Z` is a
#: no-op; on a date-only value it is a calendar label and moves nothing --
#: which is also what a four-digit tail after a bare date reads as.
#:
#: One source string for both engines: `re` reads a value, RE2 reads a column,
#: and the two are contracted to agree like every other pattern in this package.
STAMP_PATTERN = (
    r"^[ \t]*(?:(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2}))?"
    r"(?:[-T ]?(?P<hour>\d{2})(?::(?P<minute>\d{2})(?::(?P<second>\d{2}))?|(?P<compact>\d{4}))"
    r"(?:\.(?P<fraction>\d{1,9}))?)?"
    r"[ \t]*(?:Z|(?P<zsign>[+-])(?P<zhour>\d{2}):?(?P<zminute>\d{2})s?)?[ \t]*$"
)
_STAMP = re.compile(STAMP_PATTERN, re.ASCII)
_FULL_STAMP_PATTERN = r"^[0-9]{8}-[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?$"

#: The epoch as a proleptic Gregorian ordinal, from the one module that
#: declares it. Re-exported here because a reader of a FIX date reaches for it
#: beside the datatypes rather than beside the configuration windows.
EPOCH_ORDINAL = _EPOCH_ORDINAL

NANOS = 1_000_000_000
SECONDS_A_DAY = 86_400
_A_DAY = SECONDS_A_DAY * NANOS

#: Widths that cannot overflow the target: Arrow *raises* on a string it cannot
#: parse, and a raise mid-batch loses a capture over one malformed field.
_DECIMAL = r"^[+-]?(?:[0-9]{1,17}(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]{1,3})?$"


@functools.lru_cache(maxsize=8192)
def unix_of(text: str | None, day: int | None = None) -> int | None:
    """A FIX timestamp, date or time-of-day as nanoseconds since the epoch, UTC."""
    if not text:
        return None
    match = _STAMP.match(text)
    if match is None:
        return None
    year, month, dayof, hour, minute, second, compact, fraction = match.group(
        "year", "month", "day", "hour", "minute", "second", "compact", "fraction"
    )
    if compact:
        minute, second = compact[:2], compact[2:]
    if year is None and hour is None:
        return None
    if year is None:
        base = day - day % _A_DAY if day is not None else 0
    else:
        try:
            ordinal = datetime.date(int(year), int(month), int(dayof)).toordinal()
        except ValueError:
            return None
        base = (ordinal - EPOCH_ORDINAL) * _A_DAY
    offset = _zone_offset(match)
    if offset is None:
        return None
    if hour is None:
        # A zone suffix on a date names the calendar the day is in; it moves
        # no clock, and subtracting it would land east-of-UTC dates on the
        # previous civil day.
        return base
    hours, minutes, secs = int(hour), int(minute), int(second) if second else 0
    # Range-checked, because `\d{2}` is not: `99:99:99` parsed as a shape and
    # came out four days past midnight, which is a plausible-looking instant
    # and a wrong one. `60` is allowed -- the standard admits a leap second.
    if hours > 23 or minutes > 59 or secs > 60:
        return None
    # A fraction's scale is its own width: `.5` is half a second, `.000000001`
    # is one nanosecond. Padding to nine and reading it as an integer is that.
    nanos = int(fraction.ljust(9, "0")) if fraction else 0
    return base + (hours * 3600 + minutes * 60 + secs) * NANOS + nanos - offset


def _zone_offset(match: re.Match[str]) -> int | None:
    """A trailing zone offset in nanoseconds, zero when absent, None when absurd.

    Applied, where `Z` is a no-op: `09:30:00-04:00` is 13:30 UTC. The bounds
    are the civil ones -- no zone is more than 14 hours out -- so `-9945`
    stays unreadable rather than becoming an instant nobody sent.
    """
    zsign, zhour, zminute = match.group("zsign", "zhour", "zminute")
    if not zsign:
        return 0
    hours, minutes = int(zhour), int(zminute)
    if hours > 14 or minutes > 59:
        return None
    seconds = hours * 3600 + minutes * 60
    return (-seconds if zsign == "-" else seconds) * NANOS


def scalar_fix_temporal(
    text: str, arrow_type: pyarrow.DataType
) -> datetime.datetime | datetime.date | datetime.time | None:
    """Read one FIX temporal without starting an Arrow kernel pipeline."""
    nanos = unix_of(text)
    if nanos is None:
        return None
    kinds = pyarrow.types
    if kinds.is_date(arrow_type):
        days = nanos // _A_DAY
        try:
            return datetime.date.fromordinal(EPOCH_ORDINAL + days)
        except ValueError:
            return None

    divisor = {"s": NANOS, "ms": 1_000_000, "us": 1_000, "ns": 1}[arrow_type.unit]
    if kinds.is_time(arrow_type):
        canonical = (nanos % _A_DAY) // divisor * divisor
        _, within_day = divmod(canonical, _A_DAY)
        seconds, fraction = divmod(within_day, NANOS)
        hour, remainder = divmod(seconds, 3_600)
        minute, second = divmod(remainder, 60)
        return datetime.time(hour, minute, second, fraction // 1_000)

    # Arrow narrows negative timestamps toward zero. Preserve that behavior
    # before checking the destination's signed int64 storage range.
    units = nanos // divisor if nanos >= 0 else -((-nanos) // divisor)
    if not -(1 << 63) <= units < 1 << 63:
        return None
    canonical = units * divisor
    days, within_day = divmod(canonical, _A_DAY)
    seconds, fraction = divmod(within_day, NANOS)
    hour, remainder = divmod(seconds, 3_600)
    minute, second = divmod(remainder, 60)
    try:
        day = datetime.date.fromordinal(EPOCH_ORDINAL + days)
    except ValueError:
        return None
    return datetime.datetime(
        day.year,
        day.month,
        day.day,
        hour,
        minute,
        second,
        fraction // 1_000,
    )


def cast_arrow_fix(values: Any, arrow_type: pyarrow.DataType) -> Any:
    """A column of FIX text as the type its field declares, in kernels.

    The other half of `FixCodec.tag_field`: that says what a tag *is*, this
    reads a column of it. Nothing is guessed and nothing raises -- a value the
    type cannot hold is null, because a batch that died on one malformed field
    would lose every line beside it.
    """
    kinds = pyarrow.types
    if isinstance(values, pyarrow.ChunkedArray):
        chunks = [cast_arrow_fix(chunk, arrow_type) for chunk in values.chunks]
        return pyarrow.chunked_array(chunks, type=arrow_type)
    if values.type.equals(arrow_type):
        return values
    if len(values) and values.null_count == len(values):
        # A session field no message in this batch carried, which is most of
        # them on most batches. Nothing to read, and the kernels below would
        # run a regex over a column that is entirely absent.
        return pyarrow.nulls(len(values), arrow_type)
    if kinds.is_boolean(arrow_type):
        return cast_arrow_bool(values).cast(arrow_type)
    text = pyarrow.compute.utf8_trim_whitespace(values.cast(pyarrow.string(), safe=False))
    if kinds.is_temporal(arrow_type):
        return _cast_arrow_stamp(text, arrow_type)
    if kinds.is_integer(arrow_type):
        return _cast_arrow_integer(text, arrow_type)
    if kinds.is_floating(arrow_type) or kinds.is_decimal(arrow_type):
        return _only(text, _DECIMAL).cast(arrow_type, safe=False)
    return text.cast(arrow_type, safe=False)


def _only(text: Any, pattern: str) -> Any:
    """`text`, null wherever it does not match -- so the cast after cannot raise."""
    compute = pyarrow.compute
    matched = compute.fill_null(compute.match_substring_regex(text, pattern), False)
    return compute.if_else(matched, text, pyarrow.scalar(None, pyarrow.string()))


def _cast_arrow_integer(text: Any, arrow_type: pyarrow.DataType) -> Any:
    """Read the complete target integer range while nulling overflow per row."""
    compute = pyarrow.compute
    # Decimal128 is the checked staging type Arrow's integer parser lacks: it
    # admits every uint64 spelling, then a mask removes values the target cannot hold.
    readable = _only(text, r"^[+-]?[0-9]{1,20}$")
    readable = compute.replace_substring_regex(readable, r"^\+", "")
    staging = pyarrow.decimal128(21, 0)
    values = readable.cast(staging)
    bits = arrow_type.bit_width
    signed = pyarrow.types.is_signed_integer(arrow_type)
    lower = -(1 << (bits - 1)) if signed else 0
    upper = (1 << (bits - int(signed))) - 1
    inside = compute.fill_null(
        compute.and_(
            compute.greater_equal(values, pyarrow.scalar(decimal.Decimal(lower), staging)),
            compute.less_equal(values, pyarrow.scalar(decimal.Decimal(upper), staging)),
        ),
        False,
    )
    safe = compute.if_else(inside, values, pyarrow.scalar(decimal.Decimal(0), staging)).cast(
        arrow_type
    )
    return compute.if_else(inside, safe, pyarrow.scalar(None, arrow_type))


def _cast_arrow_stamp(text: Any, arrow_type: pyarrow.DataType) -> Any:
    """A FIX time column parsed without letting one malformed row stop its batch."""
    compute = pyarrow.compute
    canonical = compute.fill_null(compute.match_substring_regex(text, _FULL_STAMP_PATTERN), True)
    if compute.all(canonical).as_py():
        return _cast_arrow_full_stamp(text, arrow_type)
    return _cast_arrow_stamp_general(text, arrow_type)


def _cast_arrow_full_stamp(text: Any, arrow_type: pyarrow.DataType) -> Any:
    """A homogeneous column of canonical full FIX timestamps."""
    compute = pyarrow.compute
    integer = pyarrow.int64()

    def number(start: int, stop: int) -> Any:
        return compute.cast(compute.utf8_slice_codeunits(text, start, stop), integer)

    def remainder(values: Any, divisor: int) -> Any:
        return compute.subtract(values, compute.multiply(compute.divide(values, divisor), divisor))

    year, month, day = number(0, 4), number(4, 6), number(6, 8)
    hour, minute, second = number(9, 11), number(12, 14), number(15, 17)
    leap_year = compute.or_(
        compute.equal(remainder(year, 400), 0),
        compute.and_(
            compute.equal(remainder(year, 4), 0),
            compute.not_equal(remainder(year, 100), 0),
        ),
    )
    february = compute.equal(month, 2)
    thirty_day = compute.is_in(month, value_set=pyarrow.array([4, 6, 9, 11], integer))
    month_days = compute.subtract(
        compute.subtract(pyarrow.scalar(31, integer), compute.cast(thirty_day, integer)),
        compute.multiply(compute.cast(february, integer), 3),
    )
    month_days = compute.add(
        month_days,
        compute.cast(compute.and_(february, leap_year), integer),
    )
    valid = compute.fill_null(
        compute.and_(
            compute.and_(
                compute.and_(compute.greater(year, 0), compute.greater_equal(month, 1)),
                compute.less_equal(month, 12),
            ),
            compute.and_(
                compute.and_(compute.greater_equal(day, 1), compute.less_equal(day, month_days)),
                compute.and_(
                    compute.and_(compute.less_equal(hour, 23), compute.less_equal(minute, 59)),
                    compute.less_equal(second, 60),
                ),
            ),
        ),
        False,
    )

    # Howard Hinnant's civil-date transform gives exact proleptic Gregorian
    # epoch days without entering timestamp[ns], whose range ends in 2262.
    adjusted_year = compute.subtract(year, compute.cast(compute.less_equal(month, 2), integer))
    era = compute.divide(adjusted_year, 400)
    year_of_era = compute.subtract(adjusted_year, compute.multiply(era, 400))
    shifted_month = compute.add(
        month,
        compute.if_else(
            compute.greater(month, 2),
            pyarrow.scalar(-3, integer),
            pyarrow.scalar(9, integer),
        ),
    )
    day_of_year = compute.add(
        compute.divide(compute.add(compute.multiply(shifted_month, 153), 2), 5),
        compute.subtract(day, 1),
    )
    day_of_era = compute.add(
        compute.add(
            compute.subtract(
                compute.add(compute.multiply(year_of_era, 365), compute.divide(year_of_era, 4)),
                compute.divide(year_of_era, 100),
            ),
            compute.divide(year_of_era, 400),
        ),
        day_of_year,
    )
    epoch_days = compute.subtract(compute.add(compute.multiply(era, 146_097), day_of_era), 719_468)
    seconds = compute.add(
        compute.multiply(epoch_days, SECONDS_A_DAY),
        compute.add(
            compute.multiply(hour, 3_600),
            compute.add(compute.multiply(minute, 60), second),
        ),
    )
    fraction = compute.utf8_slice_codeunits(text, 18, 27)
    fraction = compute.if_else(
        compute.equal(compute.binary_length(fraction), 0), pyarrow.scalar("0"), fraction
    )
    nanos = compute.cast(compute.utf8_rpad(fraction, 9, "0"), pyarrow.int64())
    return _temporal(seconds, nanos, valid, arrow_type)


def _cast_arrow_stamp_general(text: Any, arrow_type: pyarrow.DataType) -> Any:
    """All admitted FIX temporal spellings."""
    compute = pyarrow.compute
    parts = compute.extract_regex(text, STAMP_PATTERN)

    def part(name: str, default: str) -> Any:
        got = compute.struct_field(parts, name)
        empty = compute.fill_null(compute.equal(compute.binary_length(got), 0), True)
        return compute.if_else(empty, pyarrow.scalar(default), got)

    def given(name: str) -> Any:
        got = compute.struct_field(parts, name)
        return compute.fill_null(compute.greater(compute.binary_length(got), 0), False)

    # A row that matched but said neither a date nor a time is not a stamp --
    # the empty string matches this pattern, every group of it empty.
    present = compute.or_(given("year"), given("hour"))
    date = compute.binary_join_element_wise(
        part("year", "1970"), part("month", "01"), part("day", "01"), "-"
    )
    # A colon-free clock carries its minute and second in `compact`, whole.
    compact = given("compact")
    minute = compute.if_else(
        compact,
        compute.utf8_slice_codeunits(part("compact", "0000"), 0, 2),
        part("minute", "00"),
    )
    second = compute.if_else(
        compact,
        compute.utf8_slice_codeunits(part("compact", "0000"), 2, 4),
        part("second", "00"),
    )
    leap = compute.equal(second, "60")
    clock = compute.binary_join_element_wise(
        part("hour", "00"),
        minute,
        compute.if_else(leap, pyarrow.scalar("59"), second),
        ":",
    )
    stamp = compute.binary_join_element_wise(date, clock, "T")
    parsed = compute.strptime(stamp, format="%Y-%m-%dT%H:%M:%S", unit="s", error_is_null=True)
    # `strptime` normalizes impossible civil dates and clocks. A round trip
    # distinguishes that normalization from a value the wire actually carried.
    canonical = compute.strftime(parsed, format="%Y-%m-%dT%H:%M:%S")
    # A trailing zone offset is applied, where `Z` is a no-op -- and bounded
    # by the civil ones, so `-9945` stays unreadable rather than becoming an
    # instant nobody sent.
    zone_hours = compute.cast(part("zhour", "0"), pyarrow.int64())
    zone_minutes = compute.cast(part("zminute", "0"), pyarrow.int64())
    zone_ok = compute.or_(
        compute.invert(given("zsign")),
        compute.and_(compute.less_equal(zone_hours, 14), compute.less_equal(zone_minutes, 59)),
    )
    zone_seconds = compute.add(
        compute.multiply(zone_hours, 3_600), compute.multiply(zone_minutes, 60)
    )
    zone_seconds = compute.if_else(
        compute.equal(part("zsign", "+"), "-"),
        compute.negate_checked(zone_seconds),
        zone_seconds,
    )
    # A zone suffix on a date-only value names the calendar, not a clock.
    zone_seconds = compute.if_else(given("hour"), zone_seconds, pyarrow.scalar(0, pyarrow.int64()))
    valid = compute.fill_null(
        compute.and_(
            compute.and_(
                compute.and_(present, compute.equal(canonical, stamp)),
                compute.or_(
                    compute.invert(given("year")),
                    compute.not_equal(part("year", "1970"), "0000"),
                ),
            ),
            zone_ok,
        ),
        False,
    )
    seconds = compute.add(parsed.cast(pyarrow.int64()), compute.cast(leap, pyarrow.int64()))
    seconds = compute.subtract(seconds, zone_seconds)
    fraction = compute.cast(compute.utf8_rpad(part("fraction", "0"), 9, "0"), pyarrow.int64())

    return _temporal(seconds, fraction, valid, arrow_type)


def _temporal(seconds: Any, fraction: Any, valid: Any, arrow_type: pyarrow.DataType) -> Any:
    """Parsed seconds and nanoseconds as the temporal type asked for."""
    compute = pyarrow.compute
    kinds = pyarrow.types
    zero = pyarrow.scalar(0, pyarrow.int64())
    safe_seconds = compute.if_else(valid, seconds, zero)
    if kinds.is_date(arrow_type):
        stamps = compute.if_else(valid, safe_seconds, pyarrow.scalar(None, pyarrow.int64()))
        return stamps.cast(pyarrow.timestamp("s")).cast(arrow_type, safe=False)

    factor = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": NANOS}[arrow_type.unit]
    divisor = NANOS // factor
    subunits = compute.divide(fraction, pyarrow.scalar(divisor, pyarrow.int64()))
    if kinds.is_time(arrow_type):
        storage = pyarrow.int32() if arrow_type.bit_width == 32 else pyarrow.int64()
        base = (
            safe_seconds.cast(pyarrow.timestamp("s"))
            .cast(arrow_type, safe=False)
            .cast(storage)
            .cast(pyarrow.int64())
        )
        units = compute.add(base, compute.if_else(valid, subunits, zero))
        values = units.cast(storage).cast(arrow_type)
        return compute.if_else(valid, values, pyarrow.scalar(None, arrow_type))

    # Bound in the destination unit before multiplying: a four-digit year fits
    # timestamp[s/us] even when it cannot fit timestamp[ns].
    # Arrow narrows negative epoch values toward zero, so a discarded fraction
    # advances their integer unit before the range check.
    remainder = compute.subtract(fraction, compute.multiply(subunits, divisor))
    rounds_toward_zero = compute.and_(compute.less(seconds, 0), compute.greater(remainder, 0))
    subunits = compute.add(subunits, compute.cast(rounds_toward_zero, pyarrow.int64()))
    carry = compute.cast(compute.equal(subunits, factor), pyarrow.int64())
    seconds = compute.add(seconds, carry)
    subunits = compute.subtract(subunits, compute.multiply(carry, factor))
    lower_second, lower_subunit = divmod(-(1 << 63), factor)
    upper_second, upper_subunit = divmod((1 << 63) - 1, factor)
    above_lower = compute.or_(
        compute.greater(seconds, lower_second),
        compute.and_(
            compute.equal(seconds, lower_second),
            compute.greater_equal(subunits, lower_subunit),
        ),
    )
    below_upper = compute.or_(
        compute.less(seconds, upper_second),
        compute.and_(
            compute.equal(seconds, upper_second),
            compute.less_equal(subunits, upper_subunit),
        ),
    )
    inside = compute.fill_null(compute.and_(valid, compute.and_(above_lower, below_upper)), False)
    safe_seconds = compute.if_else(inside, seconds, zero)
    safe_subunits = compute.if_else(inside, subunits, zero)
    units = compute.add(compute.multiply(safe_seconds, factor), safe_subunits)
    values = units.cast(arrow_type, safe=False)
    return compute.if_else(inside, values, pyarrow.scalar(None, arrow_type))
