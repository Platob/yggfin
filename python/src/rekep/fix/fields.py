"""The FIX datatype projection: what a FIX field's values are in Arrow."""

from __future__ import annotations

import datetime
import functools
import json
import re
from collections.abc import Mapping
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.fields import Field
from rekep.times import EPOCH_ORDINAL as _EPOCH_ORDINAL

#: FIX datatype -> Arrow type, keyed lowercase because the spellings drift
#: across versions (`Boolean`/`boolean`, `MultipleValueString` before FIX 4.4,
#: `MultipleStringValue` after). Two deliberate widenings:
#:
#: - **`char` is a string, not one character.** The standard caps it at one,
#:   real feeds do not -- a value that outgrew its type is still a value, and
#:   a fixed width would truncate it silently. A string holds both.
#: - Every count, length and sequence number is `int64`: FIX puts no ceiling
#:   on them, and a narrower width saves nothing a log cares about.
#:
#: The time-zoned types (`TZTimestamp`, `TZTimeOnly`) stay strings: their
#: offset is part of the value, and a naive Arrow type would drop it.
FIX_SCALARS: dict[str, pyarrow.DataType] = {
    "int": pyarrow.int64(),
    "length": pyarrow.int64(),
    "tagnum": pyarrow.int64(),
    "seqnum": pyarrow.int64(),
    "numingroup": pyarrow.int64(),
    "dayofmonth": pyarrow.int64(),
    "float": pyarrow.float64(),
    "qty": pyarrow.float64(),
    "price": pyarrow.float64(),
    "priceoffset": pyarrow.float64(),
    "amt": pyarrow.float64(),
    "percentage": pyarrow.float64(),
    "char": pyarrow.string(),
    "boolean": pyarrow.bool_(),
    "string": pyarrow.string(),
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
    "utctimestamp": pyarrow.timestamp("ns"),
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
#: One source string for both engines: `re` reads a value, RE2 reads a column,
#: and the two are contracted to agree like every other pattern in this package.
STAMP_PATTERN = (
    r"^[ \t]*(?:(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2}))?"
    r"(?:[-T ]?(?P<hour>\d{2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?"
    r"(?:\.(?P<fraction>\d{1,9}))?)?"
    r"[ \t]*Z?[ \t]*$"
)
_STAMP = re.compile(STAMP_PATTERN, re.ASCII)

#: The epoch as a proleptic Gregorian ordinal, from the one module that
#: declares it. Re-exported here because a reader of a FIX date reaches for it
#: beside the datatypes rather than beside the configuration windows.
EPOCH_ORDINAL = _EPOCH_ORDINAL

NANOS = 1_000_000_000
SECONDS_A_DAY = 86_400
_A_DAY = SECONDS_A_DAY * NANOS

#: Widths that cannot overflow the target: Arrow *raises* on a string it cannot
#: parse, and a raise mid-batch loses a capture over one malformed field.
_INTEGER = r"^[+-]?[0-9]{1,18}$"
_DECIMAL = r"^[+-]?(?:[0-9]{1,17}(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]{1,3})?$"


@functools.lru_cache(maxsize=8192)
def unix_of(text: str | None, day: int | None = None) -> int | None:
    """A FIX timestamp, date or time-of-day as nanoseconds since the epoch, UTC."""
    if not text:
        return None
    match = _STAMP.match(text)
    if match is None:
        return None
    year, month, dayof, hour, minute, second, fraction = match.group(
        "year", "month", "day", "hour", "minute", "second", "fraction"
    )
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
    if hour is None:
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
    return base + (hours * 3600 + minutes * 60 + secs) * NANOS + nanos


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
    if (
        kinds.is_integer(arrow_type)
        or kinds.is_floating(arrow_type)
        or kinds.is_decimal(arrow_type)
    ):
        pattern = _INTEGER if kinds.is_integer(arrow_type) else _DECIMAL
        readable = _only(text, pattern)
        if kinds.is_integer(arrow_type):
            # Arrow accepts `-1` but rejects FIX's equally valid `+1`.
            readable = pyarrow.compute.replace_substring_regex(readable, r"^\+", "")
        return readable.cast(arrow_type, safe=False)
    return text.cast(arrow_type, safe=False)


def _only(text: Any, pattern: str) -> Any:
    """`text`, null wherever it does not match -- so the cast after cannot raise."""
    compute = pyarrow.compute
    matched = compute.fill_null(compute.match_substring_regex(text, pattern), False)
    return compute.if_else(matched, text, pyarrow.scalar(None, pyarrow.string()))


def _cast_arrow_stamp(text: Any, arrow_type: pyarrow.DataType) -> Any:
    """`unix_of` over a whole column: one regex pass, then arithmetic.

    Assembled into ISO 8601 and handed to Arrow's own parser rather than
    reimplementing the civil-date arithmetic here. A date that parses as a
    shape but is not one -- `20260230`, `20250229` -- makes that parser raise,
    so the whole column falls back to the scalar reading, which answers null
    for exactly those rows. Vectorised for every batch, scalar for the one that
    carries a broken stamp.
    """
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
    clock = compute.binary_join_element_wise(
        part("hour", "00"), part("minute", "00"), part("second", "00"), ":"
    )
    stamp = compute.binary_join_element_wise(
        compute.binary_join_element_wise(date, clock, "T"), part("fraction", "0"), "."
    )
    stamp = compute.if_else(present, stamp, pyarrow.scalar(None, pyarrow.string()))
    try:
        nanos = stamp.cast(pyarrow.timestamp("ns")).cast(pyarrow.int64())
    except pyarrow.ArrowInvalid:
        nanos = pyarrow.array([unix_of(one) for one in text.to_pylist()], pyarrow.int64())
    return _temporal(nanos, arrow_type)


def _temporal(nanos: Any, arrow_type: pyarrow.DataType) -> Any:
    """Nanoseconds since the epoch as the temporal type asked for."""
    compute = pyarrow.compute
    kinds = pyarrow.types
    if kinds.is_date(arrow_type):
        days = compute.divide(nanos, pyarrow.scalar(_A_DAY, pyarrow.int64()))
        return days.cast(pyarrow.int32(), safe=False).cast(arrow_type, safe=False)
    if kinds.is_time(arrow_type):
        return (
            nanos.cast(pyarrow.timestamp("ns"))
            .cast(pyarrow.time64("ns"))
            .cast(arrow_type, safe=False)
        )
    return nanos.cast(pyarrow.timestamp("ns")).cast(arrow_type, safe=False)
