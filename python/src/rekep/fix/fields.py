"""The FIX datatype projection: what a FIX field's values are in Arrow."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.fields import Field

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
    # The dictionary's own slips, found by dumping every version of it
    # (`data/fix/`). They are here because the fallback is wrong for them --
    # a quantity read as text, a day of month read as text, a date read as
    # text -- and its other slips (`Stirng`, `month`) land on a string
    # either way, which is what the fallback already gives them.
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
