"""What a column is called, and what a person calls it.

Two spellings, and only one of them is an identifier. A column's **name** is
folded -- lowercase letters and digits, nothing else -- so the same field is
one name whether the dictionary spells it `SecurityID`, a bridge writes
`securityid` and a venue shouts `SECURITYID`. Matching is then equality, not
a table of respellings, and there is no snake-casing rule to keep two
generators in step with.

Its **display** is what the name is written as for a reader: the dictionary's
own spelling where FIX has one, and the same shape where it does not -- words
capitalised and run together, `SourceURL` rather than `Source URL`, because a
display is a FIX field name and no FIX field name carries a space. That is the
half the fold throws away, so it is recorded rather than recomputed.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

#: Everything a name is matched without: separators, punctuation, case.
_DROP = re.compile(r"[^a-z0-9]+", re.ASCII)

# Arrow's Unicode lowercase does not perform Python's case-fold expansions:
# `ß` stays `ß` there while `str.casefold()` makes it `ss`. FIX names are
# ASCII in ordinary traffic, so keep that path entirely in kernels and pay for
# a distinct-value Python fold only when a column actually carries Unicode.
_ARROW_DROP = r"[^a-z0-9]+"
_ARROW_ASCII = r"^[\x00-\x7f]*$"

#: Words a display does not capitalise word-wise, and what it writes instead.
#: Capitalising reads them as words -- `IsinCode`, `SettlCurrFxRateCalc` -- and
#: they are not words. Spelled out rather than upper-cased, because `IDs` is
#: not `IDS`.
ACRONYMS: Mapping[str, str] = MappingProxyType(
    {
        "ccy": "CCY",
        "cfi": "CFI",
        "fx": "FX",
        "id": "ID",
        "ids": "IDs",
        "isin": "ISIN",
        "mic": "MIC",
        "ts": "TS",
        "url": "URL",
        "utc": "UTC",
        "uuid": "UUID",
        "vwap": "VWAP",
    }
)


@functools.lru_cache(maxsize=8192)
def column_name(name: str) -> str:
    """A name as a column carries it and as a lookup matches it.

    Memoized: a parse asks this of the same few hundred spellings per message.
    """
    return _DROP.sub("", str(name).strip().casefold())


def column_names(values: Any) -> Any:
    """A string Arrow array under exactly the same fold as `column_name`."""
    import pyarrow
    import pyarrow.compute

    compute = pyarrow.compute
    if isinstance(values, pyarrow.Scalar):
        return pyarrow.scalar(
            column_name(values.as_py()) if values.is_valid else None,
            pyarrow.string(),
        )
    folded = compute.replace_substring_regex(
        compute.utf8_lower(values), pattern=_ARROW_DROP, replacement=""
    )
    if not len(values):
        return folded
    ascii_only = compute.fill_null(compute.match_substring_regex(values, _ARROW_ASCII), True)
    if compute.all(ascii_only, min_count=0).as_py():
        return folded

    source = values.combine_chunks() if isinstance(values, pyarrow.ChunkedArray) else values
    encoded = compute.dictionary_encode(source)
    dictionary = pyarrow.array(
        [column_name(value.as_py()) for value in encoded.dictionary],
        pyarrow.string(),
    )
    return compute.take(dictionary, encoded.indices)


def display_name(name: str) -> str:
    """`name` written for a reader.

    A name that already carries capitals is already a display and is returned
    as it is: `SecurityID` and `NoPartyIDs` are what FIX calls those fields,
    and no rule this file could hold would improve them. Anything else is a
    lower-case identifier, capitalised word by word and run together, with
    this domain's abbreviations left in capitals.

    Run together, not spaced: a display is the spelling of a FIX field, and
    no FIX field name carries a space. `SourceURL` reads as one name the way
    `SecurityID` does, and `Source URL` reads as two.
    """
    text = str(name).strip()
    if not text:
        return ""
    if "_" not in text and any(letter.isupper() for letter in text):
        return text
    words = [word for word in text.replace("_", " ").split(" ") if word]
    return "".join(ACRONYMS.get(word.casefold(), word[:1].upper() + word[1:]) for word in words)
