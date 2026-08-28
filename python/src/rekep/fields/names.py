"""What a column is called, and what a person calls it.

Two spellings, and only one of them is an identifier. A column's **name** is
folded -- lowercase letters and digits, nothing else -- so the same field is
one name whether the dictionary spells it `SecurityID`, a bridge writes
`securityid` and a venue shouts `SECURITYID`. Matching is then equality, not
a table of respellings, and there is no snake-casing rule to keep two
generators in step with.

Its **display** is what the name is written as for a reader: the dictionary's
own spelling where FIX has one, and title case where it does not. That is the
half the fold throws away, so it is recorded rather than recomputed.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Mapping
from types import MappingProxyType

#: Everything a name is matched without: separators, punctuation, case.
_DROP = re.compile(r"[^a-z0-9]+", re.ASCII)

#: Words a display does not title-case, and what it writes instead. Title case
#: reads them as words -- `Isin Code`, `Settl Curr Fx Rate Calc` -- and they
#: are not words. Spelled out rather than upper-cased, because `IDs` is not
#: `IDS`.
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


def display_name(name: str) -> str:
    """`name` written for a reader.

    A name that already carries capitals is already a display and is returned
    as it is: `SecurityID` and `NoPartyIDs` are what FIX calls those fields,
    and no rule this file could hold would improve them. Anything else is a
    lower-case identifier, title-cased word by word, with this domain's
    abbreviations left in capitals.
    """
    text = str(name).strip()
    if not text:
        return ""
    if "_" not in text and any(letter.isupper() for letter in text):
        return text
    words = [word for word in text.replace("_", " ").split(" ") if word]
    return " ".join(ACRONYMS.get(word.casefold(), word[:1].upper() + word[1:]) for word in words)
