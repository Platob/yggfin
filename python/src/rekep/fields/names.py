"""The one folded spelling a column stores and matches."""

from __future__ import annotations

import functools
import re
from typing import Any

#: Everything a name is matched without: separators, punctuation, case.
_DROP = re.compile(r"[^a-z0-9]+", re.ASCII)

# Arrow's Unicode lowercase does not perform Python's case-fold expansions:
# `ß` stays `ß` there while `str.casefold()` makes it `ss`. FIX names are
# ASCII in ordinary traffic, so keep that path entirely in kernels and pay for
# a distinct-value Python fold only when a column actually carries Unicode.
_ARROW_DROP = r"[^a-z0-9]+"
_ARROW_ASCII = r"^[\x00-\x7f]*$"


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
    source = values.combine_chunks() if isinstance(values, pyarrow.ChunkedArray) else values
    encoded = compute.dictionary_encode(source)
    dictionary = encoded.dictionary
    folded = compute.replace_substring_regex(
        compute.utf8_lower(dictionary), pattern=_ARROW_DROP, replacement=""
    )
    if not len(dictionary):
        return compute.take(folded, encoded.indices)
    ascii_only = compute.fill_null(compute.match_substring_regex(dictionary, _ARROW_ASCII), True)
    if compute.all(ascii_only, min_count=0).as_py():
        return compute.take(folded, encoded.indices)

    dictionary = pyarrow.array(
        [column_name(value.as_py()) for value in encoded.dictionary],
        pyarrow.string(),
    )
    return compute.take(dictionary, encoded.indices)
