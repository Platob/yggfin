"""Ordered key/value arguments parsed from otherwise opaque text."""

from __future__ import annotations

import re
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.entries import ENTRIES, Entry
from rekep.fields.arrays import (
    build_list,
    dense_counts,
    groups_of,
    null_mask,
    scattered,
    sequence,
)
from rekep.fix.message import BEGIN_VECTOR, BRIDGE_VECTOR

# A generic argument name. Capture its marker so `Entry` can remove it while
# preserving that normalization for protocol-specific conversion.
_NAME = r"[A-Za-z_][A-Za-z0-9_.\-]*"
_BARE_KEY = rf"(?:[0-9]+|{_NAME})(?:\[[^\]\r\n]+\])?(?:\.[A-Za-z0-9_.\-]+)?"
_KEY = rf"#?{_BARE_KEY}"
_WS = r"[ \t\r\n\f\x0b]"
_ASSIGNMENT = rf"{_KEY}{_WS}*="

# Prefer a separator between two marked arguments. An indexed value may itself
# contain `A=1^AB=2`; requiring the next marker keeps that inner `^A` from
# becoming the row's outer separator.
# EOT/ETX leads, because a multi-character candidate has to be tried before
# anything it contains -- the same order `fix.message.SEPARATORS` is written in.
_PUNCTUATION = r"\x04\x03|\^A|[\x01\x21-\x2c\x2f\x3a-\x3c\x3e-\x40\x5c\x5e\x60\x7b-\x7e]"
_MARKED_SEPARATOR = (
    rf"(?s)#{_BARE_KEY}{_WS}*=.*?"
    rf"(?:(?P<sep>{_PUNCTUATION}){_WS}*#|(?P<marker>#)){_BARE_KEY}{_WS}*="
)
_PAIR_PUNCTUATION_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>{_PUNCTUATION}){_WS}*{_ASSIGNMENT}"
_PAIR_WHITESPACE_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>[ \t]+){_ASSIGNMENT}"
_SEPARATORS = r"\x04\x03|\^A|[\x01|^;#]"
_TRAILING_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>{_SEPARATORS}){_WS}*$"
_STRUCTURED_PAIRS = rf"(?s){_ASSIGNMENT}.*?(?:{_SEPARATORS}){_WS}*{_ASSIGNMENT}"
_SEPARATOR_BEFORE_ASSIGNMENT = rf"(?:{_PUNCTUATION}){_WS}*{_ASSIGNMENT}"
_MARKED_BODY = rf"(?s)(?P<body>#{_BARE_KEY}{_WS}*=.*)$"
_UNMARKED_BODY = rf"(?s)(?:^|[^A-Za-z0-9_.\-\]#])(?P<body>{_BARE_KEY}{_WS}*=.*)$"
_TOKEN = rf"(?s)^{_WS}*(?P<key>{_KEY}){_WS}*=(?P<value>.*?){_WS}*$"
_DEFAULT_SEPARATOR = "\x01"


def looks_structured_arrow(messages):
    """Which rows contain two delimiter-separated assignments."""
    if isinstance(messages, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [looks_structured_arrow(chunk) for chunk in messages.chunks],
            pyarrow.bool_(),
        )
    text = pyarrow.compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
    return pyarrow.compute.fill_null(
        pyarrow.compute.match_substring_regex(text, _STRUCTURED_PAIRS), False
    )


def parse_arrow(messages):
    """Split text into ordered arguments without protocol interpretation."""
    if isinstance(messages, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [parse_arrow(chunk) for chunk in messages.chunks], type=ENTRIES
        )
    if not len(messages):
        return pyarrow.array([], type=ENTRIES)

    compute = pyarrow.compute
    text = _from_message_start(
        compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
    )
    common = _common_separators(text)
    common_rows = compute.is_valid(common)
    if compute.all(common_rows, min_count=0).as_py():
        return _parse_grouped(text, common)
    if compute.any(common_rows, min_count=0).as_py():
        positions = sequence(len(text))
        common_at = compute.filter(positions, common_rows)
        generic_at = compute.filter(positions, compute.invert(common_rows))
        return scattered(
            [
                _parse_grouped(
                    compute.filter(text, common_rows), compute.filter(common, common_rows)
                ),
                _parse_generic(compute.filter(text, compute.invert(common_rows))),
            ],
            [common_at, generic_at],
        )
    return _parse_generic(text)


def pop_arrow(
    stored,
    names: tuple[str, ...],
    *,
    case_sensitive: bool = True,
):
    """Return the first named value per row and every other argument."""
    if isinstance(stored, pyarrow.ChunkedArray):
        parts = [pop_arrow(chunk, names, case_sensitive=case_sensitive) for chunk in stored.chunks]
        return (
            pyarrow.chunked_array([found for found, _ in parts], pyarrow.string()),
            pyarrow.chunked_array([rest for _, rest in parts], ENTRIES),
        )

    rows = len(stored)
    if not rows:
        return pyarrow.nulls(0, pyarrow.string()), stored
    compute = pyarrow.compute
    entries = compute.list_flatten(stored)
    if not len(entries):
        return pyarrow.nulls(rows, pyarrow.string()), stored
    parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
    keys = compute.struct_field(entries, "key")
    wanted = pyarrow.array(names, pyarrow.string())
    if not case_sensitive:
        keys = compute.utf8_lower(keys)
        wanted = compute.utf8_lower(wanted)
    matches = compute.fill_null(compute.is_in(keys, value_set=wanted), False)
    if not compute.any(matches, min_count=0).as_py():
        return pyarrow.nulls(rows, pyarrow.string()), stored

    matched_parents = compute.filter(parents, matches)
    previous = pyarrow.concat_arrays(
        [
            pyarrow.array([-1], pyarrow.int64()),
            matched_parents.slice(0, len(matched_parents) - 1),
        ]
    )
    first = compute.not_equal(matched_parents, previous)
    found = compute.scatter(
        compute.filter(compute.struct_field(entries, "value"), matches).filter(first),
        compute.filter(matched_parents, first),
        max_index=rows - 1,
    )

    keep = compute.invert(matches)
    kept_parents = compute.filter(parents, keep)
    residual = build_list(
        ENTRIES,
        dense_counts(kept_parents, rows),
        compute.filter(entries, keep),
        null_mask(stored),
    )
    return found, residual


def _from_message_start(text: pyarrow.Array) -> pyarrow.Array:
    """Each line from where its message starts, as `parse_pairs` cuts it.

    A log writes its own prose in front of the payload, and `seq=1092` in that
    prose is an assignment: a separator inferred from the whole line reads the
    `>` of `sending >>` as the delimiter and returns the message as one entry.
    A line that names no message keeps all of itself, which is what a generic
    `A=1;B=2` argument list is.
    """
    compute = pyarrow.compute
    begun = compute.struct_field(compute.extract_regex(text, BEGIN_VECTOR), "msg")
    bridged = compute.struct_field(compute.extract_regex(text, BRIDGE_VECTOR), "msg")
    return compute.coalesce(begun, bridged, text)


def _parse_generic(text: pyarrow.Array) -> pyarrow.Array:
    """Parse rows whose separator needs the complete inference rule."""
    compute = pyarrow.compute
    marked = compute.extract_regex(text, _MARKED_SEPARATOR)
    marked_sep = compute.struct_field(marked, "sep")
    marked_separator = compute.if_else(
        compute.fill_null(compute.greater(compute.binary_length(marked_sep), 0), False),
        marked_sep,
        compute.struct_field(marked, "marker"),
    )
    punctuated = compute.struct_field(
        compute.extract_regex(text, _PAIR_PUNCTUATION_SEPARATOR), "sep"
    )
    spaced = compute.struct_field(compute.extract_regex(text, _PAIR_WHITESPACE_SEPARATOR), "sep")
    trailing = compute.struct_field(compute.extract_regex(text, _TRAILING_SEPARATOR), "sep")
    separators = compute.fill_null(
        compute.coalesce(marked_separator, punctuated, spaced, trailing),
        _DEFAULT_SEPARATOR,
    )
    return _parse_grouped(text, separators)


def _parse_grouped(text: pyarrow.Array, separators: pyarrow.Array) -> pyarrow.Array:
    """Parse rows grouped by one already-settled separator."""
    compute = pyarrow.compute
    groups = list(groups_of(separators))
    if len(groups) == 1:
        return _parse_style(text, groups[0][0].as_py())
    parts, positions = [], []
    for separator, where in groups:
        parts.append(_parse_style(compute.take(text, where), separator.as_py()))
        positions.append(where)
    return scattered(parts, positions)


def _common_separators(text: pyarrow.Array) -> pyarrow.Array:
    """Pipe or SOH where it is the generic parser's first punctuation separator."""
    compute = pyarrow.compute
    assignments = compute.find_substring_regex(text, _ASSIGNMENT)
    punctuation = compute.find_substring_regex(text, _SEPARATOR_BEFORE_ASSIGNMENT)
    hashes = compute.find_substring(text, "#")
    common: Any = pyarrow.nulls(len(text), pyarrow.string())
    for separator in ("|", "\x01", "\x04\x03"):
        found = compute.find_substring_regex(text, rf"{re.escape(separator)}{_WS}*{_ASSIGNMENT}")
        selected = compute.and_(
            compute.and_(compute.greater_equal(found, 0), compute.equal(found, punctuation)),
            compute.and_(compute.less(assignments, found), compute.less(hashes, 0)),
        )
        common = compute.if_else(selected, pyarrow.scalar(separator), common)
    return common


def _parse_style(text: Any, separator: str) -> pyarrow.Array:
    """Parse one homogeneous separator style in Arrow kernels."""
    compute = pyarrow.compute
    marked_body = compute.struct_field(compute.extract_regex(text, _MARKED_BODY), "body")
    unmarked_body = compute.struct_field(compute.extract_regex(text, _UNMARKED_BODY), "body")
    marked_at = compute.find_substring_regex(text, rf"#{_BARE_KEY}{_WS}*=")
    unmarked_at = compute.find_substring_regex(
        text, rf"(?:^|[^A-Za-z0-9_.\-\]#]){_BARE_KEY}{_WS}*="
    )
    use_marked = compute.and_(
        compute.greater_equal(marked_at, 0),
        compute.or_(compute.less(unmarked_at, 0), compute.less(marked_at, unmarked_at)),
    )
    body = compute.fill_null(compute.if_else(use_marked, marked_body, unmarked_body), "")
    tokens = compute.split_pattern(body, separator)
    parsed = compute.extract_regex(tokens.values, _TOKEN)
    keys = compute.struct_field(parsed, "key")
    matched = compute.fill_null(compute.is_valid(keys), False)
    keys = compute.filter(keys, matched)
    if separator == "#":
        parents = compute.list_parent_indices(tokens)
        marked = compute.take(use_marked, compute.filter(parents, matched))
        keys = compute.if_else(
            marked,
            compute.binary_join_element_wise("#", keys, ""),
            keys,
        )
    values = compute.utf8_trim_whitespace(
        compute.fill_null(compute.filter(compute.struct_field(parsed, "value"), matched), "")
    )

    weights = matched.cast(pyarrow.int32())
    counted = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(weights)]
    )
    offsets = compute.take(counted, tokens.offsets)
    entries = pyarrow.StructArray.from_arrays(
        Entry.structure_arrow(keys, values), fields=list(Entry.into_field().dtype)
    )
    return pyarrow.ListArray.from_arrays(offsets, entries, type=ENTRIES)
