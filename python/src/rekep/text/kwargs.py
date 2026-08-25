"""Ordered key/value arguments parsed from otherwise opaque text."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import scalar
from rekep.fields.arrays import groups_of, scattered

# A generic argument name. The marker is kept in `key`: whether `#54` means a
# field is a protocol decision, and the text stage must not erase that fact.
_NAME = r"[A-Za-z_][A-Za-z0-9_.\-]*"
_BARE_KEY = rf"(?:[0-9]+|{_NAME})(?:\[[^\]\r\n]+\])?(?:\.[A-Za-z0-9_.\-]+)?"
_KEY = rf"#?{_BARE_KEY}"
_WS = r"[ \t\r\n\f\x0b]"
_ASSIGNMENT = rf"{_KEY}{_WS}*="

# Prefer a separator between two marked arguments. An indexed value may itself
# contain `A=1^AB=2`; requiring the next marker keeps that inner `^A` from
# becoming the row's outer separator.
_PUNCTUATION = r"\^A|[\x01\x21-\x2c\x2f\x3a-\x3c\x3e-\x40\x5c\x5e\x60\x7b-\x7e]"
_MARKED_SEPARATOR = (
    rf"(?s)#{_BARE_KEY}{_WS}*=.*?"
    rf"(?:(?P<sep>{_PUNCTUATION}){_WS}*#|(?P<marker>#)){_BARE_KEY}{_WS}*="
)
_PAIR_PUNCTUATION_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>{_PUNCTUATION}){_WS}*{_ASSIGNMENT}"
_PAIR_WHITESPACE_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>[ \t]+){_ASSIGNMENT}"
_TRAILING_SEPARATOR = rf"(?s){_ASSIGNMENT}.*?(?P<sep>\^A|[\x01|^;#]){_WS}*$"
_MARKED_BODY = rf"(?s)(?P<body>#{_BARE_KEY}{_WS}*=.*)$"
_UNMARKED_BODY = rf"(?s)(?:^|[^A-Za-z0-9_.\-\]#])(?P<body>{_BARE_KEY}{_WS}*=.*)$"
_TOKEN = rf"(?s)^{_WS}*(?P<key>{_KEY}){_WS}*=(?P<value>.*?){_WS}*$"
_DEFAULT_SEPARATOR = "\x01"


@scalar(slots=True)
class Kwarg(Convertible, Mapping[str, str]):
    """One ordered argument parsed from message text."""

    key: str = ""
    """Key exactly as written, including any leading marker."""

    value: str = ""
    """Text after the first equals sign; an empty value is empty text."""

    @classmethod
    def from_stored(cls, entry: Any) -> Kwarg:
        """Normalize a scalar, mapping, or pair into one argument."""
        if isinstance(entry, cls):
            return entry
        if isinstance(entry, Mapping):
            value = entry.get("value")
            return cls(key=str(entry["key"]), value="" if value is None else str(value))
        key, value = entry
        return cls(key=str(key), value="" if value is None else str(value))

    @classmethod
    def parse_arrow(cls, messages: Any) -> Any:
        """Split text into ordered arguments without protocol interpretation."""
        if isinstance(messages, pyarrow.ChunkedArray):
            return pyarrow.chunked_array(
                [cls.parse_arrow(chunk) for chunk in messages.chunks], type=KWARGS
            )
        if not len(messages):
            return pyarrow.array([], type=KWARGS)

        compute = pyarrow.compute
        text = compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
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
        spaced = compute.struct_field(
            compute.extract_regex(text, _PAIR_WHITESPACE_SEPARATOR), "sep"
        )
        trailing = compute.struct_field(compute.extract_regex(text, _TRAILING_SEPARATOR), "sep")
        separators = compute.fill_null(
            compute.coalesce(marked_separator, punctuated, spaced, trailing),
            _DEFAULT_SEPARATOR,
        )

        groups = list(groups_of(separators))
        if len(groups) == 1:
            return _parse_style(text, groups[0][0].as_py())
        parts, positions = [], []
        for separator, where in groups:
            parts.append(_parse_style(compute.take(text, where), separator.as_py()))
            positions.append(where)
        return scattered(parts, positions)

    def __getitem__(self, name: str) -> str:
        if name not in ("key", "value"):
            raise KeyError(name)
        return getattr(self, name)

    def __iter__(self):
        return iter(("key", "value"))

    def __len__(self) -> int:
        return 2


KWARGS: pyarrow.DataType = pyarrow.list_(
    pyarrow.field("item", Kwarg.into_field().arrow_type, nullable=False)
)


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
        marked = compute.take(compute.starts_with(body, "#"), compute.filter(parents, matched))
        keys = compute.if_else(marked, compute.binary_join_element_wise("#", keys, ""), keys)
    values = compute.utf8_trim_whitespace(
        compute.fill_null(compute.filter(compute.struct_field(parsed, "value"), matched), "")
    )

    weights = matched.cast(pyarrow.int32())
    counted = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(weights)]
    )
    offsets = compute.take(counted, tokens.offsets)
    entries = pyarrow.StructArray.from_arrays(
        [keys, values], fields=list(Kwarg.into_field().arrow_type)
    )
    return pyarrow.ListArray.from_arrays(offsets, entries, type=KWARGS)
