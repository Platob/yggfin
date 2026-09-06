"""One physical text record before protocol parsing."""

from __future__ import annotations

import datetime
from typing import Annotated, Any, Self

import pyarrow
from yggdryl import scalar

from rekep.convert import Convertible
from rekep.fields import primary_key, strict_cast_batch
from rekep.times import SHAPES, Stamp, datetime_of


@scalar(slots=True)
class Message(Convertible):
    """One source line and the header fields yggdryl captured from it."""

    url: Annotated[str, primary_key()] = ""
    """Yggdryl URL of the source text object."""

    rownum: Annotated[int, primary_key()] = 0
    """1-based physical line number within the source object."""

    timestamp: datetime.datetime | None = None
    """UTC instant captured from the line header, at microsecond resolution."""

    threadname: str | None = None
    """Thread spelling captured from the line header."""

    plugin: str | None = None
    """Plugin spelling captured from the line header."""

    level: str | None = None
    """Severity spelling captured from the line header."""

    body: bytes = b""
    """Exact bytes after the matched line-header prefix."""

    def __post_init__(self) -> None:
        """Normalize the raw scalar values once."""
        self.url = str(self.url)
        self.rownum = int(self.rownum)
        if self.timestamp is not None:
            timestamp = datetime_of(self.timestamp)
            if timestamp is None:
                raise ValueError(f"timestamp={self.timestamp!r} is not an instant")
            self.timestamp = timestamp
        if isinstance(self.body, str):
            self.body = self.body.encode("utf-8")
        elif not isinstance(self.body, bytes):
            self.body = bytes(self.body)

    @classmethod
    def cast_arrow_batch(cls, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """Apply the raw Message contract to one Yggdryl text batch."""
        field = cls.field()
        timestamp = field.into_arrow_schema().field("timestamp")
        return strict_cast_batch(field, _canonical_timestamp(batch, timestamp))

    @classmethod
    def from_text(cls, text: str | bytes, **declared: Any) -> Self:
        """Build one raw record without interpreting its body."""
        declared["body"] = text.encode("utf-8") if isinstance(text, str) else bytes(text)
        return cls(**declared)


def _canonical_timestamp(
    batch: pyarrow.RecordBatch,
    target: pyarrow.Field,
) -> pyarrow.RecordBatch:
    """Read a captured timestamp as the Message Arrow type."""
    indexes = [
        index for index, field in enumerate(batch.schema) if field.name.casefold() == "timestamp"
    ]
    if len(indexes) != 1:
        return batch
    index = indexes[0]
    source = batch.column(index)
    if pyarrow.types.is_dictionary(source.type):
        source = pyarrow.compute.dictionary_decode(source)
    if not (
        pyarrow.types.is_string(source.type)
        or pyarrow.types.is_large_string(source.type)
        or pyarrow.types.is_string_view(source.type)
    ):
        return batch
    if not pyarrow.types.is_string(source.type):
        # Header timestamps are short. Normalizing the three Arrow text layouts
        # to utf8 keeps the string kernels on one supported representation.
        source = pyarrow.compute.cast(source, pyarrow.string(), safe=False)

    masks = []
    normalized = None
    nulls = pyarrow.compute.is_null(source)
    for stamp in SHAPES:
        mask = pyarrow.compute.fill_null(
            pyarrow.compute.match_substring_regex(source, rf"^(?:{stamp.pattern})$"),
            False,
        )
        masks.append((stamp, mask))
        eligible = pyarrow.compute.or_(mask, nulls)
        if bool(pyarrow.compute.all(eligible, min_count=0).as_py()):
            normalized = _stamp_timestamp(source, stamp)
            break
    if normalized is None:
        normalized = source
        for stamp, mask in masks:
            normalized = pyarrow.compute.if_else(
                mask,
                _stamp_timestamp(source, stamp),
                normalized,
            )
    normalized = pyarrow.compute.cast(normalized, target.type, safe=False)
    return batch.set_column(
        index,
        target,
        normalized,
    )


def _stamp_timestamp(source: pyarrow.Array, stamp: Stamp) -> pyarrow.Array:
    """One captured shape as canonical microsecond RFC 3339 text."""
    date = _stamp_run(source, stamp.date_at, stamp.offsets[:3], "-")
    clock = _stamp_run(source, stamp.clock_at, stamp.offsets[3:], ":")
    fraction = pyarrow.compute.utf8_slice_codeunits(source, stamp.fraction_at)
    digits = pyarrow.compute.replace_substring_regex(
        fraction,
        pattern=r"[^0-9]",
        replacement="",
    )
    padded = pyarrow.compute.binary_join_element_wise(digits, "000000", "")
    micros = pyarrow.compute.utf8_slice_codeunits(padded, 0, 6)
    return pyarrow.compute.binary_join_element_wise(
        date,
        "T",
        clock,
        ".",
        micros,
        "Z",
        "",
    )


def _stamp_run(
    source: pyarrow.Array,
    whole: tuple[int, int] | None,
    offsets: tuple[tuple[int, int], ...],
    separator: str,
) -> pyarrow.Array:
    """Copy or rebuild one fixed date/time run with Arrow kernels."""
    if whole is not None:
        return pyarrow.compute.utf8_slice_codeunits(source, *whole)
    return pyarrow.compute.binary_join_element_wise(
        *(pyarrow.compute.utf8_slice_codeunits(source, start, stop) for start, stop in offsets),
        separator,
    )
