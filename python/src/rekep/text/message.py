"""One physical text record before protocol parsing."""

from __future__ import annotations

import datetime
from typing import Annotated, Any, Self

import pyarrow
from yggdryl import scalar
from yggdryl.fix import classify_arrow_array

from rekep.convert import Convertible
from rekep.fields import derived_from, digest_key, partition_key, primary_key
from rekep.times import SHAPES, Stamp, datetime_of

#: What a record whose protocol or `MsgType` no scan could name holds.
#: `parse_fix` reads the rows that spell something else.
UNKNOWN = "unknown"

#: What an unmarked line took. A session's own log is written by the side doing
#: the sending, so a line carrying no verb is one it sent; any verb a line does
#: carry beats this.
DIRECTION = "sent"


@scalar(slots=True)
class Message(Convertible):
    """One source line and the header fields yggdryl captured from it."""

    url: Annotated[str, primary_key()] = ""
    """Yggdryl URL of the source text object."""

    rownum: Annotated[int, primary_key()] = 0
    """1-based physical line number within the source object."""

    timestamp: datetime.datetime | None = None
    """UTC instant captured from the line header, at microsecond resolution."""

    timepartition: Annotated[
        datetime.datetime | None,
        partition_key("hour"),
        derived_from("timestamp"),
    ] = None
    """Timestamp partitioned by its UTC hour in Iceberg."""

    threadname: str | None = None
    """Thread spelling captured from the line header."""

    branch: str | None = None
    """Branch spelling captured from the line header."""

    level: str | None = None
    """Severity spelling captured from the line header."""

    mimetype: str = "application/octet-stream"
    """Media type Yggdryl's shallow scan infers for the body."""

    msgtype: str = UNKNOWN
    """Raw `MsgType` the body's frame spells, or `unknown`."""

    msgdirection: str = UNKNOWN
    """`SENT`, `RECV`, or `unknown`, from the verbs beside the frame."""

    msghash: Annotated[
        bytes | None,
        digest_key(["body"], dtype=pyarrow.binary(16)),
    ] = None
    """XXH3-128 digest of the exact body bytes, filled by Yggdryl."""

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
    def apply_arrow_batch(
        cls,
        batch: pyarrow.RecordBatch,
        direction: str = DIRECTION,
    ) -> pyarrow.RecordBatch:
        """Apply the raw Message contract to one Yggdryl text batch.

        `direction` names what an unmarked line took. Classification is one
        native pass over the body column -- the same shallow scan and the same
        verbs the FIX reader itself uses -- so no row crosses into Python and
        the two stages cannot disagree.
        """
        field = cls.field()
        timestamp = field.into_arrow_schema().field("timestamp")
        prepared = _canonical_timestamp(batch, timestamp)
        prepared = _classified(prepared, direction)
        return field.apply_arrow_batch(
            prepared,
            digest=True,
            safe=False,
            nullability="strict",
        )

    @classmethod
    def from_text(cls, text: str | bytes, **declared: Any) -> Self:
        """Build one raw record without interpreting its body."""
        declared["body"] = text.encode("utf-8") if isinstance(text, str) else bytes(text)
        return cls(**declared)


def _classified(batch: pyarrow.RecordBatch, direction: str) -> pyarrow.RecordBatch:
    """Name each record's protocol, `MsgType`, and direction as three columns."""
    body = _body(batch)
    if body is None:
        return batch
    mimetype, msgtype, msgdirection = classify_arrow_array(body, direction)
    columns = {
        "mimetype": pyarrow.compute.fill_null(mimetype, "application/octet-stream"),
        "msgtype": pyarrow.compute.fill_null(msgtype, UNKNOWN),
        "msgdirection": pyarrow.compute.fill_null(msgdirection, UNKNOWN),
    }
    for name, values in columns.items():
        index = batch.schema.get_field_index(name)
        if index < 0:
            batch = batch.append_column(name, values)
        else:
            batch = batch.set_column(index, name, values)
    return batch


def _body(batch: pyarrow.RecordBatch) -> pyarrow.Array | None:
    """The record payload column, whatever Arrow layout the text reader used.

    A batch carrying no payload column names no protocol at all. One typed
    `null` -- what a zero-row source hands over -- is read as the empty bytes
    it holds, so the three columns are still built.
    """
    index = batch.schema.get_field_index("body")
    if index < 0:
        return None
    values = batch.column(index)
    if pyarrow.types.is_dictionary(values.type):
        values = pyarrow.compute.dictionary_decode(values)
    if pyarrow.types.is_null(values.type):
        return pyarrow.compute.cast(values, pyarrow.binary())
    return values


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
