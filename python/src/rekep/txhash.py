"""Time-anchored sortable identities composed from a clock and value hash.

The persisted hash composes signed `int64` epoch microseconds in its high half
with the unchanged bits of a signed `int64` value hash in its low half, stored
as sixteen big-endian bytes.
"""

from __future__ import annotations

import sys
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.fields import TimestampField

#: How many bytes one persisted event hash is.
WIDE_WIDTH = 16

#: The Arrow type of a persisted event hash.
TXHASH128 = pyarrow.binary(WIDE_WIDTH)

#: How far epoch microseconds sit above the value hash.
MICROS_SHIFT = 64

#: The mask for the low half of a persisted event hash.
DIGEST64_MASK = (1 << 64) - 1

#: What a stored event hash's bytes hold: the value, two's complement.
_WIDE_MASK = (1 << 128) - 1

#: What a signed `int64` clock can say.
_MICROS_MIN = -(1 << 63)
_MICROS_MAX = (1 << 63) - 1


# -- the same composition, one column at a time ------------------------------


# -- an event clock over its value hash -------------------------------------


def couple128(micros: int, vhash: int) -> int:
    """Compose epoch microseconds and a signed `int64` value hash."""
    ticks = int(micros)
    if not _MICROS_MIN <= ticks <= _MICROS_MAX:
        raise OverflowError(f"epoch micros {ticks} do not fit a signed int64")
    value = int(vhash)
    if not _MICROS_MIN <= value <= _MICROS_MAX:
        raise OverflowError(f"value hash {value} does not fit a signed int64")
    return (ticks << MICROS_SHIFT) | (value & DIGEST64_MASK)


def wide_of(value: Any) -> int:
    """One persisted event hash as an integer."""
    if isinstance(value, int):
        return value
    packed = int.from_bytes(bytes(value), "big")
    return packed - (1 << 128) if packed >= (1 << 127) else packed


def wide_bytes(value: Any) -> bytes | None:
    """One event hash as the sixteen bytes a column holds; `None` stays `None`.

    Big-endian two's complement keeps nonnegative epoch times in chronological
    byte order and is the one place those bytes are written.
    """
    if value is None or isinstance(value, bytes):
        return value
    return (int(value) & _WIDE_MASK).to_bytes(WIDE_WIDTH, "big")


def micros_of(value: Any) -> int:
    """The epoch microseconds an event hash is anchored to."""
    return wide_of(value) >> MICROS_SHIFT


def digest64_of(value: Any) -> int:
    """The unsigned bits in an event hash's low half."""
    return wide_of(value) & DIGEST64_MASK


def vhash_of(value: Any) -> int:
    """The signed value hash in an event hash's low half."""
    low = digest64_of(value)
    return low - (1 << 64) if low >= (1 << 63) else low


def couple128_arrow(micros: Any, vhash: Any) -> pyarrow.Array:
    """Compose epoch microseconds and signed value hashes column by column."""
    if sys.byteorder != "little":
        raise RuntimeError("couple128_arrow requires a little-endian Arrow host; use couple128")
    clock = _int64_column(micros)
    values = _int64_column(vhash)
    if len(clock) != len(values):
        raise ValueError("micros and value hash columns must have the same length")
    joined = pyarrow.compute.binary_join_element_wise(
        _big_endian_int64(clock),
        _big_endian_int64(values),
        pyarrow.scalar(b"", pyarrow.binary()),
    )
    return joined.cast(TXHASH128, safe=False)


def epoch_micros_arrow(clock: Any) -> pyarrow.Array:
    """A clock column as whole UTC epoch microseconds, `int64`."""
    compute = pyarrow.compute
    column = _column(clock)
    kinds = pyarrow.types
    if kinds.is_string(column.type) or kinds.is_large_string(column.type):
        column = compute.strptime(column, format="%Y-%m-%dT%H:%M:%S", unit="us", error_is_null=True)
    if kinds.is_date(column.type):
        column = column.cast(pyarrow.timestamp("s"), safe=False)
    if kinds.is_timestamp(column.type):
        return TimestampField.into_unix_arrow(column, "us").cast(pyarrow.int64(), safe=False)
    if not kinds.is_integer(column.type):
        raise TypeError(f"a clock column must be an instant or epoch micros, got {column.type}")
    return column.cast(pyarrow.int64())


def _column(values: Any) -> pyarrow.Array:
    if isinstance(values, pyarrow.ChunkedArray):
        return values.combine_chunks()
    if isinstance(values, pyarrow.Array):
        return values
    raise TypeError(f"expected an Arrow column, got {type(values).__name__}")


def _int64_column(micros: Any) -> pyarrow.Array:
    """The clock column as `int64`; a value out of range refuses loudly."""
    column = _column(micros)
    if not pyarrow.types.is_integer(column.type):
        raise TypeError(f"epoch micros must be an integer column, got {column.type}")
    return column.cast(pyarrow.int64())


def _big_endian_int64(column: pyarrow.Array) -> pyarrow.Array:
    """The column's signed bits in network byte order."""
    validity, data = column.buffers()[:2]
    native = pyarrow.FixedSizeBinaryArray.from_buffers(
        pyarrow.binary(8), len(column), [validity, data], offset=column.offset
    ).cast(pyarrow.binary(), safe=False)
    return pyarrow.compute.binary_reverse(native)
