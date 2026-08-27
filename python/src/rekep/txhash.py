"""Time-anchored sortable identities: `int32` epoch seconds over an XXH32 tail.

A txhash is one signed `int64` whose four high bytes are a row's epoch
seconds as a signed `int32` and whose four low bytes are the XXH32 digest of
its payload. Comparing two txhashes compares their times first, so a column
of them sorts by time while still spreading rows hashed within the same
second -- one value that is both an identity and a sort key.

The couple is exact and reversible: `seconds_of` and `digest_of` read the
halves back, and the Arrow kernels produce bit-identical values to the
Python builders row for row.
"""

from __future__ import annotations

from typing import Any

import pyarrow
import pyarrow.compute
import xxhash

#: The Arrow type every txhash is.
TXHASH = pyarrow.int64()

#: How far the epoch seconds sit above the digest.
SECONDS_SHIFT = 32

#: The four low bytes the digest owns.
DIGEST_MASK = 0xFFFF_FFFF

#: What a signed `int32` clock can say.
_SECONDS_MIN = -(1 << 31)
_SECONDS_MAX = (1 << 31) - 1


def xxh32_of(payload: bytes | str, seed: int = 0) -> int:
    """The unsigned XXH32 digest of one payload; text is digested as UTF-8."""
    raw = payload.encode("utf-8") if isinstance(payload, str) else payload
    return xxhash.xxh32_intdigest(raw, seed)


def couple(seconds: int, digest: int) -> int:
    """Pack epoch `seconds` over an unsigned 32-bit `digest`, signed `int64`."""
    ticks = int(seconds)
    if not _SECONDS_MIN <= ticks <= _SECONDS_MAX:
        raise OverflowError(f"epoch seconds {ticks} do not fit a signed int32")
    low = int(digest)
    if not 0 <= low <= DIGEST_MASK:
        raise OverflowError(f"digest {low} does not fit an unsigned int32")
    unsigned = ((ticks & DIGEST_MASK) << SECONDS_SHIFT) | low
    return unsigned - (1 << 64) if unsigned >= (1 << 63) else unsigned


def h64(seconds: Any, payload: bytes | str, seed: int = 0) -> int:
    """One txhash from epoch `seconds` and a payload.

    `seconds` is an integer count of epoch seconds; a `datetime` or anything
    else carrying `timestamp()` is read on that clock, truncated to whole
    seconds.
    """
    return couple(_seconds(seconds), xxh32_of(payload, seed))


def seconds_of(value: int) -> int:
    """The epoch seconds a txhash is anchored to."""
    return int(value) >> SECONDS_SHIFT


def digest_of(value: int) -> int:
    """The unsigned XXH32 digest a txhash carries."""
    return int(value) & DIGEST_MASK


# -- the same couple, one column at a time ----------------------------------


def h64_arrow(seconds: Any, payload: Any, seed: int = 0) -> pyarrow.Array:
    """One txhash per row: `seconds` over the XXH32 of `payload`.

    `seconds` is any integer column that fits a signed `int32`; `payload` is
    a UTF-8 or binary column. A null on either side is a null txhash. The
    digest loop walks the payload buffer once and writes straight into the
    result's buffer, so nothing is framed or copied on the way.
    """
    clock = _int32_column(seconds)
    binary = _binary_column(payload)
    rows = len(binary)
    if len(clock) != rows:
        raise ValueError("seconds and payload columns must have the same length")
    out = bytearray(8 * rows)
    if rows:
        digest = xxhash.xxh32_intdigest
        ticks = _int32_view(clock)
        offsets, data = _payload_view(binary)
        view = memoryview(out).cast("Q")
        begin = offsets[0]
        for row in range(rows):
            end = offsets[row + 1]
            view[row] = ((ticks[row] & DIGEST_MASK) << SECONDS_SHIFT) | digest(
                data[begin:end], seed
            )
            begin = end
        view.release()
    hashed = pyarrow.Int64Array.from_buffers(TXHASH, rows, [None, pyarrow.py_buffer(out)])
    if not clock.null_count and not binary.null_count:
        return hashed
    compute = pyarrow.compute
    known = compute.and_(compute.is_valid(clock), compute.is_valid(binary))
    return compute.if_else(known, hashed, pyarrow.scalar(None, TXHASH))


def seconds_arrow(values: Any) -> pyarrow.Array:
    """The epoch seconds each txhash is anchored to, as `int32`."""
    column = _column(values).cast(TXHASH)
    shifted = pyarrow.compute.shift_right(column, pyarrow.scalar(SECONDS_SHIFT, TXHASH))
    return shifted.cast(pyarrow.int32(), safe=False)


def digest_arrow(values: Any) -> pyarrow.Array:
    """The unsigned XXH32 digest each txhash carries, as `uint32`."""
    column = _column(values).cast(TXHASH)
    low = pyarrow.compute.bit_wise_and(column, pyarrow.scalar(DIGEST_MASK, TXHASH))
    return low.cast(pyarrow.uint32(), safe=False)


def _seconds(value: Any) -> int:
    """Whole epoch seconds from an integer or anything on a `timestamp()` clock."""
    when = getattr(value, "timestamp", None)
    if when is not None and not isinstance(value, (int, float)):
        return int(when())
    return int(value)


def _column(values: Any) -> pyarrow.Array:
    if isinstance(values, pyarrow.ChunkedArray):
        return values.combine_chunks()
    if isinstance(values, pyarrow.Array):
        return values
    raise TypeError(f"expected an Arrow column, got {type(values).__name__}")


def _int32_column(seconds: Any) -> pyarrow.Array:
    """The clock column as `int32`; a value out of range refuses loudly."""
    column = _column(seconds)
    if not pyarrow.types.is_integer(column.type):
        raise TypeError(f"epoch seconds must be an integer column, got {column.type}")
    return column.cast(pyarrow.int32())


def _binary_column(payload: Any) -> pyarrow.Array:
    """The payload as one plain binary column, text read as its UTF-8 bytes."""
    column = _column(payload)
    kinds = pyarrow.types
    if kinds.is_binary(column.type):
        return column
    if (
        kinds.is_string(column.type)
        or kinds.is_large_string(column.type)
        or kinds.is_large_binary(column.type)
        or kinds.is_fixed_size_binary(column.type)
    ):
        return column.cast(pyarrow.binary(), safe=False)
    raise TypeError(f"payload must be a UTF-8 or binary column, got {column.type}")


def _int32_view(clock: pyarrow.Array) -> Any:
    """The clock's values, read in place from its buffer."""
    start = clock.offset
    return memoryview(clock.buffers()[1]).cast("i")[start : start + len(clock)]


def _payload_view(binary: pyarrow.Array) -> tuple[list[int], memoryview]:
    """Row bounds and bytes of a binary column, read once from its buffers."""
    rows = len(binary)
    _, offset_buffer, data_buffer = binary.buffers()[:3]
    start = binary.offset
    offsets = memoryview(offset_buffer).cast("i")[start : start + rows + 1].tolist()
    return offsets, memoryview(data_buffer)
