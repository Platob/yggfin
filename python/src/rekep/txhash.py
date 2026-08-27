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

from rekep.fields import Field, TimestampField

#: The Arrow type every txhash is.
TXHASH = pyarrow.int64()

#: How far the epoch seconds sit above the digest.
SECONDS_SHIFT = 32

#: The four low bytes the digest owns.
DIGEST_MASK = 0xFFFF_FFFF

#: What a signed `int32` clock can say.
_SECONDS_MIN = -(1 << 31)
_SECONDS_MAX = (1 << 31) - 1

#: The Arrow type every wide txhash is: an exact integer, no scale.
TXHASH128 = pyarrow.decimal128(38, 0)

#: How far the epoch micros sit above the wide digest.
MICROS_SHIFT = 64

#: The eight low bytes the wide digest owns.
DIGEST64_MASK = (1 << 64) - 1

#: What a signed `int64` clock can say.
_MICROS_MIN = -(1 << 63)
_MICROS_MAX = (1 << 63) - 1


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


# -- the same couple, one hundred and twenty-eight bits wide -----------------


def xxh64_of(payload: bytes | str, seed: int = 0) -> int:
    """The unsigned XXH64 digest of one payload; text is digested as UTF-8."""
    raw = payload.encode("utf-8") if isinstance(payload, str) else payload
    return xxhash.xxh64_intdigest(raw, seed)


def couple128(micros: int, digest: int) -> int:
    """Pack epoch `micros` over an unsigned 64-bit `digest`, as one integer.

    The clock keeps the high sixty-four bits, so the value orders by time
    exactly as the narrow couple does, with microseconds of resolution and
    a digest wide enough that a collision inside one microsecond is not
    something a feed will meet.
    """
    ticks = int(micros)
    if not _MICROS_MIN <= ticks <= _MICROS_MAX:
        raise OverflowError(f"epoch micros {ticks} do not fit a signed int64")
    low = int(digest)
    if not 0 <= low <= DIGEST64_MASK:
        raise OverflowError(f"digest {low} does not fit an unsigned int64")
    return ticks * (1 << MICROS_SHIFT) + low


def h128(micros: Any, payload: bytes | str, seed: int = 0) -> int:
    """One wide txhash from epoch `micros` and a payload."""
    return couple128(_micros(micros), xxh64_of(payload, seed))


def micros_of(value: Any) -> int:
    """The epoch microseconds a wide txhash is anchored to."""
    return int(value) >> MICROS_SHIFT


def digest64_of(value: Any) -> int:
    """The unsigned XXH64 digest a wide txhash carries."""
    return int(value) & DIGEST64_MASK


def h128_arrow(micros: Any, payload: Any, seed: int = 0) -> pyarrow.Array:
    """One wide txhash per row: `micros` over the XXH64 of `payload`.

    The values are written straight into the sixteen-byte cells a
    `decimal128` column stores, so nothing is boxed on the way.
    """
    clock = _int64_column(micros)
    binary = _binary_column(payload)
    rows = len(binary)
    if len(clock) != rows:
        raise ValueError("micros and payload columns must have the same length")
    out = bytearray(16 * rows)
    if rows:
        digest = xxhash.xxh64_intdigest
        ticks = _int64_view(clock)
        offsets, data = _payload_view(binary)
        begin = offsets[0]
        for row in range(rows):
            end = offsets[row + 1]
            value = ticks[row] * (1 << MICROS_SHIFT) + digest(data[begin:end], seed)
            out[row * 16 : row * 16 + 16] = value.to_bytes(16, "little", signed=True)
            begin = end
    hashed = pyarrow.Decimal128Array.from_buffers(TXHASH128, rows, [None, pyarrow.py_buffer(out)])
    if not clock.null_count and not binary.null_count:
        return hashed
    compute = pyarrow.compute
    known = compute.and_(compute.is_valid(clock), compute.is_valid(binary))
    return compute.if_else(known, hashed, pyarrow.scalar(None, TXHASH128))


def micros_arrow(values: Any) -> pyarrow.Array:
    """The epoch microseconds each wide txhash is anchored to, as `int64`."""
    return pyarrow.array(
        [None if one is None else micros_of(one) for one in _column(values).to_pylist()],
        pyarrow.int64(),
    )


def digest64_arrow(values: Any) -> pyarrow.Array:
    """The unsigned XXH64 digest each wide txhash carries, as `uint64`."""
    return pyarrow.array(
        [None if one is None else digest64_of(one) for one in _column(values).to_pylist()],
        pyarrow.uint64(),
    )


def h128_arrow_arrays(clock: Any, *columns: Any, seed: int = 0) -> pyarrow.Array:
    """`h64_arrow_arrays` one hundred and twenty-eight bits wide."""
    micros = epoch_micros_arrow(clock)
    if not columns:
        raise ValueError("at least one column is required")
    rows = len(micros)
    for column in columns:
        if len(_column(column)) != rows:
            raise ValueError("every column must have the same number of rows")
    return h128_arrow(micros, _framed(columns), seed=seed)


def h128_arrow_batch(batch: Any, clock: Any, *names: str, seed: int = 0) -> pyarrow.Array:
    """`h64_arrow_batch` one hundred and twenty-eight bits wide."""
    if not names:
        raise ValueError("at least one column name is required")
    return h128_arrow_arrays(
        batch.column(clock), *(batch.column(name) for name in names), seed=seed
    )


def h128_dataclass(value: Any, clock: str, *names: str, seed: int = 0) -> int:
    """`h64_dataclass` one hundred and twenty-eight bits wide."""
    field = _struct_field_of(value)
    members = {member.name: member for member in field.fields}
    selected = names or tuple(name for name in members if name != clock)
    payload = b"".join(
        _frame(members[name].into_bytes(getattr(value, name, None))) for name in selected
    )
    anchored = _epoch_seconds(members[clock], getattr(value, clock, None))
    return couple128(_micros(anchored), xxh64_of(payload, seed))


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


# -- hashing what a row is made of ------------------------------------------


def h64_arrow_arrays(clock: Any, *columns: Any, seed: int = 0) -> pyarrow.Array:
    """One txhash per row over several columns, anchored to `clock`.

    `clock` is whatever names the instant -- a timestamp column, an epoch
    integer column, or text -- read through `TimestampField` to UTC epoch
    seconds. Each column is rendered to its field's own bytes and the
    renderings are framed together, so two rows hash alike exactly when
    every value does.
    """
    seconds = epoch_seconds_arrow(clock)
    if not columns:
        raise ValueError("at least one column is required")
    rows = len(seconds)
    for column in columns:
        if len(_column(column)) != rows:
            raise ValueError("every column must have the same number of rows")
    return h64_arrow(seconds, _framed(columns), seed=seed)


def h64_arrow_batch(batch: Any, clock: Any, *names: str, seed: int = 0) -> pyarrow.Array:
    """One txhash per row of `batch` over the columns `names` selects.

    `clock` names the column the instant comes from; `names` the columns
    that make up the identity, in the order they are hashed.
    """
    if not names:
        raise ValueError("at least one column name is required")
    return h64_arrow_arrays(batch.column(clock), *(batch.column(name) for name in names), seed=seed)


def h64_dataclass(value: Any, clock: str, *names: str, seed: int = 0) -> int:
    """One txhash of `value`'s members, anchored to its `clock` member.

    The scalar twin of `h64_arrow_batch`: the same fields, rendered to the
    same bytes and framed the same way, so a row hashed one at a time and a
    column hashed all at once agree.
    """
    field = _struct_field_of(value)
    members = {member.name: member for member in field.fields}
    selected = names or tuple(name for name in members if name != clock)
    payload = b"".join(
        _frame(members[name].into_bytes(getattr(value, name, None))) for name in selected
    )
    return couple(
        _seconds(_epoch_seconds(members[clock], getattr(value, clock, None))),
        xxh32_of(payload, seed),
    )


def epoch_seconds_arrow(clock: Any) -> pyarrow.Array:
    """A clock column as whole UTC epoch seconds, `int32`.

    A timestamp column is read on its own unit through `TimestampField`;
    text is parsed as an instant first; an integer column is already
    seconds.
    """
    compute = pyarrow.compute
    column = _column(clock)
    kinds = pyarrow.types
    if kinds.is_string(column.type) or kinds.is_large_string(column.type):
        column = compute.strptime(column, format="%Y-%m-%dT%H:%M:%S", unit="s", error_is_null=True)
    if kinds.is_timestamp(column.type):
        seconds = TimestampField.into_unix_arrow(column, "s")
        return seconds.cast(pyarrow.int32(), safe=False)
    if kinds.is_date(column.type):
        return TimestampField.into_unix_arrow(
            column.cast(pyarrow.timestamp("s"), safe=False), "s"
        ).cast(pyarrow.int32(), safe=False)
    if not kinds.is_integer(column.type):
        raise TypeError(f"a clock column must be an instant or epoch seconds, got {column.type}")
    return column.cast(pyarrow.int32())


def _epoch_seconds(field: Any, value: Any) -> Any:
    """One clock value as whole UTC epoch seconds."""
    if value is None:
        raise ValueError(f"{field.name or 'the clock'} has no value to anchor a hash to")
    read = field.cast_py(value)
    if isinstance(read, int):
        return read
    return _seconds(read)


def _framed(columns: Any) -> pyarrow.Array:
    """Every column's rendered bytes, framed and joined into one payload."""
    compute = pyarrow.compute
    parts: list[Any] = []
    for column in columns:
        rendered = _rendered(column)
        parts += [_lengths(rendered), rendered]
    return compute.binary_join_element_wise(
        *parts,
        pyarrow.scalar(b"", type=pyarrow.binary()),
        null_handling="replace",
        null_replacement=b"",
    )


def _rendered(column: Any) -> pyarrow.Array:
    """One column as the bytes its field stores it in, row by row."""
    found = _column(column)
    field = Field(name="", dtype=found.type)
    if pyarrow.types.is_binary(found.type):
        return found
    if pyarrow.types.is_string(found.type) or pyarrow.types.is_large_string(found.type):
        return found.cast(pyarrow.binary(), safe=False)
    return pyarrow.array(
        [None if one is None else field.into_bytes(one) for one in found.to_pylist()],
        pyarrow.binary(),
    )


def _lengths(rendered: pyarrow.Array) -> pyarrow.Array:
    """Each row's payload length, framed as eight big-endian bytes."""
    sizes = pyarrow.compute.binary_length(rendered).to_pylist()
    return pyarrow.array(
        [_frame_length(size) for size in sizes],
        pyarrow.binary(),
    )


def _frame_length(size: Any) -> bytes:
    return _ABSENT if size is None else int(size).to_bytes(8, "big", signed=True)


def _frame(payload: bytes) -> bytes:
    """One part's bytes behind its length, so two parts cannot run together."""
    return len(payload).to_bytes(8, "big", signed=True) + payload


#: The length an absent part is framed with, so absent and empty differ.
_ABSENT = (-1).to_bytes(8, "big", signed=True)


def _struct_field_of(value: Any) -> Any:
    """The declaration behind a dataclass instance."""
    declared = type(value)
    into_field = getattr(declared, "into_field", None)
    if into_field is None:
        raise TypeError(f"{declared.__name__} declares no field to hash")
    return into_field()


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


def _int64_column(micros: Any) -> pyarrow.Array:
    """The clock column as `int64`; a value out of range refuses loudly."""
    column = _column(micros)
    if not pyarrow.types.is_integer(column.type):
        raise TypeError(f"epoch micros must be an integer column, got {column.type}")
    return column.cast(pyarrow.int64())


def _int64_view(clock: pyarrow.Array) -> Any:
    """The clock's values, read in place from its buffer."""
    start = clock.offset
    return memoryview(clock.buffers()[1]).cast("q")[start : start + len(clock)]


def _micros(value: Any) -> int:
    """Whole epoch microseconds from an integer or a `timestamp()` clock."""
    when = getattr(value, "timestamp", None)
    if when is not None and not isinstance(value, (int, float)):
        return int(round(when() * 1_000_000))
    return int(value)


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
