"""One sortable 64-bit row id: time in the high bits, a payload hash in the low.

A row needs an identifier that is unique, stable across processes and machines,
and **ordered by time** -- so that a plain integer comparison sorts a table the
way a clock does, a range predicate on it prunes files on their min/max
statistics, and an incremental load can carry one watermark instead of a pair.

The layout is one signed 64-bit integer, and the sign bit stays zero so that
every consumer that only has a signed integer -- Arrow, parquet, Iceberg,
every SQL engine -- sorts it as the unsigned value it is::

    63       62                         21                        0
    +--------+--------------------------+-------------------------+
    | 0      | milliseconds since epoch | folded payload hash      |
    | sign   | TIME_BITS = 42           | HASH_BITS = 21           |
    +--------+--------------------------+-------------------------+

Time first means two rows compare by their millisecond and only then by their
hash, which is what makes the tiebreak *deterministic* rather than arbitrary:
the same logical row always lands on the same id, so a replay dedupes against
what is stored instead of inserting a second copy.

**The bit budget.** 63 bits to spend, and every column below is exact::

    time unit    time bits  overflow (unix)  overflow (2020-01-01)  hash bits  50% collision
    second              32       2106-02-07             2156-02-07         31   54,562 rows
    second              33       2242-03-16             2292-03-15         30   38,581 rows
    millisecond         41       2039-09-07             2089-09-06         22    2,411 rows
    millisecond  *      42       2109-05-15             2159-05-15         21    1,705 rows
    millisecond         43       2248-09-26             2298-09-26         20    1,206 rows
    millisecond         45       3084-12-12             3134-12-13         18      603 rows
    microsecond         52       2112-09-17             2162-09-17         11       53 rows
    microsecond         53       2255-06-05             2305-06-05         10       38 rows

The starred row is the default. "50% collision" is the birthday bound *within
one tick*: the number of rows sharing a single millisecond at which two of them
are as likely as not to fold to the same hash. It is
`1.1774 * sqrt(2**HASH_BITS)`, and what matters is that it is per millisecond,
not per table -- at the default 21 bits, 100 rows in one millisecond expect
0.002 collisions, 1,000 expect 0.24, and 1,705 expect 0.69. A source that
bursts harder than that wants microsecond time bits, not more hash bits.

Sorting is by time first, so a collision costs *ordering between two rows in the
same millisecond*, and identity only where the payloads were equal anyway --
which is exactly the case a dedup wants to collapse.
"""

from __future__ import annotations

import datetime
import decimal
import struct
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import numpy
import pyarrow
import xxhash

#: Seed for every hash this package mints. Fixed, module-level and never
#: derived from the environment: an id is an identity, so a seed that moved
#: between runs -- Python's own salted `hash()`, a per-process random -- would
#: mint a second id for a row that is already stored.
SEED = 0x9E3779B185EBCA87

#: Bits the folded payload hash occupies, at the bottom of the id.
HASH_BITS = 21

#: What is left for the time, once the sign bit is given up.
TIME_BITS = 63 - HASH_BITS

#: A ready-made custom epoch: milliseconds from 1970-01-01 to 2020-01-01. Time
#: bits are spent from the epoch, so moving it forward buys years at the far
#: end -- 2109 becomes 2159 -- at the cost of refusing anything before it.
#: The default everywhere here is the unix epoch (`epoch_ms=0`), because an id
#: that counts from it can be read against `recorded_at_unix` without a table.
EPOCH = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
EPOCH_MS = 1_577_836_800_000

#: What one shift of a 64-bit value has to be masked back to. Python integers
#: are unbounded, so this is what keeps the fold inside 64 bits.
MASK64 = (1 << 64) - 1

#: Milliseconds in one of the units an Arrow timestamp can carry.
_UNIT_NANOS = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}

#: Tags the canonical encoding gives each kind of value, so `1` and `"1"` and
#: `b"1"` never produce the same bytes.
_NULL = b"n"

#: Frame separator. Every value is written `tag + length + ":" + body`, which
#: is self-delimiting -- without it `("ab", "c")` and `("a", "bc")` are the
#: same bytes and therefore the same row.
_FRAME = b":"


# -- packing ----------------------------------------------------------------


def pack(
    timestamp_ms: int,
    payload_hash: int,
    *,
    hash_bits: int = HASH_BITS,
    epoch_ms: int = 0,
) -> int:
    """One id: `timestamp_ms` in the high bits, `payload_hash` folded into the low.

    `payload_hash` is the **full** 64-bit hash; it is folded here rather than
    truncated, so every bit of it reaches the id. `epoch_ms` is subtracted
    first, which is what a custom epoch is -- `EPOCH_MS` moves the overflow
    from 2109 to 2159, and refuses everything before 2020 in exchange.

    The timestamp is checked against the bits it was given, because the
    alternative is an id that silently wraps into a *smaller* number and sorts
    before rows that came years earlier.
    """
    ticks = timestamp_ms - epoch_ms
    limit = (1 << (63 - hash_bits)) - 1
    if not 0 <= ticks <= limit:
        raise ValueError(
            f"{timestamp_ms} ms is outside the {63 - hash_bits} time bits of a row id: "
            f"{_moment(0, epoch_ms)} to {_moment(limit, epoch_ms)} "
            f"(epoch_ms={epoch_ms}, hash_bits={hash_bits})"
        )
    return (ticks << hash_bits) | fold(payload_hash, hash_bits)


def unpack(row_id: int, *, hash_bits: int = HASH_BITS, epoch_ms: int = 0) -> tuple[int, int]:
    """The `(timestamp_ms, folded_hash)` an id was packed from.

    The inverse of `pack` in both directions: `unpack(pack(t, h))` is
    `(t, fold(h))`, and `pack(*unpack(i))` is `i` again, because folding a
    value that is already inside `hash_bits` returns it unchanged.
    """
    if row_id < 0:
        raise ValueError(f"{row_id} is not a row id: the sign bit is part of the time, and 0")
    return ((row_id >> hash_bits) + epoch_ms, row_id & ((1 << hash_bits) - 1))


def fold(payload_hash: int, hash_bits: int = HASH_BITS) -> int:
    """A 64-bit hash folded into `hash_bits`, by xor-shift rather than truncation.

    Truncating keeps the low bits and throws away 43 of the 64, so any hash
    whose entropy sits high -- a counter, a length, a fixed-width field -- would
    collide far more often than the birthday bound says. Two xor-shifts mix the
    whole word down first: `>>21` folds the top of the word onto the middle,
    `>>42` folds what is left onto the bottom, and the mask then keeps bits
    every input bit has reached.
    """
    value = payload_hash & MASK64
    value ^= value >> 21
    value ^= value >> 42
    return value & ((1 << hash_bits) - 1)


def _moment(ticks: int, epoch_ms: int) -> str:
    """A tick count as a date, for an error that has to say what the range *is*.

    A value far enough outside the range has no date at all -- which is the
    case being reported -- so it is quoted as the number it is.
    """
    try:
        stamp = datetime.datetime.fromtimestamp((ticks + epoch_ms) / 1000, tz=datetime.UTC)
    except (OverflowError, OSError, ValueError):
        return f"{ticks + epoch_ms} ms"
    return stamp.strftime("%Y-%m-%d")


# -- hashing ----------------------------------------------------------------


def hash_payload(payload: bytes) -> int:
    """xxh3-64 of bytes that are already canonical, under this package's seed.

    xxh3 is not an implementation detail that can be swapped: the digest *is*
    the low half of every id, so a different function -- or the same one under
    a different seed -- mints a different id for a row that is already stored,
    and a replay inserts it a second time. That is why `xxhash` is a hard
    dependency rather than an extra, and why there is no fallback here.
    """
    return xxhash.xxh3_64_intdigest(payload, seed=SEED)


def signed(value: int) -> int:
    """A 64-bit value as the signed integer an Arrow `int64` column holds.

    `hash_payload` returns the digest unsigned, the way xxhash does, and half
    of those do not fit an int64 -- so a column of them is written through
    here. The bits are untouched: this is a reinterpretation, and `fold` reads
    either sign back to the same value.
    """
    value &= MASK64
    return value - (1 << 64) if value >= (1 << 63) else value


def canonical(row: Mapping[str, Any] | Sequence[Any]) -> bytes:
    """A logical row as the one byte string that stands for it.

    Two producers that build the same row in different orders, or in different
    processes, have to hash the same bytes -- so the encoding fixes everything
    that is otherwise a choice:

    - **Field order.** A mapping is written in sorted key order, never in
      insertion order, so a dict built column by column and one built from a
      database row agree. A sequence keeps its own order, because there
      position *is* the field name.
    - **Encoding.** Text is UTF-8, integers are decimal ASCII (exact at any
      width), floats are eight big-endian IEEE-754 bytes with `-0.0`
      normalised to `0.0` and every NaN to one canonical NaN, and times are
      microseconds since the unix epoch with a naive value read as UTC.
    - **Nulls.** A null is its own tag, never an empty string: `{"venue": None}`
      and `{"venue": ""}` are different rows and must not share an id.
    - **Framing.** Every value is `tag + length + ":" + body`, so `("ab", "c")`
      and `("a", "bc")` cannot produce the same bytes.

    Unicode is *not* normalised: `"é"` composed and decomposed are different
    bytes and therefore different rows. Normalise upstream if the source mixes
    them -- doing it here would hide it.
    """
    return _encoded(row)


def hash_row(row: Mapping[str, Any] | Sequence[Any]) -> int:
    """`hash_payload(canonical(row))`, the two halves nobody should call apart."""
    return hash_payload(canonical(row))


def row_id(
    timestamp: datetime.datetime | int,
    row: Mapping[str, Any] | Sequence[Any] | bytes,
    *,
    hash_bits: int = HASH_BITS,
    epoch_ms: int = 0,
) -> int:
    """The id of one row: its time, and the hash of what it says.

    `timestamp` is a datetime (naive read as UTC) or milliseconds since the
    unix epoch. `row` is a mapping or a sequence to canonicalise, or bytes that
    already are -- a raw log line is its own canonical form, and re-encoding it
    would only make it a different row.
    """
    payload = row if isinstance(row, bytes) else canonical(row)
    return pack(
        _millis(timestamp),
        hash_payload(payload),
        hash_bits=hash_bits,
        epoch_ms=epoch_ms,
    )


def _millis(timestamp: datetime.datetime | int) -> int:
    """A moment as whole milliseconds since the unix epoch."""
    if isinstance(timestamp, datetime.datetime):
        moment = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=datetime.UTC)
        return int(moment.timestamp() * 1000)
    return int(timestamp)


def _encoded(value: Any) -> bytes:
    """One value as tagged, framed, self-delimiting bytes."""
    if value is None:
        return _NULL + b"0" + _FRAME
    if isinstance(value, bool):  # before int: a bool *is* an int in Python
        return _framed(b"b", b"1" if value else b"0")
    if isinstance(value, int):
        return _framed(b"i", str(value).encode())
    if isinstance(value, float):
        # -0.0 == 0.0 and NaN != NaN, so both are normalised: a row that
        # compares equal to another must not hash differently from it.
        if value != value:  # noqa: PLR0124 - the NaN test
            return _framed(b"f", struct.pack(">d", float("nan")))
        return _framed(b"f", struct.pack(">d", value + 0.0))
    if isinstance(value, decimal.Decimal):
        return _framed(b"d", str(value.normalize()).encode())
    if isinstance(value, str):
        return _framed(b"s", value.encode())
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _framed(b"x", bytes(value))
    if isinstance(value, datetime.datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=datetime.UTC)
        return _framed(b"t", str(int(moment.timestamp() * 1_000_000)).encode())
    if isinstance(value, datetime.date):
        return _framed(b"D", value.isoformat().encode())
    if isinstance(value, datetime.time):
        return _framed(b"T", value.isoformat().encode())
    if isinstance(value, uuid.UUID):
        return _framed(b"u", value.bytes)
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda item: str(item[0]))
        body = b"".join(_encoded(str(key)) + _encoded(item) for key, item in items)
        return _counted(b"m", len(items), body)
    if isinstance(value, (Sequence, Iterable)):
        items = list(value)
        return _counted(b"l", len(items), b"".join(_encoded(item) for item in items))
    raise TypeError(
        f"{type(value).__name__} has no canonical form, so a row holding one has no stable id; "
        "convert it to a string, bytes or a number first"
    )


def _framed(tag: bytes, body: bytes) -> bytes:
    return tag + str(len(body)).encode() + _FRAME + body


def _counted(tag: bytes, count: int, body: bytes) -> bytes:
    return tag + str(count).encode() + _FRAME + body


# -- whole columns ----------------------------------------------------------


def pack_arrow(
    timestamps: Any,
    payload_hashes: Any,
    *,
    unit: str = "ms",
    hash_bits: int = HASH_BITS,
    epoch_ms: int = 0,
) -> pyarrow.Array:
    """`pack` over whole columns, in uint64 kernels and never row by row.

    `timestamps` is an Arrow timestamp column -- whose own unit is used, and
    whose `ms` case is a **zero-copy** reinterpret, since a timestamp is
    already an int64 of ticks (`epoch_millis`) -- an int64 column of `unit`
    ticks, or a numpy array of either. `payload_hashes` is the full 64-bit
    hash per row, signed or unsigned.

    Everything is `numpy.uint64` on purpose. A shift whose other operand is a
    Python `int` promotes the array to float64 on numpy 1.x, and float64 holds
    53 bits: the low bits of every id -- the whole hash half -- would be
    rounded away, and the ids would still look plausible.
    """
    ticks = epoch_millis(timestamps, unit=unit, epoch_ms=epoch_ms)
    limit = (1 << (63 - hash_bits)) - 1
    if ticks.size:
        low, high = int(ticks.min()), int(ticks.max())
        if low < 0 or high > limit:
            raise ValueError(
                f"a timestamp is outside the {63 - hash_bits} time bits of a row id: the column "
                f"spans {_moment(low, epoch_ms)}..{_moment(high, epoch_ms)} ({low}..{high} ms), "
                f"an id holds {_moment(0, epoch_ms)}..{_moment(limit, epoch_ms)} (0..{limit} ms) "
                f"with epoch_ms={epoch_ms}, hash_bits={hash_bits}"
            )
    packed = (ticks.astype(numpy.uint64) << numpy.uint64(hash_bits)) | fold_numpy(
        payload_hashes, hash_bits
    )
    return pyarrow.array(packed.view(numpy.int64), type=pyarrow.int64())


def unpack_arrow(
    ids: Any, *, hash_bits: int = HASH_BITS, epoch_ms: int = 0
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """`unpack` over a whole column: the times, and the folded hashes."""
    packed = _uint64(ids)
    if packed.size and int(packed.view(numpy.int64).min()) < 0:
        raise ValueError("a row id with its sign bit set was never packed here")
    ticks = (packed >> numpy.uint64(hash_bits)).astype(numpy.int64) + numpy.int64(epoch_ms)
    folded = (packed & numpy.uint64((1 << hash_bits) - 1)).astype(numpy.int64)
    return pyarrow.array(ticks, type=pyarrow.int64()), pyarrow.array(folded, type=pyarrow.int64())


def fold_numpy(payload_hashes: Any, hash_bits: int = HASH_BITS) -> numpy.ndarray:
    """`fold` over a whole column, in uint64 -- the same value, per row."""
    value = _uint64(payload_hashes)
    value = value ^ (value >> numpy.uint64(21))
    value = value ^ (value >> numpy.uint64(42))
    return value & numpy.uint64((1 << hash_bits) - 1)


def epoch_millis(timestamps: Any, *, unit: str = "ms", epoch_ms: int = 0) -> numpy.ndarray:
    """A time column as int64 milliseconds since `epoch_ms`.

    A `timestamp("ms")` column is **zero-copy** all the way to numpy: an Arrow
    timestamp is already an int64 of ticks, so viewing it as int64 shares the
    buffer and nothing is converted per row. Any other unit is one integer
    division -- floored, not truncated, so a pre-epoch time lands on the
    millisecond it is inside rather than the one above it.
    """
    if isinstance(timestamps, pyarrow.ChunkedArray):
        timestamps = timestamps.combine_chunks()
    if isinstance(timestamps, pyarrow.Array):
        if pyarrow.types.is_timestamp(timestamps.type):
            unit = timestamps.type.unit
        elif not pyarrow.types.is_integer(timestamps.type):
            raise TypeError(
                f"{timestamps.type} is not a time column: hand over a timestamp column or the "
                "integer ticks of one"
            )
        if timestamps.null_count:
            raise ValueError("a null timestamp has no row id: it cannot be ordered or found again")
        ticks = timestamps.view(pyarrow.int64()).to_numpy(zero_copy_only=True)
    else:
        ticks = numpy.asarray(timestamps)
        if ticks.dtype != numpy.int64:
            ticks = ticks.astype(numpy.int64, copy=False)
    nanos = _UNIT_NANOS.get(str(unit))
    if nanos is None:
        raise ValueError(f"{unit!r} is not an Arrow time unit ({', '.join(_UNIT_NANOS)})")
    milli = _UNIT_NANOS["ms"]
    if nanos == milli:
        millis = ticks
    elif nanos < milli:  # us, ns: floor, so a pre-epoch tick lands inside its own millisecond
        millis = numpy.floor_divide(ticks, numpy.int64(milli // nanos))
    else:  # seconds, the only unit coarser than a millisecond
        millis = ticks * numpy.int64(nanos // milli)
    return millis - numpy.int64(epoch_ms) if epoch_ms else millis


def _uint64(values: Any) -> numpy.ndarray:
    """Whatever holds 64-bit values, as a uint64 array without a conversion per row."""
    if isinstance(values, pyarrow.ChunkedArray):
        values = values.combine_chunks()
    if isinstance(values, pyarrow.Array):
        if values.null_count:
            raise ValueError("a null hash has no row id: nothing would identify the row")
        values = values.view(pyarrow.int64()).to_numpy(zero_copy_only=True)
    array = numpy.asarray(values)
    if array.dtype == numpy.uint64:
        return array
    if array.dtype == numpy.int64:
        return array.view(numpy.uint64)
    return array.astype(numpy.uint64, copy=False)
