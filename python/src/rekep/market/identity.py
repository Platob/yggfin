"""Cross-language signed `int64` identities over a byte-exact frame."""

from __future__ import annotations

import functools
import struct
import sys
import uuid
from typing import Any

import pyarrow
import pyarrow.compute
import xxhash

#: The Arrow type every identifier in this package is.
HASH = pyarrow.int64()

#: The immutable wire protocol implemented here and in `docs/contracts/identity.md`.
IDENTITY_PROTOCOL = "rekep-identity-v1"

#: An identifier nothing has computed yet. Zero rather than null, because
#: `hash` and `xhash` are NOT NULL columns: a row that reaches a store still
#: holding it is one nobody hashed, which is a bug worth seeing as a repeated
#: key rather than as a constraint violation at the very end.
NIL = 0

#: The length an absent part is framed with. Not zero -- that is an empty part,
#: which is a different fact.
ABSENT_LENGTH = -1

#: How a part's length is written: eight bytes, little-endian, signed.
LENGTH = struct.Struct("<q")

#: The fixed-width float payload, compiled once like `LENGTH`.
FLOAT = struct.Struct("<d")

#: Every NaN spelling maps here, so payload bits cannot vary by producer.
CANONICAL_NAN = bytes.fromhex("000000000000f87f")

#: The framed length of an absent part, precomputed because it is the one
#: constant the framing writes over and over.
ABSENT_FRAME = LENGTH.pack(ABSENT_LENGTH)


def hash_of(*parts: Any) -> int:
    """Return the v1 signed `int64` identity named by `parts`."""
    return hash_bytes(frame(parts))


def hash_bytes(raw: bytes) -> int:
    """Hash one unframed blob with XXH3-64 seed 0 and return signed bits."""
    value = xxhash.xxh3_64_intdigest(raw)
    return value - (1 << 64) if value >= (1 << 63) else value


def hash_bytes_arrow(raw: Any) -> pyarrow.Array:
    """Hash each unframed UTF-8 or binary value as one indivisible blob."""
    if sys.byteorder != "little":
        raise RuntimeError("hash_bytes_arrow requires a little-endian Arrow host; use hash_bytes")
    binary = _binary(raw)
    if isinstance(binary, pyarrow.Scalar):
        binary = pyarrow.array([binary.as_py()], type=pyarrow.binary())
    return _digested(binary)


#: Cached prefixes cover nearly every market identity part.
_PREFIXES = tuple(LENGTH.pack(size) for size in range(256))

#: What every `int64` part frames to: eight bytes of length, then eight of
#: value. Fixed, which is what makes a run of them one `struct.pack`.
_INT_LENGTH = 8


@functools.cache
def _int_run(pairs: int) -> struct.Struct:
    """The layout of `pairs` framed `int64` parts, compiled once per width."""
    return struct.Struct(f"<{pairs}q")


def frame(parts: tuple[Any, ...]) -> bytes:
    """Convert and length-prefix `parts` into the v1 identity frame."""
    if not parts:
        raise TypeError("an identifier needs at least one part to frame")
    out: list[bytes] = []
    # A long identity is mostly one long run of plain integers -- a book's live
    # order hashes, an event's parent digests -- and every one of them frames
    # to the same sixteen bytes: the constant length, then the value. Packing a
    # whole run at once writes the exact same frame as one part at a time, in
    # one call rather than four per part. The run is built already interleaved,
    # which measured faster than interleaving a constant into it afterwards.
    run: list[int] = []
    for part in parts:
        if type(part) is int:
            run.append(_INT_LENGTH)
            run.append(part)
            continue
        if run:
            out.append(_packed(run))
            run = []
        raw = part_bytes(part)
        if raw is None:
            out.append(ABSENT_FRAME)
            continue
        size = len(raw)
        out.append(_PREFIXES[size] if size < 256 else LENGTH.pack(size))
        out.append(raw)
    if run:
        out.append(_packed(run))
    return b"".join(out)


def _packed(run: list[int]) -> bytes:
    """One already-interleaved run of framed `int64` parts.

    Refusing, as one part at a time does, whatever Rust cannot hold as an
    `i64` -- and re-framing the run singly when that happens, so the error
    names the value rather than the run it was in.
    """
    try:
        return _int_run(len(run)).pack(*run)
    except struct.error:
        return b"".join(_PREFIXES[_INT_LENGTH] + _int64_bytes(value) for value in run[1::2])


def part_bytes(part: Any) -> bytes | None:
    """Convert one supported scalar into its portable v1 payload."""
    if part is None:
        return None
    # Exact builtins are the hot path; enum subclasses follow the same explicit
    # conversions below rather than acquiring a second wire representation.
    kind = type(part)
    if kind is str:
        return part.encode("utf-8")
    if kind is int:
        return _int64_bytes(part)
    if kind is float:
        return CANONICAL_NAN if part != part else FLOAT.pack(part)
    if kind is bool:
        return b"\x01" if part else b"\x00"
    if kind is bytes:
        return part
    if isinstance(part, bytes | bytearray | memoryview):
        return bytes(part)
    if isinstance(part, str):
        return part.encode("utf-8")
    if isinstance(part, bool):  # before int: a bool is one
        return b"\x01" if part else b"\x00"
    if isinstance(part, int):
        return _int64_bytes(part)
    if isinstance(part, float):
        return CANONICAL_NAN if part != part else FLOAT.pack(part)
    if isinstance(part, uuid.UUID):
        return part.bytes
    raise TypeError(
        "identity parts must be None, UTF-8 str, bytes-like, bool, signed int64, "
        f"float, or UUID; got {type(part).__qualname__}"
    )


def framed_arrow(*columns: Any) -> pyarrow.Array:
    """The v1 frame of `columns`, one payload per row.

    What `hash_arrow` digests, exposed because a time-anchored identity
    digests the same bytes -- the framing is the contract, the digest is
    not.
    """
    if not columns:
        raise TypeError("an identifier needs at least one part to hash")
    if sys.byteorder != "little":
        raise RuntimeError("framing requires a little-endian Arrow host; use frame")
    framed: list[Any] = []
    rows = 1
    for column in columns:
        binary = _binary(column)
        if isinstance(binary, pyarrow.Array):
            rows = len(binary)
        framed += [_length(binary), binary]
    joined = pyarrow.compute.binary_join_element_wise(
        *framed,
        pyarrow.scalar(b"", type=pyarrow.binary()),
        null_handling="replace",
        null_replacement=b"",
    )
    if isinstance(joined, pyarrow.Scalar):
        joined = pyarrow.array([joined.as_py()] * rows, type=pyarrow.binary())
    return joined


def hash_arrow(*columns: Any) -> pyarrow.Array:
    """Return one v1 identity per row from scalar or Arrow parts."""
    return _digested(framed_arrow(*columns))


def arrow_of(values: Any) -> pyarrow.Array:
    """Identifiers as an `int64` column, whatever they are spelled as."""
    if isinstance(values, pyarrow.ChunkedArray):
        return pyarrow.chunked_array([arrow_of(chunk) for chunk in values.chunks], type=HASH)
    if isinstance(values, pyarrow.Array):
        return values if values.type == HASH else values.cast(HASH, safe=False)
    return pyarrow.array(list(values), type=HASH)


# -- helpers ----------------------------------------------------------------


def _int64_bytes(value: int) -> bytes:
    """One signed `int64`, refusing values Rust cannot represent as `i64`."""
    try:
        return LENGTH.pack(value)
    except struct.error:
        raise OverflowError(f"identity integer {value} does not fit signed int64") from None


def _binary(column: Any) -> Any:
    """Convert a supported Arrow part to the same payload as `part_bytes`."""
    if isinstance(column, pyarrow.ChunkedArray):
        column = column.combine_chunks()
    if isinstance(column, pyarrow.Array):
        kinds = pyarrow.types
        if isinstance(column, pyarrow.ExtensionArray):
            return _binary(column.storage)
        if kinds.is_dictionary(column.type):
            return _binary(column.dictionary_decode())
        if kinds.is_null(column.type):
            return pyarrow.nulls(len(column), type=pyarrow.binary())
        if kinds.is_binary(column.type):
            return column
        if kinds.is_fixed_size_binary(column.type) or kinds.is_large_binary(column.type):
            return column.cast(pyarrow.binary(), safe=False)
        if kinds.is_string(column.type) or kinds.is_large_string(column.type):
            return column.cast(pyarrow.binary(), safe=False)
        # A number is its own bytes, and the column already holds them -- see
        # `part_bytes`. Cast to the canonical width first, so an `int32` and a
        # Python `int` are one part, then reinterpret rather than render.
        if kinds.is_boolean(column.type):
            return _reinterpreted(column.cast(pyarrow.uint8()), 1)
        if kinds.is_integer(column.type):
            try:
                widened = column.cast(pyarrow.int64())
            except pyarrow.ArrowInvalid:
                raise OverflowError("Arrow identity integers must fit signed int64") from None
            return _reinterpreted(widened, 8)
        if kinds.is_floating(column.type):
            wide = column.cast(pyarrow.float64())
            binary = _reinterpreted(wide, 8)
            nan = pyarrow.compute.is_nan(wide)
            if pyarrow.compute.any(nan).as_py():
                return pyarrow.compute.if_else(
                    nan,
                    pyarrow.scalar(CANONICAL_NAN, type=pyarrow.binary()),
                    binary,
                )
            return binary
        raise TypeError(f"unsupported Arrow identity part type: {column.type}")
    if isinstance(column, pyarrow.Scalar):
        if pyarrow.types.is_binary(column.type):
            return column
        return pyarrow.scalar(part_bytes(column.as_py()), type=pyarrow.binary())
    # A plain Python value broadcasts, and is spelled by `part_bytes` -- the
    # same one `hash_of` uses -- so one value cannot get two spellings by
    # arriving as a scalar here and as a part there.
    return pyarrow.scalar(part_bytes(column), type=pyarrow.binary())


def _length(part: Any) -> Any:
    """Each value's byte length, as the eight bytes the frame writes it as.

    `binary_length` is offsets arithmetic and costs no pass over the data; an
    absent value is filled with `-1` *before* the reinterpret, so a null frames
    as a length and not as a null the join would then have to replace.
    """
    compute = pyarrow.compute
    length = compute.binary_length(part)
    if isinstance(length, pyarrow.Scalar):
        length = pyarrow.array([length.as_py()], type=pyarrow.int64())
        filled = compute.fill_null(length, ABSENT_LENGTH).cast(pyarrow.int64())
        return _reinterpreted(filled, 8)[0]
    filled = compute.fill_null(length, ABSENT_LENGTH).cast(pyarrow.int64())
    return _reinterpreted(filled, 8)


def _reinterpreted(column: pyarrow.Array, width: int) -> pyarrow.Array:
    """A fixed-width numeric column read as its own bytes, without copying them.

    The buffer already holds exactly what `part_bytes` packs, so this is a view
    over it: no kernel runs and no bytes move.
    """
    validity, data = column.buffers()[:2]
    fixed = pyarrow.FixedSizeBinaryArray.from_buffers(
        pyarrow.binary(width), len(column), [validity, data], offset=column.offset
    )
    return fixed.cast(pyarrow.binary(), safe=False)


def _digested(joined: pyarrow.Array) -> pyarrow.Array:
    """One xxh3-64 per row of a binary column, straight out of its buffers.

    The row loop is the whole cost, so it does the least a row can be: the
    offsets are read once into a list, each row's end is the next row's begin,
    and the digest is stored through a `Q` view of the output rather than
    packed into it. Native byte order, which `hash_arrow` has already refused
    to be anything but little -- and `from_buffers` reads the result natively
    too, so a forced `<Q` here would only disagree with it. 1.8x the packed
    loop it replaces, with no copy of the data buffer (benchmarks/bench_market.py).
    """
    digest = xxhash.xxh3_64_intdigest
    rows = len(joined)
    out = bytearray(8 * rows)
    if rows:
        _, offset_buffer, data_buffer = joined.buffers()[:3]
        wide = pyarrow.types.is_large_binary(joined.type)
        start = joined.offset
        offsets = (
            memoryview(offset_buffer).cast("q" if wide else "i")[start : start + rows + 1].tolist()
        )
        data = memoryview(data_buffer)
        view = memoryview(out).cast("Q")
        begin = offsets[0]
        for row in range(rows):
            end = offsets[row + 1]
            view[row] = digest(data[begin:end])
            begin = end
        view.release()
    return pyarrow.Int64Array.from_buffers(HASH, rows, [None, pyarrow.py_buffer(out)])
