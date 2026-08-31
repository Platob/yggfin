"""Cross-language signed integer identities over a byte-exact frame."""

from __future__ import annotations

import functools
import struct
import sys
import uuid
from typing import Any

import pyarrow
import pyarrow.compute
import xxhash

from rekep import txhash

#: The Arrow type shared by event, lifecycle, and reference identities.
HASH = pyarrow.binary(txhash.WIDE_WIDTH)

#: The zero digest written for a required wide identity nobody could name.
NIL_BYTES = b"\x00" * txhash.WIDE_WIDTH

#: The immutable wire protocol implemented here and in `docs/contracts/identity.md`.
IDENTITY_PROTOCOL = "rekep-identity-v1"

#: An identifier nothing has computed yet. Zero rather than null, because
#: `hash`, `vhash`, and `xhash` are NOT NULL columns: a stored zero exposes a
#: row nobody hashed as a repeated key rather than a late constraint failure.
#: Wide identities use sixteen zero bytes for the same sentinel at Arrow boundaries.
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
    """Return the v1 signed identity named by `parts`."""
    return hash_bytes(frame(parts))


def hash_bytes(raw: bytes) -> int:
    """Hash one unframed blob with XXH3-64 seed 0 and return signed bits."""
    value = xxhash.xxh3_64_intdigest(raw)
    return value - (1 << 64) if value >= (1 << 63) else value


def hash_bytes_arrow(raw: Any) -> pyarrow.Array:
    """Hash each unframed UTF-8 or binary value to a signed `int64`."""
    if sys.byteorder != "little":
        raise RuntimeError("hash_bytes_arrow requires a little-endian Arrow host; use hash_bytes")
    binary = _binary(raw)
    if isinstance(binary, pyarrow.Scalar):
        binary = pyarrow.array([binary.as_py()], type=pyarrow.binary())
    return _digested(binary)


def hash128_bytes(raw: bytes) -> int:
    """Hash one unframed blob with XXH3-128 seed 0 and return signed bits."""
    return txhash.wide_of(xxhash.xxh3_128_digest(raw))


def hash128_bytes_arrow(raw: Any) -> pyarrow.Array:
    """Hash each unframed UTF-8 or binary value to its sixteen-byte digest."""
    binary = _binary(raw)
    if isinstance(binary, pyarrow.Scalar):
        binary = pyarrow.array([binary.as_py()], type=pyarrow.binary())
    return _digested128(binary)


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
    # value hashes -- and every one of them frames to the same sixteen bytes:
    # the constant length, then the value. Packing a whole run writes the exact
    # same frame in one call rather than four per part. The run is interleaved,
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

    What `hash_arrow` digests, exposed for callers that cache an assembled
    frame before computing its value hash.
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
    """Identities at their sixteen-byte stored width for nested framing."""
    if isinstance(values, pyarrow.ChunkedArray):
        return pyarrow.chunked_array([arrow_of(chunk) for chunk in values.chunks], type=HASH)
    if isinstance(values, pyarrow.Array):
        if values.type == HASH:
            return values
        if pyarrow.types.is_integer(values.type):
            return _widened(values)
        return values.cast(HASH, safe=False)
    return pyarrow.array([hash_bytes_of(one) for one in values], type=HASH)


def hash_bytes_of(value: Any) -> bytes | None:
    """One in-memory identity as its sixteen stored bytes."""
    return txhash.wide_bytes(value)


def hash_int_of(value: Any) -> int | None:
    """One stored or integer identity as the signed value a reader uses."""
    return None if value is None else txhash.wide_of(value)


# -- stored rows -------------------------------------------------------------


def stored_member(name: str, value: Any) -> Any:
    """One member as a column stores it, by the name that says what it is.

    Named rather than typed because the vectorized builders assemble a batch
    member by member and never hold the whole row. Exact event hashes and
    lifecycle identities share a stored width, though only `hash` has a clock.
    """
    if value is None:
        return None
    if name in _WIDE_MEMBERS:
        return hash_bytes_of(value)
    if name in _WIDE_LIST_MEMBERS:
        return [hash_bytes_of(one) for one in value]
    return value


def read_member(name: str, value: Any) -> Any:
    """One member as a reader works in it, whichever spelling it arrived as.

    The inverse of `stored_member`, and tolerant on purpose: a document
    carries an identifier as a number, a stored row as its bytes, and one
    reader has to read both.
    """
    if value is None:
        return None
    if name in _WIDE_MEMBERS:
        return hash_int_of(value) or NIL
    if name in _WIDE_LIST_MEMBERS:
        return [hash_int_of(one) for one in value]
    return value


#: Members whose Arrow representation is a sixteen-byte identity.
_WIDE_MEMBERS = frozenset(("hash", "xhash", "prevhash", "instrumentxhash"))

#: Lists whose items use the same sixteen-byte anchored-hash representation.
_WIDE_LIST_MEMBERS = frozenset(("linkhashes", "parenthash"))

#: Every member a stored row spells differently from a document.
ROW_SPELLED = frozenset((*_WIDE_MEMBERS, *_WIDE_LIST_MEMBERS))


# -- helpers ----------------------------------------------------------------


def _int64_bytes(value: int) -> bytes:
    """One integer part, eight bytes, refusing what Rust cannot hold as an `i64`.

    A stored event or lifecycle identity is wider and enters a frame through
    `hash_bytes_of`; value hashes fit this width directly.
    """
    try:
        return LENGTH.pack(value)
    except struct.error:
        raise OverflowError(f"identity integers must fit signed int64; got {value}") from None


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


def _widened(values: pyarrow.Array) -> pyarrow.Array:
    """Signed `int64` values sign-extended into big-endian sixteen-byte cells."""
    if sys.byteorder != "little":
        raise RuntimeError("widening requires a little-endian Arrow host; use hash_bytes_of")
    try:
        column = values.cast(pyarrow.int64())
    except pyarrow.ArrowInvalid:
        raise OverflowError("Arrow identity integers must fit signed int64") from None
    compute = pyarrow.compute
    high = compute.if_else(
        compute.less(column, pyarrow.scalar(0, pyarrow.int64())),
        pyarrow.scalar(b"\xff" * 8, pyarrow.binary()),
        pyarrow.scalar(b"\x00" * 8, pyarrow.binary()),
    )
    low = compute.binary_reverse(_reinterpreted(column, 8))
    joined = compute.binary_join_element_wise(
        high,
        low,
        pyarrow.scalar(b"", pyarrow.binary()),
    )
    return joined.cast(HASH, safe=False)


def _digested(joined: pyarrow.Array) -> pyarrow.Array:
    """One signed XXH3-64 per row, straight out of the binary buffers."""
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
        values = memoryview(out).cast("Q")
        begin = offsets[0]
        for row in range(rows):
            end = offsets[row + 1]
            values[row] = digest(data[begin:end])
            begin = end
        values.release()
    hashed = pyarrow.Int64Array.from_buffers(pyarrow.int64(), rows, [None, pyarrow.py_buffer(out)])
    if not joined.null_count:
        return hashed
    return pyarrow.compute.if_else(
        pyarrow.compute.is_valid(joined), hashed, pyarrow.scalar(None, pyarrow.int64())
    )


def _digested128(joined: pyarrow.Array) -> pyarrow.Array:
    """One XXH3-128 digest per row in its canonical big-endian byte spelling."""
    digest = xxhash.xxh3_128_digest
    rows = len(joined)
    out = bytearray(txhash.WIDE_WIDTH * rows)
    if rows:
        _, offset_buffer, data_buffer = joined.buffers()[:3]
        wide = pyarrow.types.is_large_binary(joined.type)
        start = joined.offset
        offsets = (
            memoryview(offset_buffer).cast("q" if wide else "i")[start : start + rows + 1].tolist()
        )
        data = memoryview(data_buffer)
        begin = offsets[0]
        for row in range(rows):
            end = offsets[row + 1]
            cell = row * txhash.WIDE_WIDTH
            out[cell : cell + txhash.WIDE_WIDTH] = digest(data[begin:end])
            begin = end
    hashed = pyarrow.FixedSizeBinaryArray.from_buffers(HASH, rows, [None, pyarrow.py_buffer(out)])
    if not joined.null_count:
        return hashed
    return pyarrow.compute.if_else(
        pyarrow.compute.is_valid(joined), hashed, pyarrow.scalar(None, HASH)
    )
