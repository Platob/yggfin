"""What an event is identified by, and the exact bytes that identity is computed from.

Every identifier here is a signed **`int64`**, and every one of them is
`xxh3-64` over a frame this module specifies down to the byte. Both halves are
deliberate.

**`int64`, not sixteen fixed bytes.** `fixed_size_binary[16]` is the better
identifier on paper -- 128 bits collide at a scale nothing reaches -- and it is
the worse one in practice, because half the ecosystem below Arrow reads it as
something else: Doris surfaces it as `char(16)` of raw bytes that render as
mojibake, Spark cannot *create* one (only write into one), and Iceberg's own
`uuid` reaches Spark as a string. An `int64` is the same column in every engine
there is, and is a join key, a sort key and a bucket source in all of them.

What that costs is collision margin, and the cost is smaller than it looks: the
primary key is `(unix, hash)`, so two identifiers only collide *in the table*
if they also fall on the same nanosecond. The birthday bound applies per
instant rather than across the capture, and a nanosecond holding enough
distinct events to matter is not a nanosecond.

**The frame is a specification, not an implementation detail.** An identifier
that a Java or a Rust producer computes has to be the same number, so the bytes
that go into the digest are written here rather than left to whatever
`str.join` happened to do::

    frame := part*
    part  := int64 length, little-endian, then that many bytes
           | int64 -1,     little-endian, and nothing        (an absent part)

    a part's bytes:
        text          UTF-8
        bytes         themselves
        bool          one byte, 0x01 or 0x00
        int           int64,   little-endian
        float         float64, little-endian (IEEE-754)
        anything else Arrow's rendering of it, UTF-8

    digest := xxh3-64 of the frame, seed 0, read as a signed int64
              (two's complement -- the unsigned value reinterpreted, not clamped)

Length-prefixing rather than a separator is what makes it injective: a
separator alone stops `("AB", "C")` and `("A", "BC")` colliding, but not a part
that contains the separator -- and a number's own bytes contain any given byte
about six times in a hundred. `-1` for an absent part is why a missing client
order id and an empty one are different facts.

A number is its own bytes and never its text, because text needs a *formatter*
and there are two here -- a scalar builder and a vectorised one. They
disagreed: Python writes `10.0`, `1e-07` and `38983288990.155754` where Arrow
writes `10`, `1e-7` and `3.8983288990155754e+10`.

The frame records what a part *is* and not what type it arrived as, so two
parts with the same bytes are one part: `0` and `0.0` are eight zero bytes
either way. A type tag would remove that and cost a kernel pass per part in the
vectorised builder, for a case a call site never has -- a position holds a
price or a version, never one and then the other.
"""

from __future__ import annotations

import struct
import uuid
from typing import Any

import pyarrow
import pyarrow.compute
import xxhash

#: The Arrow type every identifier in this package is.
HASH = pyarrow.int64()

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

#: The framed length of an absent part, precomputed because it is the one
#: constant the framing writes over and over.
ABSENT_FRAME = LENGTH.pack(ABSENT_LENGTH)


def hash_of(*parts: Any) -> int:
    """The identifier `parts` name, as a signed `int64`.

    Each part is framed behind its own byte length, and the frame is hashed --
    the layout is in this module's docstring, and it is a specification another
    language implements rather than a detail of this one::

        hash_of("Order", "XNAS", "AAPL", "cl-1")
    """
    return hash_bytes(frame(parts))


def hash_bytes(raw: bytes) -> int:
    """The identifier one blob has: xxh3-64, signed.

    Not `hash_of`: that frames several parts so their split cannot be forged.
    One blob -- a log line, a wire message -- has no split to forge, so it is
    hashed as it stands, and a caller who wants the framed form asks for it by
    name.

    Signed because Arrow's `int64` is, and by reinterpretation rather than by
    clamping: the same sixty-four bits, read as two's complement, which is what
    every other language will do with them too.
    """
    value = xxhash.xxh3_64_intdigest(raw)
    return value - (1 << 64) if value >= (1 << 63) else value


#: Every length prefix a part shorter than 256 bytes takes, packed once. A
#: part is a symbol, a venue or an eight-byte number, so this is nearly all of
#: them -- and `LENGTH.pack` is a call where a tuple index is not: 2.43 us a
#: frame against 1.90, measured over seven parts.
_PREFIXES = tuple(LENGTH.pack(size) for size in range(256))


def frame(parts: tuple[Any, ...]) -> bytes:
    """`parts` as the bytes the digest is taken over -- the layout, in one place."""
    out = []
    for part in parts:
        raw = part_bytes(part)
        if raw is None:
            out.append(ABSENT_FRAME)
            continue
        size = len(raw)
        out.append(_PREFIXES[size] if size < 256 else LENGTH.pack(size))
        out.append(raw)
    return b"".join(out)


def _int_bytes(part: int) -> bytes:
    """An `int` exactly: eight little-endian bytes, or its text past int64."""
    try:
        return LENGTH.pack(part)
    except struct.error:
        # Wider than an int64, which Arrow has no scalar for either.
        return str(part).encode()


#: The exact types `part_bytes` settles without a subclass walk, spelled the
#: same way the walk below spells them -- this is a fast path and never a
#: second definition of the layout.
_EXACT: dict[type, Any] = {
    str: str.encode,
    int: _int_bytes,
    float: struct.Struct("<d").pack,
    bool: lambda part: b"\x01" if part else b"\x00",
    bytes: lambda part: part,
}


def part_bytes(part: Any) -> bytes | None:
    """One part as the bytes both builders hash it as; None is an absent part.

    A number is its own bytes, little-endian, and never its text: text needs a
    formatter, a scalar builder and a vectorised one are two of them, and they
    disagree. The bytes have nothing to disagree about, they are exact where a
    rendering is lossy, and the vectorised path reinterprets the column's own
    buffer rather than rendering anything at all.
    """
    if part is None:
        return None
    # Exact type first, and only then the `isinstance` walk below. The four
    # types that are almost every part -- a string, an int, a float, a bool --
    # are settled in one dict probe instead of up to five subclass checks,
    # which is a third of the cost of hashing in the book fold. Keyed on the
    # exact type, so a subclass (a `Ranged` code, which *is* an int) still
    # takes the walk and still means what it meant.
    exact = _EXACT.get(type(part))
    if exact is not None:
        return exact(part)
    if isinstance(part, bytes | bytearray | memoryview):
        return bytes(part)
    if isinstance(part, str):
        return part.encode()
    if isinstance(part, bool):  # before int: a bool is one
        return b"\x01" if part else b"\x00"
    if isinstance(part, int):
        try:
            return LENGTH.pack(part)
        except struct.error:
            # Wider than an int64, which Arrow has no scalar for either.
            return str(part).encode()
    if isinstance(part, float):
        return struct.pack("<d", part)
    if isinstance(part, uuid.UUID):
        return part.bytes
    try:
        rendered = pyarrow.scalar(part).cast(pyarrow.string()).as_py()
    except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, TypeError):
        # Something Arrow has no scalar for. `str` is then the only spelling
        # there is, and a column of it would have to be built as text anyway.
        return str(part).encode()
    return b"" if rendered is None else rendered.encode()


def hash_arrow(*columns: Any) -> pyarrow.Array:
    """One identifier per row, from whole columns -- the vectorised `hash_of`.

    The same frame, built with kernels: one length per column, one join over
    every length and column at once, and then one digest per row taken straight
    out of the joined buffer rather than out of Python objects.

    Everything is joined as **binary** with an empty separator, because the
    lengths are the framing -- there is no separator in the layout at all. A
    length is the column's own `binary_length` reinterpreted as its eight
    bytes, so it costs offsets arithmetic and no formatting.

    A scalar argument broadcasts, which is how a shape name or a venue is put
    in front of the columns that vary::

        hash_arrow("Order", batch.column("symbol"), batch.column("client_order_id"))
    """
    if not columns:
        raise TypeError("an identifier needs at least one part to hash")
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
    return _digested(joined)


def arrow_of(values: Any) -> pyarrow.Array:
    """Identifiers as an `int64` column, whatever they are spelled as."""
    if isinstance(values, pyarrow.ChunkedArray):
        return pyarrow.chunked_array([arrow_of(chunk) for chunk in values.chunks], type=HASH)
    if isinstance(values, pyarrow.Array):
        return values if values.type == HASH else values.cast(HASH, safe=False)
    return pyarrow.array(list(values), type=HASH)


# -- helpers ----------------------------------------------------------------


def _binary(column: Any) -> Any:
    """One part as `binary`, which is the one type every part can become.

    A scalar stays a scalar so the kernel broadcasts it instead of this
    building a column of one repeated value per batch.
    """
    if isinstance(column, pyarrow.ChunkedArray):
        column = column.combine_chunks()
    if isinstance(column, pyarrow.Array):
        kinds = pyarrow.types
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
            return _reinterpreted(column.cast(pyarrow.int64()), 8)
        if kinds.is_floating(column.type):
            return _reinterpreted(column.cast(pyarrow.float64()), 8)
        return column.cast(pyarrow.string(), safe=False).cast(pyarrow.binary(), safe=False)
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

    The offsets and the data are read as memory rather than as Python objects:
    `to_pylist()` would allocate a `bytes` per row from data the buffer already
    holds, which measured as most of the cost of building an identifier column.

    The digests are packed as unsigned and the buffer is typed `int64`, so the
    signed reading is the reinterpretation itself -- no branch per row, and the
    same two's complement any other language would get.
    """
    digest = xxhash.xxh3_64_intdigest
    rows = len(joined)
    out = bytearray(8 * rows)
    if rows:
        _, offset_buffer, data_buffer = joined.buffers()[:3]
        wide = pyarrow.types.is_large_binary(joined.type)
        offsets = memoryview(offset_buffer).cast("q" if wide else "i")
        data = memoryview(data_buffer).cast("B")
        start = joined.offset
        pack_into = struct.pack_into
        for row in range(rows):
            begin, end = offsets[start + row], offsets[start + row + 1]
            pack_into("<Q", out, row * 8, digest(data[begin:end]))
    return pyarrow.Int64Array.from_buffers(HASH, rows, [None, pyarrow.py_buffer(out)])
