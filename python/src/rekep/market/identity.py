"""The sixteen fixed bytes an event is identified by, and how a column of them is built.

Every identifier here is **16 bytes wide, exactly** -- `fixed_size_binary[16]`
in Arrow, `fixed[16]` in Iceberg -- and that width is the design, not a
detail:

- **It is wide enough to be an identifier and not a hash.** A 64-bit hash
  collides once in a few billion rows by birthday, which a day of ticks
  reaches; at 128 bits the same argument needs more rows than the venue will
  ever print. An identifier that collides silently merges two lifecycles, and
  nothing downstream can tell that it happened.
- **It is fixed, so it costs no offsets.** A `fixed_size_binary[16]` column is
  one flat buffer -- no offsets array, no indirection per row -- which is what
  makes a join or a group-by on it a memcmp over contiguous memory. The same
  identifier as text is 32 or 36 bytes plus a 4-byte offset each, and every
  comparison chases a pointer.
- **It survives the whole chain.** Arrow `fixed_size_binary[16]` maps to
  Iceberg `fixed[16]` and back with the bytes unchanged, which was checked
  rather than assumed. Iceberg's `uuid` type does not: pyiceberg reads it back
  as `extension<arrow.uuid>`, an Arrow *extension* type that an engine which
  does not know the extension sees as something else -- so the same sixteen
  bytes are spelled as `fixed[16]` here and read as a `uuid.UUID` in Python,
  which is the half of the trade with no cost.

The Python type is `uuid.UUID` because it is the standard 16-byte value with a
canonical text form: an identifier printed in a log or a URL is
`str(event.hash)`, and it is the same bytes.
"""

from __future__ import annotations

import struct
import uuid
from typing import Any

import pyarrow
import pyarrow.compute
import xxhash

#: What the parts of an identifier are joined with, between each part and the
#: byte length that precedes it.
SEPARATOR = b":"

#: What a null part, and its length, are joined as. Not the empty string: a
#: missing client order id and an empty one are different facts about an order,
#: and joining both to `b""` would give them one lifecycle. Not a digit either,
#: so it can never be read as a length.
ABSENT = b"\x00"

#: The Arrow type every identifier in this package is.
HASH = pyarrow.binary(16)

#: Sixteen zero bytes: an identifier that has not been computed yet. It is what
#: an unsaved event carries, and it is deliberately a *value* rather than a
#: null, because `hash` and `xhash` are NOT NULL columns -- a row that reaches a
#: store still holding it is one nobody hashed, which is a bug worth seeing as
#: a repeated key rather than as a constraint violation at the very end.
NIL = uuid.UUID(bytes=bytes(16))


def hash_of(*parts: Any) -> uuid.UUID:
    """The sixteen bytes identifying `parts`, as a `uuid.UUID`.

    xxh3-128, which is the 128-bit half of the hash this package already
    depends on for log lines -- so there is one hash in the build, not two,
    and the identifier is reproducible in any process rather than being
    whatever `hash()` was seeded with.

    **Each part is hashed behind its own byte length**, as `4:AAPL:4:cl-1`,
    and that is what makes the encoding injective rather than merely tidy. A
    plain separator does not: it stops `("AB", "C")` and `("A", "BC")` landing
    on one digest, but not a part that contains the separator itself -- and a
    raw sixteen-byte identifier used as a part contains any given byte about
    six times in a hundred. Length-prefixed, the split is recoverable from the
    bytes, so no two different tuples of parts can produce one identifier.

    A `None` part joins as `ABSENT`, which is not a digit and so can never be
    read as a length::

        hash_of("Order", "XNAS", "AAPL", "cl-1")
    """
    return uuid.UUID(bytes=xxhash.xxh3_128_digest(SEPARATOR.join(_encoded(parts))))


def hash_bytes(raw: bytes) -> uuid.UUID:
    """The sixteen bytes identifying one blob -- a log line, a wire message.

    Not `hash_of`: that composes several parts and length-prefixes each so the
    split cannot be forged. One blob has no split to forge, so it is hashed as
    it stands, and a caller who wants the composed form asks for it by name.
    Same hash and same (absent) seed as everything else here, so an identifier
    is reproducible in any process.
    """
    return uuid.UUID(bytes=xxhash.xxh3_128_digest(raw))


def hash_arrow(*columns: Any) -> pyarrow.Array:
    """One identifier per row, from whole columns -- the vectorised `hash_of`.

    The same length-prefixed encoding, built with kernels: one length per
    column, one join over every column and length at once, and then one digest
    per row read straight out of the joined buffer rather than out of Python
    strings. Both are measured in `benchmarks/bench_market.py`.

    Everything is joined as **binary**, not as text. That is what lets an
    identifier column be a part of another identifier -- raw bytes are not
    valid UTF-8, and casting them to a string raises -- and it is also what
    stops a `large_string` column, which is what pyiceberg hands back, from
    failing to find a kernel beside a plain `string` one.

    A scalar argument broadcasts, which is how a shape name or a venue is put
    in front of the columns that vary::

        hash_arrow("Order", batch.column("symbol"), batch.column("client_order_id"))
    """
    if not columns:
        raise TypeError("an identifier needs at least one part to hash")
    arrays = [_binary(column) for column in columns]
    rows = next((len(array) for array in arrays if isinstance(array, pyarrow.Array)), 1)
    parts: list[Any] = []
    for array in arrays:
        parts.append(_length(array))
        parts.append(array)
    joined = pyarrow.compute.binary_join_element_wise(
        *parts,
        pyarrow.scalar(SEPARATOR, type=pyarrow.binary()),
        null_handling="replace",
        null_replacement=ABSENT,
    )
    if isinstance(joined, pyarrow.Scalar):
        joined = pyarrow.array([joined.as_py()] * rows, type=pyarrow.binary())
    return _digested(joined)


def arrow_of(values: Any) -> pyarrow.Array:
    """Identifiers as a `fixed_size_binary[16]` column, whatever they are spelled as.

    A `uuid.UUID`, 16 raw bytes and the canonical text form all arrive as the
    same sixteen bytes, because all three are how one identifier gets written
    down between here and a store.
    """
    if isinstance(values, pyarrow.ChunkedArray):
        return pyarrow.chunked_array([arrow_of(chunk) for chunk in values.chunks], type=HASH)
    if isinstance(values, pyarrow.Array) and values.type == HASH:
        return values
    return pyarrow.array([_bytes_of(value) for value in values], type=HASH)


def uuids_of(array: Any) -> list[uuid.UUID | None]:
    """A `fixed_size_binary[16]` column read back as identifiers."""
    return [None if value is None else uuid.UUID(bytes=value) for value in array.to_pylist()]


# -- helpers ----------------------------------------------------------------


def _encoded(parts: tuple[Any, ...]) -> list[bytes]:
    """`parts` as the length-prefixed byte parts the digest is taken over."""
    encoded: list[bytes] = []
    for part in parts:
        raw = part_bytes(part)
        if raw is None:
            encoded += [ABSENT, ABSENT]
            continue
        encoded += [str(len(raw)).encode(), raw]
    return encoded


def part_bytes(part: Any) -> bytes | None:
    """One part as the bytes both builders hash it as; None is a missing part.

    **A number is its own bytes**, little-endian: an `int` is the eight bytes
    of an `int64`, a `float` the eight of a `float64`, a `bool` one byte. Not
    its text -- and that is the whole point. Text needs a *formatter*, there
    are two of them here, and they disagree: Python writes `10.0`, `1e-07` and
    `38983288990.155754` where Arrow writes `10`, `1e-7` and
    `3.8983288990155754e+10`. A scalar builder spelling a price one way and the
    vectorised one spelling it another gave the same event two identifiers, and
    no test caught it because they only ever compared the two over strings.

    Reproducing Arrow's formatter in Python would be the same duplication that
    caused it. The bytes have no formatter to disagree about, they are exact
    where a rendering is lossy, and they are faster on both sides -- the
    vectorised path reinterprets the column's own buffer and does no work at
    all.

    Little-endian is pinned rather than native because an identifier has to
    mean the same thing on the machine that reads it back; Arrow's in-memory
    layout is little-endian on every platform this runs on, so the two agree.

    A `uuid.UUID` is its sixteen bytes, matching the `fixed_size_binary[16]`
    column the same identifier arrives in, and not its thirty-six characters.
    Anything else -- a date, a timestamp, a decimal -- is spelled by asking
    Arrow to spell it, which is the one renderer the vectorised path can use.

    Because a number is its bytes, `10` and `10.0` are now *different* parts,
    where a text encoding made them one. That is the safer direction, and a
    call site keeps one type per position anyway.
    """
    if part is None:
        return None
    if isinstance(part, bytes | bytearray | memoryview):
        return bytes(part)
    if isinstance(part, uuid.UUID):
        return part.bytes
    if isinstance(part, str):
        return part.encode()
    if isinstance(part, bool):  # before int: a bool is one
        return b"\x01" if part else b"\x00"
    if isinstance(part, int):
        try:
            return struct.pack("<q", part)
        except struct.error:
            # Wider than an int64, which Arrow has no scalar for either.
            return str(part).encode()
    if isinstance(part, float):
        return struct.pack("<d", part)
    try:
        rendered = pyarrow.scalar(part).cast(pyarrow.string()).as_py()
    except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, TypeError):
        # Something Arrow has no scalar for. `str` is then the only spelling
        # there is, and a column of it would have to be built as text anyway.
        return str(part).encode()
    return b"" if rendered is None else rendered.encode()


def _bytes_of(value: Any) -> bytes | None:
    """One identifier as its sixteen bytes, refusing anything that is not."""
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value.bytes
    if isinstance(value, bytes | bytearray | memoryview):
        raw = bytes(value)
        if len(raw) != 16:
            raise ValueError(f"an identifier is 16 bytes, not {len(raw)}: {raw!r}")
        return raw
    return uuid.UUID(str(value)).bytes


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
        if pyarrow.types.is_fixed_size_binary(column.type) or pyarrow.types.is_large_binary(
            column.type
        ):
            return column.cast(pyarrow.binary())
        return column.cast(pyarrow.string()).cast(pyarrow.binary())
    # A plain Python value broadcasts, and is spelled by `part_bytes` -- the
    # same one `hash_of` uses -- so one value cannot get two spellings by
    # arriving as a scalar here and as a part there.
    return pyarrow.scalar(part_bytes(column), type=pyarrow.binary())


def _reinterpreted(column: pyarrow.Array, width: int) -> pyarrow.Array:
    """A fixed-width numeric column read as its own bytes, without copying them.

    The buffer already holds exactly what `part_bytes` packs, so this is a view
    over it: no kernel runs and no bytes move. The validity buffer comes along,
    so a null stays null and the join replaces it with `ABSENT` like any other
    missing part -- the undefined bytes under a null are never read.
    """
    validity, data = column.buffers()[:2]
    fixed = pyarrow.FixedSizeBinaryArray.from_buffers(
        pyarrow.binary(width), len(column), [validity, data], offset=column.offset
    )
    return fixed.cast(pyarrow.binary(), safe=False)


def _length(part: Any) -> Any:
    """The byte length of each value of `part`, as the binary text of a number.

    `binary_length` is offsets arithmetic and costs no pass over the
    characters; the cast to text is what the join needs, and it is the only
    reason a length is spelled in digits rather than in bytes.
    """
    length = pyarrow.compute.binary_length(part)
    return length.cast(pyarrow.string()).cast(pyarrow.binary())


def _digested(joined: pyarrow.Array) -> pyarrow.Array:
    """One xxh3-128 per row of a binary column, straight out of its buffers.

    The offsets and the data are read as memory rather than as Python objects:
    `to_pylist()` would allocate a `bytes` per row from data the buffer
    already holds, which measured as most of the cost of building an
    identifier column.
    """
    digest = xxhash.xxh3_128_digest
    rows = len(joined)
    out = bytearray(16 * rows)
    if rows:
        _, offset_buffer, data_buffer = joined.buffers()[:3]
        wide = pyarrow.types.is_large_binary(joined.type)
        offsets = memoryview(offset_buffer).cast("q" if wide else "i")
        data = memoryview(data_buffer).cast("B")
        start = joined.offset
        for row in range(rows):
            begin, end = offsets[start + row], offsets[start + row + 1]
            out[row * 16 : row * 16 + 16] = digest(data[begin:end])
    return pyarrow.FixedSizeBinaryArray.from_buffers(HASH, rows, [None, pyarrow.py_buffer(out)])
