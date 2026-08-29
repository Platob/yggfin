"""What an identifier is, and the frame it is computed from.

The frame is a *specification* -- another language has to produce the same
number -- so these pin the bytes, not only the behaviour. And the vectorised
builder is a second implementation of the scalar one, so every test that
matters compares them rather than either against itself: that is the only
check that catches a buffer walk reading one byte short, and the only one that
would have caught the two of them spelling a float differently.
"""

from __future__ import annotations

import json
import pathlib
import struct
import uuid

import pyarrow
import pytest

import rekep.market.identity as identity
from rekep.market.identity import (
    ABSENT_FRAME,
    ABSENT_LENGTH,
    HASH,
    IDENTITY_PROTOCOL,
    NIL,
    arrow_of,
    frame,
    hash_arrow,
    hash_bytes,
    hash_bytes_arrow,
    hash_bytes_of,
    hash_int_of,
    hash_of,
    part_bytes,
    read_member,
    stored_member,
)

SYMBOLS = ["AAPL", "MSFT", "AAPL", None, ""]
IDS = ["cl-1", "cl-2", "cl-1", "cl-3", "cl-4"]
VECTORS = pathlib.Path(__file__).parents[3] / "docs" / "assets" / "identity-v1.json"


def built(*columns: object) -> list[int]:
    return [hash_int_of(one) for one in hash_arrow(*columns).to_pylist()]


def vector_part(declared: dict[str, object]) -> object:
    """Materialize one language-neutral golden-vector part."""
    kind = declared["type"]
    if kind == "null":
        return None
    if kind == "utf8":
        return declared["value"]
    if kind == "bytes":
        return bytes.fromhex(str(declared["hex"]))
    if kind == "bool":
        return declared["value"]
    if kind == "i64":
        return int(str(declared["value"]))
    if kind == "f64":
        return struct.unpack(">d", bytes.fromhex(str(declared["bits"])))[0]
    if kind == "uuid":
        return uuid.UUID(str(declared["value"]))
    raise AssertionError(f"unknown vector part {kind!r}")


def vector_column(part: object) -> pyarrow.Array:
    """Represent a golden-vector part through the Arrow implementation."""
    if part is None:
        return pyarrow.array([None], type=pyarrow.string())
    if isinstance(part, uuid.UUID):
        return pyarrow.array([part.bytes], type=pyarrow.binary(16))
    return pyarrow.array([part])


# -- what an identifier is ---------------------------------------------------


def test_an_identifier_is_a_native_signed_int64() -> None:
    value = hash_of("Order", "AAPL")
    assert isinstance(value, int)
    assert -(2**63) <= value < 2**63
    assert hash_arrow("Order", pyarrow.array(["AAPL"])).type == pyarrow.int64()
    assert HASH == pyarrow.binary(16)
    assert hash_int_of(hash_bytes_of(value)) == value


def test_nothing_hashed_yet_is_zero_and_not_null() -> None:
    """`hash` is NOT NULL, so an unhashed row is a visible repeat, not a late failure."""
    assert NIL == 0


def test_the_same_parts_give_the_same_number_in_any_process() -> None:
    assert hash_of("Order", "AAPL", 1) == hash_of("Order", "AAPL", 1)
    assert hash_of("Order", "AAPL", 1) != hash_of("Order", "AAPL", 2)


def test_the_wire_protocol_has_an_explicit_version() -> None:
    assert IDENTITY_PROTOCOL == "rekep-identity-v1"


def test_the_published_cross_language_vectors_pin_frame_and_digest() -> None:
    corpus = json.loads(VECTORS.read_text(encoding="utf-8"))
    assert (corpus["protocol"], corpus["algorithm"], corpus["seed"]) == (
        IDENTITY_PROTOCOL,
        "XXH3-64",
        0,
    )
    assert len(corpus["raw_vectors"]) == 3
    for vector in corpus["raw_vectors"]:
        raw = bytes.fromhex(vector["raw_hex"])
        digest = hash_bytes(raw)
        assert f"{digest & ((1 << 64) - 1):016x}" == vector["digest_hex"], vector["name"]
        assert digest == vector["signed_i64"], vector["name"]
    assert len(corpus["vectors"]) == 16
    for vector in corpus["vectors"]:
        parts = tuple(vector_part(part) for part in vector["parts"])
        raw = frame(parts)
        digest = hash_of(*parts)
        assert raw.hex() == vector["frame_hex"], vector["name"]
        assert f"{digest & ((1 << 64) - 1):016x}" == vector["digest_hex"], vector["name"]
        assert digest == vector["signed_i64"], vector["name"]
        assert built(*(vector_column(part) for part in parts)) == [digest], vector["name"]


# -- the frame, byte for byte ------------------------------------------------


def test_a_part_is_framed_behind_its_length_as_eight_little_endian_bytes() -> None:
    """The layout another language implements. Pinned, not described."""
    assert frame(("AB",)) == struct.pack("<q", 2) + b"AB"
    assert frame(("",)) == struct.pack("<q", 0)
    assert frame(("A", "B")) == struct.pack("<q", 1) + b"A" + struct.pack("<q", 1) + b"B"


def test_length_prefixes_are_int64_at_the_cached_boundary() -> None:
    for size in (0, 1, 255, 256):
        assert frame((b"x" * size,))[:8] == struct.pack("<q", size)


def test_an_absent_part_is_framed_as_a_length_of_minus_one() -> None:
    """Not zero -- that is an empty part, which is a different fact about an order."""
    assert ABSENT_LENGTH == -1
    assert ABSENT_FRAME == struct.pack("<q", -1)
    assert frame((None,)) == ABSENT_FRAME
    assert frame((None,)) != frame(("",))


def test_a_run_of_integers_frames_exactly_as_one_at_a_time() -> None:
    """A long identity is packed in runs; the bytes are the same either way.

    Which is the whole licence for packing them: a book with a thousand live
    orders is a thousand integers, and another language reading this frame
    knows nothing about how many `struct` calls wrote it.
    """
    one_at_a_time = b"".join(struct.pack("<q", 8) + struct.pack("<q", value) for value in range(64))
    assert frame(tuple(range(64))) == one_at_a_time
    mixed = ("X", 1, 2, None, 3.5, 3, 4, b"z", 5)
    assert frame(mixed) == b"".join(
        ABSENT_FRAME
        if part is None
        else struct.pack("<q", len(part_bytes(part))) + part_bytes(part)
        for part in mixed
    )
    assert frame((True, 1)) != frame((1, 1)), "a bool is not an integer part"


def test_an_integer_a_run_cannot_hold_is_still_named_by_its_error() -> None:
    """Packed together, refused one by one: the message has to name the value."""
    with pytest.raises(OverflowError, match=str(2**63)):
        frame((1, 2**63, 3))


def test_a_number_is_framed_as_its_own_bytes() -> None:
    """No formatter, so there is nothing for two implementations to disagree about."""
    assert part_bytes(42) == struct.pack("<q", 42)
    assert part_bytes(10.0) == struct.pack("<d", 10.0)
    assert part_bytes(True) == b"\x01"
    assert part_bytes(False) == b"\x00"
    assert part_bytes("AAPL") == b"AAPL"
    assert part_bytes(b"\x01") == b"\x01"
    assert part_bytes(None) is None


def test_the_digest_is_the_unsigned_value_reinterpreted_and_never_clamped() -> None:
    """Two's complement, which is what another language will get from the same bits."""
    import xxhash

    for parts in (("a",), ("a", "b"), (1,), (None,)):
        unsigned = xxhash.xxh3_64_intdigest(frame(parts))
        expected = unsigned - (1 << 64) if unsigned >= (1 << 63) else unsigned
        assert hash_of(*parts) == expected


def test_a_blob_is_hashed_as_it_stands_and_not_framed() -> None:
    """A whole line has no split to forge, so there is nothing to frame."""
    assert hash_bytes(b"a line") != hash_of("a line")
    assert hash_bytes(b"a line") == hash_bytes(b"a line")


def test_unframed_arrow_hashes_match_scalar_bytes() -> None:
    values = pyarrow.array(["a line", "", "café"])
    found = hash_bytes_arrow(values)
    assert found.type == pyarrow.int64()
    assert found.to_pylist() == [hash_bytes(value.encode("utf-8")) for value in values.to_pylist()]


def test_an_empty_composite_is_refused_in_both_builders() -> None:
    with pytest.raises(TypeError, match="at least one part"):
        hash_of()
    with pytest.raises(TypeError, match="at least one part"):
        frame(())
    with pytest.raises(TypeError, match="at least one part"):
        hash_arrow()


# -- injectivity -------------------------------------------------------------


def test_the_length_prefix_keeps_a_composed_key_from_merging() -> None:
    assert hash_of("AB", "C") != hash_of("A", "BC")


def test_a_missing_part_is_not_an_empty_one() -> None:
    assert hash_of("x", None) != hash_of("x", "")


#: Tuples chosen to be exactly the ones a separator alone confuses.
ADVERSARIAL = [
    ("AB", "C"),
    ("A", "BC"),
    ("ABC",),
    ("A", "B", "C"),
    ("A:B",),
    ("A", ":B"),
    ("1:A:1:B",),
    ("A", "B"),
    ("", "AB"),
    ("AB", ""),
    ("", "", "AB"),
    (None, "AB"),
    ("AB", None),
    ("\x00", "AB"),
    (b"\x01\x1f\xff", "AB"),
    (0,),
    (1,),
    (1.0,),
    (True,),
]


def test_no_two_different_tuples_of_parts_share_an_identifier() -> None:
    seen: dict[int, tuple[object, ...]] = {}
    for parts in ADVERSARIAL:
        digest = hash_of(*parts)
        assert digest not in seen, f"{parts!r} collides with {seen[digest]!r}"
        seen[digest] = parts


@pytest.mark.parametrize("parts", ADVERSARIAL, ids=repr)
def test_the_column_builder_agrees_on_every_adversarial_tuple(parts: tuple[object, ...]) -> None:
    columns = [
        pyarrow.array([part], type=pyarrow.binary() if isinstance(part, bytes) else None)
        for part in parts
    ]
    assert built(*columns) == [hash_of(*parts)]


# -- the two builders --------------------------------------------------------

#: Every kind of part a call site passes, with the Arrow column the same value
#: arrives in. Text-only coverage let the two builders disagree on floats,
#: booleans and identifiers all the way into a release.
PARTS: list[tuple[str, object, object, object]] = [
    ("float", 10.0, 10.0, pyarrow.float64()),
    ("float negative", -0.5, -0.5, pyarrow.float64()),
    ("float tiny", 1e-7, 1e-7, pyarrow.float64()),
    ("float huge", 1e300, 1e300, pyarrow.float64()),
    ("float whole", 3.0, 3.0, pyarrow.float64()),
    ("int", 42, 42, pyarrow.int64()),
    ("int narrow", 42, 42, pyarrow.int32()),
    ("int negative", -7, -7, pyarrow.int64()),
    ("int zero", 0, 0, pyarrow.int64()),
    ("bool true", True, True, pyarrow.bool_()),
    ("bool false", False, False, pyarrow.bool_()),
    ("str", "AAPL", "AAPL", pyarrow.string()),
    ("str empty", "", "", pyarrow.string()),
    ("str wide", "AAPL", "AAPL", pyarrow.large_string()),
    ("bytes", b"\x01\x02", b"\x01\x02", pyarrow.binary()),
    ("absent", None, None, pyarrow.string()),
]


@pytest.mark.parametrize("label,part,stored,dtype", PARTS, ids=[row[0] for row in PARTS])
def test_the_three_ways_a_part_arrives_all_hash_the_same(
    label: str, part: object, stored: object, dtype: object
) -> None:
    """A part is one value, whichever door it comes in by."""
    scalar = hash_of("X", part)
    column = built("X", pyarrow.array([stored], type=dtype))[0]
    broadcast = built("X", part)[0]
    assert scalar == column, f"{label}: the column builder disagrees"
    assert scalar == broadcast, f"{label}: a broadcast scalar disagrees"


def test_the_column_builder_agrees_with_the_scalar_one_row_by_row() -> None:
    column = built("Order", pyarrow.array(SYMBOLS), pyarrow.array(IDS))
    assert column == [
        hash_of("Order", symbol, identifier)
        for symbol, identifier in zip(SYMBOLS, IDS, strict=True)
    ]


def test_a_sliced_batch_hashes_the_rows_it_actually_holds() -> None:
    symbols, ids = pyarrow.array(SYMBOLS), pyarrow.array(IDS)
    whole = built("Order", symbols, ids)
    assert built("Order", symbols.slice(1, 3), ids.slice(1, 3)) == whole[1:4]


def test_a_chunked_column_hashes_as_one_column() -> None:
    chunked = pyarrow.chunked_array([pyarrow.array(SYMBOLS[:2]), pyarrow.array(SYMBOLS[2:])])
    assert built("Order", chunked, pyarrow.array(IDS)) == built(
        "Order", pyarrow.array(SYMBOLS), pyarrow.array(IDS)
    )


def test_a_dictionary_column_hashes_its_values_not_its_indices() -> None:
    encoded = pyarrow.array(["MSFT", "AAPL", "MSFT"]).dictionary_encode()
    assert built("Order", encoded) == [
        hash_of("Order", "MSFT"),
        hash_of("Order", "AAPL"),
        hash_of("Order", "MSFT"),
    ]


@pytest.mark.parametrize("dtype", [pyarrow.null(), pyarrow.int64(), pyarrow.float64()])
def test_null_is_absent_whatever_supported_arrow_column_carries_it(
    dtype: pyarrow.DataType,
) -> None:
    assert built(pyarrow.array([None], type=dtype)) == [hash_of(None)]


def test_an_identifier_column_can_itself_be_a_part() -> None:
    """A child event hashes its parents, and a parent enters as its stored bytes."""
    parents = arrow_of([hash_of("a"), hash_of("b")])
    made = built("Book", parents)
    assert made[0] != made[1]
    assert made[0] == hash_of("Book", hash_bytes_of(hash_of("a")))


def test_no_rows_is_no_rows() -> None:
    assert built("Order", pyarrow.array([], type=pyarrow.string())) == []


def test_hashing_nothing_is_refused() -> None:
    with pytest.raises(TypeError, match="at least one part"):
        hash_arrow()


# -- the edges of a part -----------------------------------------------------


def test_a_part_is_its_bytes_and_nothing_more() -> None:
    assert hash_of(b"AB") == hash_of("AB")
    assert hash_of(b"") == hash_of("")


def test_a_number_and_its_text_are_different_parts() -> None:
    """A number is its own eight bytes, so it cannot collide with a rendering of itself."""
    assert hash_of(65) != hash_of("65")
    assert hash_of(10) != hash_of(10.0), "an int64 and a float64 are different bytes"


def test_two_parts_with_the_same_bytes_are_one_part() -> None:
    """The whole of the equivalence the frame leaves, and it is deliberate.

    `0` and `0.0` are eight zero bytes either way, so they are the same part --
    the frame records what a part *is*, not what type it arrived as. A type tag
    would remove it and cost a kernel pass per part in the vectorised builder,
    for a case a call site never has: a position holds a price or a version,
    never one and then the other.
    """
    assert hash_of(0) == hash_of(0.0)
    assert hash_of(0.0) != hash_of(-0.0), "which the bytes still tell apart"
    assert hash_of(b"AB") == hash_of("AB")


def test_zero_and_negative_zero_are_different_parts() -> None:
    """Their bytes differ, and a merge key that could not tell them apart is a
    war story this repository already has."""
    assert hash_of(0.0) != hash_of(-0.0)


def test_every_nan_payload_is_canonical_in_scalar_and_arrow_paths() -> None:
    values = [
        struct.unpack(">d", bytes.fromhex(bits))[0]
        for bits in ("7ff8000000000000", "7ff0000000000001", "fff8000000000042")
    ]
    expected = hash_of(values[0])
    assert [hash_of(value) for value in values] == [expected] * 3
    assert built(pyarrow.array(values, type=pyarrow.float64())) == [expected] * 3


def test_vector_hashing_refuses_an_ambiguous_big_endian_arrow_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(identity.sys, "byteorder", "big")
    with pytest.raises(RuntimeError, match="little-endian"):
        hash_arrow(pyarrow.array([1], type=pyarrow.int64()))
    with pytest.raises(RuntimeError, match="little-endian"):
        arrow_of(pyarrow.array([1], type=pyarrow.int64()))
    assert hash_of(1), "the scalar implementation writes little-endian explicitly"


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
def test_an_integer_outside_the_portable_width_is_refused(value: int) -> None:
    with pytest.raises(OverflowError, match="signed int64"):
        hash_of("X", value)


@pytest.mark.parametrize("value", [[1, 2], {"a": 1}, object()])
def test_an_unsupported_part_is_refused_instead_of_stringified(value: object) -> None:
    with pytest.raises(TypeError, match="identity parts must be"):
        hash_of("X", value)


def test_an_unsupported_arrow_type_is_refused_instead_of_stringified() -> None:
    values = pyarrow.array([pyarrow.scalar("2024-03-14").cast(pyarrow.date32()).as_py()])
    with pytest.raises(TypeError, match="date32"):
        hash_arrow("X", values)


def test_an_unsigned_arrow_integer_outside_int64_is_refused() -> None:
    with pytest.raises(OverflowError, match="signed int64"):
        hash_arrow("X", pyarrow.array([2**63], type=pyarrow.uint64()))


def test_an_identifier_from_elsewhere_is_hashed_as_its_bytes() -> None:
    """A `uuid.UUID` is not stored here any more, but a caller may still pass one."""
    assert part_bytes(uuid.UUID(int=7)) == uuid.UUID(int=7).bytes


def test_bytes_like_values_share_their_declared_raw_payload() -> None:
    raw = b"\x00\x01\xff"
    assert part_bytes(raw) == part_bytes(bytearray(raw)) == part_bytes(memoryview(raw))
    assert hash_of(raw) == hash_of(bytearray(raw)) == hash_of(memoryview(raw))


# -- stored hashes and nested identities ------------------------------------


def test_an_identity_is_padded_only_when_it_enters_a_nested_frame() -> None:
    values = [hash_of("a"), None]
    for column in (values, pyarrow.array(values, type=pyarrow.int64())):
        assert arrow_of(column).type == HASH
        assert [hash_int_of(one) for one in arrow_of(column).to_pylist()] == values


def test_a_narrow_identifier_column_widens() -> None:
    narrow = pyarrow.array([-1, 2], type=pyarrow.int32())
    widened = arrow_of(narrow)
    assert widened.type == HASH
    assert [hash_int_of(one) for one in widened.to_pylist()] == [-1, 2]


def test_a_chunked_column_converts_chunk_by_chunk() -> None:
    chunked = pyarrow.chunked_array([pyarrow.array([1], type=pyarrow.int32())])
    assert arrow_of(chunked).type == HASH


def test_a_sliced_identity_column_keeps_its_sign_and_nulls() -> None:
    values = pyarrow.array([99, -1, None, 2], pyarrow.int64())[1:]
    assert [hash_int_of(one) for one in arrow_of(values).to_pylist()] == [-1, None, 2]


def test_only_time_anchored_members_change_spelling_in_a_stored_row() -> None:
    version = (17 << 64) | ((-9) & ((1 << 64) - 1))
    assert stored_member("hash", version) == hash_bytes_of(version)
    assert stored_member("prevhash", version) == hash_bytes_of(version)
    assert stored_member("parenthash", [version]) == [hash_bytes_of(version)]
    assert read_member("hash", hash_bytes_of(version)) == version
    assert read_member("parenthash", [hash_bytes_of(version)]) == [version]
    for name in ("vhash", "xhash", "instrumentxhash", "linkedhashes"):
        value = [-9] if name == "linkedhashes" else -9
        assert stored_member(name, value) == value
        assert read_member(name, value) == value


def test_a_supported_unhashable_lifecycle_part_bypasses_the_cache() -> None:
    """A mutable bytes view is portable even though the lifecycle cache cannot key it."""
    import dataclasses

    from rekep.market import Order
    from rekep.market.identity import hash_of

    @dataclasses.dataclass
    class Binary(Order):
        """An order whose lifecycle is raw protocol bytes."""

        def life_parts(self) -> tuple[object, ...]:
            return (bytearray(b"order-1"),)

    one = Binary(unix=1, code="BTC-USD")
    assert one.life_hash() == hash_of("Binary", bytearray(b"order-1"))
