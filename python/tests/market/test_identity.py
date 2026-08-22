"""What an identifier is, and the frame it is computed from.

The frame is a *specification* -- another language has to produce the same
number -- so these pin the bytes, not only the behaviour. And the vectorised
builder is a second implementation of the scalar one, so every test that
matters compares them rather than either against itself: that is the only
check that catches a buffer walk reading one byte short, and the only one that
would have caught the two of them spelling a float differently.
"""

from __future__ import annotations

import datetime
import struct
import uuid

import pyarrow
import pytest

from rekep.market.identity import (
    ABSENT_FRAME,
    ABSENT_LENGTH,
    HASH,
    NIL,
    arrow_of,
    frame,
    hash_arrow,
    hash_bytes,
    hash_of,
    part_bytes,
)

SYMBOLS = ["AAPL", "MSFT", "AAPL", None, ""]
IDS = ["cl-1", "cl-2", "cl-1", "cl-3", "cl-4"]


def built(*columns: object) -> list[int]:
    return hash_arrow(*columns).to_pylist()


# -- what an identifier is ---------------------------------------------------


def test_an_identifier_is_a_signed_int64() -> None:
    """The one column every engine below Arrow reads the same way."""
    value = hash_of("Order", "AAPL")
    assert isinstance(value, int)
    assert -(2**63) <= value < 2**63
    assert hash_arrow("Order", pyarrow.array(["AAPL"])).type == HASH == pyarrow.int64()


def test_nothing_hashed_yet_is_zero_and_not_null() -> None:
    """`hash` is NOT NULL, so an unhashed row is a visible repeat, not a late failure."""
    assert NIL == 0


def test_the_same_parts_give_the_same_number_in_any_process() -> None:
    assert hash_of("Order", "AAPL", 1) == hash_of("Order", "AAPL", 1)
    assert hash_of("Order", "AAPL", 1) != hash_of("Order", "AAPL", 2)


# -- the frame, byte for byte ------------------------------------------------


def test_a_part_is_framed_behind_its_length_as_eight_little_endian_bytes() -> None:
    """The layout another language implements. Pinned, not described."""
    assert frame(("AB",)) == struct.pack("<q", 2) + b"AB"
    assert frame(("",)) == struct.pack("<q", 0)
    assert frame(("A", "B")) == struct.pack("<q", 1) + b"A" + struct.pack("<q", 1) + b"B"


def test_an_absent_part_is_framed_as_a_length_of_minus_one() -> None:
    """Not zero -- that is an empty part, which is a different fact about an order."""
    assert ABSENT_LENGTH == -1
    assert ABSENT_FRAME == struct.pack("<q", -1)
    assert frame((None,)) == ABSENT_FRAME
    assert frame((None,)) != frame(("",))


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


@pytest.mark.parametrize("label,part,stored,arrow_type", PARTS, ids=[row[0] for row in PARTS])
def test_the_three_ways_a_part_arrives_all_hash_the_same(
    label: str, part: object, stored: object, arrow_type: object
) -> None:
    """A part is one value, whichever door it comes in by."""
    scalar = hash_of("X", part)
    column = built("X", pyarrow.array([stored], type=arrow_type))[0]
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


def test_an_identifier_column_can_itself_be_a_part() -> None:
    """A child event hashes its parents, and a parent is an int64 now."""
    parents = pyarrow.array([hash_of("a"), hash_of("b")], type=HASH)
    made = built("Book", parents)
    assert made[0] != made[1]
    assert made[0] == hash_of("Book", hash_of("a"))


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


def test_an_integer_too_wide_for_an_int64_still_hashes() -> None:
    """Nothing may take the digest down; a part that does not fit is text."""
    assert part_bytes(2**70) == str(2**70).encode()
    assert hash_of("X", 2**70) == hash_of("X", str(2**70))


def test_a_date_is_still_spelled_by_arrow() -> None:
    """The one renderer the vectorised path can use, for what has no width here."""
    assert part_bytes(datetime.date(2024, 3, 14)) == b"2024-03-14"


def test_an_identifier_from_elsewhere_is_hashed_as_its_bytes() -> None:
    """A `uuid.UUID` is not stored here any more, but a caller may still pass one."""
    assert part_bytes(uuid.UUID(int=7)) == uuid.UUID(int=7).bytes


def test_something_arrow_has_no_scalar_for_still_hashes() -> None:
    class Odd:
        def __str__(self) -> str:
            return "odd"

    assert part_bytes(Odd()) == b"odd"
    assert hash_of("X", Odd()) == hash_of("X", "odd")


# -- a column of identifiers -------------------------------------------------


def test_a_column_of_identifiers_is_int64_whatever_it_arrives_as() -> None:
    values = [hash_of("a"), None]
    for column in (values, pyarrow.array(values, type=pyarrow.int64())):
        assert arrow_of(column).type == HASH
        assert arrow_of(column).to_pylist() == values


def test_a_narrow_identifier_column_widens() -> None:
    narrow = pyarrow.array([1, 2], type=pyarrow.int32())
    assert arrow_of(narrow).type == HASH and arrow_of(narrow).to_pylist() == [1, 2]


def test_a_chunked_column_converts_chunk_by_chunk() -> None:
    chunked = pyarrow.chunked_array([pyarrow.array([1], type=pyarrow.int32())])
    assert arrow_of(chunked).type == HASH


def test_a_lifecycle_part_no_cache_can_key_on_is_still_hashed() -> None:
    """The cache is a cache: a subclass whose lifecycle names a list gets the
    identifier it would have got without one."""
    import dataclasses

    from rekep.market import Order
    from rekep.market.identity import hash_of

    @dataclasses.dataclass
    class Listed(Order):
        """An order whose lifecycle is spelled as a list."""

        def life_parts(self) -> tuple[object, ...]:
            return ([1, 2, 3],)

    one = Listed(unix=1, symbol="BTC-USD")
    assert one.life_hash() == hash_of("Listed", [1, 2, 3])
