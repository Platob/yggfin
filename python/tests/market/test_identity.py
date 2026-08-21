"""Sixteen bytes, and the two ways of building them agreeing exactly.

The vectorised builder is a second implementation of the scalar one, so every
test here that matters compares them rather than comparing either against
itself: that is the only check that catches a buffer walk that reads one byte
short on a sliced batch.
"""

from __future__ import annotations

import datetime
import struct
import uuid

import pyarrow
import pytest

from rekep.market.identity import (
    ABSENT,
    HASH,
    SEPARATOR,
    arrow_of,
    hash_arrow,
    hash_of,
    part_bytes,
    uuids_of,
)

SYMBOLS = ["AAPL", "MSFT", "AAPL", None, ""]
IDS = ["cl-1", "cl-2", "cl-1", "cl-3", "cl-4"]


def test_an_identifier_is_sixteen_bytes_and_nothing_else() -> None:
    built = hash_of("Order", "AAPL")
    assert isinstance(built, uuid.UUID)
    assert len(built.bytes) == 16
    assert hash_arrow("Order", pyarrow.array(["AAPL"])).type == HASH == pyarrow.binary(16)


def test_the_same_parts_give_the_same_bytes_in_any_process() -> None:
    """Reproducible, so two captures of one event deduplicate rather than double."""
    assert hash_of("Order", "AAPL", 1) == hash_of("Order", "AAPL", 1)
    assert hash_of("Order", "AAPL", 1) != hash_of("Order", "AAPL", 2)


def test_the_separator_keeps_a_composed_key_from_merging() -> None:
    """`("AB", "C")` and `("A", "BC")` are different keys and must stay different."""
    assert hash_of("AB", "C") != hash_of("A", "BC")


def test_a_missing_part_is_not_an_empty_one() -> None:
    """A client order id nobody sent and one sent empty are different facts."""
    assert hash_of("x", None) != hash_of("x", "")


def test_a_part_that_contains_the_separator_cannot_impersonate_two_parts() -> None:
    """The whole reason each part is hashed behind its own length."""
    smuggled = "A" + SEPARATOR.decode() + "B"
    assert hash_of(smuggled) != hash_of("A", "B")
    assert hash_of("1:A:1:B") != hash_of("A", "B")


def test_a_part_holding_the_absent_marker_is_not_a_missing_one() -> None:
    assert hash_of(ABSENT.decode()) != hash_of(None)


def test_the_column_builder_agrees_with_the_scalar_one_row_by_row() -> None:
    """The reference comparison: two implementations, checked against each other."""
    column = hash_arrow("Order", pyarrow.array(SYMBOLS), pyarrow.array(IDS))
    assert uuids_of(column) == [
        hash_of("Order", symbol, identifier)
        for symbol, identifier in zip(SYMBOLS, IDS, strict=True)
    ]


def test_a_sliced_batch_hashes_the_rows_it_actually_holds() -> None:
    """A buffer walk that ignored the array's offset would hash the wrong rows."""
    symbols, ids = pyarrow.array(SYMBOLS), pyarrow.array(IDS)
    whole = uuids_of(hash_arrow("Order", symbols, ids))
    sliced = uuids_of(hash_arrow("Order", symbols.slice(1, 3), ids.slice(1, 3)))
    assert sliced == whole[1:4]


def test_a_chunked_column_hashes_as_one_column() -> None:
    chunked = pyarrow.chunked_array([pyarrow.array(SYMBOLS[:2]), pyarrow.array(SYMBOLS[2:])])
    assert uuids_of(hash_arrow("Order", chunked, pyarrow.array(IDS))) == uuids_of(
        hash_arrow("Order", pyarrow.array(SYMBOLS), pyarrow.array(IDS))
    )


def test_a_wide_string_column_hashes_the_same_as_a_narrow_one() -> None:
    """pyiceberg hands strings back as `large_string`, which has 64-bit offsets."""
    narrow = pyarrow.array(SYMBOLS, type=pyarrow.string())
    wide = pyarrow.array(SYMBOLS, type=pyarrow.large_string())
    assert uuids_of(hash_arrow("Order", wide)) == uuids_of(hash_arrow("Order", narrow))


def test_an_identifier_column_can_itself_be_a_part() -> None:
    """A child event hashes its parents, so the bytes have to join as something."""
    parents = arrow_of([hash_of("a"), hash_of("b")])
    built = hash_arrow("Book", parents)
    assert built.type == HASH
    assert built[0].as_py() != built[1].as_py()


def test_no_rows_is_no_rows_rather_than_an_error() -> None:
    assert len(hash_arrow("Order", pyarrow.array([], type=pyarrow.string()))) == 0


def test_the_three_spellings_of_one_identifier_are_one_value() -> None:
    """A UUID, its bytes and its text all arrive at the store as the same column."""
    built = hash_of("Order", "AAPL")
    assert arrow_of([built, built.bytes, str(built)]).to_pylist() == [built.bytes] * 3


def test_a_column_round_trips_through_python_and_back() -> None:
    column = hash_arrow("Order", pyarrow.array(SYMBOLS))
    assert arrow_of(uuids_of(column)).equals(column)


def test_something_that_is_not_sixteen_bytes_is_refused_rather_than_padded() -> None:
    """Padding would make two different identifiers into one, in silence."""
    with pytest.raises(ValueError, match="16 bytes"):
        arrow_of([b"short"])


def test_hashing_nothing_is_refused() -> None:
    with pytest.raises(TypeError, match="at least one part"):
        hash_arrow()


#: Tuples chosen to be exactly the ones a separator alone confuses: the same
#: characters split differently, the marker bytes inside a value, and a value
#: that spells out the encoding the digest is taken over.
ADVERSARIAL = [
    ("AB", "C"),
    ("A", "BC"),
    ("ABC",),
    ("A", "B", "C"),
    ("A:B",),
    ("A", ":B"),
    ("A:", "B"),
    ("1:A:1:B",),
    ("A", "B"),
    ("", "AB"),
    ("AB", ""),
    ("", "", "AB"),
    (None, "AB"),
    ("AB", None),
    ("\x00", "AB"),
    (b"\x01\x1f\xff", "AB"),
]


def test_no_two_different_tuples_of_parts_share_an_identifier() -> None:
    """Injectivity, on the tuples a plain separator gets wrong."""
    built: dict[uuid.UUID, tuple[object, ...]] = {}
    for parts in ADVERSARIAL:
        digest = hash_of(*parts)
        assert digest not in built, f"{parts!r} collides with {built[digest]!r}"
        built[digest] = parts


def test_a_part_is_its_bytes_and_nothing_more() -> None:
    """Text and bytes that are the same bytes are the same part."""
    assert hash_of(b"AB") == hash_of("AB")
    assert hash_of(b"") == hash_of("")


def test_a_number_and_its_text_are_different_parts() -> None:
    """A number is its own eight bytes, so it cannot collide with a rendering
    of itself -- which a text encoding made it do. The safer direction, and a
    call site keeps one type per position anyway."""
    assert hash_of(65) != hash_of("65")
    assert hash_of(10) != hash_of(10.0), "an int64 and a float64 are different bytes"
    assert hash_of(1.0) != hash_of("1")


def test_zero_and_negative_zero_are_different_parts() -> None:
    """Their bytes differ, and a merge key that could not tell them apart is a
    war story this repository already has."""
    assert hash_of(0.0) != hash_of(-0.0)


@pytest.mark.parametrize("parts", ADVERSARIAL, ids=repr)
def test_the_column_builder_agrees_on_every_adversarial_tuple(parts: tuple[object, ...]) -> None:
    """The two implementations must agree where it is hardest, not only where it is easy."""
    columns = [
        pyarrow.array([part], type=pyarrow.binary() if isinstance(part, bytes) else None)
        for part in parts
    ]
    assert uuids_of(hash_arrow(*columns)) == [hash_of(*parts)]


#: Every kind of part a call site actually passes, with the Arrow column the
#: same value arrives in. Text-only coverage is what let the two builders
#: disagree on floats, booleans and identifiers all the way into a release:
#: `BookSide` hashes `px` and `qty`, and every event hashes its `xhash`.
PARTS: list[tuple[str, object, object, object]] = [
    ("float", 10.0, 10.0, pyarrow.float64()),
    ("float negative", -0.5, -0.5, pyarrow.float64()),
    ("float tiny", 1e-7, 1e-7, pyarrow.float64()),
    ("float huge", 1e300, 1e300, pyarrow.float64()),
    ("float whole", 3.0, 3.0, pyarrow.float64()),
    ("int", 42, 42, pyarrow.int64()),
    ("int negative", -7, -7, pyarrow.int64()),
    ("int zero", 0, 0, pyarrow.int64()),
    ("bool true", True, True, pyarrow.bool_()),
    ("bool false", False, False, pyarrow.bool_()),
    ("str", "AAPL", "AAPL", pyarrow.string()),
    ("str empty", "", "", pyarrow.string()),
    ("bytes", b"\x01\x02", b"\x01\x02", pyarrow.binary()),
    ("identifier", uuid.UUID(int=7), uuid.UUID(int=7).bytes, HASH),
    ("absent", None, None, pyarrow.string()),
]


@pytest.mark.parametrize("label,part,stored,arrow_type", PARTS, ids=[row[0] for row in PARTS])
def test_the_three_ways_a_part_arrives_all_hash_the_same(
    label: str, part: object, stored: object, arrow_type: object
) -> None:
    """A part is one value, whichever door it comes in by.

    Python spells `10.0`, `True` and `1e-07` where Arrow spells `10`, `true`
    and `1e-7`, and a `uuid.UUID` is thirty-six characters in Python and
    sixteen bytes in a column. Left alone, the scalar builder and the
    vectorised one gave the same event two identifiers.
    """
    scalar = hash_of("X", part)
    column = uuids_of(hash_arrow("X", pyarrow.array([stored], type=arrow_type)))[0]
    broadcast = uuids_of(hash_arrow("X", part))[0]
    assert scalar == column, f"{label}: the column builder disagrees"
    assert scalar == broadcast, f"{label}: a broadcast scalar disagrees"


def test_the_parts_a_book_side_hashes_are_covered_here() -> None:
    """Otherwise the table above can drift away from what the code passes."""
    covered = {kind.split()[0] for kind, *_ in PARTS}
    assert {"identifier", "int", "float", "absent"} <= covered, (
        "`BookSide._versioned` hashes an xhash, a version, an instant and two "
        "prices that may be null -- every one of them has to be in PARTS"
    )


def test_an_identifier_is_hashed_as_its_bytes_and_not_as_its_text() -> None:
    """Which is what the column it lives in holds."""
    identifier = uuid.UUID(int=7)
    assert hash_of("X", identifier) == hash_of("X", identifier.bytes)
    assert hash_of("X", identifier) != hash_of("X", str(identifier))


def test_a_number_is_hashed_as_its_own_bytes() -> None:
    """No formatter, so there is nothing for two of them to disagree about."""
    assert part_bytes(10.0) == struct.pack("<d", 10.0)
    assert part_bytes(42) == struct.pack("<q", 42)
    assert part_bytes(True) == b"\x01"
    assert part_bytes(False) == b"\x00"
    assert part_bytes(None) is None
    assert part_bytes(b"\x01") == b"\x01"
    assert part_bytes("AAPL") == b"AAPL"
    assert part_bytes(uuid.UUID(int=1)) == uuid.UUID(int=1).bytes


def test_an_integer_too_wide_for_arrow_still_hashes() -> None:
    """Nothing may take the digest down; a part Arrow has no scalar for is text."""
    assert part_bytes(2**70) == str(2**70).encode()
    assert hash_of("X", 2**70) == hash_of("X", str(2**70))


def test_a_date_is_still_spelled_by_arrow() -> None:
    """The one renderer the vectorised path can use, for what has no width here."""
    assert part_bytes(datetime.date(2024, 3, 14)) == b"2024-03-14"


def test_something_arrow_has_no_scalar_for_still_hashes() -> None:
    """A part this package has never seen must not take the digest down."""

    class Odd:
        def __str__(self) -> str:
            return "odd"

    assert part_bytes(Odd()) == b"odd"
    assert hash_of("X", Odd()) == hash_of("X", "odd")


def test_a_book_side_built_twice_gets_one_identifier() -> None:
    """The contract the divergence broke: the same event is the same row."""
    from rekep.market import BookSide, Order, Side, State

    def built() -> BookSide:
        side = BookSide(side=Side.BID, xhash=uuid.UUID(int=3), unix=1_000)
        side.append_order(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW, unix=1_001))
        return side

    assert built().hash == built().hash


#: Every way a column of identifiers arrives. Iterating an Arrow array instead
#: of converting it handed the per-value path `pyarrow.Scalar` objects, which
#: came out as `badly formed hexadecimal UUID string` -- a message naming
#: neither the column nor the way out, on inputs the docstring promises.
COLUMNS = [
    ("fixed16", lambda u: pyarrow.array([u.bytes, None], type=HASH)),
    ("binary", lambda u: pyarrow.array([u.bytes, None], type=pyarrow.binary())),
    ("large_binary", lambda u: pyarrow.array([u.bytes, None], type=pyarrow.large_binary())),
    ("string", lambda u: pyarrow.array([str(u), None], type=pyarrow.string())),
    ("large_string", lambda u: pyarrow.array([str(u), None], type=pyarrow.large_string())),
    ("python list", lambda u: [u, None]),
    ("bytes list", lambda u: [u.bytes, None]),
    ("text list", lambda u: [str(u), None]),
]


@pytest.mark.parametrize("label,build", COLUMNS, ids=[row[0] for row in COLUMNS])
def test_every_spelling_of_a_column_of_identifiers_converts(label: str, build) -> None:
    identifier = uuid.UUID(int=7)
    built = arrow_of(build(identifier))
    assert built.type == HASH, label
    assert uuids_of(built) == [identifier, None], label


def test_a_chunked_column_converts_chunk_by_chunk() -> None:
    identifier = uuid.UUID(int=7)
    chunked = pyarrow.chunked_array(
        [
            pyarrow.array([identifier.bytes], type=pyarrow.binary()),
            pyarrow.array([None], type=pyarrow.binary()),
        ]
    )
    built = arrow_of(chunked)
    assert built.type == HASH
    assert uuids_of(built.combine_chunks()) == [identifier, None]


def test_a_column_of_the_wrong_width_is_refused_by_its_width() -> None:
    with pytest.raises(ValueError, match="16 bytes"):
        arrow_of(pyarrow.array([b"12345678"], type=pyarrow.binary(8)))


def test_a_column_that_is_not_identifiers_at_all_is_refused_by_its_type() -> None:
    with pytest.raises(TypeError, match="not int64"):
        arrow_of(pyarrow.array([1, 2]))
