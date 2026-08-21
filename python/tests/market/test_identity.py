"""Sixteen bytes, and the two ways of building them agreeing exactly.

The vectorised builder is a second implementation of the scalar one, so every
test here that matters compares them rather than comparing either against
itself: that is the only check that catches a buffer walk that reads one byte
short on a sliced batch.
"""

from __future__ import annotations

import uuid

import pyarrow
import pytest

from rekep.market.identity import ABSENT, HASH, SEPARATOR, arrow_of, hash_arrow, hash_of, uuids_of

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
    """Deliberate, and the one equivalence there is: text and bytes that are
    the same bytes are the same part, so a call site must keep one type per
    position rather than mixing two that mean different things."""
    assert hash_of(b"AB") == hash_of("AB")
    assert hash_of(b"") == hash_of("")
    assert hash_of(65) == hash_of("65")


@pytest.mark.parametrize("parts", ADVERSARIAL, ids=repr)
def test_the_column_builder_agrees_on_every_adversarial_tuple(parts: tuple[object, ...]) -> None:
    """The two implementations must agree where it is hardest, not only where it is easy."""
    columns = [
        pyarrow.array([part], type=pyarrow.binary() if isinstance(part, bytes) else None)
        for part in parts
    ]
    assert uuids_of(hash_arrow(*columns)) == [hash_of(*parts)]
