"""The time-anchored txhash couple: epoch seconds over an XXH32 tail."""

import datetime

import pyarrow
import pytest
import xxhash

from rekep import txhash


def test_the_couple_is_exact_and_reversible() -> None:
    value = txhash.h64(1_700_000_000, b"payload")
    assert txhash.seconds_of(value) == 1_700_000_000
    assert txhash.digest_of(value) == xxhash.xxh32_intdigest(b"payload")
    assert value == txhash.couple(1_700_000_000, txhash.digest_of(value))


def test_text_is_digested_as_utf8() -> None:
    assert txhash.h64(0, "café") == txhash.h64(0, "café".encode())
    assert txhash.xxh32_of("") == xxhash.xxh32_intdigest(b"")


def test_a_seed_moves_the_digest_not_the_clock() -> None:
    plain, seeded = txhash.h64(7, b"x"), txhash.h64(7, b"x", seed=11)
    assert plain != seeded
    assert txhash.seconds_of(plain) == txhash.seconds_of(seeded) == 7


def test_time_orders_first_so_the_column_sorts_by_it() -> None:
    """Later seconds always sort after earlier ones, whatever the digests."""
    earlier = txhash.couple(100, txhash.DIGEST_MASK)
    later = txhash.couple(101, 0)
    before_epoch = txhash.couple(-1, txhash.DIGEST_MASK)
    assert before_epoch < txhash.couple(0, 0) <= earlier < later
    assert txhash.seconds_of(before_epoch) == -1


def test_a_clock_outside_int32_is_refused() -> None:
    with pytest.raises(OverflowError, match="int32"):
        txhash.couple(1 << 31, 0)
    with pytest.raises(OverflowError, match="int32"):
        txhash.couple(0, 1 << 32)


def test_a_timestamp_clock_is_read_in_whole_seconds() -> None:
    when = datetime.datetime(2024, 1, 2, 3, 4, 5, 678_900, tzinfo=datetime.UTC)
    assert txhash.seconds_of(txhash.h64(when, b"")) == int(when.timestamp())


def test_the_kernel_matches_the_scalar_row_for_row() -> None:
    seconds = [0, 1_700_000_000, -5, 2_147_483_647]
    payloads = ["", "one", "café", "longer payload with more bytes"]
    hashed = txhash.h64_arrow(
        pyarrow.array(seconds, pyarrow.int32()), pyarrow.array(payloads, pyarrow.string())
    )
    assert hashed.type == pyarrow.int64()
    assert hashed.to_pylist() == [
        txhash.h64(tick, text) for tick, text in zip(seconds, payloads, strict=True)
    ]
    seeded = txhash.h64_arrow(
        pyarrow.array(seconds, pyarrow.int64()),
        pyarrow.array([text.encode() for text in payloads], pyarrow.binary()),
        seed=11,
    )
    assert seeded.to_pylist() == [
        txhash.h64(tick, text, seed=11) for tick, text in zip(seconds, payloads, strict=True)
    ]


def test_the_kernel_reads_a_slice_where_it_stands() -> None:
    seconds = pyarrow.array([9, 10, 11, 12], pyarrow.int32())[1:3]
    payloads = pyarrow.array(["a", "b", "c", "d"])[1:3]
    assert txhash.h64_arrow(seconds, payloads).to_pylist() == [
        txhash.h64(10, "b"),
        txhash.h64(11, "c"),
    ]


def test_a_null_on_either_side_is_a_null_txhash() -> None:
    hashed = txhash.h64_arrow(
        pyarrow.array([1, None, 3, 4], pyarrow.int32()),
        pyarrow.array(["a", "b", None, "d"]),
    )
    assert hashed.to_pylist() == [txhash.h64(1, "a"), None, None, txhash.h64(4, "d")]


def test_chunked_and_empty_columns_are_welcome() -> None:
    chunked = pyarrow.chunked_array([["a"], ["b"]])
    ticks = pyarrow.chunked_array([[1], [2]], pyarrow.int32())
    assert txhash.h64_arrow(ticks, chunked).to_pylist() == [
        txhash.h64(1, "a"),
        txhash.h64(2, "b"),
    ]
    empty = txhash.h64_arrow(
        pyarrow.array([], pyarrow.int32()), pyarrow.array([], pyarrow.string())
    )
    assert len(empty) == 0 and empty.type == pyarrow.int64()


def test_the_halves_come_back_out_of_a_column() -> None:
    values = pyarrow.array([txhash.h64(100, b"a"), txhash.h64(-3, b"b"), None], pyarrow.int64())
    assert txhash.seconds_arrow(values).to_pylist() == [100, -3, None]
    assert txhash.digest_arrow(values).to_pylist() == [
        txhash.xxh32_of(b"a"),
        txhash.xxh32_of(b"b"),
        None,
    ]
    assert txhash.seconds_arrow(values).type == pyarrow.int32()
    assert txhash.digest_arrow(values).type == pyarrow.uint32()


def test_a_clock_column_outside_int32_is_refused() -> None:
    with pytest.raises(pyarrow.ArrowInvalid):
        txhash.h64_arrow(pyarrow.array([1 << 31], pyarrow.int64()), pyarrow.array(["x"]))


def test_mismatched_lengths_and_wrong_types_are_refused() -> None:
    with pytest.raises(ValueError, match="same length"):
        txhash.h64_arrow(pyarrow.array([1], pyarrow.int32()), pyarrow.array(["a", "b"]))
    with pytest.raises(TypeError, match="integer column"):
        txhash.h64_arrow(pyarrow.array([1.5]), pyarrow.array(["a"]))
    with pytest.raises(TypeError, match="binary column"):
        txhash.h64_arrow(pyarrow.array([1], pyarrow.int32()), pyarrow.array([2]))
