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
    assert txhash.h64(when.replace(tzinfo=None), b"") == txhash.h64(when, b"")
    assert txhash.h128(when.replace(tzinfo=None), b"") == txhash.h128(when, b"")


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


# -- hashing what a row is made of -------------------------------------------


def _batch() -> pyarrow.RecordBatch:
    when = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=datetime.UTC)
    return pyarrow.RecordBatch.from_arrays(
        [
            pyarrow.array([when, when], pyarrow.timestamp("us", tz="UTC")),
            pyarrow.array(["BTC", "ETH"]),
            pyarrow.array([1, 2], pyarrow.int32()),
        ],
        names=["unix", "symbol", "qty"],
    )


def test_several_columns_hash_as_one_framed_payload() -> None:
    batch = _batch()
    hashed = txhash.h64_arrow_arrays(
        batch.column("unix"), batch.column("symbol"), batch.column("qty")
    )
    assert hashed.type == pyarrow.int64()
    assert txhash.seconds_arrow(hashed).to_pylist() == [1_700_000_000] * 2
    assert hashed[0].as_py() != hashed[1].as_py(), "different rows, different digests"
    assert txhash.h64_arrow_batch(batch, "unix", "symbol", "qty").to_pylist() == hashed.to_pylist()


def test_framing_keeps_neighbouring_values_apart() -> None:
    """`ab` then `c` must not hash like `a` then `bc`."""
    clock = pyarrow.array([0, 0], pyarrow.int32())
    left = txhash.h64_arrow_arrays(clock, pyarrow.array(["ab", "a"]), pyarrow.array(["c", "bc"]))
    assert left[0].as_py() != left[1].as_py()


def test_a_clock_may_be_a_timestamp_epoch_seconds_or_text() -> None:
    when = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=datetime.UTC)
    payload = pyarrow.array(["x"])
    stamps = txhash.h64_arrow_arrays(
        pyarrow.array([when], pyarrow.timestamp("us", tz="UTC")), payload
    )
    seconds = txhash.h64_arrow_arrays(pyarrow.array([1_700_000_000], pyarrow.int32()), payload)
    spelled = txhash.h64_arrow_arrays(pyarrow.array(["2023-11-14T22:13:20"]), payload)
    assert stamps.to_pylist() == seconds.to_pylist() == spelled.to_pylist()
    nanos = pyarrow.array([1_700_000_000_000_000_000], pyarrow.timestamp("ns", tz="UTC"))
    assert txhash.epoch_seconds_arrow(nanos).to_pylist() == [1_700_000_000]
    assert txhash.epoch_seconds_arrow(nanos).type == pyarrow.int32()


def test_a_dataclass_hashes_exactly_as_its_batch_does() -> None:
    """The whole point of one renderer: a row hashed alone and a column hashed
    all at once are the same value."""
    import pyarrow as pa

    from rekep import Convertible, scalar

    @scalar
    class Trade(Convertible):
        unix: datetime.datetime
        symbol: str
        qty: int

    paris = datetime.timezone(datetime.timedelta(hours=1))
    when = datetime.datetime(2023, 11, 14, 23, 13, 20, tzinfo=paris)
    rows = [Trade(unix=when, symbol="BTC", qty=1), Trade(unix=when, symbol="ETH", qty=2)]
    scalars = [txhash.h64_dataclass(row, "unix", "symbol", "qty") for row in rows]
    field = Trade.into_field()
    batch = pa.RecordBatch.from_arrays(
        [
            field.field("unix").cast_arrow_array(pa.array([row.unix for row in rows])),
            pa.array([row.symbol for row in rows]),
            field.field("qty").cast_arrow_array(pa.array([row.qty for row in rows])),
        ],
        names=["unix", "symbol", "qty"],
    )
    assert txhash.h64_arrow_batch(batch, "unix", "symbol", "qty").to_pylist() == scalars


def test_the_selectors_and_the_shapes_are_checked() -> None:
    batch = _batch()
    with pytest.raises(ValueError, match="at least one column"):
        txhash.h64_arrow_arrays(batch.column("unix"))
    with pytest.raises(ValueError, match="at least one column name"):
        txhash.h64_arrow_batch(batch, "unix")
    with pytest.raises(ValueError, match="same number of rows"):
        txhash.h64_arrow_arrays(batch.column("unix"), pyarrow.array(["only-one"]))
    with pytest.raises(TypeError, match="clock column"):
        txhash.h64_arrow_arrays(pyarrow.array([1.5]), pyarrow.array(["x"]))


# -- one hundred and twenty-eight bits wide ----------------------------------


def test_the_wide_couple_is_exact_and_reversible() -> None:
    value = txhash.h128(1_700_000_000_000_000, b"payload")
    assert txhash.micros_of(value) == 1_700_000_000_000_000
    assert txhash.digest64_of(value) == xxhash.xxh64_intdigest(b"payload")
    assert value == txhash.couple128(1_700_000_000_000_000, txhash.digest64_of(value))
    assert value < 10**38, "and it fits the decimal a column stores it in"


def test_the_wide_couple_orders_by_time_first() -> None:
    assert txhash.h128(1, b"zzz") < txhash.h128(2, b"aaa")
    assert txhash.couple128(-1, txhash.DIGEST64_MASK) < txhash.couple128(0, 0)


def test_a_wide_clock_or_digest_out_of_range_is_refused() -> None:
    with pytest.raises(OverflowError, match="int64"):
        txhash.couple128(1 << 63, 0)
    with pytest.raises(OverflowError, match="int64"):
        txhash.couple128(0, 1 << 64)


def test_the_wide_kernel_matches_the_scalar_row_for_row() -> None:
    micros = [0, 1_700_000_000_000_000, -5]
    payloads = ["", "one", "café"]
    hashed = txhash.h128_arrow(
        pyarrow.array(micros, pyarrow.int64()), pyarrow.array(payloads, pyarrow.string())
    )
    assert hashed.type == txhash.TXHASH128
    assert [txhash.wide_of(one) for one in hashed.to_pylist()] == [
        txhash.h128(tick, text) for tick, text in zip(micros, payloads, strict=True)
    ]


def test_the_wide_halves_come_back_out_of_a_column() -> None:
    values = pyarrow.array([txhash.wide_bytes(txhash.h128(100, b"a")), None], txhash.TXHASH128)
    assert txhash.micros_arrow(values).to_pylist() == [100, None]
    assert txhash.digest64_arrow(values).to_pylist() == [txhash.xxh64_of(b"a"), None]
    assert txhash.micros_arrow(values).type == pyarrow.int64()
    assert txhash.digest64_arrow(values).type == pyarrow.uint64()


def test_a_null_on_either_side_is_a_null_wide_txhash() -> None:
    hashed = txhash.h128_arrow(
        pyarrow.array([1, None, 3], pyarrow.int64()), pyarrow.array(["a", "b", None])
    )
    assert [None if one is None else txhash.wide_of(one) for one in hashed.to_pylist()] == [
        txhash.h128(1, "a"),
        None,
        None,
    ]


def test_the_wide_builders_agree_across_arrays_batch_and_dataclass() -> None:
    from rekep import Convertible, scalar

    @scalar
    class Trade(Convertible):
        unix: datetime.datetime
        symbol: str
        qty: int

    paris = datetime.timezone(datetime.timedelta(hours=1))
    when = datetime.datetime(2023, 11, 14, 23, 13, 20, 678_900, tzinfo=paris)
    rows = [Trade(unix=when, symbol="BTC", qty=1), Trade(unix=when, symbol="ETH", qty=2)]
    field = Trade.into_field()
    batch = pyarrow.RecordBatch.from_arrays(
        [
            field.field("unix").cast_arrow_array(pyarrow.array([row.unix for row in rows])),
            pyarrow.array([row.symbol for row in rows]),
            field.field("qty").cast_arrow_array(pyarrow.array([row.qty for row in rows])),
        ],
        names=["unix", "symbol", "qty"],
    )
    columns = txhash.h128_arrow_arrays(
        batch.column("unix"), batch.column("symbol"), batch.column("qty")
    )
    selected = txhash.h128_arrow_batch(batch, "unix", "symbol", "qty")
    assert [txhash.wide_of(one) for one in columns.to_pylist()] == [
        txhash.wide_of(one) for one in selected.to_pylist()
    ]
    assert [txhash.wide_of(one) for one in selected.to_pylist()] == [
        txhash.h128_dataclass(row, "unix", "symbol", "qty") for row in rows
    ]
    assert (
        txhash.epoch_micros_arrow(batch.column("unix")).to_pylist() == [1_700_000_000_678_900] * 2
    )
