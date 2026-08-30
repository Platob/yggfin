"""The time-anchored event hash: epoch microseconds over a signed value hash."""

import datetime

import pyarrow
import pytest

from rekep import txhash

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


# -- an event clock over its value hash -------------------------------------


@pytest.mark.parametrize("vhash", [-(2**63), -1, 0, 1, 2**63 - 1])
def test_the_event_hash_composition_is_exact_and_reversible(vhash: int) -> None:
    value = txhash.couple128(1_700_000_000_000_000, vhash)
    assert txhash.micros_of(value) == 1_700_000_000_000_000
    assert txhash.vhash_of(value) == vhash
    assert txhash.digest64_of(value) == vhash & txhash.DIGEST64_MASK
    assert txhash.vhash_of(txhash.wide_bytes(value)) == vhash
    assert value == txhash.couple128(txhash.micros_of(value), txhash.vhash_of(value))


def test_the_event_hash_integer_orders_by_signed_time_first() -> None:
    assert txhash.couple128(1, 2**63 - 1) < txhash.couple128(2, -(2**63))
    assert txhash.couple128(-1, -1) < txhash.couple128(0, 0)


def test_stored_event_hashes_order_nonnegative_times_chronologically() -> None:
    later = txhash.wide_bytes(txhash.couple128(1, -(2**63)))
    earlier = txhash.wide_bytes(txhash.couple128(0, 2**63 - 1))
    assert earlier < later


@pytest.mark.parametrize("micros,vhash", [(1 << 63, 0), (0, 1 << 63), (0, -(2**63) - 1)])
def test_an_event_clock_or_value_hash_out_of_range_is_refused(micros: int, vhash: int) -> None:
    with pytest.raises(OverflowError, match="int64"):
        txhash.couple128(micros, vhash)


def test_the_event_hash_kernel_matches_the_scalar_row_for_row() -> None:
    micros = [0, 1_700_000_000_000_000, -5, 9]
    vhashes = [-(2**63), -1, 0, 2**63 - 1]
    hashed = txhash.couple128_arrow(
        pyarrow.array(micros, pyarrow.int64()), pyarrow.array(vhashes, pyarrow.int64())
    )
    assert hashed.type == txhash.TXHASH128
    assert hashed.to_pylist() == [
        txhash.wide_bytes(txhash.couple128(tick, vhash))
        for tick, vhash in zip(micros, vhashes, strict=True)
    ]


def test_the_event_hash_kernel_reads_slices_where_they_stand() -> None:
    micros = pyarrow.array([10, 11, 12, 13], pyarrow.int64())[1:3]
    vhashes = pyarrow.array([-10, -11, -12, -13], pyarrow.int64())[1:3]
    found = txhash.couple128_arrow(micros, vhashes)
    assert [txhash.wide_of(one) for one in found.to_pylist()] == [
        txhash.couple128(11, -11),
        txhash.couple128(12, -12),
    ]


def test_a_null_on_either_side_is_a_null_event_hash() -> None:
    hashed = txhash.couple128_arrow(
        pyarrow.array([1, None, 3], pyarrow.int64()),
        pyarrow.array([-1, 2, None], pyarrow.int64()),
    )
    assert [None if one is None else txhash.wide_of(one) for one in hashed.to_pylist()] == [
        txhash.couple128(1, -1),
        None,
        None,
    ]


def test_event_hash_columns_must_align_and_be_integers() -> None:
    with pytest.raises(ValueError, match="same length"):
        txhash.couple128_arrow(pyarrow.array([1]), pyarrow.array([2, 3]))
    with pytest.raises(TypeError, match="integer column"):
        txhash.couple128_arrow(pyarrow.array([1.5]), pyarrow.array([2]))


def test_empty_event_hash_columns_produce_the_declared_type() -> None:
    empty = pyarrow.array([], pyarrow.int64())
    found = txhash.couple128_arrow(empty, empty)
    assert len(found) == 0
    assert found.type == txhash.TXHASH128


def test_event_hash_composition_refuses_an_ambiguous_big_endian_arrow_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(txhash.sys, "byteorder", "big")
    with pytest.raises(RuntimeError, match="little-endian"):
        txhash.couple128_arrow(pyarrow.array([1]), pyarrow.array([2]))
    assert txhash.couple128(1, 2)


def test_a_timestamp_clock_converts_to_epoch_microseconds() -> None:
    paris = datetime.timezone(datetime.timedelta(hours=1))
    when = datetime.datetime(2023, 11, 14, 23, 13, 20, 678_900, tzinfo=paris)
    values = pyarrow.array([when], pyarrow.timestamp("us", tz="UTC"))
    assert txhash.epoch_micros_arrow(values).to_pylist() == [1_700_000_000_678_900]
