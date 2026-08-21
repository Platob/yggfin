import datetime
import decimal
import os
import subprocess
import sys
import uuid
from pathlib import Path

import numpy
import pyarrow
import pytest

from rekep import ids

#: A moment inside every range the tests use, and the one the log fixtures sit
#: on, so an id here is an id the parser would mint.
MOMENT = 1_786_665_901_147  # 2026-08-14 00:05:01.147 UTC

#: Derived from the layout rather than restated, then pinned below, so a change
#: to HASH_BITS cannot move both sides of an assertion together.
TIME_BITS = 63 - ids.HASH_BITS
MAX_MILLIS = (1 << TIME_BITS) - 1


def test_the_layout_is_what_the_tests_assume() -> None:
    assert ids.HASH_BITS == 21
    assert ids.TIME_BITS == TIME_BITS == 42
    assert MAX_MILLIS == 4_398_046_511_103
    # The overflow year the module documents, and the birthday bound beside it.
    overflow = datetime.datetime.fromtimestamp(MAX_MILLIS / 1000, tz=datetime.UTC)
    assert overflow.strftime("%Y-%m-%d") == "2109-05-15"
    assert round(1.1774 * (2**ids.HASH_BITS) ** 0.5) == 1705


# -- packing ----------------------------------------------------------------


@pytest.mark.parametrize("millis", [0, 1, MOMENT, MAX_MILLIS])
@pytest.mark.parametrize("payload", [b"", b"a line", b"\xff" * 64])
def test_pack_and_unpack_are_inverses(millis: int, payload: bytes) -> None:
    packed = ids.pack(millis, ids.hash_payload(payload))
    assert ids.unpack(packed) == (millis, ids.fold(ids.hash_payload(payload)))
    assert ids.pack(*ids.unpack(packed)) == packed


@pytest.mark.parametrize("millis", [0, 1, MOMENT, MAX_MILLIS])
def test_the_sign_bit_stays_clear(millis: int) -> None:
    """Every consumer downstream has a signed int64 and sorts it as one."""
    packed = ids.pack(millis, 2**64 - 1)
    assert packed >= 0
    assert packed >> 63 == 0
    assert packed < 2**63


def test_time_orders_before_hash() -> None:
    """A later millisecond is a larger id, whatever the hashes are."""
    early = ids.pack(MOMENT, 2**64 - 1)
    late = ids.pack(MOMENT + 1, 0)
    assert early < late


def test_within_one_millisecond_the_hash_is_the_tiebreak() -> None:
    """Deterministic, not arbitrary: the same payload always sorts the same way."""
    rows = [b"alpha", b"beta", b"gamma", b"delta"]
    packed = {row: ids.pack(MOMENT, ids.hash_payload(row)) for row in rows}
    assert len({value >> ids.HASH_BITS for value in packed.values()}) == 1, "one millisecond"
    assert sorted(packed, key=lambda row: packed[row]) == sorted(
        rows, key=lambda row: ids.fold(ids.hash_payload(row))
    )
    assert [ids.pack(MOMENT, ids.hash_payload(row)) for row in rows] == list(packed.values())


def test_a_custom_epoch_moves_the_window() -> None:
    """Bits are spent from the epoch: 2020 buys fifty years and refuses 2019."""
    packed = ids.pack(MOMENT, 0, epoch_ms=ids.EPOCH_MS)
    assert ids.unpack(packed, epoch_ms=ids.EPOCH_MS) == (MOMENT, 0)
    assert packed < ids.pack(MOMENT, 0), "the same moment is a smaller id from a later epoch"
    with pytest.raises(ValueError, match="outside the 42 time bits"):
        ids.pack(ids.EPOCH_MS - 1, 0, epoch_ms=ids.EPOCH_MS)


@pytest.mark.parametrize("millis", [-1, MAX_MILLIS + 1, 1 << 62])
def test_a_timestamp_that_does_not_fit_is_refused(millis: int) -> None:
    """Wrapping would mint an id that sorts *before* rows from years earlier."""
    with pytest.raises(ValueError, match="time bits of a row id"):
        ids.pack(millis, 0)


def test_the_range_is_named_in_the_refusal() -> None:
    with pytest.raises(ValueError, match=r"1970-01-01 to 2109-05-15"):
        ids.pack(MAX_MILLIS + 1, 0)


def test_a_negative_id_was_never_packed_here() -> None:
    with pytest.raises(ValueError, match="sign bit"):
        ids.unpack(-1)


@pytest.mark.parametrize("hash_bits", [8, 16, 21, 24, 32])
def test_any_hash_width_still_packs_and_unpacks(hash_bits: int) -> None:
    """The width is a knob, and the time range moves with it -- see the table."""
    widest = (1 << (63 - hash_bits)) - 1
    for millis in (0, widest):
        packed = ids.pack(millis, 2**64 - 1, hash_bits=hash_bits)
        assert packed >> 63 == 0
        assert ids.unpack(packed, hash_bits=hash_bits)[0] == millis
    with pytest.raises(ValueError, match="time bits of a row id"):
        ids.pack(widest + 1, 0, hash_bits=hash_bits)


# -- folding ----------------------------------------------------------------


def test_folding_uses_the_whole_word_not_the_low_bits() -> None:
    """Truncation would map every one of these to zero."""
    high_only = [1 << bit for bit in range(ids.HASH_BITS, 64)]
    folded = [ids.fold(value) for value in high_only]
    assert all(value & ((1 << ids.HASH_BITS) - 1) == 0 for value in high_only)
    assert all(value != 0 for value in folded), "every high bit reaches the low bits"


def test_folding_is_idempotent_on_a_value_that_already_fits() -> None:
    """What makes `pack(*unpack(i)) == i` true rather than nearly true."""
    for value in (0, 1, (1 << ids.HASH_BITS) - 1):
        assert ids.fold(value) == value


def test_a_signed_hash_folds_to_the_same_value() -> None:
    digest = ids.hash_payload(b"a line")
    assert ids.fold(ids.signed(digest)) == ids.fold(digest)
    assert -(2**63) <= ids.signed(digest) < 2**63


# -- hashing ----------------------------------------------------------------


def test_the_hash_is_xxh3_under_the_declared_seed() -> None:
    """The digest *is* the low half of every id, so it is pinned, not chosen."""
    xxhash = pytest.importorskip("xxhash")
    assert ids.hash_payload(b"a line") == xxhash.xxh3_64_intdigest(b"a line", seed=ids.SEED)
    assert ids.SEED == 0x9E3779B185EBCA87


def test_the_hash_is_pinned_to_a_literal() -> None:
    """A value nobody can move without noticing: ids already stored depend on it."""
    assert ids.hash_payload(b"") == 573_944_526_771_794_253
    assert ids.hash_payload(b"a line") == 13_684_663_062_259_894_221


# -- canonical bytes --------------------------------------------------------


def test_a_mapping_hashes_the_same_whatever_order_it_was_built_in() -> None:
    assert ids.canonical({"b": 1, "a": None}) == ids.canonical({"a": None, "b": 1})
    assert ids.hash_row({"b": 1, "a": None}) == ids.hash_row({"a": None, "b": 1})


def test_a_null_is_not_an_empty_string() -> None:
    """Two different rows, so two different ids -- the sentinel is explicit."""
    assert ids.canonical({"venue": None}) != ids.canonical({"venue": ""})


def test_fields_cannot_run_into_each_other() -> None:
    """Framing: without it, ('ab', 'c') and ('a', 'bc') are the same row."""
    assert ids.canonical(["ab", "c"]) != ids.canonical(["a", "bc"])
    assert ids.canonical({"a": "b", "c": "d"}) != ids.canonical({"a": "bc", "": "d"})


def test_a_value_and_its_spelling_are_different_rows() -> None:
    assert ids.canonical([1]) != ids.canonical(["1"])
    assert ids.canonical([1]) != ids.canonical([b"1"])
    assert ids.canonical([True]) != ids.canonical([1])


def test_floats_that_compare_equal_hash_equal() -> None:
    assert ids.canonical([-0.0]) == ids.canonical([0.0])
    assert ids.canonical([float("nan")]) == ids.canonical([float("nan")])


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        0,
        -1,
        2**70,
        1.5,
        "text",
        b"bytes",
        decimal.Decimal("1.100"),
        datetime.datetime(2026, 8, 14, tzinfo=datetime.UTC),
        datetime.date(2026, 8, 14),
        datetime.time(12, 30),
        uuid.UUID("00000000-0000-0000-0000-000000000001"),
        [1, "two", None],
        {"a": [1, {"b": None}]},
    ],
)
def test_every_supported_kind_encodes_and_is_stable(value: object) -> None:
    assert ids.canonical([value]) == ids.canonical([value])
    assert isinstance(ids.hash_row([value]), int)


def test_a_decimal_hashes_by_value_not_by_spelling() -> None:
    assert ids.canonical([decimal.Decimal("1.10")]) == ids.canonical([decimal.Decimal("1.1")])


def test_a_naive_datetime_is_read_as_utc() -> None:
    naive = datetime.datetime(2026, 8, 14, 0, 5, 1)  # noqa: DTZ001 - that is the point
    aware = naive.replace(tzinfo=datetime.UTC)
    assert ids.canonical([naive]) == ids.canonical([aware])


def test_a_kind_with_no_canonical_form_is_refused() -> None:
    with pytest.raises(TypeError, match="no canonical form"):
        ids.canonical([object()])


def test_row_id_takes_bytes_or_a_row() -> None:
    line = b"2026-08-14 00:05:01.147_250 [t] [d] (INFO) hello"
    assert ids.row_id(MOMENT, line) == ids.pack(MOMENT, ids.hash_payload(line))
    row = {"symbol": "XPAR", "size": 5}
    assert ids.row_id(MOMENT, row) == ids.pack(MOMENT, ids.hash_row(row))


def test_row_id_takes_a_datetime() -> None:
    moment = datetime.datetime(2026, 8, 14, 0, 5, 1, 147_000, tzinfo=datetime.UTC)
    assert ids.row_id(moment, b"x") == ids.row_id(MOMENT, b"x")


# -- whole columns ----------------------------------------------------------


def test_the_column_path_agrees_with_the_scalar_one() -> None:
    payloads = [b"alpha", b"beta", b"gamma"]
    millis = [MOMENT, MOMENT, MOMENT + 7]
    hashes = [ids.hash_payload(payload) for payload in payloads]
    packed = ids.pack_arrow(
        pyarrow.array(millis, pyarrow.timestamp("ms")),
        pyarrow.array([ids.signed(value) for value in hashes], pyarrow.int64()),
    )
    assert packed.type == pyarrow.int64()
    assert packed.to_pylist() == [
        ids.pack(moment, value) for moment, value in zip(millis, hashes, strict=True)
    ]


@pytest.mark.parametrize(
    ("unit", "factor"), [("s", 1 / 1000), ("ms", 1), ("us", 1000), ("ns", 1_000_000)]
)
def test_every_time_unit_lands_on_the_same_millisecond(unit: str, factor: float) -> None:
    millis = MOMENT - MOMENT % 1000  # a whole second, so the coarse unit is exact
    ticks = int(millis * factor)
    column = pyarrow.array([ticks], pyarrow.timestamp(unit))
    packed = ids.pack_arrow(column, pyarrow.array([0], pyarrow.int64()))
    assert packed.to_pylist() == [ids.pack(millis, 0)]


def test_an_integer_column_needs_its_unit_named() -> None:
    """A bare int64 carries no unit, so the caller says which one it is.

    Getting it wrong is loud rather than quiet here: nanoseconds read as
    milliseconds are a million times too large and fall outside the time bits.
    """
    ticks = numpy.array([MOMENT * 1_000_000], dtype=numpy.int64)
    hashes = numpy.array([0], dtype=numpy.int64)
    assert ids.pack_arrow(ticks, hashes, unit="ns").to_pylist() == [ids.pack(MOMENT, 0)]
    with pytest.raises(ValueError, match="time bits of a row id"):
        ids.pack_arrow(ticks, hashes)


def test_a_millisecond_column_reaches_numpy_without_a_copy() -> None:
    """A timestamp is already int64 ticks, so the view shares the buffer."""
    column = pyarrow.array([MOMENT, MOMENT + 1], pyarrow.timestamp("ms"))
    ticks = ids.epoch_millis(column)
    assert ticks.tolist() == [MOMENT, MOMENT + 1]
    assert column.view(pyarrow.int64()).buffers()[1].address == column.buffers()[1].address


def test_the_column_path_never_promotes_to_float64() -> None:
    """float64 holds 53 bits: the whole hash half would be rounded away."""
    hashes = numpy.array([ids.signed(2**64 - 1)], dtype=numpy.int64)
    packed = ids.pack_arrow(
        pyarrow.array([MAX_MILLIS], pyarrow.timestamp("ms")), hashes, hash_bits=ids.HASH_BITS
    )
    assert packed.to_pylist() == [ids.pack(MAX_MILLIS, 2**64 - 1)]
    assert ids.fold_numpy(hashes).dtype == numpy.uint64


def test_unpack_arrow_is_the_inverse_of_pack_arrow() -> None:
    millis = pyarrow.array([MOMENT, MOMENT + 1], pyarrow.timestamp("ms"))
    hashes = pyarrow.array([ids.signed(ids.hash_payload(b"a")), 7], pyarrow.int64())
    packed = ids.pack_arrow(millis, hashes)
    times, folded = ids.unpack_arrow(packed)
    assert times.to_pylist() == [MOMENT, MOMENT + 1]
    assert folded.to_pylist() == [ids.fold(ids.hash_payload(b"a")), 7]


def test_the_column_path_refuses_what_the_scalar_one_refuses() -> None:
    with pytest.raises(ValueError, match="time bits of a row id"):
        ids.pack_arrow(
            pyarrow.array([MAX_MILLIS + 1], pyarrow.timestamp("ms")),
            pyarrow.array([0], pyarrow.int64()),
        )


def test_a_null_has_no_id() -> None:
    with pytest.raises(ValueError, match="null timestamp"):
        ids.pack_arrow(
            pyarrow.array([MOMENT, None], pyarrow.timestamp("ms")),
            pyarrow.array([0, 0], pyarrow.int64()),
        )
    with pytest.raises(ValueError, match="null hash"):
        ids.pack_arrow(
            pyarrow.array([MOMENT, MOMENT], pyarrow.timestamp("ms")),
            pyarrow.array([0, None], pyarrow.int64()),
        )


def test_an_empty_column_packs_to_an_empty_column() -> None:
    packed = ids.pack_arrow(
        pyarrow.array([], pyarrow.timestamp("ms")), pyarrow.array([], pyarrow.int64())
    )
    assert packed.to_pylist() == []


def test_a_chunked_column_is_handled() -> None:
    millis = pyarrow.chunked_array([[MOMENT], [MOMENT + 1]], pyarrow.timestamp("ms"))
    hashes = pyarrow.chunked_array([[0], [1]], pyarrow.int64())
    assert ids.pack_arrow(millis, hashes).to_pylist() == [
        ids.pack(MOMENT, 0),
        ids.pack(MOMENT + 1, 1),
    ]


def test_a_column_that_is_not_a_time_is_refused() -> None:
    with pytest.raises(TypeError, match="not a time column"):
        ids.pack_arrow(pyarrow.array(["now"]), pyarrow.array([0], pyarrow.int64()))


def test_ids_sort_a_column_the_way_the_clock_does() -> None:
    """What the layout is for: order by id is order by time, then by payload."""
    moments = [MOMENT + 2, MOMENT, MOMENT, MOMENT + 1]
    payloads = [b"d", b"b", b"a", b"c"]
    packed = ids.pack_arrow(
        pyarrow.array(moments, pyarrow.timestamp("ms")),
        pyarrow.array([ids.signed(ids.hash_payload(row)) for row in payloads], pyarrow.int64()),
    )
    order = pyarrow.compute.array_sort_indices(packed).to_pylist()
    assert [moments[index] for index in order] == sorted(moments)


# -- determinism ------------------------------------------------------------

#: Run in a fresh interpreter with hash randomisation *on*, which is what
#: `hash()` would be salted by. Printed as text so the parent compares the
#: bytes another process produced, not an object it shares.
CHILD = """
import json, sys
sys.path.insert(0, {source!r})
from rekep import ids

row = {{"symbol": "XPAR", "size": 5, "venue": None, "tags": ["a", "b"]}}
print(json.dumps({{
    "hash": ids.hash_payload(b"a line"),
    "row": ids.hash_row(row),
    "canonical": ids.canonical(row).decode("latin-1"),
    "id": ids.row_id({moment}, row),
    "seed": ids.SEED,
}}))
"""


def child_ids(seed: str) -> dict:
    """One id, minted by a separate interpreter under `PYTHONHASHSEED`."""
    source = str(Path(__file__).resolve().parents[1] / "src")
    environment = {**os.environ, "PYTHONHASHSEED": seed}
    finished = subprocess.run(  # noqa: S603 - our own interpreter, our own script
        [sys.executable, "-c", CHILD.format(source=source, moment=MOMENT)],
        capture_output=True,
        text=True,
        check=True,
        env=environment,
    )
    import json

    return json.loads(finished.stdout)


def test_ids_are_the_same_in_another_process() -> None:
    """An id is an identity: a second process must mint the one already stored."""
    row = {"symbol": "XPAR", "size": 5, "venue": None, "tags": ["a", "b"]}
    here = {
        "hash": ids.hash_payload(b"a line"),
        "row": ids.hash_row(row),
        "canonical": ids.canonical(row).decode("latin-1"),
        "id": ids.row_id(MOMENT, row),
        "seed": ids.SEED,
    }
    assert child_ids("random") == here


def test_two_processes_with_different_hash_seeds_agree() -> None:
    """The one thing `hash()` could not do -- which is why it is not used."""
    assert child_ids("random") == child_ids("0")
    assert child_ids("1") == child_ids("12345")
