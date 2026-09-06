"""Benchmark plain and gzip text objects into raw `Message` batches."""

from __future__ import annotations

import gzip
import pathlib
import sys
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pyarrow
from yggdryl import IOBase, TextOptions

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import best_of, parser  # noqa: E402

from rekep.text import Message  # noqa: E402
from rekep.times import MESSAGE_HEADER, datetime_of  # noqa: E402

# Yggdryl's default when TextOptions leaves the bound unset.
BATCH_ROW_SIZE = 65_536
FIELD = Message.field()
SCHEMA = FIELD.into_arrow_schema()


def timestamp(index: int) -> str:
    """The deterministic source spelling for one row."""
    return f"2026-08-14 00:05:{index % 60:02d}.{index % 1_000:03d}_{index % 997:03d}"


def body(index: int) -> bytes:
    """One representative wire payload retained without interpretation."""
    return (
        f"sending >> 8=FIX.4.4|35=D|11=ORD-{index:010d}|55=S{index % 512}|"
        f"38={index % 10_000 + 1}|44={index % 10_000 / 100:.2f}|10=000|"
    ).encode()


def line(index: int) -> bytes:
    """One log row matched by the current default message header."""
    level = "WARN" if index % 7 == 0 else "INFO"
    return (
        (f"{timestamp(index)} [worker-{index % 16}] [feed-{index % 4}] ({level}) ").encode()
        + body(index)
        + b"\n"
    )


def corpus(rows: int) -> bytes:
    """A bounded capture whose first and last records are reproducible."""
    decoded = bytearray()
    for index in range(rows):
        decoded.extend(line(index))
    return bytes(decoded)


def text_options() -> TextOptions:
    """The options used by `parse_messages`."""
    options = TextOptions()
    options.with_rownum = 1
    options.rowheader = MESSAGE_HEADER
    options.autotype = False
    return options


OPTIONS = text_options()


@dataclass(frozen=True)
class Case:
    """One resource binding and content coding."""

    name: str
    source: Callable[[], IOBase]
    filename: str


def message_batches(source: IOBase) -> Iterator[pyarrow.RecordBatch]:
    """The exact yggdryl-to-Message boundary used by `parse_messages`."""
    reader = source.read_arrow_reader(options=OPTIONS)
    try:
        for batch in reader:
            yield Message.cast_arrow_batch(batch)
    finally:
        reader.close()
        source.close()


def raw_drain(case: Case) -> int:
    """Drain yggdryl batches before the Message boundary."""
    source = case.source()
    reader = source.read_arrow_reader(options=OPTIONS)
    try:
        return sum(batch.num_rows for batch in reader)
    finally:
        reader.close()
        source.close()


def drain(case: Case) -> int:
    """Read and cast every batch without collecting the stream."""
    return sum(batch.num_rows for batch in message_batches(case.source()))


def first_batch(case: Case) -> int:
    """Read and cast the first batch, closing the source immediately after it."""
    batches = message_batches(case.source())
    try:
        batch = next(batches, None)
        return 0 if batch is None else batch.num_rows
    finally:
        batches.close()


def expected(index: int) -> dict[str, object]:
    """The endpoint values independent of the resource's diagnostic URL."""
    return {
        "rownum": index + 1,
        "timestamp": datetime_of(timestamp(index)),
        "threadname": f"worker-{index % 16}",
        "plugin": f"feed-{index % 4}",
        "level": "WARN" if index % 7 == 0 else "INFO",
        "body": body(index),
    }


def verify(case: Case, rows: int) -> pyarrow.Table:
    """Assert the streamed schema, count, and endpoint rows before timing."""
    batches = list(message_batches(case.source()))
    assert batches and all(batch.schema.equals(SCHEMA, check_metadata=True) for batch in batches)
    assert all(0 < batch.num_rows <= BATCH_ROW_SIZE for batch in batches)
    assert SCHEMA.field("timestamp").type == pyarrow.timestamp("us", tz="UTC")
    table = pyarrow.Table.from_batches(batches, schema=SCHEMA)
    assert table.num_rows == rows
    assert first_batch(case) == min(rows, BATCH_ROW_SIZE)
    first, last = table.slice(0, 1).to_pylist()[0], table.slice(rows - 1, 1).to_pylist()[0]
    for row, index in ((first, 0), (last, rows - 1)):
        url = row.pop("url")
        assert isinstance(url, str) and url.endswith(case.filename)
        assert row == expected(index)
    return table


def cases(root: pathlib.Path, decoded: bytes) -> tuple[tuple[Case, ...], int]:
    """The plain and compressed URI paths `parse_messages` runs."""
    plain = root / "messages.log"
    coded = root / "messages.log.gz"
    encoded = gzip.compress(decoded, compresslevel=6, mtime=0)
    plain.write_bytes(decoded)
    coded.write_bytes(encoded)

    return (
        (
            Case(
                "URI local plain",
                lambda: IOBase.from_uri(plain.as_uri()),
                plain.name,
            ),
            Case(
                "URI local gzip",
                lambda: IOBase.from_uri(coded.as_uri()),
                coded.name,
            ),
        ),
        len(encoded),
    )


def sweep(rows: int, repeat: int) -> None:
    """Measure native text, exact Message batches, and first-batch latency."""
    decoded = corpus(rows)
    with tempfile.TemporaryDirectory(prefix="rekep-message-bench-") as directory:
        selected, encoded_size = cases(pathlib.Path(directory), decoded)
        verified = [verify(case, rows) for case in selected]
        comparable = [
            table.select([name for name in table.schema.names if name != "url"])
            for table in verified
        ]
        assert comparable[0].equals(comparable[1]), "plain and gzip rows differ"
        del comparable, verified

        print(
            f"{rows:,} rows, {len(decoded) / 2**20:.2f} decoded MiB, "
            f"{encoded_size / 2**20:.2f} gzip MiB, "
            f"{BATCH_ROW_SIZE:,} rows/batch, best of {repeat}"
        )
        print(
            f"  {'case':<20} {'yggdryl rows/s':>15} {'Message rows/s':>15} "
            f"{'decoded MiB/s':>15} {'first batch ms':>15}"
        )
        for case in selected:
            raw_seconds = best_of(lambda case=case: raw_drain(case), repeat)
            seconds = best_of(lambda case=case: drain(case), repeat)
            first_seconds = best_of(lambda case=case: first_batch(case), repeat)
            print(
                f"  {case.name:<20} {rows / raw_seconds:>15,.0f} {rows / seconds:>15,.0f} "
                f"{len(decoded) / seconds / 2**20:>15.1f} {first_seconds * 1_000:>15.2f}"
            )


def main() -> int:
    options = parser(__doc__, rows=100_000, repeat=3)
    arguments = options.parse_args()
    rows = 10_000 if arguments.quick else arguments.rows
    repeat = 1 if arguments.quick else arguments.repeat
    if rows <= 0:
        options.error("--rows must be positive")
    if repeat <= 0:
        options.error("--repeat must be positive")
    sweep(rows, repeat)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
