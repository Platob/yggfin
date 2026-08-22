"""Turning a capture of market logs into the orders, executions and books in it."""

from __future__ import annotations

import dataclasses
import os
import sys
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, ClassVar

import pyarrow

from rekep.convert import Convertible
from rekep.dataset import Dataset
from rekep.fields import StructField
from rekep.filesystems import resolve
from rekep.market.book import Book, BookIterator
from rekep.market.event import HOUR, MarketEvent
from rekep.market.fix import FixEvents
from rekep.market.orders import Execution, Order
from rekep.market.reference import Reference
from rekep.tasks.logs import DEFAULT_COMMIT_ROW_SIZE
from rekep.tasks.task import Task, TaskRun
from rekep.text.text_file import DEFAULT_BATCH_ROW_SIZE, TextFile
from rekep.text.text_files import TextFiles
from rekep.urls import Url

#: The shapes this lands, keyed by the name their table is called after. The
#: order is the order they depend on: an execution names the order it happened
#: to, a book is folded from both, and an instrument is what all three are
#: about.
SHAPES: dict[str, type[Convertible]] = {
    "orders": Order,
    "executions": Execution,
    "books": Book,
    "instruments": Reference,
}


@dataclasses.dataclass
class ParseMarket(Task):
    """Read market logs, and land the events they carry as market tables.

    The second half of the pipeline `parse_logs` starts: that one sorts a
    capture into a table per kind of line, keeping the line; this one reads
    those lines as FIX and lands what they *mean* -- an orders table, an
    executions table, a book table folded from them, and an instrument table
    of what the capture taught it about the instruments themselves.

    **Reading is a `Dataset`**, whichever kind. Point `source` at a folder of
    logs and it is read as text; point it at a document naming a store
    (`{kind: iceberg, name: logs.order_logs, ...}`) and it is read from there,
    which is how this chains onto `parse_logs` without re-reading the capture.
    Either way the task sees Arrow batches with a `message` column in them.

    **One pass, and the fold is in it.** `BookIterator` keeps a mutable state
    per instrument -- its live orders and its sorted prices -- rather than the
    events of every instrument, so what the job holds is the book rather than
    the capture, and a book is built only when the instant it belongs to has
    closed. The orders and executions go to their own tables on the way past,
    which is why the capture is read once and not twice.

    Set `books: false` to skip it, which is what a job landing raw events for
    something else to fold should do.
    """

    KIND: ClassVar[str] = "parse_market"

    source: str | Mapping[str, Any] = ""
    """Folder, file, or a dataset document naming the store to read."""

    column: str = "message"
    """Which column of the source carries the FIX text."""

    pattern: str = "*"
    """Which files in the folder to read; ignored unless `source` is a folder."""

    recursive: bool = True
    """Whether to descend into subdirectories of `source`."""

    timezone: str | None = None
    """IANA zone the wall clock in a log header belongs to; None reads it as UTC."""

    venue: str | None = None
    """Which feed this capture came off, when the messages do not say."""

    catalog: str = "rekep"
    """Name of the Iceberg catalog the targets live in."""

    namespace: str = "market"
    """Namespace the targets are created under."""

    table: str = "{shape}"
    """Target name per shape: `orders`, `executions`, `books` or `instruments`."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """How to reach the catalog -- `type`, `uri`, `warehouse`, credentials."""

    books: bool = True
    """Whether to fold and land books as well as the events they are folded from."""

    snapshot_every: int = HOUR
    """Emit a book on every multiple of this even where nothing moved; `0` for none.

    An hour by default, which is what makes "the book at 14:00" a point lookup
    on a table partitioned by the hour rather than a scan backwards for the
    last row before it.
    """

    merge_by: bool = True
    """Skip rows a target already holds; False appends everything, duplicates included."""

    batch_row_size: int = DEFAULT_BATCH_ROW_SIZE
    """Rows the reader puts in one batch."""

    commit_row_size: int = DEFAULT_COMMIT_ROW_SIZE
    """Rows a target buffers before it commits, and so what bounds memory."""

    limit: int | None = None
    """Stop after this many source rows; None reads the whole capture. For a dry run."""

    workers: int = 1
    """How many processes translate the capture; `0` is one per CPU, `1` is here.

    **Processes and not threads**, and that was measured rather than assumed:
    every hot path in this job is pure Python, so a thread pool is *slower*
    than doing the work here -- 0.86x on four threads, against a 0.98x ceiling
    for four threads doing nothing but spin. pyarrow releases the GIL exactly
    where the data is already in Arrow, which is the parquet write and nothing
    else this touches.

    **Translation and not the fold.** Reading a line as the events it carries
    is per-line work with no state, so it shards perfectly; folding is a fold
    and needs one instrument's events in order. The parent keeps the fold,
    which after the encoder work is the smaller half, and the worker gets the
    part that is embarrassingly parallel.

    What it costs is a pickle of the events back -- measured at about 213 bytes
    and 11 us a row against 85 us a message of translation, so roughly a
    seventh of what it buys.
    """

    shard_row_size: int = 4_096
    """Lines a worker is given at a time; what bounds the memory a fan-out adds."""

    # -- running -------------------------------------------------------------

    def run(self) -> TaskRun:
        """Read the capture, land what it means, and say what went where."""
        started = self._timed()
        report = TaskRun(task=self._named())
        targets: dict[str, Dataset] = {}
        buffers: dict[str, list[Any]] = {}

        events = self._tapped(report, buffers, targets)
        if not self.books:
            for _ in events:
                pass
        else:
            folding = BookIterator(events=events, snapshot_every=self.snapshot_every)
            for book in folding.books:
                self._hold("books", book, buffers, targets, report)
            # After, and not interleaved: draining the instruments would drive
            # the same source the books are being pulled from. What it holds is
            # a row per instrument per thing learnt about it -- bounded by the
            # capture's instruments, not by its length.
            for known in folding.instruments:
                self._hold("instruments", known, buffers, targets, report)
        for name in list(buffers):
            self._flush(name, buffers, targets, report)

        report.seconds = self._timed() - started
        return report

    def _tapped(
        self, report: TaskRun, buffers: dict[str, list[Any]], targets: dict[str, Dataset]
    ) -> Iterator[MarketEvent]:
        """Every event the source carries, landed in its own table on the way past.

        A tap and not a second pass: the orders and the executions are rows in
        their own right *and* what the book is folded from, and reading the
        capture twice to have both would double the only part of this job that
        touches the disk.
        """
        if self.workers == 1:
            for batch in self.into_arrow_batches():
                report.rows += batch.num_rows
                for event in self.into_events(batch):
                    self._landed(event, buffers, targets, report)
                    yield event
            return
        for event in self.into_parallel_events(report):
            self._landed(event, buffers, targets, report)
            yield event

    def _landed(
        self,
        event: MarketEvent,
        buffers: dict[str, list[Any]],
        targets: dict[str, Dataset],
        report: TaskRun,
    ) -> None:
        """One translated event held for the table it belongs to."""
        self._hold("orders" if event.is_order() else "executions", event, buffers, targets, report)

    def _hold(
        self,
        shape: str,
        row: Any,
        buffers: dict[str, list[Any]],
        targets: dict[str, Dataset],
        report: TaskRun,
    ) -> None:
        """One row buffered for its target, committed once the buffer is worth it."""
        held = buffers.setdefault(shape, [])
        held.append(row)
        if len(held) >= self.commit_row_size:
            self._flush(shape, buffers, targets, report)

    # -- the pieces a caller may want on their own ---------------------------

    def into_events(self, batch: pyarrow.RecordBatch) -> Iterator[MarketEvent]:
        """Every order and execution one batch of lines carries, in line order.

        The recorded time comes off the row when the source has one, so a
        message that carries no clock of its own still lands where the log
        says it arrived -- and `unix` stays the transaction time regardless,
        which is what `TRANSACTED` is about.
        """
        messages = batch.column(self.column).to_pylist()
        recorded = batch.column("unix").to_pylist() if "unix" in batch.schema.names else None
        for index, message in enumerate(messages):
            if not message:
                continue
            yield from FixEvents.from_text(
                message, venue=self.venue, runix=recorded[index] if recorded else 0
            )

    def into_parallel_events(self, report: TaskRun | None = None) -> Iterator[MarketEvent]:
        """Every event the capture carries, translated across processes.

        `executor.map` with an explicit chunk size, so the order events come
        back in is the order the lines were read in -- which a fold depends on
        and a shard would otherwise destroy.

        How the workers are started is `_context`'s, and it is a correctness
        question before it is a speed one.
        """
        import concurrent.futures

        context = _context()
        workers = self.workers or (os.cpu_count() or 1)
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers, mp_context=context
        ) as pool:
            for found in pool.map(_translated, self._shards(report), chunksize=1):
                yield from found

    def _shards(
        self, report: TaskRun | None = None
    ) -> Iterator[tuple[str, str | None, int, list[tuple[str, int]]]]:
        """The capture cut into units of work, each a run of lines and their clocks.

        Plain tuples of text and integers, because they cross a process boundary:
        an Arrow batch would too, but the worker wants the lines one at a time
        and slicing them out on this side is work the parent would do serially.
        """
        held: list[tuple[str, int]] = []
        for batch in self.into_arrow_batches():
            if report is not None:
                report.rows += batch.num_rows
            messages = batch.column(self.column).to_pylist()
            names = batch.schema.names
            recorded = batch.column("unix").to_pylist() if "unix" in names else None
            for index, message in enumerate(messages):
                if not message:
                    continue
                held.append((message, recorded[index] if recorded else 0))
                if len(held) >= self.shard_row_size:
                    yield (self.column, self.venue, len(held), held)
                    held = []
        if held:
            yield (self.column, self.venue, len(held), held)

    def into_arrow_batches(self) -> Iterator[pyarrow.RecordBatch]:
        """The source, batch by batch; `limit` cuts the last one rather than reading on."""
        read = 0
        for batch in self.source_dataset().read_arrow_reader(batch_row_size=self.batch_row_size):
            if self.limit is not None and read + batch.num_rows > self.limit:
                batch = batch.slice(0, self.limit - read)
            if batch.num_rows == 0:
                continue
            read += batch.num_rows
            yield batch
            if self.limit is not None and read >= self.limit:
                return

    def source_dataset(self) -> Dataset:
        """The capture as a dataset, whatever kind of store it is in.

        A mapping is a dataset document and is built by `Dataset.from_dict`,
        which dispatches on its `kind`. A string is a location, and whether it
        names a folder or one file is settled by asking the filesystem rather
        than by the caller saying so -- getting that wrong reads zero rows and
        reports success.
        """
        if isinstance(self.source, Mapping):
            return Dataset.from_dict(self.source)
        declared = {"timezone": self.timezone}
        url = Url.from_string(str(self.source)).into_string()
        filesystem, path = resolve(url)
        info = filesystem.get_file_info(path)
        if info.type == pyarrow.fs.FileType.NotFound:
            raise FileNotFoundError(f"{url} does not exist")
        if info.type == pyarrow.fs.FileType.Directory:
            return TextFiles.from_folder(
                url, pattern=self.pattern, recursive=self.recursive, **declared
            )
        return TextFile.from_url(url, **declared)

    def target_name(self, shape: str) -> str:
        """The table one shape lands in: `market.orders`, `market.books`."""
        name = self.table.format(shape=shape)
        return f"{self.namespace}.{name}" if self.namespace else name

    def target_field(self, shape: str) -> StructField:
        """The shape one target holds -- the declaration, which is the contract."""
        return SHAPES[shape].FIELD

    def target(self, shape: str) -> Dataset:
        """The dataset one shape lands in, created on the first write to it."""
        from rekep.iceberg import IcebergDataset

        return IcebergDataset(
            name=self.target_name(shape),
            catalog=self.catalog,
            properties=dict(self.properties),
            struct=self.target_field(shape),
        )

    def into_arrow_table(self, shape: str, events: Iterable[Convertible]) -> pyarrow.Table:
        """A run of events as the table its target is written from.

        The schema is handed to `from_pylist` and never inferred from the
        rows, and that is not a style preference: `into_dict` leaves out a
        member that is None, and `from_pylist` with no schema builds one from
        the **first row's keys**. A first book with no bid -- which is what
        the first book of a capture usually is -- therefore defined a schema
        with no `bid_px`, `spread`, `micro_px` or `imbalance` in it, and every
        row after it was cast onto that and came back null. Silently, and for
        the whole table.

        With the declaration handed over, a missing key is a null in *that
        row* and nothing else moves.
        """
        schema = self.target_field(shape).into_arrow_schema()
        rows = [event.into_dict() for event in events]
        if not rows:
            return pyarrow.Table.from_batches([], schema=schema)
        return pyarrow.Table.from_batches([pyarrow.RecordBatch.from_pylist(rows, schema=schema)])

    # -- helpers -------------------------------------------------------------

    def _flush(
        self,
        shape: str,
        buffers: dict[str, list[Any]],
        targets: dict[str, Dataset],
        report: TaskRun,
    ) -> None:
        """One target's buffer appended and counted, and the buffer let go.

        What landed is the difference in `IcebergDataset.records`, which
        documents what it costs and when it cannot say; a store that cannot say
        has every row counted as landed.
        """
        held = buffers.pop(shape, [])
        if not held:
            return
        dataset = targets.get(shape)
        if dataset is None:
            dataset = targets[shape] = self.target(shape)
        before = dataset.records
        dataset.append_arrow_table(self.into_arrow_table(shape, held), merge_by=self.merge_by)
        after = dataset.records
        landed = len(held) if before is None or after is None else after - before
        name = self.target_name(shape)
        report.written[name] = report.written.get(name, 0) + landed
        report.skipped += len(held) - landed


def _context() -> Any:
    """How to start a worker here: safely if that is possible, and say so if not.

    A bare `fork` copies a process that may already have threads in it --
    pyarrow starts them, and so does a test runner -- and the child inherits
    their locks without the threads that would ever release them. CPython warns
    about that today and Python 3.14 makes it an error. `forkserver` forks from
    a process started clean and single-threaded, so its children are safe, and
    `set_forkserver_preload` pays this package's import once for the server
    rather than once per worker.

    What it needs in exchange is a `__main__` a child can re-import, which is
    ordinary `multiprocessing` semantics and is why every fan-out in Python
    asks to be run from a script rather than from a REPL. Where there is no
    such `__main__` -- an interactive session, `python -c`, a heredoc -- a
    forkserver child dies re-preparing it, so `fork` is used instead and its
    hazard is the lesser one: an interactive session is not usually the thing
    with a thread pool inside it.

    With neither, a fan-out cannot be started at all, and this says which
    knob to turn rather than failing later with a broken pool.
    """
    import multiprocessing
    import os.path

    available = multiprocessing.get_all_start_methods()
    main = sys.modules.get("__main__")
    named = getattr(main, "__file__", None)
    reachable = bool(named) and os.path.isfile(named)
    if reachable and "forkserver" in available:
        context = multiprocessing.get_context("forkserver")
        context.set_forkserver_preload(["rekep.tasks.market"])
        return context
    if "fork" in available:
        return multiprocessing.get_context("fork")
    if reachable:
        return multiprocessing.get_context("spawn")
    raise RuntimeError(
        "a fan-out needs a `__main__` its workers can import, and this process has "
        f"none ({named!r}). Run the task from a script, or set `workers=1` to "
        "translate in this process."
    )


def _translated(shard: tuple[str, str | None, int, list[tuple[str, int]]]) -> list[MarketEvent]:
    """One shard of lines as the market events they carry. Runs in a worker.

    A module-level function and not a method, because a bound method drags the
    whole task -- its catalog properties, its source, its buffers -- through
    the pickle to every worker, and none of that is what the work needs.
    """
    _, venue, _, lines = shard
    found: list[MarketEvent] = []
    for message, recorded in lines:
        found.extend(FixEvents.from_text(message, venue=venue, runix=recorded))
    return found
