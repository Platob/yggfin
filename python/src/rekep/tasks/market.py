"""Turning a capture of market logs into the orders, executions and books in it."""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, ClassVar

import pyarrow

from rekep.dataset import Dataset
from rekep.fields import StructField
from rekep.filesystems import resolve
from rekep.logs.text_file import DEFAULT_BATCH_ROW_SIZE, TextFile
from rekep.logs.text_files import TextFiles
from rekep.market.book import Book
from rekep.market.event import MarketEvent
from rekep.market.fix import FixEvents
from rekep.market.orders import Execution, Order
from rekep.tasks.logs import DEFAULT_COMMIT_ROW_SIZE
from rekep.tasks.task import Task, TaskRun
from rekep.urls import Url

#: The shapes this lands, keyed by the name their table is called after. The
#: order is the order they are written in, and it is the order they depend on:
#: an execution names the order it happened to, and a book is folded from both.
SHAPES: dict[str, type[MarketEvent]] = {
    "orders": Order,
    "executions": Execution,
    "books": Book,
}


@dataclasses.dataclass
class ParseMarket(Task):
    """Read market logs, and land the events they carry as market tables.

    The second half of the pipeline `parse_logs` starts: that one sorts a
    capture into a table per kind of line, keeping the line; this one reads
    those lines as FIX and lands what they *mean* -- an orders table, an
    executions table, and a book table folded from them.

    **Reading is a `Dataset`**, whichever kind. Point `source` at a folder of
    logs and it is read as text; point it at a document naming a store
    (`{kind: iceberg, name: logs.order_logs, ...}`) and it is read from there,
    which is how this chains onto `parse_logs` without re-reading the capture.
    Either way the task sees Arrow batches with a `message` column in them.

    **Books are folded per instrument, and that is why they are last.**
    `Book.from_events` needs one instrument's events in time order, so the
    orders and executions are grouped by `instrument_hash` and each group
    folded on its own. That grouping is the reason the book pass is not
    streaming the way the other two are: a fold has to see a whole
    instrument's stream, and the memory it costs is the live orders of every
    instrument in the capture rather than the capture itself.

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
    """Target name per shape; `{shape}` is `orders`, `executions` or `books`."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """How to reach the catalog -- `type`, `uri`, `warehouse`, credentials."""

    books: bool = True
    """Whether to fold and land books as well as the events they are folded from."""

    merge_by: bool = True
    """Skip rows a target already holds; False appends everything, duplicates included."""

    batch_row_size: int = DEFAULT_BATCH_ROW_SIZE
    """Rows the reader puts in one batch."""

    commit_row_size: int = DEFAULT_COMMIT_ROW_SIZE
    """Rows a target buffers before it commits, and so what bounds memory."""

    limit: int | None = None
    """Stop after this many source rows; None reads the whole capture. For a dry run."""

    # -- running -------------------------------------------------------------

    def run(self) -> TaskRun:
        """Read the capture, land what it means, and say what went where."""
        started = self._timed()
        report = TaskRun(task=self._named())
        targets: dict[str, Dataset] = {}
        buffers: dict[str, list[MarketEvent]] = {}
        folding: dict[int, list[MarketEvent]] = {}

        for batch in self.into_arrow_batches():
            report.rows += batch.num_rows
            for event in self.into_events(batch):
                name = "orders" if event.is_order() else "executions"
                buffers.setdefault(name, []).append(event)
                if self.books:
                    folding.setdefault(event.instrument_hash, []).append(event)
                if len(buffers[name]) >= self.commit_row_size:
                    self._flush(name, buffers, targets, report)
        for name in list(buffers):
            self._flush(name, buffers, targets, report)

        for held in folding.values():
            for book in Book.from_events(held):
                buffers.setdefault("books", []).append(book)
                if len(buffers["books"]) >= self.commit_row_size:
                    self._flush("books", buffers, targets, report)
        self._flush("books", buffers, targets, report)

        report.seconds = self._timed() - started
        return report

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

    def into_arrow_table(self, shape: str, events: Iterable[MarketEvent]) -> pyarrow.Table:
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
        buffers: dict[str, list[MarketEvent]],
        targets: dict[str, Dataset],
        report: TaskRun,
    ) -> None:
        """One target's buffer appended and counted, and the buffer let go.

        What landed is the difference in the target's own record count, which
        Iceberg answers from the snapshot summary; a store that cannot say has
        every row counted as landed, because reading the table back to
        decorate a report would cost more than the write it reports on.
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
