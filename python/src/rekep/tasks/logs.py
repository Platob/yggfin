"""Parsing a capture into one table per kind of event it holds."""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from typing import Any, ClassVar

import pyarrow
import pyarrow.compute
import pyarrow.fs

from rekep.dataset import Dataset
from rekep.fields import StructField
from rekep.filesystems import resolve
from rekep.logs.log import LogRules
from rekep.logs.text_file import DEFAULT_BATCH_ROW_SIZE, TextFile
from rekep.logs.text_files import TextFiles
from rekep.market.enums import EventType
from rekep.tasks.task import Task, TaskRun
from rekep.urls import Url

#: Rows a target accumulates before it commits. A store that commits per call
#: wants a commit worth having, and a fan-out holds one buffer per target -- so
#: this is also what bounds the memory the whole job uses, at roughly
#: `targets * commit_row_size` rows.
DEFAULT_COMMIT_ROW_SIZE = 250_000


@dataclasses.dataclass
class ParseLogs(Task):
    """Parse a capture and land each line in the table for what it is about.

    One pass over the input, fanning out: every batch the parser yields is cut
    by `etype` and each part appended to its own table -- `order_logs`,
    `execution_logs`, `unknown_logs`. Not one pass per type, which would reread
    and reparse the whole capture once for every kind of line in it, and not a
    staging table either, which would write every row twice.

    **Streaming, with the memory that costs stated.** The parser is a reader
    and stays one; what a fan-out has to add is a buffer per target, because
    the rows for one table arrive interleaved with every other table's. Each
    buffer flushes at `commit_row_size`, so the job holds at most one commit's
    worth per target rather than the capture.

    **Appending, not writing.** `merge_by` is an append's merge: a row whose
    key a target already holds is dropped and the rest are inserted, nothing
    stored is ever rewritten, and no delete file is produced. That is what
    makes re-running this over a capture that grew by a day cost the day.

    A line nothing classifies still lands, in `unknown_logs`: dropping it would
    make the job lossy in exactly the case a new log format shows up.
    """

    KIND: ClassVar[str] = "parse_logs"

    source: str = ""
    """Folder or file to read, as a URI or a local path."""

    pattern: str = "*"
    """Which files in the folder to read; ignored when `source` is one file."""

    recursive: bool = True
    """Whether to descend into subdirectories of `source`."""

    timezone: str | None = None
    """IANA zone the wall clock in the log header belongs to; None reads it as UTC."""

    rules: LogRules = dataclasses.field(default_factory=LogRules)
    """What decides each line's `etype`, first match winning, `UNKNOWN` otherwise."""

    catalog: str = "rekep"
    """Name of the Iceberg catalog the targets live in."""

    namespace: str = "logs"
    """Namespace the targets are created under."""

    table: str = "{event_type}_logs"
    """Target name per kind; `{event_type}` is the lower-cased `EventType` name."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """How to reach the catalog -- `type`, `uri`, `warehouse`, credentials."""

    static_values: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Constant columns every row carries: which bridge, desk or environment."""

    merge_by: bool = True
    """Skip rows a target already holds; False appends everything, duplicates included."""

    batch_row_size: int = DEFAULT_BATCH_ROW_SIZE
    """Rows the parser puts in one batch."""

    commit_row_size: int = DEFAULT_COMMIT_ROW_SIZE
    """Rows a target buffers before it commits, and so what bounds memory."""

    limit: int | None = None
    """Stop after this many rows; None reads the whole capture. For a dry run."""

    # -- running -------------------------------------------------------------

    def run(self) -> TaskRun:
        """Parse the capture, fan it out, and say what landed where."""
        started = self._timed()
        report = TaskRun(task=self._named())
        targets: dict[int, Dataset] = {}
        buffers: dict[int, list[pyarrow.RecordBatch]] = {}
        held: dict[int, int] = {}

        for batch in self.into_arrow_batches():
            report.rows += batch.num_rows
            for code, part in self.split(batch):
                buffers.setdefault(code, []).append(part)
                held[code] = held.get(code, 0) + part.num_rows
                if held[code] >= self.commit_row_size:
                    self._flush(code, targets, buffers, held, report)
        for code in list(buffers):
            self._flush(code, targets, buffers, held, report)

        report.seconds = self._timed() - started
        return report

    def split(self, batch: pyarrow.RecordBatch) -> Iterator[tuple[int, pyarrow.RecordBatch]]:
        """`batch` cut into one part per `etype` present in it.

        One filter per distinct code, not per row, and only for the codes the
        batch actually holds -- a capture that is all heartbeats does one
        filter, not one per kind this package knows about. A batch of a single
        code is handed through whole rather than filtered against itself.
        """
        column = batch.column("etype")
        codes = pyarrow.compute.unique(column).to_pylist()
        if len(codes) == 1:
            yield int(codes[0]), batch
            return
        for code in sorted(codes):
            yield int(code), batch.filter(pyarrow.compute.equal(column, code))

    # -- the pieces a caller may want on their own ---------------------------

    def into_arrow_batches(self) -> Iterator[pyarrow.RecordBatch]:
        """The parsed capture, batch by batch, `etype` already decided.

        A generator over the source's own reader, so nothing is materialised:
        `limit` cuts the last batch rather than reading past it.
        """
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
        """The capture as a dataset: a folder of logs, or one log.

        Told apart by asking the filesystem rather than by the caller saying
        so, because a path that names a directory and a path that names a file
        look the same in a configuration file -- and getting it wrong reads
        zero rows and reports success.
        """
        declared = {
            "timezone": self.timezone,
            "static_values": self.static_values,
            "rules": self.rules,
        }
        url = self.source_url
        filesystem, path = resolve(url)
        info = filesystem.get_file_info(path)
        if info.type == pyarrow.fs.FileType.NotFound:
            raise FileNotFoundError(f"{url} does not exist")
        if info.type == pyarrow.fs.FileType.Directory:
            return TextFiles.from_folder(
                url, pattern=self.pattern, recursive=self.recursive, **declared
            )
        return TextFile.from_url(url, **declared)

    @property
    def source_url(self) -> str:
        """`source` as a URI, so a local path in a document works like a remote one.

        Through `Url`, which is the package's one parser for a location: a bare
        path, a `file://` URI, `s3://bucket/key` and a Windows drive letter all
        come out as the same URI a store is opened with, and sniffing for
        `://` at this call site would get the last of those wrong.
        """
        return Url.from_string(self.source).into_string()

    def target_name(self, code: int) -> str:
        """The table one `etype` lands in: `logs.order_logs`, `logs.unknown_logs`."""
        event_type = EventType.from_code(code).name.lower()
        name = self.table.format(event_type=event_type, etype=code)
        return f"{self.namespace}.{name}" if self.namespace else name

    def target_field(self) -> StructField:
        """The shape every target has: a parsed log, plus the constant columns."""
        return self.source_dataset().into_struct_field()

    def target(self, code: int) -> Dataset:
        """The dataset one `etype` lands in, created on the first write to it."""
        from rekep.iceberg import IcebergDataset

        return IcebergDataset(
            name=self.target_name(code),
            catalog=self.catalog,
            properties=dict(self.properties),
            struct=self.target_field(),
        )

    # -- helpers -------------------------------------------------------------

    def _flush(
        self,
        code: int,
        targets: dict[int, Dataset],
        buffers: dict[int, list[pyarrow.RecordBatch]],
        held: dict[int, int],
        report: TaskRun,
    ) -> None:
        """One target's buffer appended and counted, and the buffer let go.

        The target is built once and kept: an `IcebergDataset` resolves its
        catalog and loads its table lazily, and rebuilding it per commit would
        be a catalog round trip per flush -- free on SQLite, a network hop on
        REST or Glue.

        What landed is the difference in the target's own record count, which
        Iceberg keeps in the snapshot summary and answers from metadata. A
        store that cannot say (`records` is None) has every row counted as
        landed, because the alternative is reading the table back to decorate
        a report -- which would cost more than the write it is reporting on.
        """
        batches = buffers.pop(code, [])
        rows = held.pop(code, 0)
        if not rows:
            return
        dataset = targets.get(code)
        if dataset is None:
            dataset = targets[code] = self.target(code)
        before = dataset.records
        dataset.append_arrow_table(pyarrow.Table.from_batches(batches), merge_by=self.merge_by)
        after = dataset.records
        landed = rows if before is None or after is None else after - before
        name = self.target_name(code)
        report.written[name] = report.written.get(name, 0) + landed
        report.skipped += rows - landed
