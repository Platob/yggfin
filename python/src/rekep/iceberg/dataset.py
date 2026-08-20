"""One Iceberg table as a dataset, with the maintenance it needs to stay fast."""

from __future__ import annotations

import dataclasses
import datetime
from collections.abc import Iterator, Sequence
from functools import cached_property
from typing import Any

import pyarrow
import pyarrow.fs

from rekep.dataset import Dataset, arrow_chunks
from rekep.fields import StructField, field_of
from rekep.filesystems import resolve
from rekep.iceberg.catalog import IcebergCatalog

#: The branch a read or a write lands on when nothing names one -- pyiceberg's
#: own default, spelled out here so the two cannot disagree about it.
MAIN = "main"

#: How long a file must have been unreferenced before `cleanup` deletes it. A
#: writer that is committing right now has files on disk that no snapshot
#: mentions yet; deleting those would break it, so orphans have to be old.
ORPHAN_AGE = datetime.timedelta(days=3)

#: How big one commit's output files get. Narrower than it sounds: pyiceberg
#: derives rows-per-file from the *in-memory* size of the table being written
#: and only ever splits a single commit -- it has no cross-commit state, so it
#: cannot fill a file across commits. `commit_row_size` and `compact` are the
#: levers on file count; this one decides how a large commit is sliced.
TARGET_FILE_SIZE = "write.target-file-size-bytes"

#: Lets Iceberg merge small manifests as it commits. **Inert on its own**:
#: pyiceberg only merges once a snapshot has more than `MIN_MANIFESTS_TO_MERGE`
#: manifests, and that defaults to 100 -- so a stream of a few dozen commits
#: keeps one manifest per commit and pays for all of them at planning time.
MERGE_MANIFESTS = "commit.manifest-merge.enabled"
MIN_MANIFESTS_TO_MERGE = "commit.manifest.min-count-to-merge"

#: How many old `metadata.json` versions a table keeps, and whether the ones
#: past that are deleted rather than left behind. A stream writes one per
#: commit, so without this the metadata directory outgrows the data.
PREVIOUS_VERSIONS = "write.metadata.previous-versions-max"
DELETE_OLD_METADATA = "write.metadata.delete-after-commit.enabled"

#: Distinct key values a merge will name one by one in its scan filter before
#: falling back to a range. `In(day, [...])` prunes an identity partition
#: exactly; a range only prunes what the file bounds happen to exclude. The
#: ceiling is not ours: pyiceberg's evaluators give up on an `In` of more than
#: `IN_PREDICATE_LIMIT` literals and stop pruning altogether -- measured, the
#: cliff is exactly there: 200 keys plan one file of twenty, 201 plan all
#: twenty. So this is that limit, not a number of our own.
MERGE_IN_LIMIT = 200

#: Rows a commit carries when nothing says otherwise. A stream that commits per
#: batch lands a file and a snapshot per batch, and every later scan pays for
#: both; one that never commits until the end holds the whole stream in memory.
#: Measured on the log pipeline (`benchmarks/bench_iceberg.py`), this is where
#: throughput stops improving and file count starts mattering.
DEFAULT_COMMIT_ROW_SIZE = 1_000_000

#: Snapshot property this package stamps on the commits `compact` makes. A
#: partition whose last commit carries it has had nothing land in it since it
#: was rewritten, which is the only reliable way to know that rewriting it
#: again would change nothing: a size rule cannot tell, because pyiceberg sizes
#: its output files from *in-memory* bytes and a part that legitimately needs
#: several files would otherwise be replanned forever.
COMPACTION_MARK = "rekep.compaction"

#: The file a Hadoop-style catalog keeps its current version number in. Nothing
#: in the metadata references it, so a sweep has to know the name.
HADOOP_POINTER = "version-hint.text"

#: Rows per parquet row group. Iceberg's default is a million, which makes
#: nearly every file this package writes a single row group -- and a filter can
#: only skip a *row group*, so one row group per file means a filter that got
#: past the file bounds reads the whole file.
ROW_GROUP_LIMIT = "write.parquet.row-group-limit"

#: Properties a table is created with when `optimize_commits` is left on.
#: Measured over 40 commits, against Iceberg's defaults: manifests 40 -> 4,
#: `metadata.json` files 41 -> 21, and scan planning 61 ms -> 9 ms, at no
#: commit-time cost. The manifest merge is the one that matters, and it does
#: nothing without its threshold.
COMMIT_PROPERTIES = {
    MERGE_MANIFESTS: "true",
    MIN_MANIFESTS_TO_MERGE: "10",
    PREVIOUS_VERSIONS: "20",
    DELETE_OLD_METADATA: "true",
    TARGET_FILE_SIZE: str(256 * 1024 * 1024),
    ROW_GROUP_LIMIT: str(128 * 1024),
}


@dataclasses.dataclass(eq=False)
class IcebergDataset(Dataset):
    """An Iceberg table, read and written as Arrow through pyiceberg.

    Nothing about Iceberg is reimplemented here: pyiceberg plans the scans,
    writes the files and commits the snapshots. What this adds is the two ends
    -- the shape (`StructField`) the data is cast onto, and the streaming that
    keeps a commit from happening once per batch -- plus the maintenance an
    Iceberg table needs and nobody enjoys writing::

        logs = IcebergDataset(
            name="trading.logs",
            catalog="local",
            properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///wh"},
            struct=Log.FIELD,
        )
        logs.write_arrow(batches, merge_by=True, commit_row_size=1_000_000)
        logs.read_arrow_table(row_filter="date = '2026-08-14'")
        logs.optimize()          # merge manifests, compact files, expire and sweep

    `struct` is optional: with one, the table is created from it (schema,
    documentation, identifier fields and partition spec included) the first
    time it is written; without one, the table's own schema is the shape.

    Every read, write and maintenance verb takes `branch` (and reads take
    `snapshot_id`), so a job can work on a branch and publish it later without
    a second dataset object.
    """

    #: Table identifier, `namespace.name`.
    name: str

    #: Catalog name pyiceberg loads, with `properties` as its configuration.
    catalog: str = "default"
    properties: dict[str, str] = dataclasses.field(default_factory=dict)

    #: The declared shape. None means "whatever the table says".
    struct: StructField | None = None

    #: Branch reads and writes use unless a call names another; None is `main`.
    branch: str | None = None

    #: Rows one commit carries when a write does not name a size. Iceberg lands
    #: a file and a snapshot per commit, so this is the knob that decides how
    #: much a later scan has to plan; pass `commit_row_size=0` to a write for
    #: one commit over the whole stream.
    commit_row_size: int | None = DEFAULT_COMMIT_ROW_SIZE

    #: Columns each chunk is sorted by before it is written. Off by default,
    #: because it costs a sort per commit -- and worth it wherever reads filter
    #: on those columns: measured, a top-5% filter over one 600k-row commit
    #: took 214 ms unsorted and 22 ms sorted, the same single file either way.
    sort_by: Sequence[str] | None = None

    #: Whether a merge plans its own scan instead of handing the whole chunk to
    #: `Table.upsert`. Same algorithm, same result -- see `merge_arrow_table`
    #: for why it is worth several orders of magnitude on a composite key.
    plan_merges: bool = True

    #: Whether a table created here gets `COMMIT_PROPERTIES`. The defaults are
    #: Iceberg's, and Iceberg's defaults are not tuned for a stream.
    optimize_commits: bool = True

    #: Only used when the table is created: where it lives and what it carries.
    location: str | None = None
    table_properties: dict[str, str] = dataclasses.field(default_factory=dict)

    # -- the table ----------------------------------------------------------

    @cached_property
    def store(self) -> IcebergCatalog:
        """The catalog this table lives in."""
        return IcebergCatalog(name=self.catalog, properties=self.properties)

    @property
    def iceberg_catalog(self) -> Any:
        """The pyiceberg catalog, for what this package does not wrap."""
        return self.store.catalog

    @cached_property
    def iceberg_table(self) -> Any:
        """The pyiceberg table this dataset is."""
        return self.store.load_table(self.name)

    @property
    def exists(self) -> bool:
        """Whether the table is there yet."""
        return self.store.table_exists(self.name)

    def create_with_field(self, field: StructField, **kwargs: Any) -> IcebergDataset:
        """Create the table from `field`: schema, keys, partitioning and docs.

        Idempotent, and the only place a table is created -- a write that
        lands on a fresh catalog comes through here. The namespace is created
        with it, because a table is not a thing you can have without one.
        """
        if self.exists:
            return self
        namespace = self.name.rpartition(".")[0]
        if namespace:
            self.store.create_namespace(namespace)
        schema = field.into_iceberg_schema()
        defaults = COMMIT_PROPERTIES if self.optimize_commits else {}
        table = self.iceberg_catalog.create_table(
            self.name,
            schema=schema,
            location=kwargs.pop("location", self.location),
            partition_spec=field.into_iceberg_partition_spec(schema),
            properties={**defaults, **self.table_properties, **kwargs.pop("properties", {})},
        )
        self.__dict__["iceberg_table"] = table
        return self

    def get_or_create_table(self) -> Any:
        """The pyiceberg table, created from the declared shape when absent.

        A table already loaded is handed straight back: `exists` is a catalog
        round trip, which is free on SQLite and a network hop on a REST or Glue
        catalog -- and this is called once per write.
        """
        if "iceberg_table" in self.__dict__:
            return self.iceberg_table
        if not self.exists:
            if self.struct is None:
                raise ValueError(
                    f"{self.name!r} does not exist and this dataset declares no shape; "
                    "give it `struct=`, or create it with create_with(...)"
                )
            self.create_with_field(self.struct)
        return self.iceberg_table

    def refresh(self) -> IcebergDataset:
        """Drop what was loaded, so the next call sees other writers' commits."""
        for view in ("iceberg_table", "table_field"):
            self.__dict__.pop(view, None)
        return self

    # -- what it holds ------------------------------------------------------

    @cached_property
    def table_field(self) -> StructField:
        """The table's own shape: its schema, docs, keys and partitioning."""
        table = self.iceberg_table
        return StructField.from_iceberg_schema(
            table.schema(), self.name.rpartition(".")[2], spec=table.spec()
        )

    def into_struct_field(self) -> StructField:
        """The declared shape, or the table's own when nothing was declared."""
        return self.struct if self.struct is not None else self.table_field

    def add_fields(self, source: Any = None, *, dry_run: bool = False) -> list[str]:
        """Add the columns `source` has and the table lacks; skip when there are none.

        Schema evolution as a merge, in Iceberg's own terms: pyiceberg's
        `union_by_name` matches by name, adds what is new as optional (rows
        already written have nothing to put in it) and leaves everything else
        alone, at every level. Ids are Iceberg's business -- an incoming Arrow
        schema that carries none is numbered on the way in.

        Returns the columns it added, so "nothing to do" is an empty list and
        never a commit; `dry_run=True` reports without touching the table.
        """
        target = self.target_field(source)
        current = self.table_field
        added = [name for name in target.names if name not in current.names]
        if not added or dry_run:
            return added
        table = self.iceberg_table
        with table.update_schema() as update:
            update.union_by_name(target.into_iceberg_schema())
        self.refresh()
        if self.struct is not None:
            # The declared shape *is* what writes cast onto, so evolving the
            # table without it would drop the new columns at the next write.
            self.struct = target
        return added

    # -- reading ------------------------------------------------------------

    def read_arrow_reader(
        self,
        schema: Any = None,
        *,
        row_filter: Any = None,
        columns: Sequence[str] | None = None,
        snapshot_id: int | None = None,
        limit: int | None = None,
        branch: str | None = None,
    ) -> pyarrow.RecordBatchReader:
        """Stream the table, pushing what it can down to the scan planner.

        `row_filter` and `columns` are the planner's business, not ours: handing
        them over is what lets Iceberg skip whole files on partition and column
        statistics rather than reading them to throw the rows away. Use
        `scan_plan` to see whether a filter actually skipped anything -- the
        rows that come back never say so.

        `limit` is **not** a planning hint in pyiceberg: it is applied to the
        rows, after the files it could have skipped have been opened. Take what
        you need from the reader instead when that matters. `snapshot_id` reads
        an older state, `branch` another line of it.

        With no `schema` the reader is pyiceberg's own, untouched -- the fastest
        path, and the one that keeps the widths the store uses. With one, every
        batch is cast onto it on the way out **and the projection follows from
        it**: asking for a narrow shape reads narrow columns, rather than
        reading all of them and dropping the rest after the fact. Name
        `columns` to override that.
        """
        target = None if schema is None else self.target_field(schema)
        scan = self.iceberg_table.scan(
            selected_fields=self._selected(columns, target),
            snapshot_id=snapshot_id,
            limit=limit,
            **({"row_filter": row_filter} if row_filter is not None else {}),
        )
        reference = branch or self.branch
        if reference and snapshot_id is None:
            scan = scan.use_ref(reference)
        reader = scan.to_arrow_batch_reader()
        if target is None:
            return reader
        return target.cast_arrow_reader(reader)

    def _selected(self, columns: Sequence[str] | None, target: StructField | None) -> tuple:
        """Which columns the scan reads: what was asked for, or what the shape needs.

        A column the target declares and the table does not have is left out of
        the projection -- pyiceberg would refuse the name, and the cast fills it
        with nulls anyway (or refuses it, if it may not be null).
        """
        if columns:
            return tuple(columns)
        if target is None:
            return ("*",)
        stored = self.table_field.names
        wanted = tuple(name for name in target.names if name in set(stored))
        # Nothing in common: the rows still have to be counted, but reading
        # every column of them to hand back a table of nulls would be absurd.
        return wanted or (stored[0],)

    # -- writing ------------------------------------------------------------

    def write_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Write a stream into the table, one commit per chunk.

        The stream is cast onto `schema` -- this dataset's shape by default --
        so a nearly-right batch lands instead of failing pyiceberg's own schema
        check, and a column the source never produced is filled when it may be.
        The table is created from the declared shape if it is not there yet:
        a write appends, and appending to nothing is a create.

        `merge_by=True` merges on the primary key the shape declares, a list of
        names merges on those, and falsy appends. A merge is pyiceberg's own
        upsert: it plans the matching rows itself, which is a job for the engine
        that holds the statistics, not for this code.
        """
        table = self.get_or_create_table()
        reader = self.target_field(schema).cast_arrow_reader(source)
        join = self.merge_columns(merge_by)
        reference = branch or self.branch or MAIN
        rows = self.commit_row_size if commit_row_size is None else commit_row_size
        for chunk in arrow_chunks(reader, rows):
            chunk = self.sorted(chunk)
            if not join:
                table.append(chunk, snapshot_properties=properties or {}, branch=reference)
            elif self.plan_merges:
                self.merge_arrow_table(chunk, join, branch=reference, properties=properties)
            else:
                table.upsert(chunk, join_cols=join, branch=reference)

    def merge_arrow_table(
        self,
        chunk: pyarrow.Table,
        merge_by: bool | Sequence[str] | None = True,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> tuple[int, int]:
        """One chunk merged into the table: `(rows updated, rows inserted)`.

        The algorithm is pyiceberg's own -- find the stored rows a chunk
        matches, overwrite the ones whose non-key columns changed, append the
        rest -- and its own helpers do the row-level work, so the result is the
        one `Table.upsert` would produce, down to the schema check it makes
        first. What changes is **how the matching rows are found**.

        Two refusals are deliberately stricter than the library's, because both
        of the alternatives are a corrupted table: a stored table with
        duplicate merge keys is refused wherever the copies are (pyiceberg
        checks one record batch at a time, so copies in two files slip past it
        and it writes a third), and a null merge key is refused outright (no
        predicate can find the row it would match, so it would be inserted
        again).

        `Table.upsert` builds its scan filter as one equality term per incoming
        row (`Or(And(k1 = .., k2 = ..), ...)` for a composite key), then binds
        that same expression to Arrow once per matched batch to decide what to
        insert. Both are quadratic in the chunk: measured on a two-column key,
        500 rows upsert at ~700 rows/s and 4,000 rows at ~440, and it gets
        worse from there (`benchmarks/bench_iceberg.py`).

        **Where the win is**: on the insert-dominated stream this exists for --
        new keys, or a replay of rows that have not changed -- the difference
        is orders of magnitude, because the scan prunes to nothing and no
        overwrite happens. When most rows genuinely *change*, the delete half
        still carries pyiceberg's exact per-row filter (it has to: a range
        would delete rows the chunk never touched), and the win is closer to
        2x.

        Here the scan is filtered by the chunk's **key ranges** -- two terms per
        key column, whatever the chunk's size -- which every matching row
        satisfies, so the scan returns a superset and nothing can be missed.
        The rows to insert then come from one Arrow anti-join instead of a bound
        expression per batch. A stream whose keys are all new prunes to zero
        files and becomes a plain append, which is the case a log ingest hits
        every time.

        Only the full upsert (update matched, insert unmatched) is implemented;
        for the other three combinations call `iceberg_table.upsert` directly.
        """
        from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible
        from pyiceberg.table import upsert_util

        join = self.merge_columns(merge_by)
        if not join:
            raise ValueError("merge_arrow_table needs columns to merge on")
        if chunk.num_rows == 0:
            # Nothing to match: planning a scan for it would read the table to
            # discover that, and `_key_ranges` has no bounds to build from.
            return 0, 0
        if upsert_util.has_duplicate_rows(chunk, join):
            raise ValueError(
                "Duplicate rows found in source dataset based on the key columns. "
                "No upsert executed"
            )
        table = self.get_or_create_table()
        # The same check `Table.upsert` makes, and for the same reason: a chunk
        # that is missing a column, or carries one at another precision, would
        # otherwise be written as nulls or silently downcast. Making it here
        # keeps the two paths interchangeable.
        _check_pyarrow_schema_compatible(
            table.schema(), provided_schema=chunk.schema, format_version=table.format_version
        )
        # The chunk's own shape is the one everything is brought onto: an Arrow
        # join refuses to match a `string` key against the `large_string` a scan
        # hands back, and converting what was *read* costs less than converting
        # what is being written -- a streaming merge reads far fewer rows than
        # it writes. It also keeps a write that carries a column the table does
        # not have an error, exactly as an append of the same rows would be,
        # rather than a silent drop.
        shape = field_of(chunk.schema)
        reference = branch or self.branch or MAIN
        scan = table.scan(row_filter=_key_ranges(chunk, join))
        if reference in table.refs():
            scan = scan.use_ref(reference)
        # The batch reader, not `to_arrow()`: pyiceberg's two read paths disagree
        # about string widths (`string` from one, `large_string` from the other),
        # and only one of them is the shape the table reports.
        matched = shape.cast_arrow_table(scan.to_arrow_batch_reader().read_all())
        # The scan filter is a *superset* -- a range covers stored keys the
        # chunk never mentions -- so the rows it brought back are narrowed to
        # the ones the chunk actually references before anything looks at them.
        # Without this, a duplicate key stored anywhere inside the chunk's key
        # range aborts a merge that has nothing to do with it.
        matched = _semi_join(matched, chunk, join)

        # An Arrow join hands back nullable columns whatever it was given, and
        # pyiceberg checks a write against the table's own requiredness -- so
        # both halves go back onto the stored shape before they are committed.
        updates = shape.cast_arrow_table(_changed(chunk, matched, join))
        inserts = shape.cast_arrow_table(_unmatched(chunk, matched, join))
        if len(updates) == 0 and len(inserts) == 0:
            return 0, 0
        with table.transaction() as transaction:
            if len(updates) > 0:
                from pyiceberg.expressions import And

                # The match filter decides what is *deleted*, so it stays exact;
                # the ranges only narrow it. Never the other way round: a range
                # alone would delete rows the chunk never touched. Past 200
                # literals the exact filter stops pruning files on its own, and
                # this is what keeps a 201-row update from re-reading the table.
                transaction.overwrite(
                    updates,
                    overwrite_filter=And(
                        upsert_util.create_match_filter(updates, join),
                        _key_ranges(updates, join),
                    ),
                    branch=reference,
                    snapshot_properties=properties or {},
                )
            if len(inserts) > 0:
                transaction.append(inserts, branch=reference, snapshot_properties=properties or {})
        # No `refresh()`: a commit updates the table object it was made on, in
        # place, and this runs once per chunk -- reloading it from the catalog
        # here would be a round trip per commit to learn what we just did.
        # `refresh()` is for seeing *other* writers, and stays the caller's.
        return len(updates), len(inserts)

    def sorted(self, chunk: pyarrow.Table) -> pyarrow.Table:
        """`chunk` in `sort_by` order, or exactly as it came when nothing says.

        Sorting is not about the file's contents -- Iceberg does not care what
        order rows are stored in -- it is about the *bounds* recorded around
        them. Inside a file those bounds are per row group, and a filter skips
        a row group it cannot match without decoding it: on one 600k-row commit
        a top-5% filter took 214 ms unsorted and 22 ms sorted, reading the same
        single file. (Which is also why `write.parquet.row-group-limit` is set:
        with Iceberg's default of a million rows there would be one row group
        and nothing to skip.)

        What it does **not** do is narrow *file* bounds for a stream that
        arrives shuffled -- a chunk of shuffled rows still spans the whole key
        range whatever order it is written in. File bounds come from chunks
        that are already roughly ordered, which is what a log is.
        """
        if not self.sort_by:
            return chunk
        return chunk.sort_by([(name, "ascending") for name in self.sort_by])

    def delete(self, row_filter: Any = None, *, branch: str | None = None) -> None:
        """Delete the rows a filter matches, in one commit."""
        table = self.iceberg_table
        reference = branch or self.branch or MAIN
        if row_filter is None:
            table.delete(branch=reference)
        else:
            table.delete(row_filter, branch=reference)

    # -- snapshots and branches ---------------------------------------------

    def snapshots(self) -> pyarrow.Table:
        """Every snapshot, as Iceberg's own metadata table."""
        return self.iceberg_table.inspect.snapshots()

    def refs(self) -> dict[str, Any]:
        """Branches and tags, by name."""
        return dict(self.iceberg_table.refs())

    def create_branch(self, name: str, snapshot_id: int | None = None) -> IcebergDataset:
        """Branch off the current state, or off `snapshot_id`."""
        table = self.iceberg_table
        head = table.current_snapshot()
        current = snapshot_id or (head.snapshot_id if head else None)
        if current is None:
            raise ValueError(f"{self.name!r} has no snapshot to branch from; write to it first")
        with table.manage_snapshots() as manage:
            manage.create_branch(snapshot_id=current, branch_name=name)
        return self.refresh()

    def remove_branch(self, name: str) -> IcebergDataset:
        """Drop a branch, keeping whatever `main` still references."""
        with self.iceberg_table.manage_snapshots() as manage:
            manage.remove_branch(name)
        return self.refresh()

    def rollback(self, snapshot_id: int) -> IcebergDataset:
        """Move the current branch back to an earlier snapshot."""
        with self.iceberg_table.manage_snapshots() as manage:
            manage.rollback_to_snapshot(snapshot_id)
        return self.refresh()

    # -- maintenance --------------------------------------------------------

    def data_files(self) -> pyarrow.Table:
        """Every data file the current snapshot holds, as Iceberg's own metadata."""
        return self.iceberg_table.inspect.data_files()

    def scan_plan(
        self,
        row_filter: Any = None,
        *,
        columns: Sequence[str] | None = None,
        snapshot_id: int | None = None,
        branch: str | None = None,
    ) -> dict[str, int]:
        """What a read would touch, without reading it: files, rows, bytes.

        Metadata only, and the one honest way to see whether a filter prunes:
        the rows a scan *returns* say nothing about the files it *opened*, so a
        filter that Iceberg cannot use (`!=`, a range over a bucketed column, a
        nested field, a column written with no metrics) looks perfect in the
        results and reads the whole table.

        Compares against the unfiltered plan, so `skipped` is what the filter
        actually bought::

            quotes.scan_plan("day = '2026-08-14'")
            {'files': 2, 'rows': 20000, 'bytes': 190_000, 'total_files': 16, 'skipped': 14}
        """
        table = self.iceberg_table
        planned = self._planned(table, row_filter, columns, snapshot_id, branch)
        whole = self._planned(table, None, columns, snapshot_id, branch)
        return {
            **planned,
            "total_files": whole["files"],
            "skipped": whole["files"] - planned["files"],
        }

    def _planned(
        self,
        table: Any,
        row_filter: Any,
        columns: Sequence[str] | None,
        snapshot_id: int | None,
        branch: str | None,
    ) -> dict[str, int]:
        scan = table.scan(
            selected_fields=tuple(columns) if columns else ("*",),
            snapshot_id=snapshot_id,
            **({"row_filter": row_filter} if row_filter is not None else {}),
        )
        reference = branch or self.branch
        if reference and snapshot_id is None and reference in table.refs():
            scan = scan.use_ref(reference)
        tasks = list(scan.plan_files())
        return {
            "files": len(tasks),
            "rows": sum(task.file.record_count for task in tasks),
            "bytes": sum(task.file.file_size_in_bytes for task in tasks),
        }

    def compaction_plan(self, min_files: int = 2) -> list[tuple[Any, int]]:
        """`(row filter, file count)` for every part of the table worth rewriting.

        Partition by partition when every partition field is an identity of a
        column -- then a partition *is* a predicate, and rewriting one touches
        nothing else. Otherwise the transform hides which rows are where, so
        the only honest plan is the whole table at once.

        **A part already compacted is not planned again.** Whether a rewrite
        can improve anything is not something file counts can answer: pyiceberg
        decides how many files a commit produces from the *in-memory* size of
        what it is given, so a part that legitimately needs ten files still
        reports ten afterwards -- and a plan that only counts files rewrites it
        forever, doubling the table on every run. What settles it is whether
        anything has landed since, which the partition's last commit says.
        """
        table = self.iceberg_table
        partitions = table.inspect.partitions()
        if partitions.num_rows == 0:
            return []
        compacted = self.compacted_snapshots()
        rows = [
            row
            for row in partitions.to_pylist()
            if row.get("last_updated_snapshot_id") not in compacted
        ]
        if not rows:
            return []

        spec = table.spec()
        identities = [
            (field.name, table.schema().find_column_name(field.source_id))
            for field in spec.fields
            if str(field.transform) == "identity"
        ]
        if len(identities) != len(spec.fields):
            # No partition field, or one whose transform hides which rows it
            # holds: the table is only addressable as a whole.
            total = int(sum(row["file_count"] for row in rows))
            return [(None, total)] if total >= min_files else []

        plan: list[tuple[Any, int]] = []
        for row in rows:
            count = int(row["file_count"])
            if count < min_files:
                continue
            values = row["partition"]
            terms = [
                f"{column} = {_literal(values[name])}"
                for name, column in identities
                if values.get(name) is not None
            ]
            plan.append((" and ".join(terms) if terms else None, count))
        return plan

    def compacted_snapshots(self) -> set[int]:
        """Snapshots this package's compaction wrote, by id."""
        return {
            snapshot.snapshot_id
            for snapshot in self.iceberg_table.snapshots()
            if snapshot.summary is not None
            and snapshot.summary.additional_properties.get(COMPACTION_MARK)
        }

    def compact(
        self,
        *,
        min_files: int = 2,
        row_filter: Any = None,
        target_file_size: int | None = None,
        branch: str | None = None,
    ) -> int:
        """Rewrite fragmented parts of the table, one commit each.

        A stream that commits often lands many small files, and a scan pays for
        every one of them. Compaction reads a part back and writes it out as
        Iceberg would have written it in one go -- how big the output files are
        is `write.target-file-size-bytes`, Iceberg's own knob, never a size
        this code picks.

        Returns how many files were rewritten. `row_filter` compacts one part
        of the table and nothing else, which is also how a table too big to
        read at once is compacted: a partition at a time.
        """
        if target_file_size:
            self.set_properties({TARGET_FILE_SIZE: str(target_file_size)})
        plan = (
            # What that filter's own scan plans, not the whole table's files:
            # counting every manifest to report a number about one partition is
            # both slower and wrong.
            [(row_filter, self.scan_plan(row_filter)["files"])]
            if row_filter is not None
            else self.compaction_plan(min_files)
        )
        rewritten = 0
        for part, count in plan:
            data = self.read_arrow_table(row_filter=part, branch=branch)
            if data.num_rows == 0:
                continue
            self.iceberg_table.overwrite(
                data,
                overwrite_filter=part if part is not None else _always_true(),
                branch=branch or self.branch or MAIN,
                snapshot_properties={COMPACTION_MARK: "true"},
            )
            rewritten += count
        return rewritten

    def cleanup(
        self,
        *,
        retain: int = 1,
        older_than: datetime.datetime | datetime.timedelta | None = None,
        remove_orphans: bool = True,
        orphan_age: datetime.timedelta = ORPHAN_AGE,
        metadata: bool = True,
        dry_run: bool = False,
    ) -> dict[str, int]:
        """Expire old snapshots, then delete the files they stranded.

        pyiceberg's expiry is metadata-only: it forgets snapshots, it does not
        remove what they were keeping alive, so a table that is only ever
        expired grows garbage instead of shrinking. This does both -- and the
        sweep is deliberately conservative: a file is deleted only when no live
        snapshot references it *and* it is older than `orphan_age`, because a
        writer that is committing right now has files on disk that no snapshot
        mentions yet.

        Returns `{"expired": n, "deleted": m, "bytes": b}`. `dry_run=True`
        reports what it *would* expire, and then counts the files that are
        already orphaned -- **not** the ones expiring would strand, which is
        strictly more. A dry run under-reports the sweep on purpose: it is the
        one number that cannot be known without committing the expiry.
        """
        expired = self._expirable(retain, older_than)
        report = {"expired": len(expired), "deleted": 0, "bytes": 0}
        if expired and not dry_run:
            with self.iceberg_table.maintenance.expire_snapshots() as expire:
                expire.by_ids(expired)
            self.refresh()
        if not remove_orphans:
            return report
        orphans = self.orphan_files(orphan_age, metadata=metadata)
        report["deleted"] = len(orphans)
        report["bytes"] = int(sum(size for _, size in orphans))
        if not dry_run:
            filesystem, _ = resolve(self.iceberg_table.location())
            for path, _ in orphans:
                filesystem.delete_file(path)
        return report

    def orphan_files(
        self, older_than: datetime.timedelta = ORPHAN_AGE, *, metadata: bool = True
    ) -> list[tuple[str, int]]:
        """Files under the table that nothing live references any more.

        Two directories, because a stream fills both: `data/`, where expiry
        strands the files old snapshots held, and `metadata/`, which grows with
        the *commit count* -- one `metadata.json` and one manifest per commit,
        so a table with a single live data file can easily carry a hundred
        metadata files.

        The live set is deliberately over-broad: every retained snapshot's
        manifest list and every manifest reachable from it, every entry in the
        metadata log, and the current metadata pointer. Anything younger than
        `older_than` is spared whether or not it is referenced, because a writer
        committing right now has files on disk that no snapshot mentions yet.

        Listed through `pyarrow.fs`, like every other file this package touches,
        so an object store is walked by the same handle the reads use.
        """
        table = self.iceberg_table
        filesystem, root = resolve(table.location())
        cutoff = datetime.datetime.now(datetime.UTC) - older_than
        directories = {"data": self._live_data(table)}
        if metadata:
            directories["metadata"] = self._live_metadata(table)

        found = []
        for directory, live in directories.items():
            selector = pyarrow.fs.FileSelector(
                f"{root.rstrip('/')}/{directory}", recursive=True, allow_not_found=True
            )
            for info in filesystem.get_file_info(selector):
                if info.type != pyarrow.fs.FileType.File or _path_of(info.path) in live:
                    continue
                # A Hadoop-style catalog keeps its pointer beside the metadata
                # and nothing inside the metadata names it: reading the table
                # is how you would find out it had been swept.
                if info.base_name == HADOOP_POINTER:
                    continue
                if info.mtime and info.mtime > cutoff:
                    continue
                found.append((info.path, info.size))
        return found

    def _live_data(self, table: Any) -> set[str]:
        """Every data and delete file any retained snapshot still holds."""
        return {
            _path_of(path) for path in table.inspect.all_files().column("file_path").to_pylist()
        }

    def _live_metadata(self, table: Any) -> set[str]:
        """Every metadata file a retained snapshot, the log, or the pointer names.

        Deleting one of these does not lose a row; it loses the *table*. So the
        set is built from every direction at once -- the current pointer, the
        metadata log, every snapshot's manifest list and its manifests, and the
        statistics the metadata registers -- and a file is only swept when none
        of them mention it.

        The statistics are the ones nothing else reaches: a Puffin file another
        engine wrote sits in `metadata/` beside everything else, is named only
        by `metadata.statistics`, and is exactly as old as the snapshot it
        describes -- so an age rule does not save it either.
        """
        live = {_path_of(table.metadata_location)}
        for entry in table.metadata.metadata_log:
            live.add(_path_of(entry.metadata_file))
        for snapshot in table.snapshots():
            if snapshot.manifest_list:
                live.add(_path_of(snapshot.manifest_list))
            for manifest in snapshot.manifests(table.io):
                live.add(_path_of(manifest.manifest_path))
        for statistics in (*table.metadata.statistics, *table.metadata.partition_statistics):
            live.add(_path_of(statistics.statistics_path))
        return live

    def optimize(self, *, min_files: int = 2, retain: int = 1, **kwargs: Any) -> dict[str, int]:
        """Merge manifests, compact files, then expire and sweep -- in that order.

        The order is the point: compacting first makes the snapshots that
        cleanup then expires, and merging manifests first means the compaction
        commits land in fewer of them. One call is the whole routine a table
        written by a streaming job needs.
        """
        if self.iceberg_table.properties.get(MERGE_MANIFESTS) != "true":
            # Only when it is not already on: a no-op commit is still a metadata
            # version, and a routine that runs on a schedule would spend one
            # every time.
            self.set_properties({MERGE_MANIFESTS: "true"})
        rewritten = self.compact(min_files=min_files, **kwargs)
        report = self.cleanup(retain=retain)
        return {"rewritten": rewritten, **report}

    def set_properties(self, properties: dict[str, str]) -> IcebergDataset:
        """Set table properties, in one commit."""
        table = self.get_or_create_table()
        with table.transaction() as transaction:
            transaction.set_properties(**properties)
        return self

    def _expirable(
        self, retain: int, older_than: datetime.datetime | datetime.timedelta | None
    ) -> list[int]:
        """Snapshot ids old enough to expire, keeping the last `retain` and every ref."""
        table = self.iceberg_table
        kept = {reference.snapshot_id for reference in table.refs().values()}
        snapshots = sorted(table.snapshots(), key=lambda snapshot: snapshot.timestamp_ms)
        cutoff = _cutoff_ms(older_than)
        candidates = snapshots[: max(len(snapshots) - retain, 0)]
        return [
            snapshot.snapshot_id
            for snapshot in candidates
            if snapshot.snapshot_id not in kept
            and (cutoff is None or snapshot.timestamp_ms < cutoff)
        ]


# -- helpers ----------------------------------------------------------------


def _key_ranges(chunk: pyarrow.Table, join: Sequence[str]) -> Any:
    """A predicate every row matching `chunk` on `join` must satisfy.

    A stored row can only equal an incoming one if it agrees on every key
    column, so *anything* implied by that is a safe scan filter -- and the
    cheapest such filter is the one that does not grow with the chunk: the
    values a key column takes, when there are few of them, and its range when
    there are many. Two terms per column either way, against `Table.upsert`'s
    one term per row.

    The `In` form matters beyond predicate size: an identity-partitioned key
    column prunes to exactly its partitions, where a range only prunes what the
    file bounds happen to exclude. A chunk of entirely new keys plans to no
    files at all, which is what turns a merge into an append.
    """
    from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThanOrEqual

    terms = []
    for column in join:
        values = chunk.column(column)
        if values.null_count:
            # A null never equals anything in Iceberg, so no predicate can find
            # the stored row a null key would match -- the merge would insert a
            # second one. pyiceberg refuses a null literal too, one row later.
            raise ValueError(
                f"column {column!r} is a merge key and cannot be null; "
                "a null key matches nothing, so merging on it would duplicate rows"
            )
        distinct = pyarrow.compute.unique(values.combine_chunks())
        if len(distinct) == 0:
            continue
        if len(distinct) <= MERGE_IN_LIMIT:
            terms.append(In(column, distinct.to_pylist()))
            continue
        bounds = pyarrow.compute.min_max(values).as_py()
        low, high = bounds["min"], bounds["max"]
        if low is None or high is None:  # every key is null: nothing to bound
            continue
        terms.append(And(GreaterThanOrEqual(column, low), LessThanOrEqual(column, high)))
    if not terms:
        return _always_true()
    return And(*terms) if len(terms) > 1 else terms[0]


#: Marker columns the joins below carry; named like pyiceberg's so a table that
#: already has one is refused there rather than corrupted here.
SOURCE_INDEX = "__source_index"
TARGET_INDEX = "__target_index"


def _keys_of(table: pyarrow.Table, join: Sequence[str], marker: str) -> pyarrow.Table:
    """Just the key columns, numbered, and normalised for Arrow's equality.

    Two reasons the joins never see the whole table. **Arrow refuses nested
    columns as join payload**, so a struct, list or map anywhere in the row
    would make a merge crash rather than fall back; carrying only the keys and
    an index, then taking the rows back by that index, keeps every column type
    out of Acero's way.

    And Arrow's equality is not Iceberg's on one point: `-0.0` and `0.0` are
    the same number to IEEE 754 and to Python, and pyiceberg compares them as
    equal -- but they hash apart in a join, which would insert a duplicate key.
    Normalising the sign of zero on float key columns keeps the two agreeing.
    The *values written* are never touched: this table is only the join.
    """
    columns = []
    for name in join:
        column = table.column(name).combine_chunks()
        if pyarrow.types.is_floating(column.type):
            zero = pyarrow.scalar(0.0, column.type)
            column = pyarrow.compute.if_else(pyarrow.compute.equal(column, zero), zero, column)
        columns.append(column)
    keys = pyarrow.Table.from_arrays(columns, names=list(join))
    from rekep.fields import arrays

    return keys.append_column(marker, arrays.sequence(table.num_rows))


def _align_keys(matched: pyarrow.Table, chunk: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """`matched` with its key columns at the chunk's types, so a join can run."""
    for name in join:
        wanted = chunk.schema.field(name).type
        if matched.schema.field(name).type == wanted:
            continue
        index = matched.schema.get_field_index(name)
        matched = matched.set_column(
            index, matched.schema.field(index).with_type(wanted), matched.column(name).cast(wanted)
        )
    return matched


def _semi_join(matched: pyarrow.Table, chunk: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """The rows of `matched` whose key the chunk references."""
    if matched.num_rows == 0:
        return matched
    kept = _keys_of(matched, join, TARGET_INDEX).join(
        _keys_of(chunk, join, SOURCE_INDEX).select(list(join)),
        keys=list(join),
        join_type="left semi",
    )
    return matched.take(kept.column(TARGET_INDEX))


def _changed(chunk: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """Rows of `chunk` a stored row matches but does not equal.

    The same answer as pyiceberg's `get_rows_to_update`, computed in kernels.
    Theirs joins on the keys and then **compares the matched rows in Python**,
    one `slice(i, 1)` and one `as_py()` per non-key column per row -- about
    50 microseconds a row, which is the whole cost of a merge that updates
    anything. Here the pairs are gathered with `take` and compared column by
    column, so a million matched rows cost a handful of vectorised passes.

    Null semantics are theirs: two nulls are equal, a null and a value are not.

    Whatever Arrow cannot do here, pyiceberg's own function does instead. That
    is not only the obvious cases -- a struct, a list, a map have no equality
    kernel -- but every one that cannot be enumerated in advance: an extension
    type such as Iceberg's uuid, two tables carrying the same column at
    different types, a naive timestamp against a zoned one. Rather than guess
    which kernels exist, the fast path is *attempted*, and any Arrow refusal
    hands the whole comparison back to the library.
    """
    from pyiceberg.table import upsert_util

    compare = [name for name in chunk.column_names if name not in set(join)]
    if not compare or len(matched) == 0 or len(chunk) == 0:
        return upsert_util.get_rows_to_update(chunk, matched, join)
    try:
        # Only the *keys* are aligned, because only the join needs them to be.
        # Leaving the compared columns as they came is what makes a pair Arrow
        # refuses -- a naive timestamp against a zoned one -- fall back to the
        # library instead of being quietly cast into agreement.
        matched = _align_keys(matched, chunk, join)
    except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, pyarrow.ArrowTypeError):
        return upsert_util.get_rows_to_update(chunk, matched, join)
    if upsert_util.has_duplicate_rows(matched, join):
        raise ValueError("Target table has duplicate rows, aborting upsert")
    try:
        return _changed_by_kernel(chunk, matched, join, compare)
    except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, pyarrow.ArrowTypeError):
        return upsert_util.get_rows_to_update(chunk, matched, join)


def _changed_by_kernel(
    chunk: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str], compare: Sequence[str]
) -> pyarrow.Table:
    """`_changed`'s fast path: one pass per column instead of one pass per row."""
    keys = list(join)
    pairs = _keys_of(chunk, keys, SOURCE_INDEX).join(
        _keys_of(matched, keys, TARGET_INDEX), keys=keys, join_type="inner"
    )
    if pairs.num_rows == 0:
        return chunk.schema.empty_table()

    left = chunk.take(pairs.column(SOURCE_INDEX))
    right = matched.take(pairs.column(TARGET_INDEX))
    compute = pyarrow.compute
    differs = pyarrow.array([False] * pairs.num_rows)
    for name in compare:
        one, other = left.column(name), right.column(name)
        unequal = compute.fill_null(compute.not_equal(one, other), False)
        # `not_equal` is null when either side is: a null against a value is a
        # difference, two nulls are not.
        only_one_null = compute.xor(compute.is_null(one), compute.is_null(other))
        differs = compute.or_(differs, compute.or_(unequal, only_one_null))
    return chunk.take(compute.filter(pairs.column(SOURCE_INDEX), differs))


def _unmatched(chunk: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """The rows of `chunk` no row of `matched` shares a key with.

    One Arrow anti-join over the keys alone, rather than binding a per-row
    equality expression and filtering with it once per batch, which is what
    makes the insert half of a merge linear instead of quadratic.
    """
    if matched.num_rows == 0:
        return chunk
    fresh = _keys_of(chunk, join, SOURCE_INDEX).join(
        _keys_of(matched, join, TARGET_INDEX).select(list(join)),
        keys=list(join),
        join_type="left anti",
    )
    return chunk.take(fresh.column(SOURCE_INDEX))


def _always_true() -> Any:
    from pyiceberg.expressions import AlwaysTrue

    return AlwaysTrue()


def _literal(value: Any) -> str:
    """One partition value as its filter literal."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return f"'{value}'"


def _path_of(location: str) -> str:
    """A file location without its scheme, which is how `pyarrow.fs` names it."""
    return location.split("://", 1)[-1]


def _cutoff_ms(older_than: datetime.datetime | datetime.timedelta | None) -> int | None:
    """The instant a snapshot must predate to be expirable, in Iceberg's millis."""
    if older_than is None:
        return None
    if isinstance(older_than, datetime.timedelta):
        older_than = datetime.datetime.now(datetime.UTC) - older_than
    return int(older_than.timestamp() * 1000)
