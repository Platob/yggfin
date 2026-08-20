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
from rekep.fields import StructField
from rekep.filesystems import resolve
from rekep.iceberg.catalog import IcebergCatalog

#: The branch a read or a write lands on when nothing names one -- pyiceberg's
#: own default, spelled out here so the two cannot disagree about it.
MAIN = "main"

#: How long a file must have been unreferenced before `cleanup` deletes it. A
#: writer that is committing right now has files on disk that no snapshot
#: mentions yet; deleting those would break it, so orphans have to be old.
ORPHAN_AGE = datetime.timedelta(days=3)

#: Table property that decides how big a compacted file comes out. Iceberg's
#: own knob: `compact` picks *what* to rewrite, never how to slice the output.
TARGET_FILE_SIZE = "write.target-file-size-bytes"

#: Turning this on lets Iceberg merge small manifests as it commits, which is
#: half of what keeps planning fast on a table written in many small batches.
MERGE_MANIFESTS = "commit.manifest-merge.enabled"


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
        table = self.iceberg_catalog.create_table(
            self.name,
            schema=schema,
            location=kwargs.pop("location", self.location),
            partition_spec=field.into_iceberg_partition_spec(schema),
            properties={**self.table_properties, **kwargs.pop("properties", {})},
        )
        self.__dict__["iceberg_table"] = table
        return self

    def get_or_create_table(self) -> Any:
        """The pyiceberg table, created from the declared shape when absent."""
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

        `row_filter` (a pyiceberg expression or its string form), `columns` and
        `limit` are the planner's business, not ours: handing them over is what
        lets Iceberg skip whole files on partition and column statistics rather
        than reading them to throw the rows away. `snapshot_id` reads an older
        state, `branch` another line of it.

        With no `schema` the reader is pyiceberg's own, untouched -- the fastest
        path, and the one that keeps the widths the store uses. With one, every
        batch is cast onto it on the way out.
        """
        scan = self.iceberg_table.scan(
            selected_fields=tuple(columns) if columns else ("*",),
            snapshot_id=snapshot_id,
            limit=limit,
            **({"row_filter": row_filter} if row_filter is not None else {}),
        )
        reference = branch or self.branch
        if reference and snapshot_id is None:
            scan = scan.use_ref(reference)
        reader = scan.to_arrow_batch_reader()
        if schema is None:
            return reader
        return self.target_field(schema).cast_arrow_reader(reader)

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
        for chunk in arrow_chunks(reader, commit_row_size):
            if join:
                table.upsert(chunk, join_cols=join, branch=reference)
            else:
                table.append(chunk, snapshot_properties=properties or {}, branch=reference)

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

    def compaction_plan(self, min_files: int = 2) -> list[tuple[Any, int]]:
        """`(row filter, file count)` for every part of the table worth rewriting.

        Partition by partition when every partition field is an identity of a
        column -- then a partition *is* a predicate, and rewriting one touches
        nothing else. Otherwise the transform hides which rows are where, so
        the only honest plan is the whole table at once.
        """
        table = self.iceberg_table
        partitions = table.inspect.partitions()
        if partitions.num_rows == 0:
            return []
        identities = [
            (field.name, table.schema().find_column_name(field.source_id))
            for field in table.spec().fields
        ]
        if not identities:
            total = int(sum(partitions.column("file_count").to_pylist()))
            return [(None, total)] if total >= min_files else []
        if any(str(field.transform) != "identity" for field in table.spec().fields):
            total = int(sum(partitions.column("file_count").to_pylist()))
            return [(None, total)] if total >= min_files else []

        plan: list[tuple[Any, int]] = []
        values = partitions.column("partition").to_pylist()
        counts = partitions.column("file_count").to_pylist()
        for row, count in zip(values, counts, strict=True):
            if count < min_files:
                continue
            terms = [
                f"{column} = {_literal(row[name])}"
                for name, column in identities
                if row.get(name) is not None
            ]
            plan.append((" and ".join(terms) if terms else None, int(count)))
        return plan

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
            [(row_filter, self.data_files().num_rows)]
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
            )
            rewritten += count
        self.refresh()
        return rewritten

    def cleanup(
        self,
        *,
        retain: int = 1,
        older_than: datetime.datetime | datetime.timedelta | None = None,
        remove_orphans: bool = True,
        orphan_age: datetime.timedelta = ORPHAN_AGE,
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

        Returns `{"expired": n, "deleted": m, "bytes": b}`; `dry_run=True`
        reports the same numbers without touching anything.
        """
        expired = self._expirable(retain, older_than)
        report = {"expired": len(expired), "deleted": 0, "bytes": 0}
        if expired and not dry_run:
            with self.iceberg_table.maintenance.expire_snapshots() as expire:
                expire.by_ids(expired)
            self.refresh()
        if not remove_orphans:
            return report
        orphans = self.orphan_files(orphan_age)
        report["deleted"] = len(orphans)
        report["bytes"] = int(sum(size for _, size in orphans))
        if not dry_run:
            filesystem, _ = resolve(self.iceberg_table.location())
            for path, _ in orphans:
                filesystem.delete_file(path)
        return report

    def orphan_files(self, older_than: datetime.timedelta = ORPHAN_AGE) -> list[tuple[str, int]]:
        """Data files under the table that no live snapshot references.

        Listed through `pyarrow.fs`, like every other file this package
        touches, so an object store is walked by the same handle the reads use.
        """
        table = self.iceberg_table
        filesystem, root = resolve(table.location())
        selector = pyarrow.fs.FileSelector(
            f"{root.rstrip('/')}/data", recursive=True, allow_not_found=True
        )
        cutoff = datetime.datetime.now(datetime.UTC) - older_than
        referenced = table.inspect.all_files().column("file_path").to_pylist()
        live = {_path_of(path) for path in referenced}
        found = []
        for info in filesystem.get_file_info(selector):
            if info.type != pyarrow.fs.FileType.File or _path_of(info.path) in live:
                continue
            if info.mtime and info.mtime > cutoff:
                continue
            found.append((info.path, info.size))
        return found

    def optimize(self, *, min_files: int = 2, retain: int = 1, **kwargs: Any) -> dict[str, int]:
        """Merge manifests, compact files, then expire and sweep -- in that order.

        The order is the point: compacting first makes the snapshots that
        cleanup then expires, and merging manifests first means the compaction
        commits land in fewer of them. One call is the whole routine a table
        written by a streaming job needs.
        """
        self.set_properties({MERGE_MANIFESTS: "true"})
        rewritten = self.compact(min_files=min_files, **kwargs)
        report = self.cleanup(retain=retain)
        return {"rewritten": rewritten, **report}

    def set_properties(self, properties: dict[str, str]) -> IcebergDataset:
        """Set table properties, in one commit."""
        table = self.get_or_create_table()
        with table.transaction() as transaction:
            transaction.set_properties(**properties)
        return self.refresh()

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
