"""One Iceberg table as a dataset, with the maintenance it needs to stay fast."""

from __future__ import annotations

import dataclasses
import datetime
import functools
import json
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

#: Table property holding what compaction has already settled: a JSON object
#: mapping "<branch>/<partition>" to the snapshot that part was rewritten at. A
#: part whose partition has had nothing land in it since is not planned again,
#: which is the only reliable way to know that rewriting it would change
#: nothing: a size rule cannot tell, because pyiceberg sizes its output files
#: from *in-memory* bytes and a part that legitimately needs several files
#: would otherwise be replanned forever.
#:
#: A **table property** and not a snapshot summary, which is where this lived
#: first: expiry deletes snapshots, and `optimize` expires immediately after it
#: compacts, so the mark was gone before the next run could read it. Measured
#: on two partitions that each need several files, that run alternated between
#: them forever -- 50 files rewritten, then 40, then 41, then 40 -- while the
#: rows never changed.
#:
#: What is recorded is `[file count, record count]` and not a snapshot id, for
#: the same reason: expiring the snapshot that last touched a partition makes
#: Iceberg report its `last_updated_snapshot_id` as null, so an id compares
#: unequal to itself one sweep later. Counts are a property of the data.
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
    #: `Table.upsert`. Same algorithm, same rows -- and worth two orders of
    #: magnitude on a stream of new or unchanged keys, about 2x when the rows
    #: genuinely change. `merge_arrow_table` says why, and where it is stricter.
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
        never a commit; `dry_run=True` reports without touching the table. The
        names are **dotted paths**, because that is what evolution can add: a
        member gained by a struct, a list's item or a map's value is a new
        column to `union_by_name`, and comparing top-level names alone reported
        nothing to do and then let the next write drop the value.
        """
        target = self.target_field(source)
        current = self.table_field
        held = set(current.leaf_names())
        added = [name for name in target.leaf_names() if name not in held]
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

        A read pinned to a `snapshot_id` or a `branch` reads under the schema
        *that snapshot* was written with, which a rename or a drop since makes
        a different one. The shape's columns are matched to it by field id, so
        a renamed column is found and comes back under the name the shape asked
        for it by -- matching by name would have filled it with nulls.
        """
        reference = self._reference(branch, snapshot_id)
        target = None if schema is None else self.target_field(schema)
        table = self.iceberg_table
        # Pinned *before* the projection is chosen: a scan on a ref or a
        # snapshot id projects under that snapshot's schema, so which names it
        # will answer to is not known until it is pinned.
        scan = table.scan(
            snapshot_id=snapshot_id,
            limit=limit,
            **({"row_filter": row_filter} if row_filter is not None else {}),
        )
        if reference:
            scan = scan.use_ref(reference)
        found: dict[str, str] = {}
        if columns:
            scan = scan.select(*columns)
        elif target is not None:
            found = self._selected(target, scan)
            scan = scan.select(*found)
        reader = scan.to_arrow_batch_reader()
        if target is None:
            return reader
        return target.cast_arrow_reader(_renamed(reader, found))

    def _reference(self, branch: str | None, snapshot_id: int | None) -> str | None:
        """The branch a read follows, or None when a snapshot id decides instead.

        Naming both is a contradiction and is refused, the way pyiceberg
        refuses it ("Cannot override ref, already set snapshot id"): a snapshot
        id names one state exactly, and nothing checks that it belongs to that
        branch. Ignoring one of the two silently is the worst of the three
        available answers. The dataset's *own* `branch` is not a contradiction
        -- it is a default, and an explicit snapshot id is how a caller reads
        past it.
        """
        if snapshot_id is None:
            return branch or self.branch
        if branch is not None:
            raise ValueError(
                f"snapshot_id={snapshot_id} and branch={branch!r} name two different states; "
                "a snapshot id is already exact, so pass one or the other"
            )
        return None

    def _selected(self, target: StructField, scan: Any) -> dict[str, str]:
        """`{the scan's name: the target's name}` for every column it can fill.

        Matched by **field id** first, never by name alone. A rename is
        metadata-only, so a scan pinned to a snapshot older than one answers to
        the *old* names: a column renamed since would be left out of the
        projection and then filled with nulls, and a column added since would
        be asked for by a name that snapshot never had. Ids are what a rename
        does not change.

        A name the current schema does not carry at all -- a column dropped
        since -- is looked up in that snapshot's own names instead, which is
        what pyiceberg does with the same `selected_fields`: the column is
        still on disk and still readable, and reading it is what was asked for.

        Whichever way it was found, the column comes back under the name the
        *target* used to ask for it. A column the target declares and that
        snapshot really does not have is left out -- the cast fills it with
        nulls, or refuses it if it may not be null, which is the same answer
        either way.
        """
        current = {field.name: field.field_id for field in self.iceberg_table.schema().fields}
        pinned = {field.field_id: field.name for field in scan.projection().fields}
        by_name = set(pinned.values())
        wanted = {}
        for name in target.names:
            stored = pinned.get(current.get(name, -1)) or (name if name in by_name else None)
            if stored is not None:
                wanted[stored] = name
        # Nothing in common: one column is named because a scan must project
        # something, and what comes back is a table of no columns and no rows
        # -- which is pyiceberg's own answer to an empty projection too.
        return wanted or {next(iter(pinned.values())): next(iter(pinned.values()))}

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
        names merges on those, and falsy appends. A merge goes through
        `merge_arrow_table`, which plans the rows it has to look at from the
        chunk's key ranges; `plan_merges=False` hands the chunk to
        `Table.upsert` instead, for the same rows and a great deal more time.
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
                table.upsert(
                    chunk,
                    join_cols=join,
                    branch=reference,
                    snapshot_properties=properties or {},
                )

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
        rest -- and its own helpers do the row-level work, so the rows this
        leaves behind are the rows `Table.upsert` would leave behind, down to
        the schema check it makes first. What changes is **how the matching
        rows are found**.

        Four refusals are deliberately stricter than the library's, because
        every alternative is a corrupted table:

        - a stored table with duplicate merge keys, wherever the copies are
          (pyiceberg checks one record batch at a time, so copies in two files
          slip past it and it writes a third);
        - a null merge key, and a NaN one -- no predicate can name either, so
          the stored row is never found and a second one is inserted, again on
          every later merge;
        - a chunk that does not carry every column the table has: the schema
          check allows a missing *optional* column, and a merge that took it
          would write nulls over whatever is stored there.

        And two are deliberately more forgiving, because refusing costs data
        and accepting cannot: a `-0.0` key matches the `0.0` it equals (Arrow
        hashes them apart, so the library inserts a duplicate key), and a chunk
        whose columns are in another order is merged rather than rejected.

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
        if SOURCE_INDEX in join or TARGET_INDEX in join:
            # pyiceberg's own message, because the joins here reach these names
            # before its check does and would fail on the duplicate instead.
            raise ValueError(
                f"{SOURCE_INDEX} and {TARGET_INDEX} are reserved for joining DataFrames"
            )
        if upsert_util.has_duplicate_rows(chunk, join):
            raise ValueError(
                "Duplicate rows found in source dataset based on the key columns. "
                "No upsert executed"
            )
        table = self.get_or_create_table()
        # The check `Table.upsert` makes, on the configuration it reads it
        # from: a chunk carrying a column the table does not have, or one at a
        # precision Iceberg cannot store, is refused here exactly as it would
        # be there. What it does *not* cover is a column the chunk leaves out,
        # which it allows whenever the field is optional -- and a merge that
        # allowed that would write nulls over whatever is stored.
        _check_pyarrow_schema_compatible(
            table.schema(),
            provided_schema=chunk.schema,
            format_version=table.format_version,
            downcast_ns_timestamp_to_us=_downcasts_ns(),
        )
        # `schema().fields`, not `column_names`: the latter names nested members
        # too (`book.key`), and a merge only ever writes whole top-level columns.
        stored = [member.name for member in table.schema().fields]
        missing = [name for name in stored if name not in chunk.column_names]
        if missing:
            raise ValueError(
                f"chunk is missing {missing}, and a merge writes the row it matches: the stored "
                "values would become nulls. Cast it onto the table's shape before merging"
            )
        if chunk.num_rows == 0:
            # Nothing to match, and the schema was still worth checking: a scan
            # for it would read the table to discover that, and `_key_ranges`
            # has no bounds to build from.
            return 0, 0
        # The chunk's own shape is the one everything is brought onto: an Arrow
        # join refuses to match a `string` key against the `large_string` a scan
        # hands back, and converting what was *read* costs less than converting
        # what is being written -- a streaming merge reads far fewer rows than
        # it writes.
        shape = field_of(chunk.schema)
        reference = branch or self.branch or MAIN
        scan = table.scan(row_filter=_key_ranges(chunk, join))
        if reference in table.refs():
            scan = scan.use_ref(reference)
        # The batch reader, not `to_arrow()`: the two read paths disagree about
        # string widths -- `to_arrow()` hands back `string` where the reader
        # hands back the `large_string` the table itself reports -- and this one
        # streams, which is what a merge of an arbitrary chunk needs.
        matched = _under_current_names(table, scan.to_arrow_batch_reader().read_all())
        matched = shape.cast_arrow_table(matched)
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
        # With no filter the two plans *are* the same plan, and planning is what
        # this call costs: on 40 files, doing it twice took 17.1 ms against 8.6,
        # for a `skipped` that is zero by construction. `compact` asks for one
        # partition at a time through here, so the doubling was per partition.
        whole = (
            planned
            if row_filter is None
            else self._planned(table, None, columns, snapshot_id, branch)
        )
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
        reference = self._reference(branch, snapshot_id)
        if reference:
            # Not guarded by `in table.refs()`: this reports what a *read* would
            # touch, and a read of a branch that is not there raises. Planning
            # main instead and calling it the answer would be a lie.
            scan = scan.use_ref(reference)
        tasks = list(scan.plan_files())
        return {
            "files": len(tasks),
            "rows": sum(task.file.record_count for task in tasks),
            "bytes": sum(task.file.file_size_in_bytes for task in tasks),
        }

    def compaction_plan(
        self, min_files: int = 2, *, branch: str | None = None
    ) -> list[tuple[Any, int]]:
        """`(row filter, file count)` for every part of the table worth rewriting.

        Partition by partition when every partition field is an identity of a
        column -- then a partition *is* a predicate, and rewriting one touches
        nothing else. Otherwise (a transform that hides which rows are where,
        or no partitioning at all) the only honest plan is the whole table at
        once, which means reading it: `row_filter` on `compact` is how a table
        too big for that is compacted a piece at a time.

        Predicates are built as expressions rather than as filter strings. A
        string has to be parsed back, and an apostrophe in a partition value or
        a timestamp partition made that parse fail -- on ordinary values, in
        the very case this says it handles. A null partition value is
        `IsNull`, not a skipped term: dropping it planned the whole table under
        one partition's name and rewrote every other partition's files with it.

        **A part already compacted is not planned again.** Whether a rewrite
        can improve anything is not something file counts can answer: pyiceberg
        decides how many files a commit produces from the *in-memory* size of
        what it is given, so a part that legitimately needs ten files still
        reports ten afterwards -- and a plan that only counts files rewrites it
        forever, doubling the table on every run. What settles it is whether
        anything has landed since, which `COMPACTION_MARK` records.

        `branch` plans that branch's snapshot. Without it the plan came from
        main whatever branch the rewrite then went to, so a branch never
        settled and, once main was compacted, was never planned at all.
        """
        return [(part, count) for _, part, count in self._plan_rows(min_files, branch)]

    def _plan_rows(self, min_files: int, branch: str | None) -> list[tuple[str, Any, int]]:
        """`compaction_plan`, with the mark key each part is recorded under."""
        table = self.iceberg_table
        reference = branch or self.branch or MAIN
        rows = self._partition_rows(reference)
        if not rows:
            return []
        marks = self.compaction_marks()
        fresh = [row for row in rows if marks.get(_mark_key(reference, row)) != _counts(row)]
        if not fresh:
            return []

        spec = table.spec()
        identities = [
            (field.name, table.schema().find_column_name(field.source_id))
            for field in spec.fields
            if str(field.transform) == "identity"
        ]
        if not identities or len(identities) != len(spec.fields):
            # No partition field, or one whose transform hides which rows it
            # holds: the table is only addressable as a whole -- which means
            # reading it whole, so `row_filter` is the escape hatch for a table
            # that does not fit.
            total = int(sum(row["file_count"] for row in fresh))
            return [(f"{reference}/", None, total)] if total >= min_files else []

        plan: list[tuple[str, Any, int]] = []
        for row in fresh:
            count = int(row["file_count"])
            if count < min_files:
                continue
            key = _mark_key(reference, row)
            plan.append((key, _partition_filter(row["partition"], identities), count))
        return plan

    def _partition_rows(self, reference: str) -> list[dict]:
        """One row per partition of that branch's head, as Iceberg reports them."""
        table = self.iceberg_table
        head = table.refs().get(reference)
        rows = table.inspect.partitions(snapshot_id=head.snapshot_id if head is not None else None)
        return rows.to_pylist() if rows.num_rows else []

    def compaction_marks(self) -> dict[str, list[int]]:
        """What compaction settled: `{"<branch>/<partition>": [files, rows]}`."""
        stored = self.iceberg_table.properties.get(COMPACTION_MARK)
        if not stored:
            return {}
        try:
            return {
                key: [int(value) for value in counts] for key, counts in json.loads(stored).items()
            }
        except (TypeError, ValueError):
            # Someone else's value under our key: plan everything rather than
            # refuse to run, and let the next compaction overwrite it.
            return {}

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
        read at once is compacted: a partition at a time. A filtered run
        records nothing in `COMPACTION_MARK` -- what it rewrote is whatever the
        caller's filter covered, which may be a fraction of a partition, and
        marking the whole partition settled would leave the rest of it
        unplanned for good.
        """
        if target_file_size:
            self.set_properties({TARGET_FILE_SIZE: str(target_file_size)})
        reference = branch or self.branch or MAIN
        plan = (
            # What that filter's own scan plans, not the whole table's files:
            # counting every manifest to report a number about one partition is
            # both slower and wrong.
            [("", row_filter, self.scan_plan(row_filter, branch=branch)["files"])]
            if row_filter is not None
            else self._plan_rows(min_files, branch)
        )
        rewritten = 0
        touched = []
        for key, part, count in plan:
            data = self.read_arrow_table(row_filter=part, branch=branch)
            if data.num_rows == 0:
                continue
            self.iceberg_table.overwrite(
                data,
                overwrite_filter=part if part is not None else _always_true(),
                branch=reference,
            )
            rewritten += count
            touched.append(key)
        if touched and row_filter is None:
            self._mark_settled(reference, touched)
        return rewritten

    def _mark_settled(self, reference: str, keys: Sequence[str]) -> None:
        """Record what the parts just rewritten hold, so they are not replanned.

        Read back after the commits rather than predicted from them: the counts
        that matter are the ones the next plan will compare against, and they
        are whatever Iceberg now reports.
        """
        settled = dict(self.compaction_marks())
        wanted = set(keys)
        for row in self._partition_rows(reference):
            key = _mark_key(reference, row)
            if key in wanted:
                settled[key] = _counts(row)
        if "" in wanted:  # the whole-table plan, which has no partition of its own
            rows = self._partition_rows(reference)
            settled[f"{reference}/"] = [
                int(sum(row["file_count"] for row in rows)),
                int(sum(row["record_count"] for row in rows)),
            ]
        self.set_properties({COMPACTION_MARK: json.dumps(settled)})

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
        # Before anything is looked at, and not once per chunk the way a write
        # must not: what another writer has committed since this object loaded
        # the table is invisible to the live set, and a file missing from the
        # live set is a file this deletes. One catalog round trip against
        # listing a whole store is nothing.
        self.refresh()
        expired = self._expirable(retain, older_than)
        report = {"expired": len(expired), "deleted": 0, "bytes": 0}
        if expired and not dry_run:
            with self.iceberg_table.maintenance.expire_snapshots() as expire:
                expire.by_ids(expired)
            self.refresh()
        if not remove_orphans:
            return report
        orphans = self._orphans(orphan_age, metadata=metadata)
        report["deleted"] = len(orphans)
        report["bytes"] = int(sum(size for _, _, size in orphans))
        if not dry_run:
            for filesystem, path, _ in orphans:
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

        **Where** those two directories are is Iceberg's answer, not a guess:
        `write.data.path` and `write.metadata.path` move either of them, often
        to another store entirely, and a table Spark created carries them
        whether or not this code knows. Assuming `<location>/data` made the
        sweep a silent no-op on such a table -- the listing found nothing and
        `allow_not_found` swallowed it.

        The live set is deliberately over-broad: every retained snapshot's
        manifest list and every manifest reachable from it, every entry in the
        metadata log, the statistics the metadata registers, and the current
        metadata pointer -- read from the catalog now, not from whatever this
        object loaded when it was made. A dataset that has been open a while
        has not seen the other writers, and a file missing from the live set is
        a file this deletes: measured, a stale sweeper deleted twelve of
        another writer's files and left the table unreadable.

        Anything younger than `older_than` is spared whether or not it is
        referenced, and that is the **only** protection against a writer
        committing *during* the sweep -- one has files on disk that no snapshot
        mentions yet. Three days by default; lowering it is safe when nothing
        else is writing, and nowhere else.

        Listed through `pyarrow.fs`, like every other file this package touches,
        so an object store is walked by the same handle the reads use -- and
        compared **relative to the directory**, resolved once through that same
        `pyarrow.fs`. Stripping the scheme by hand instead is what made this
        dangerous: `file:/tmp/x`, `abfss://container@account.../x` and a
        Windows drive letter all resolve to something a string split does not
        produce, so every live file fell out of the live set and `cleanup`
        deleted the whole table.
        """
        self.refresh()
        return [(path, size) for _, path, size in self._orphans(older_than, metadata=metadata)]

    def _orphans(
        self, older_than: datetime.timedelta, *, metadata: bool
    ) -> list[tuple[Any, str, int]]:
        """`orphan_files`, each path beside the filesystem that lists it.

        Which is the one that can delete it: `write.data.path` often points at
        another store entirely, and one handle built from the table's location
        would delete against the wrong one.
        """
        table = self.iceberg_table
        cutoff = datetime.datetime.now(datetime.UTC) - older_than
        directories = [(self._data_path(table), self._live_data(table))]
        if metadata:
            directories.append((self._metadata_path(table), self._live_metadata(table)))

        found = []
        for directory, live in directories:
            filesystem, base = resolve(directory)
            bases = (directory.rstrip("/"), base.rstrip("/"), _path_of(directory).rstrip("/"))
            relative = {_relative(path, bases) for path in live}
            selector = pyarrow.fs.FileSelector(base, recursive=True, allow_not_found=True)
            for info in filesystem.get_file_info(selector):
                if info.type != pyarrow.fs.FileType.File:
                    continue
                if _relative(info.path, bases) in relative:
                    continue
                # A Hadoop-style catalog keeps its pointer beside the metadata
                # and nothing inside the metadata names it: reading the table
                # is how you would find out it had been swept.
                if info.base_name == HADOOP_POINTER:
                    continue
                if info.mtime and info.mtime > cutoff:
                    continue
                found.append((filesystem, info.path, info.size))
        return found

    def _data_path(self, table: Any) -> str:
        """Where this table's data files live, as Iceberg decides it."""
        return self._locations(table).data_path

    def _metadata_path(self, table: Any) -> str:
        """Where this table's metadata files live, as Iceberg decides it."""
        return self._locations(table).metadata_path

    def _locations(self, table: Any) -> Any:
        """pyiceberg's own location provider for this table."""
        from pyiceberg.table.locations import load_location_provider

        return load_location_provider(table.location(), table.properties)

    def _live_data(self, table: Any) -> set[str]:
        """Every data and delete file any retained snapshot still holds.

        Walked from the manifests rather than read off `inspect.all_files()`,
        which builds -- per data file -- the column sizes, value counts, null
        counts and a decoded lower *and* upper bound for every field in the
        schema, so that one column of paths can be kept. Measured on 40 columns
        and 80 snapshots: 550 ms against 143, and the gap grows with the column
        count. `_live_metadata` walks the same manifests, so this is the
        module's existing idiom rather than a second way of doing it.
        """
        live = set()
        for _, manifest in _manifests(table):
            for entry in manifest.fetch_manifest_entry(table.io):
                live.add(entry.data_file.file_path)
        return live

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
        live = {table.metadata_location}
        for entry in table.metadata.metadata_log:
            live.add(entry.metadata_file)
        for snapshot, manifest in _manifests(table):
            if snapshot.manifest_list:
                live.add(snapshot.manifest_list)
            live.add(manifest.manifest_path)
        for statistics in (*table.metadata.statistics, *table.metadata.partition_statistics):
            live.add(statistics.statistics_path)
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
        if (
            pyarrow.types.is_floating(values.type)
            and pyarrow.compute.any(pyarrow.compute.is_nan(values)).as_py()
        ):
            # And no literal can name a NaN, which the two branches below
            # disagree about: `In` refuses it (pyiceberg will not build the
            # literal), while `min_max` skips it and hands back a range the
            # stored row falls outside -- so the merge would insert a second
            # copy, and a third next time, without ever raising.
            raise ValueError(
                f"column {column!r} is a merge key and cannot be NaN; "
                "no predicate can name a NaN, so merging on it would duplicate rows"
            )
        distinct = _distinct_under(values, MERGE_IN_LIMIT)
        if distinct is not None:
            if len(distinct) == 0:
                continue
            terms.append(In(column, distinct.to_pylist()))
            continue
        # Neither bound can be null here: the column has rows, no nulls and no
        # NaN, which is everything `min_max` would have skipped.
        bounds = pyarrow.compute.min_max(values).as_py()
        terms.append(
            And(GreaterThanOrEqual(column, bounds["min"]), LessThanOrEqual(column, bounds["max"]))
        )
    if not terms:
        return _always_true()
    return And(*terms) if len(terms) > 1 else terms[0]


def _downcasts_ns() -> bool:
    """Whether Iceberg is configured to accept nanosecond timestamps by rounding.

    Read the way pyiceberg reads it, from the same configuration, because the
    check it guards is pyiceberg's: hard-coding it to False would refuse a
    write the library itself accepts, which is a divergence introduced by the
    very check that exists to remove them.
    """
    from pyiceberg.io.pyarrow import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE
    from pyiceberg.utils.config import Config

    return Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False


def _under_current_names(table: Any, source: Any) -> Any:
    """`source` with its columns named the way the table names them *now*.

    Takes a table or a reader; a reader is renamed batch by batch, so nothing
    is materialised to do it.

    A scan pinned to a ref reads under that snapshot's schema, not the current
    one -- and a rename is metadata-only, so until something else commits, the
    branch head still carries the old names. Matching by name there would
    compare a renamed column against nulls and rewrite every row it read.

    Field ids are what a rename does not change, and pyiceberg puts them on the
    columns it hands back, so they are what the names are recovered from. Top
    level only: that is what a merge joins and compares on.
    """
    current = {field.field_id: field.name for field in table.schema().fields}
    names = []
    for field in source.schema:
        identifier = (field.metadata or {}).get(b"PARQUET:field_id")
        renamed = current.get(int(identifier)) if identifier is not None else None
        names.append(renamed or field.name)
    if names == source.schema.names:
        return source
    if isinstance(source, pyarrow.Table):
        return source.rename_columns(names)
    return (batch.rename_columns(names) for batch in source)


def _distinct_under(values: Any, limit: int) -> Any:
    """The column's distinct values, or None when there are more than `limit`.

    Hashing a whole column to *then* discover it has too many distinct values
    to name is the expensive way to learn nothing: on a million rows `unique`
    costs 50 ms on int64 keys and 115 ms on strings, against 0.3 ms and 13 ms
    for the `min_max` that is all a range needs. So a slice one longer than the
    limit is hashed first -- if that alone already has more distinct values
    than the limit, so does the column, and the full pass is never made.

    Measured on a 400k-row chunk, which is the whole of `_key_ranges`: a
    high-cardinality integer key 27.0 ms -> 0.4 ms, an integer and a string key
    69.9 ms -> 6.3 ms. The probe is a tax only where the `In` form was going to
    win anyway -- an eight-value partition key pays 1.3 ms -> 1.5 ms for it.
    """
    head = pyarrow.compute.unique(values.combine_chunks().slice(0, limit + 1))
    if len(head) > limit:
        return None
    distinct = pyarrow.compute.unique(values.combine_chunks())
    return distinct if len(distinct) <= limit else None


#: Marker columns the joins below carry, named like pyiceberg's reserved pair.
#: A merge key of either name is refused before a join ever runs, with the
#: library's own message -- the joins reach these names first, and would fail
#: on the duplicate column rather than on what is actually wrong.
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
    differs = None
    for name in compare:
        one, other = left.column(name), right.column(name)
        unequal = compute.fill_null(compute.not_equal(one, other), False)
        # `not_equal` is null when either side is: a null against a value is a
        # difference, two nulls are not.
        only_one_null = compute.xor(compute.is_null(one), compute.is_null(other))
        column = compute.or_(unequal, only_one_null)
        # The first column *is* the running answer -- seeding one with a
        # Python list of falses costs 14 ms a million rows, and buys nothing.
        differs = column if differs is None else compute.or_(differs, column)
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


def _renamed(reader: Any, names: dict[str, str]) -> Any:
    """`reader`'s batches under the names the caller asked for them by.

    Batch by batch, so nothing is materialised: a stream stays a stream. The
    mapping is `{what the scan called it: what the caller called it}`, which
    for anything but a pinned read across a rename is the identity.
    """
    if not names or all(stored == asked for stored, asked in names.items()):
        return reader
    return (
        batch.rename_columns([names.get(name, name) for name in batch.schema.names])
        for batch in reader
    )


def _manifests(table: Any) -> Iterator[tuple[Any, Any]]:
    """`(snapshot, manifest)` for every manifest any retained snapshot reaches.

    Deduped on the manifest path: a manifest that two snapshots share is read
    once, which is most of them on a table written by a stream.
    """
    seen = set()
    for snapshot in table.snapshots():
        for manifest in snapshot.manifests(table.io):
            if manifest.manifest_path in seen:
                continue
            seen.add(manifest.manifest_path)
            yield snapshot, manifest


def _mark_key(branch: str, row: Any) -> str:
    """A stable name for one partition of one branch, for `COMPACTION_MARK`."""
    partition = row.get("partition") or {}
    if not partition:
        return f"{branch}/"
    values = ",".join(f"{name}={partition[name]!r}" for name in sorted(partition))
    return f"{branch}/{values}"


def _counts(row: Any) -> list[int]:
    """What a partition holds: `[file count, record count]`.

    The pair `COMPACTION_MARK` compares. Both, because either alone misses a
    change: rows can land without the file count moving once, and a rewrite
    changes files without changing rows.
    """
    return [int(row["file_count"]), int(row["record_count"])]


def _partition_filter(partition: Any, identities: Sequence[tuple[str, str]]) -> Any:
    """The predicate one partition *is*, as an expression rather than a string.

    A string would have to be parsed back, and an apostrophe in a value or a
    timestamp partition makes that parse fail -- on ordinary values, in the
    case `compaction_plan` says it handles. A null value is `IsNull` and not a
    dropped term: dropping it left a predicate that matched every other
    partition too.
    """
    from pyiceberg.expressions import And, EqualTo, IsNull

    terms = [
        IsNull(column) if partition.get(name) is None else EqualTo(column, partition[name])
        for name, column in identities
    ]
    if not terms:
        return None
    return functools.reduce(And, terms)


def _always_true() -> Any:
    from pyiceberg.expressions import AlwaysTrue

    return AlwaysTrue()


def _literal(value: Any) -> str:
    """One partition value as its filter literal."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return f"'{value}'"


def _path_of(location: str) -> str:
    """A file location without its scheme, as one of the spellings to try.

    Never on its own as "the path `pyarrow.fs` would use": it is not.
    `file:/tmp/x` keeps its scheme, `abfss://container@account.dfs.../x`
    resolves to `container/x`, and a Windows drive letter loses a leading
    slash. `_relative` tries this beside the ones `resolve` produces.
    """
    return location.split("://", 1)[-1]


def _relative(path: str, bases: Sequence[str]) -> str:
    """`path` under whichever of `bases` it is spelled against, tail only.

    The one comparison that survives a store naming its files differently from
    the URI the metadata records: both sides are reduced to what follows the
    directory they are in, so how the directory itself is spelled stops
    mattering.
    """
    for base in sorted(bases, key=len, reverse=True):
        if base and path.startswith(base):
            return path[len(base) :].lstrip("/")
    return path.lstrip("/")


def _cutoff_ms(older_than: datetime.datetime | datetime.timedelta | None) -> int | None:
    """The instant a snapshot must predate to be expirable, in Iceberg's millis."""
    if older_than is None:
        return None
    if isinstance(older_than, datetime.timedelta):
        older_than = datetime.datetime.now(datetime.UTC) - older_than
    return int(older_than.timestamp() * 1000)
