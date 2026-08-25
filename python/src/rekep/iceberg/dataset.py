"""One Iceberg table as a dataset, with the maintenance it needs to stay fast."""

from __future__ import annotations

import dataclasses
import datetime
import functools
import itertools
import json
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from functools import cached_property
from typing import Any

import pyarrow
import pyarrow.fs

from rekep.dataset import (
    SOURCE_INDEX,
    TARGET_INDEX,
    Dataset,
    anti_join,
    arrow_chunks,
    first_rows,
    keys_of,
    normalised_keys,
    semi_join,
)
from rekep.fields import Field, StructField, arrays
from rekep.filesystems import resolve
from rekep.iceberg.catalog import IcebergCatalog
from rekep.iceberg.fields import metrics_for

#: The branch a read or a write lands on when nothing names one -- pyiceberg's
#: own default, spelled out here so the two cannot disagree about it.
MAIN = "main"

#: How long a file must have been unreferenced before `cleanup` deletes it. A
#: writer that is committing right now has files on disk that no snapshot
#: mentions yet; deleting those would break it, so orphans have to be old.
ORPHAN_AGE = datetime.timedelta(days=3)

#: No grace period at all: sweep whatever is unreferenced, however new. What a
#: caller asks for when nothing else is writing -- a maintenance window, or a
#: test -- and it has to be taken literally, because the alternative compares
#: this machine's clock against the store's and spares a file on the runs where
#: they disagree.
_NO_GRACE = datetime.timedelta(0)

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

#: How many ranges a key column past `MERGE_IN_LIMIT` is described by. One was
#: the answer before, and one prunes nothing on a chunk whose keys sit in a few
#: bands of a wide range -- a backfill, or a replay of two days into a month.
#: Eight because the predicate stays trivial next to the exact filter it is
#: ANDed with, and because the gaps past the eighth widest are the ones that
#: were not going to skip a file anyway.
MERGE_RANGE_BANDS = 8

#: How many rows a group of the merge key must average before the delete
#: filter is grouped at all. Every group costs a term of its own to build, so
#: below a few rows each the grouping is slower than the tree it saves --
#: measured on 5,000 rows, groups of 500 build in 0.09x the library's time,
#: groups of 50 in 0.23x, and groups of 1.6 in 4.4x.
MERGE_GROUP_GAIN = 8

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

#: What `maybe_optimize` calls fragmented: this many manifests or snapshots on
#: the branch, or this many files the compaction planner would rewrite. Loose
#: on purpose -- `optimize` is cheap to run and expensive to need, and the
#: signals cost no store round trips beyond what the write already cached --
#: but not zero, because a table of two commits does not need a maintenance
#: pass appended to every stream.
AUTO_OPTIMIZE_MANIFESTS = 8
AUTO_OPTIMIZE_SNAPSHOTS = 16
AUTO_OPTIMIZE_FILES = 16

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
    """An Iceberg table, read and written as Arrow through pyiceberg."""

    @classmethod
    @functools.cache
    def into_kind(cls) -> str:
        """Document kind registered with `Dataset`."""
        return "iceberg"

    #: Table identifier, `namespace.name`.
    name: str

    #: Catalog name pyiceberg loads, with `properties` as its configuration.
    catalog: str = "default"
    properties: dict[str, str] = dataclasses.field(default_factory=dict)

    #: The declared shape. None means "whatever the table says".
    field: StructField | None = None

    #: Branch reads and writes use unless a call names another; None is `main`.
    branch: str | None = None

    #: Rows one commit carries when a write does not name a size. Iceberg lands
    #: a file and a snapshot per commit, so this is the knob that decides how
    #: much a later scan has to plan; pass `commit_row_size=0` to a write for
    #: one commit over the whole stream.
    commit_row_size: int | None = DEFAULT_COMMIT_ROW_SIZE

    #: Columns each chunk is sorted by before it is written. None means the
    #: shape's own `sort_key()` declarations, because a table that records a
    #: sort order and then writes rows in another one has recorded a wish: the
    #: order is what makes a filter skip row groups, and measured, a top-5%
    #: filter over one 600k-row commit took 214 ms unsorted and 22 ms sorted,
    #: the same single file either way. An explicit `[]` opts out.
    sort_by: Sequence[str] | None = None

    #: Whether a merge plans its own scan instead of handing the whole chunk to
    #: `Table.upsert`. Same algorithm, same rows -- and worth two orders of
    #: magnitude on a stream of new or unchanged keys, about 2x when the rows
    #: genuinely change. `merge_arrow_table` says why, and where it is stricter.
    plan_merges: bool = True

    #: Whether a table created here gets `COMMIT_PROPERTIES`. The defaults are
    #: Iceberg's, and Iceberg's defaults are not tuned for a stream.
    optimize_commits: bool = True

    #: Whether a write stream ends by asking `maybe_optimize` whether the
    #: table has fragmented enough to be worth an `optimize` -- the check is
    #: metadata already in memory, the run only happens past the
    #: `AUTO_OPTIMIZE_*` thresholds. Off by default for one reason: `optimize`
    #: expires snapshots, and whether yesterday's snapshots are still wanted
    #: is not something a writer can decide for its readers.
    auto_optimize: bool = False

    #: Only used when the table is created: where it lives and what it carries.
    location: str | None = None
    table_properties: dict[str, str] = dataclasses.field(default_factory=dict)

    # -- the table ----------------------------------------------------------

    @cached_property
    def store(self) -> IcebergCatalog:
        """The catalog this table lives in."""
        store = IcebergCatalog(name=self.catalog, properties=self.properties)
        self.__dict__["_owns_store"] = True
        return store

    def close(self) -> None:
        """Release this handle's owned catalog without loading it."""
        self.__dict__.pop("iceberg_table", None)
        self.__dict__.pop("table_field", None)
        self.__dict__.pop("_insert_upper", None)
        store = self.__dict__.pop("store", None)
        owns_store = self.__dict__.pop("_owns_store", False)
        if store is not None and owns_store:
            store.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

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

    @property
    def records(self) -> int | None:
        """How many rows the current snapshot holds, from its summary, or None."""
        if not self.exists:
            return 0
        snapshot = self.iceberg_table.current_snapshot()
        if snapshot is None:
            return 0
        try:
            return int(snapshot.summary["total-records"])
        except (AttributeError, KeyError, TypeError, ValueError):
            return None

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
        defaults = {**(COMMIT_PROPERTIES if self.optimize_commits else {}), **metrics_for(field)}
        table = self.iceberg_catalog.create_table(
            self.name,
            schema=schema,
            location=kwargs.pop("location", self.location),
            partition_spec=field.into_iceberg_partition_spec(schema),
            # Declared at creation, because Iceberg records a sort order on the
            # table and every writer through it honours it -- a shape that says
            # how it is read is a shape that says how it should be laid out.
            sort_order=field.into_iceberg_sort_order(schema, self.sort_by),
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
            if self.field is None:
                raise ValueError(
                    f"{self.name!r} does not exist and this dataset declares no shape; "
                    "give it `field=`, or create it with create_with(...)"
                )
            self.create_with_field(self.field)
        return self.iceberg_table

    def refresh(self) -> IcebergDataset:
        """Drop what was loaded, so the next call sees other writers' commits."""
        for view in ("iceberg_table", "table_field", "_insert_upper"):
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
        return self.field if self.field is not None else self.table_field

    def derived_columns(self) -> dict[str, tuple[str, ...]]:
        """Columns the declared shape says are a function of other columns.

        Read from the declaration and not from the table: Iceberg records a
        partition spec, not why a column holds what it does, so a shape read
        back from a table says nothing here -- and saying nothing costs a merge
        pruning, never correctness.
        """
        return self.field.derived_keys() if self.field is not None else {}

    def add_fields(self, source: Any = None, *, dry_run: bool = False) -> list[str]:
        """Add the columns `source` has and the table lacks; skip when there are none."""
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
        if self.field is not None:
            # The declared shape *is* what writes cast onto, so evolving the
            # table without it would drop the new columns at the next write.
            self.field = target
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
        order_by: str | Sequence[str] | None = None,
    ) -> pyarrow.RecordBatchReader:
        """Stream the table, optionally merging files on lexicographic sort keys.

        A table that was never written reads as no rows, not as a failure: on
        the first interval of a fresh catalog every stage reads an upstream
        that its own upstream has not created yet, and "nothing there" is the
        true answer to that -- so it is answered once here rather than by an
        `exists` guard at each call site. A source root under `TextFiles`
        refuses instead, because nothing in the pipeline creates one.
        """
        ordering = (order_by,) if isinstance(order_by, str) else tuple(order_by or ())
        reference = self._reference(branch, snapshot_id)
        target = None if schema is None else self.target_field(schema)
        if not self.exists:
            return self._empty_reader(target, columns)
        table = self.iceberg_table
        # Pinned *before* the projection is chosen: a scan on a ref or a
        # snapshot id projects under that snapshot's schema, so which names it
        # will answer to is not known until it is pinned.
        scan = table.scan(
            snapshot_id=snapshot_id,
            # A scan limit is applied independently while files are merged.
            # Leave it global when ordering, then cut the merged reader once.
            limit=None if ordering else limit,
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
        projected = {field.name for field in scan.projection().fields}
        missing = [name for name in ordering if name not in projected]
        if missing:
            raise ValueError(
                f"order_by={order_by!r} is not projected; include {missing!r} in "
                "`columns` or `schema`"
            )
        reader = (
            _ordered_reader(scan, scan.plan_files(), ordering)
            if ordering
            else _limited_reader(scan, limit)
        )
        if limit is not None:
            reader = _reader_limit(reader, limit)
        if target is None:
            return reader
        return target.cast_arrow_reader(_renamed(reader, found))

    def _empty_reader(
        self, target: StructField | None, columns: Sequence[str] | None
    ) -> pyarrow.RecordBatchReader:
        """No rows, under the shape the caller asked to read.

        With neither a schema nor a declared shape there is nothing to answer
        with, and loading the absent table raises what that deserves.
        """
        if target is None:
            target = self.target_field()
        arrow = target.into_arrow_schema()
        if columns:
            arrow = pyarrow.schema([arrow.field(name) for name in columns])
        return pyarrow.RecordBatchReader.from_batches(arrow, iter(()))

    def _reference(self, branch: str | None, snapshot_id: int | None) -> str | None:
        """The branch a read follows, or None when a snapshot id decides instead."""
        if snapshot_id is None:
            return branch or self.branch
        if branch is not None:
            raise ValueError(
                f"snapshot_id={snapshot_id} and branch={branch!r} name two different states; "
                "a snapshot id is already exact, so pass one or the other"
            )
        return None

    def _selected(self, target: StructField, scan: Any) -> dict[str, str]:
        """`{the scan's name: the target's name}` for every column it can fill."""
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

    def overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] = True,
        commit_row_size: int | None = None,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Replaces the rows whose keys match and inserts the rest, one commit per chunk."""
        # An upsert or an unconditional append can put a key beyond an
        # insert-only writer's known maximum. Its cheap monotonic proof is no
        # longer complete after either operation.
        self.__dict__.pop("_insert_upper", None)
        self.get_or_create_table()
        reader = self.target_field(schema).cast_arrow_reader(source)
        join = self.merge_columns(merge_by)
        if not join:
            raise ValueError(
                f"merge_by={merge_by!r} names nothing to match on, and an overwrite "
                "replaces the rows whose keys match -- pass True for the primary key "
                "or the columns to match on, or use append_arrow_* to add rows blindly"
            )
        reference = branch or self.branch or MAIN
        rows = self.commit_row_size if commit_row_size is None else commit_row_size
        for chunk in arrow_chunks(reader, rows):
            chunk = self.sorted(chunk)
            if self.plan_merges:
                self.merge_arrow_table(chunk, join, branch=reference, properties=properties)
            else:
                self.iceberg_table.upsert(
                    chunk,
                    join_cols=join,
                    branch=reference,
                    snapshot_properties=properties or {},
                )
        if self.auto_optimize:
            self.maybe_optimize(branch=branch)

    def _append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        commit_row_size: int | None = None,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Add every row, matching nothing: one Iceberg append per chunk."""
        self.__dict__.pop("_insert_upper", None)
        table = self.get_or_create_table()
        reader = self.target_field(schema).cast_arrow_reader(source)
        reference = branch or self.branch or MAIN
        rows = self.commit_row_size if commit_row_size is None else commit_row_size
        snapshot = properties or {}
        for chunk in arrow_chunks(reader, rows):
            table.append(self.sorted(chunk), snapshot_properties=snapshot, branch=reference)
        if self.auto_optimize:
            self.maybe_optimize(branch=branch)

    def merge_arrow_table(
        self,
        chunk: pyarrow.Table,
        merge_by: bool | Sequence[str] | None = True,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> tuple[int, int]:
        """One chunk merged into the table: `(rows updated, rows inserted)`."""
        from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible
        from pyiceberg.table import upsert_util

        self.__dict__.pop("_insert_upper", None)
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
        chunk = normalised_keys(chunk, join)
        shape = Field.from_(chunk.schema)
        reference = branch or self.branch or MAIN
        derived = self.derived_columns()
        scan = table.scan(row_filter=_key_ranges(chunk, join, derived))
        if reference in table.refs():
            scan = scan.use_ref(reference)
        # Planned once and read from that plan: `to_arrow_batch_reader` plans
        # again on its own, and a streaming merge pays planning per chunk.
        tasks = list(scan.plan_files())
        if not tasks:
            # No stored file overlaps the chunk's key ranges, so no stored row
            # can match: the merge *is* an append, with nothing read and
            # nothing to compare. A stream of new keys -- the log-ingest case
            # this exists for -- lands every chunk here.
            table.append(chunk, snapshot_properties=properties or {}, branch=reference)
            return 0, chunk.num_rows
        # The batch reader, not `to_arrow()`: the two read paths disagree about
        # string widths -- `to_arrow()` hands back `string` where the reader
        # hands back the `large_string` the table itself reports -- and this one
        # streams, which is what a merge of an arbitrary chunk needs.
        matched = _under_current_names(table, _planned_reader(scan, tasks).read_all())
        if matched.num_rows == 0:
            # Files overlapped the ranges but held none of the keys: the same
            # append, minus the casts and joins that would narrow nothing.
            table.append(chunk, snapshot_properties=properties or {}, branch=reference)
            return 0, chunk.num_rows
        matched = shape.cast_arrow_table(matched)
        # The scan filter is a *superset* -- a range covers stored keys the
        # chunk never mentions -- so the rows it brought back are narrowed to
        # the ones the chunk actually references before anything looks at them.
        # Without this, a duplicate key stored anywhere inside the chunk's key
        # range aborts a merge that has nothing to do with it.
        matched = semi_join(matched, chunk, join)
        if matched.num_rows == 0:
            # The range overlapped stored rows, but the exact keys did not.
            # There is nothing left to compare or anti-join: this is an append.
            table.append(chunk, snapshot_properties=properties or {}, branch=reference)
            return 0, chunk.num_rows

        # An Arrow join hands back nullable columns whatever it was given, and
        # pyiceberg checks a write against the table's own requiredness -- so
        # both halves go back onto the stored shape before they are committed.
        updates = shape.cast_arrow_table(_changed(chunk, matched, join))
        inserts = shape.cast_arrow_table(anti_join(chunk, matched, join))
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
                        _match_filter(updates, join),
                        _key_ranges(updates, join, derived),
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

    def append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> int:
        """Append a stream, inserting only the keys the table does not hold yet."""
        join = self.merge_columns(merge_by)
        if not join:
            inserted = 0

            def counted() -> Iterator[pyarrow.RecordBatch]:
                nonlocal inserted
                for batch in source:
                    inserted += batch.num_rows
                    yield batch

            self._append_arrow_reader(
                counted(), schema, commit_row_size, branch=branch, properties=properties
            )
            return inserted
        self.get_or_create_table()
        reader = self.target_field(schema).cast_arrow_reader(source)
        rows = self.commit_row_size if commit_row_size is None else commit_row_size
        inserted = 0
        for chunk in arrow_chunks(reader, rows):
            inserted += self.insert_arrow_table(
                self.sorted(chunk), join, branch=branch, properties=properties
            )
        if self.auto_optimize:
            self.maybe_optimize(branch=branch)
        return inserted

    def insert_arrow_table(
        self,
        chunk: pyarrow.Table,
        merge_by: bool | Sequence[str] | None = True,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> int:
        """One chunk appended where no stored row matches: rows inserted."""
        join = self.merge_columns(merge_by)
        if not join:
            raise ValueError("insert_arrow_table needs columns to merge on")
        if SOURCE_INDEX in join or TARGET_INDEX in join:
            raise ValueError(
                f"{SOURCE_INDEX} and {TARGET_INDEX} are reserved for joining DataFrames"
            )
        table = self.get_or_create_table()
        if chunk.num_rows == 0:
            return 0
        chunk = first_rows(normalised_keys(chunk, join), join)
        reference = branch or self.branch or MAIN
        # `_key_ranges` raises on a null or NaN key before the scan is even
        # built, which is the same refusal a merge makes.
        row_filter = _key_ranges(chunk, join, self.derived_columns())
        span = self._insert_span(chunk, join, reference)
        head = table.current_snapshot()
        empty = reference == MAIN and head is None
        snapshot_id = head.snapshot_id if head is not None else None
        if span is not None and self._strictly_after_inserted(span, empty, snapshot_id):
            # Once this object has filled an empty table, the upper bound is
            # exact. A later chunk strictly above it cannot match a stored key,
            # so planning manifests and files can only rediscover that fact.
            table.append(chunk, snapshot_properties=properties or {}, branch=reference)
            self._remember_inserted(span, table, establish=empty)
            return chunk.num_rows
        scan = table.scan(row_filter=row_filter)
        if reference in table.refs():
            scan = scan.use_ref(reference)
        # Keys only -- an append never compares a non-key column, so it never
        # reads one -- but chosen by field **id** and not by name. A scan
        # pinned to a branch reads under *that* snapshot's schema, so a key
        # renamed on main since the branch was cut is not a name it answers
        # to: naming it raised `Could not find column`, on a branch every other
        # verb here reads and writes happily. `_under_current_names` puts the
        # names back on the way out.
        keys = Field.from_(pyarrow.schema([chunk.schema.field(name) for name in join]))
        wanted = self._selected(keys, scan)
        if set(wanted.values()) != set(join):
            # That snapshot does not carry every key column -- one added since
            # the branch was cut -- so no row on it can match a key, and every
            # row of the chunk is new.
            table.append(chunk, snapshot_properties=properties or {}, branch=reference)
            return chunk.num_rows
        scan = scan.select(*wanted)
        # Planned once, like a merge: no overlapping file, or overlapping files
        # with none of the keys, means every row is new and nothing needs the
        # anti-join -- the replayed stream pays a plan, the fresh one an append.
        tasks = list(scan.plan_files())
        matched = (
            _under_current_names(table, _planned_reader(scan, tasks).read_all()) if tasks else None
        )
        if matched is None or matched.num_rows == 0:
            table.append(chunk, snapshot_properties=properties or {}, branch=reference)
            self._remember_inserted(span, table, establish=empty)
            return chunk.num_rows
        # Onto the chunk's own key columns, so the anti-join can run: a scan
        # hands back `large_string` where the chunk carries `string`.
        fresh = anti_join(chunk, keys.cast_arrow_table(matched), join)
        if fresh.num_rows:
            table.append(fresh, snapshot_properties=properties or {}, branch=reference)
            self._remember_inserted(span, table)
        return fresh.num_rows

    def _insert_span(
        self, chunk: pyarrow.Table, join: Sequence[str], reference: str
    ) -> tuple[tuple[str, tuple[str, ...], str], pyarrow.Scalar, pyarrow.Scalar] | None:
        """The first declared sort key's `(cache key, minimum, maximum)`."""
        ordered = self.sort_columns()
        if not ordered or ordered[0] not in join:
            return None
        name = ordered[0]
        try:
            bounds = pyarrow.compute.min_max(chunk.column(name))
        except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError):
            return None
        lower, upper = bounds["min"], bounds["max"]
        if not lower.is_valid or not upper.is_valid:
            return None
        return (reference, tuple(join), name), lower, upper

    def _strictly_after_inserted(
        self,
        span: tuple[tuple[str, tuple[str, ...], str], pyarrow.Scalar, pyarrow.Scalar],
        empty: bool,
        snapshot_id: int | None,
    ) -> bool:
        """Whether `span` is provably disjoint from every inserted key."""
        key, lower, _ = span
        remembered = self.__dict__.get("_insert_upper", {}).get(key)
        if remembered is None:
            return empty
        upper, bounded_snapshot = remembered
        if bounded_snapshot != snapshot_id:
            bounds = self.__dict__["_insert_upper"]
            del bounds[key]
            if not bounds:
                del self.__dict__["_insert_upper"]
            return False
        return bool(pyarrow.compute.greater(lower, upper).as_py())

    def _remember_inserted(
        self,
        span: tuple[tuple[str, tuple[str, ...], str], pyarrow.Scalar, pyarrow.Scalar] | None,
        table: Any,
        *,
        establish: bool = False,
    ) -> None:
        """Advance an exact insert-only upper bound; never infer one mid-table."""
        if span is None:
            return
        key, _, upper = span
        remembered = self.__dict__.get("_insert_upper")
        if remembered is None:
            if not establish:
                return
            remembered = self.__dict__.setdefault("_insert_upper", {})
        previous = remembered.get(key)
        if previous is None and not establish:
            return
        bounded = previous[0] if previous is not None else None
        if bounded is None or pyarrow.compute.greater(upper, bounded).as_py():
            bounded = upper
        head = table.current_snapshot()
        remembered[key] = (bounded, head.snapshot_id if head is not None else None)

    def sorted(self, chunk: pyarrow.Table) -> pyarrow.Table:
        """`chunk` in `sort_by` order, or exactly as it came when nothing says."""
        names = self.sort_columns()
        if not names or chunk.num_rows < 2 or _in_sort_order(chunk, names):
            return chunk
        return chunk.sort_by([(name, "ascending") for name in names])

    def sort_columns(self) -> list[str]:
        """Columns a chunk is sorted by: what was asked for, or what is declared."""
        if self.sort_by is not None:
            return list(self.sort_by)
        shape = self.field
        return list(shape.sort_keys()) if shape is not None else []

    def delete(self, row_filter: Any = None, *, branch: str | None = None) -> None:
        """Delete the rows a filter matches, in one commit."""
        self.__dict__.pop("_insert_upper", None)
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
        """What a read would touch, without reading it: files, rows, bytes."""
        table = self.iceberg_table
        planned = self._planned(table, row_filter, columns, snapshot_id, branch)
        # With no filter the two plans *are* the same plan, and planning is what
        # this call costs: on 40 files, doing it twice took 17.1 ms against 8.6,
        # for a `skipped` that is zero by construction.
        #
        # With one, the second plan is only there for the *count* of files the
        # unfiltered scan would touch -- and Iceberg records that per snapshot,
        # so it is already in the metadata this object holds. Measured on 17
        # files: 15.6 ms for the pair against 3.7 ms for the filtered plan
        # alone. A snapshot whose summary does not say sends it back to the
        # planner, which is what the number cost before.
        total = planned["files"]
        if row_filter is not None:
            stored = _stored_files(self._snapshot(table, snapshot_id, branch))
            total = (
                stored
                if stored is not None
                else self._planned(table, None, columns, snapshot_id, branch)["files"]
            )
        return {**planned, "total_files": total, "skipped": total - planned["files"]}

    def _snapshot(self, table: Any, snapshot_id: int | None, branch: str | None) -> Any:
        """The snapshot a read of that state would be answered from.

        The same choice `_planned` makes with `use_ref`: a snapshot id names one
        exactly, a branch names its head, and neither means whatever the table
        currently points at.
        """
        if snapshot_id is not None:
            return table.metadata.snapshot_by_id(snapshot_id)
        reference = self._reference(branch, None)
        if not reference:
            return table.current_snapshot()
        head = table.refs().get(reference)
        return table.metadata.snapshot_by_id(head.snapshot_id) if head is not None else None

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
        """`(row filter, file count)` for every part of the table worth rewriting."""
        return [(part, count) for _, part, count in self._plan_rows(min_files, branch)]

    def _plan_rows(self, min_files: int, branch: str | None) -> list[tuple[str, Any, int]]:
        """`compaction_plan`, with the mark key each part is recorded under."""
        table = self.iceberg_table
        reference = branch or self.branch or MAIN
        rows = self._partition_rows(reference)
        if not rows:
            return []
        marks = self.compaction_marks()

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
            #
            # And it settles as a whole, against every partition's counts added
            # up. Asking the *per-partition* question here -- which is what
            # this did -- compared marks that only the identity branch ever
            # writes, so a `day` or `bucket[16]` partition matched none of
            # them, recorded none of them, and had its whole table read back
            # and rewritten on every single `optimize`. Measured on four
            # commits over four days: 16 files rewritten, then 4, then 4,
            # forever, with `compaction_marks()` empty throughout. An
            # unpartitioned table settled only because its one partition is
            # empty and happens to share this key.
            key = _mark_key(reference, None)
            whole = _totals(rows)
            if marks.get(key) == whole:
                return []
            return [(key, None, whole[0])] if whole[0] >= min_files else []

        plan: list[tuple[str, Any, int]] = []
        for row in rows:
            key = _mark_key(reference, row.get("partition"))
            if marks.get(key) == _counts(row):
                continue
            count = int(row["file_count"])
            if count < min_files:
                continue
            plan.append((key, _partition_filter(row["partition"], identities), count))
        return plan

    def _partition_rows(self, reference: str) -> list[dict]:
        """One row per partition of that branch's head, as Iceberg reports them."""
        table = self.iceberg_table
        key = (reference, table.metadata_location)
        held = self.__dict__.get("_partitions")
        if held is not None and held[0] == key:
            return held[1]
        head = table.refs().get(reference)
        found = table.inspect.partitions(snapshot_id=head.snapshot_id if head is not None else None)
        rows = found.to_pylist() if found.num_rows else []
        self.__dict__["_partitions"] = (key, rows)
        return rows

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
        """Rewrite fragmented parts of the table, one commit each."""
        if target_file_size:
            self.set_properties({TARGET_FILE_SIZE: str(target_file_size)})
        reference = branch or self.branch or MAIN
        plan = (
            # What that filter's own scan plans, not the whole table's files:
            # counting every manifest to report a number about one partition is
            # both slower and wrong. `_planned` and not `scan_plan`, which
            # plans the *unfiltered* scan as well to report what the filter
            # skipped -- a second walk of every manifest for a number this
            # throws away.
            [
                (
                    "",
                    row_filter,
                    self._planned(self.iceberg_table, row_filter, None, None, branch)["files"],
                )
            ]
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
        are whatever Iceberg now reports. Read **once**: the whole-branch mark
        is those same rows added up, and asking Iceberg for them a second time
        was a second walk of every manifest for an answer already in hand.
        """
        settled = dict(self.compaction_marks())
        wanted = set(keys)
        rows = self._partition_rows(reference)
        for row in rows:
            key = _mark_key(reference, row.get("partition"))
            if key in wanted:
                settled[key] = _counts(row)
        whole = _mark_key(reference, None)
        if whole in wanted:
            # The plan that can only address the table as a whole -- no
            # partitioning, or transforms that hide which rows are where.
            settled[whole] = _totals(rows)
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
        """Expire old snapshots, then delete the files they stranded."""
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
            # No `refresh()`: expiry commits on the table object this holds and
            # updates it in place -- the snapshots are gone and
            # `metadata_location` has moved before this line. Reloading would
            # be a catalog round trip to learn what we just did.
        if not remove_orphans:
            return report
        orphans = self._orphans(orphan_age, metadata=metadata)
        report["deleted"] = len(orphans)
        report["bytes"] = int(sum(size for *_, size in orphans))
        if not dry_run:
            self._sweep(orphans)
        return report

    def orphan_files(
        self, older_than: datetime.timedelta = ORPHAN_AGE, *, metadata: bool = True
    ) -> list[tuple[str, int]]:
        """Files under the table that nothing live references any more."""
        self.refresh()
        return [(path, size) for _, path, _, size in self._orphans(older_than, metadata=metadata)]

    def _orphans(
        self, older_than: datetime.timedelta, *, metadata: bool
    ) -> list[tuple[Any, str, str, int]]:
        """`orphan_files`, as `(filesystem, path, location, size)`."""
        table = self.iceberg_table
        cutoff = datetime.datetime.now(datetime.UTC) - older_than
        # **One** live set, against **every** listing. The two halves used to
        # guard only their own directory, which holds exactly as long as the
        # two directories are disjoint -- and `write.data.path` is an arbitrary
        # location, so they need not be. Point it at the table root, which is
        # what a table written flat does, and the data listing walks
        # `metadata/` too: measured, ten orphans reported and all ten of them
        # the current pointer, the manifest lists and the manifests. `cleanup`
        # would have deleted the table. A file is live when *anything* live
        # names it, never when the directory it happens to sit under does.
        data, files = self._live(table)
        live = data | files
        directories = [self._data_path(table)]
        if metadata:
            directories.append(self._metadata_path(table))

        found: dict[str, tuple[Any, str, str, int]] = {}
        for directory in directories:
            filesystem, base = resolve(directory)
            bases = (directory.rstrip("/"), base.rstrip("/"), _path_of(directory).rstrip("/"))
            # Reduced against *these* bases, which is what makes a live file in
            # another directory comparable at all: a metadata location under a
            # data directory that contains it comes back as `metadata/x.avro`,
            # and so does the listing's own path for it.
            relative = set()
            #: Live files this directory's spellings cannot reduce -- a
            #: location recorded as `file:/w/x` under a `file:///w` directory,
            #: which `add_files` produces and an `s3a://` file under an `s3://`
            #: table does too. They are held by base name instead, which is
            #: weaker and is the right way to be wrong: every name Iceberg
            #: mints carries a UUID, so a false match is a file left behind
            #: rather than a live one deleted.
            by_name = set()
            for path in live:
                reduced = _relative(path, bases)
                (relative if reduced is not None else by_name).add(
                    reduced if reduced is not None else path.rsplit("/", 1)[-1]
                )
            selector = pyarrow.fs.FileSelector(base, recursive=True, allow_not_found=True)
            for info in filesystem.get_file_info(selector):
                if info.type != pyarrow.fs.FileType.File:
                    continue
                name = _relative(info.path, bases)
                # None cannot happen for a path this listing returned -- it came
                # from `base` -- and if it ever did, not deleting is the answer.
                if name is None or name in relative or info.base_name in by_name:
                    continue
                # A Hadoop-style catalog keeps its pointer beside the metadata
                # and nothing inside the metadata names it: reading the table
                # is how you would find out it had been swept.
                if info.base_name == HADOOP_POINTER:
                    continue
                # A positive age is a **grace period**, for the one hazard a
                # sweep cannot otherwise see: a writer with files on disk that
                # no snapshot names yet. Zero says there is no such writer, and
                # it has to mean it -- a file written a moment ago can carry an
                # mtime a moment in the *future*, because a filesystem stamps
                # from its own clock and the two need not agree. Comparing
                # anyway spared a file the caller had just asked to have taken,
                # on whichever run the two clocks happened to disagree.
                if older_than > _NO_GRACE and info.mtime and info.mtime > cutoff:
                    continue
                # Keyed by path, because one nested directory inside another is
                # listed under both and a file deleted twice raises the second
                # time -- which would abort the sweep and lose its report.
                found.setdefault(
                    info.path, (filesystem, info.path, f"{directory.rstrip('/')}/{name}", info.size)
                )
        return list(found.values())

    def _sweep(self, orphans: Sequence[tuple[Any, str, str, int]]) -> None:
        """Delete what the sweep found -- from the store, and from the cache."""
        # At the point of use, like every other pyiceberg import here: the
        # module has to import without the extra installed.
        from rekep.iceberg.fileio import CONTENT_CACHE

        for filesystem, path, location, _ in orphans:
            CONTENT_CACHE.evict(location)
            try:
                filesystem.delete_file(path)
            except FileNotFoundError:
                # Already gone -- another sweeper reached it between the
                # listing and here. That is the outcome this wanted, and
                # raising would abandon every orphan after it and throw away
                # the report of the ones before.
                continue

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

    def _live(self, table: Any) -> tuple[set[str], set[str]]:
        """`(data files, metadata files)` nothing may delete, from **one** walk."""
        data: set[str] = set()
        files: set[str] = {table.metadata_location}
        files.update(entry.metadata_file for entry in table.metadata.metadata_log)
        files.update(
            statistics.statistics_path
            for statistics in (*table.metadata.statistics, *table.metadata.partition_statistics)
        )
        files.update(
            snapshot.manifest_list for snapshot in table.snapshots() if snapshot.manifest_list
        )
        for _, manifest in _manifests(table):
            files.add(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(table.io):
                data.add(entry.data_file.file_path)
        return data, files

    def maybe_optimize(
        self, *, min_files: int = 2, branch: str | None = None, **kwargs: Any
    ) -> dict[str, int] | None:
        """`optimize`, but only once cheap signals say the table needs it."""
        table = self.iceberg_table
        reference = branch or self.branch or MAIN
        head = table.refs().get(reference)
        if head is None:
            return None
        snapshot = table.metadata.snapshot_by_id(head.snapshot_id)
        manifests = len(snapshot.manifests(table.io)) if snapshot is not None else 0
        fragmented = (
            len(table.metadata.snapshots) >= AUTO_OPTIMIZE_SNAPSHOTS
            or manifests >= AUTO_OPTIMIZE_MANIFESTS
            or (
                # None is "the summary does not say", and the only safe reading
                # of that is to ask the planner after all.
                (_stored_files(snapshot) or AUTO_OPTIMIZE_FILES) >= AUTO_OPTIMIZE_FILES
                and sum(count for _, _, count in self._plan_rows(min_files, branch))
                >= AUTO_OPTIMIZE_FILES
            )
        )
        if not fragmented:
            return None
        return self.optimize(min_files=min_files, branch=branch, **kwargs)

    def optimize(
        self,
        *,
        min_files: int = 2,
        retain: int = 1,
        older_than: datetime.datetime | datetime.timedelta | None = None,
        remove_orphans: bool = True,
        orphan_age: datetime.timedelta = ORPHAN_AGE,
        metadata: bool = True,
        **kwargs: Any,
    ) -> dict[str, int]:
        """Merge manifests, compact files, then expire and sweep -- in that order."""
        if self.iceberg_table.properties.get(MERGE_MANIFESTS) != "true":
            # Only when it is not already on: a no-op commit is still a metadata
            # version, and a routine that runs on a schedule would spend one
            # every time.
            self.set_properties({MERGE_MANIFESTS: "true"})
        rewritten = self.compact(min_files=min_files, **kwargs)
        report = self.cleanup(
            retain=retain,
            older_than=older_than,
            remove_orphans=remove_orphans,
            orphan_age=orphan_age,
            metadata=metadata,
        )
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


def _ordered_reader(
    scan: Any, tasks: Iterable[Any], columns: Sequence[str]
) -> pyarrow.RecordBatchReader:
    """Read partition paths in order, merging their sorted files on `columns`.

    Iceberg sort orders describe file layout, not result order. Plans may list
    newer manifests first, so a stateful consumer cannot use plan order.
    Partitions are independent storage streams: finish one canonical path
    before opening the next, and merge overlapping file ranges only within it.
    """
    from pyiceberg.conversions import from_bytes
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    target = schema_to_pyarrow(scan.projection())
    primary = columns[0]
    field = scan.projection().find_field(primary)

    def bound(task: Any, upper: bool) -> Any | None:
        values = task.file.upper_bounds if upper else task.file.lower_bounds
        raw = (values or {}).get(field.field_id)
        return None if raw is None else from_bytes(field.field_type, raw)

    def batches() -> Iterator[pyarrow.RecordBatch]:
        for _, partition in _partition_tasks(scan, tasks):
            ranged = [(bound(task, False), bound(task, True), task) for task in partition]
            if any(lower is None or upper is None for lower, upper, _ in ranged):
                groups = [partition]
            else:
                ranged.sort(key=lambda item: (item[0], str(item[2].file.file_path)))
                groups = []
                held: list[Any] = []
                high = None
                for lower, upper, task in ranged:
                    # Equal primary boundaries overlap when a secondary key
                    # decides business order, so only a strict gap concatenates.
                    if held and lower > high:
                        groups.append(held)
                        held = []
                        high = None
                    held.append(task)
                    high = upper if high is None or upper > high else high
                if held:
                    groups.append(held)
            for group in groups:
                if len(group) == 1:
                    yield from _sorted_task_batches(scan, group[0], columns)
                else:
                    yield from _merge_task_batches(scan, group, columns)

    return pyarrow.RecordBatchReader.from_batches(target, batches()).cast(target)


def _sorted_task_batches(
    scan: Any, task: Any, columns: Sequence[str]
) -> Iterator[pyarrow.RecordBatch]:
    """One file's batches, refusing a table that only claims to be sorted."""
    previous = None
    for batch in _planned_reader(scan, [task]):
        if not batch.num_rows:
            continue
        if not _reader_in_sort_order(batch, columns):
            raise ValueError(
                f"Iceberg file {task.file.file_path!s} is not ordered on {list(columns)!r}"
            )
        first = _row_key(batch, columns, 0)
        last = _row_key(batch, columns, batch.num_rows - 1)
        if previous is not None and first < previous:
            raise ValueError(
                f"Iceberg file {task.file.file_path!s} is not ordered on {list(columns)!r}"
            )
        previous = last
        yield batch


def _merge_task_batches(
    scan: Any, tasks: Sequence[Any], columns: Sequence[str]
) -> Iterator[pyarrow.RecordBatch]:
    """K-way merge overlapping sorted files, moving slices rather than rows."""
    streams = [iter(_sorted_task_batches(scan, task, columns)) for task in tasks]
    batches: list[pyarrow.RecordBatch | None] = [None] * len(streams)
    offsets = [0] * len(streams)

    def advance(index: int) -> bool:
        while batches[index] is None or offsets[index] >= batches[index].num_rows:
            batches[index] = next(streams[index], None)
            offsets[index] = 0
            if batches[index] is None:
                return False
            if batches[index].num_rows:
                return True
        return True

    for index in range(len(streams)):
        advance(index)
    while True:
        live = [index for index in range(len(streams)) if advance(index)]
        if not live:
            return
        starts = {
            index: _row_key(batches[index], columns, offsets[index])  # type: ignore[arg-type]
            for index in live
        }
        chosen = min(live, key=lambda index: (starts[index], index))
        others = [starts[index] for index in live if index != chosen]
        batch = batches[chosen]
        assert batch is not None
        stop = (
            batch.num_rows
            if not others
            else _upper_bound(batch, columns, min(others), offsets[chosen])
        )
        yield batch.slice(offsets[chosen], stop - offsets[chosen])
        offsets[chosen] = stop


def _row_key(batch: pyarrow.RecordBatch, columns: Sequence[str], index: int) -> tuple[Any, ...]:
    """One row's lexicographic key, with Arrow's null-last order made explicit."""
    return tuple(
        (value is None, value)
        for column in columns
        for value in (batch.column(column)[index].as_py(),)
    )


def _reader_in_sort_order(batch: pyarrow.RecordBatch, columns: Sequence[str]) -> bool:
    """Whether a physical batch follows Arrow's ascending, null-last ordering."""
    compute = pyarrow.compute
    ordered = None
    for name in reversed(list(columns)):
        column = batch.column(name)
        before, after = column[:-1], column[1:]
        before_null, after_null = compute.is_null(before), compute.is_null(after)
        less = compute.or_(
            compute.and_(compute.invert(before_null), after_null),
            compute.fill_null(compute.less(before, after), False),
        )
        equal = compute.or_(
            compute.and_(before_null, after_null),
            compute.fill_null(compute.equal(before, after), False),
        )
        ordered = less if ordered is None else compute.or_(less, compute.and_(equal, ordered))
    if ordered is None:
        return True
    return bool(compute.all(ordered, min_count=0).as_py())


def _upper_bound(
    batch: pyarrow.RecordBatch,
    columns: Sequence[str],
    sought: tuple[Any, ...],
    start: int,
) -> int:
    """First row whose lexicographic key is strictly greater than `sought`."""
    low, high = start, batch.num_rows
    while low < high:
        middle = (low + high) // 2
        if _row_key(batch, columns, middle) <= sought:
            low = middle + 1
        else:
            high = middle
    return low


def _reader_limit(reader: pyarrow.RecordBatchReader, limit: int) -> pyarrow.RecordBatchReader:
    """Cut a reader after global ordering, rather than once per source file."""
    schema = reader.schema

    def batches() -> Iterator[pyarrow.RecordBatch]:
        remaining = limit
        if remaining <= 0:
            return
        for batch in reader:
            taken = batch.slice(0, remaining)
            if taken.num_rows:
                yield taken
            remaining -= taken.num_rows
            if remaining <= 0:
                return

    return pyarrow.RecordBatchReader.from_batches(schema, batches()).cast(schema)


def _planned_reader(scan: Any, tasks: Iterable[Any]) -> pyarrow.RecordBatchReader:
    """Read a planned stream one partition at a time with bounded read-ahead."""
    from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow

    target = schema_to_pyarrow(scan.projection())

    def arrow(limit: int | None) -> Any:
        return ArrowScan(
            scan.table_metadata,
            scan.io,
            scan.projection(),
            scan.row_filter,
            scan.case_sensitive,
            limit,
        )

    def generate() -> Iterator[pyarrow.RecordBatch]:
        taken = 0
        for _, partition in _partition_tasks(scan, tasks):
            # ArrowScan loads delete files per call. Keep a partition sharing
            # one together so its delete file is not reopened per read-ahead group.
            groups = (
                (partition,)
                if any(task.delete_files for task in partition)
                else _grouped(partition, _read_ahead())
            )
            for group in groups:
                # The limit is what is left globally, not once per partition.
                for batch in arrow(
                    None if scan.limit is None else scan.limit - taken
                ).to_record_batches(group):
                    yield batch
                    taken += batch.num_rows
                if scan.limit is not None and taken >= scan.limit:
                    return

    return pyarrow.RecordBatchReader.from_batches(target, generate()).cast(target)


def _partition_tasks(scan: Any, tasks: Iterable[Any]) -> Iterator[tuple[str, list[Any]]]:
    """Planned tasks grouped by canonical partition path, in path order."""
    planned, identity = _ordered_partition_tasks(scan, tasks)
    for (path, _), grouped in itertools.groupby(planned, key=identity):
        yield path, list(grouped)


def _ordered_partition_tasks(
    scan: Any, tasks: Iterable[Any]
) -> tuple[list[Any], Callable[[Any], tuple[str, int]]]:
    """Planned tasks plus their canonical partition identity, path-sorted."""
    planned = tasks if isinstance(tasks, list) else list(tasks)
    metadata = getattr(scan, "table_metadata", None)
    held_specs = getattr(metadata, "specs", None)
    held_specs = held_specs() if callable(held_specs) else held_specs
    specs = (
        held_specs
        if isinstance(held_specs, Mapping)
        else {spec.spec_id: spec for spec in (held_specs or ())}
    )
    schema = metadata.schema() if metadata is not None else None

    def identity(task: Any) -> tuple[str, int]:
        data = task.file
        spec_id = int(getattr(data, "spec_id", 0) or 0)
        partition = getattr(data, "partition", None)
        spec = specs.get(spec_id)
        if spec is not None and schema is not None:
            try:
                return spec.partition_to_path(partition, schema), spec_id
            except (KeyError, TypeError, ValueError):
                pass
        return str(partition or ""), spec_id

    planned.sort(key=lambda task: (*identity(task), str(getattr(task.file, "file_path", ""))))
    return planned, identity


def _grouped(tasks: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    """`tasks` in slices of `size`, in plan order."""
    for start in range(0, len(tasks), size):
        yield tasks[start : start + size]


def _read_ahead() -> int:
    """How many planned files a read has in flight, from the pool's own width.

    pyiceberg's shared executor decides how many files can be decoded at once;
    reading further ahead than that fills memory without filling the pool. Its
    width is not exposed, so it is read off the `ThreadPoolExecutor` -- and if
    a future one stops saying, one file at a time is the answer that cannot be
    wrong about memory.
    """
    from pyiceberg.utils.concurrent import ExecutorFactory

    return max(int(getattr(ExecutorFactory.get_or_create(), "_max_workers", 0) or 0), 1)


def _limited_reader(scan: Any, limit: int | None) -> pyarrow.RecordBatchReader:
    """`scan`'s reader, opening only the files the `limit` can need."""
    tasks, _ = _ordered_partition_tasks(scan, scan.plan_files())
    if limit is None:
        return _planned_reader(scan, tasks)
    exact = _always_true()
    filtered = getattr(scan, "row_filter", exact) != exact
    taken, rows = [], 0
    for task in tasks:
        if rows >= limit:
            break
        if (
            task.delete_files
            or task.residual != exact
            or (filtered and _null_partition(task.file.partition))
        ):
            return _planned_reader(scan, tasks)
        taken.append(task)
        rows += task.file.record_count
    return _planned_reader(scan, taken)


def _null_partition(partition: Any) -> bool:
    """Whether a file's partition record holds a null in any field."""
    return any(partition[index] is None for index in range(len(partition)))


def _in_sort_order(chunk: pyarrow.Table, names: Sequence[str]) -> bool:
    """Whether `chunk`'s rows are already ascending on `names`, lexicographically."""
    compute = pyarrow.compute
    ordered = None
    for name in reversed(list(names)):
        column = chunk.column(name).combine_chunks()
        before, after = column[:-1], column[1:]
        if ordered is None:
            ordered = compute.less_equal(before, after)
            continue
        ordered = compute.or_(
            compute.less(before, after),
            compute.and_(compute.equal(before, after), ordered),
        )
    if ordered is None:
        return True
    # `min_count=0`, so a chunk with one row -- no adjacent pair to compare --
    # answers yes rather than null. `all` of nothing is true.
    return bool(compute.all(compute.fill_null(ordered, False), min_count=0).as_py())


def _key_ranges(
    chunk: pyarrow.Table,
    join: Sequence[str],
    derived: Mapping[str, Sequence[str]] | None = None,
) -> Any:
    """A predicate every row matching `chunk` on `join` must satisfy."""
    from pyiceberg.expressions import And

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
        if _has_nan(values):
            # And no literal can name a NaN, which the two branches below
            # disagree about: `In` refuses it (pyiceberg will not build the
            # literal), while `min_max` skips it and hands back a range the
            # stored row falls outside -- so the merge would insert a second
            # copy, and a third next time, without ever raising.
            raise ValueError(
                f"column {column!r} is a merge key and cannot be NaN; "
                "no predicate can name a NaN, so merging on it would duplicate rows"
            )
        term = _column_term(column, values)
        if term is not None:
            terms.append(term)
    for column in _derivable(chunk, join, derived):
        values = chunk.column(column)
        if values.null_count or _has_nan(values):
            # Nothing here is required, so nothing here raises -- but a term
            # that cannot name every value the column takes is a filter that
            # excludes a row it should have matched, and this one is only ever
            # allowed to be too wide.
            continue
        term = _column_term(column, values)
        if term is not None:
            terms.append(term)
    if not terms:
        return _always_true()
    return And(*terms) if len(terms) > 1 else terms[0]


def _derivable(
    chunk: pyarrow.Table, join: Sequence[str], derived: Mapping[str, Sequence[str]] | None
) -> list[str]:
    """Columns of `chunk` a row matching on `join` must agree on, keys aside.

    A declared derivation is only usable when the chunk *has* the column and
    every source it names is a key here: derived from `unix` alone it holds
    for a merge on `(unix, hash)`, and derived from a column nothing joins on
    it holds for nothing.
    """
    if not derived:
        return []
    keyed = set(join)
    return [
        name
        for name, sources in derived.items()
        if name not in keyed and name in chunk.column_names and keyed.issuperset(sources)
    ]


def _column_term(column: str, values: Any) -> Any | None:
    """One conjunct covering every value `column` takes in the chunk, or None."""
    from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThanOrEqual, Or

    distinct = _distinct_under(values, MERGE_IN_LIMIT)
    if distinct is None:
        # Neither bound can be null here: the column has rows, no nulls and no
        # NaN, which is everything `min_max` would have skipped. None means the
        # bounds cannot be expressed, and a missing term is a wider filter --
        # which is the direction this one is allowed to be wrong in.
        return _banded(column, values)
    if len(distinct) == 0:
        return None
    named = In(column, distinct.to_pylist())
    if _has_zero(values):
        # `In` of more than one literal reaches Arrow as `pc.is_in`, which
        # hashes `-0.0` apart from the `0.0` it equals -- so a row stored as
        # `-0.0` would not come back and would be inserted a second time. This
        # filter is a superset anyway; widening it costs a few rows the
        # semi-join then drops.
        return Or(named, And(GreaterThanOrEqual(column, 0.0), LessThanOrEqual(column, 0.0)))
    return named


def _has_nan(values: Any) -> bool:
    """Whether a floating column holds a NaN, which no literal can name."""
    return (
        pyarrow.types.is_floating(values.type)
        and pyarrow.compute.any(pyarrow.compute.is_nan(values)).as_py()
    )


def _banded(column: str, values: Any) -> Any | None:
    """The narrowest union of ranges that still covers every value in `values`."""
    compute = pyarrow.compute
    try:
        bounds = compute.min_max(values).as_py()
        whole = _between(column, bounds["min"], bounds["max"])
    except (ValueError, pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, OverflowError):
        # A bound this column cannot be *said* in -- a nanosecond timestamp,
        # which pyarrow refuses to hand back as a `datetime` at all. No term is
        # the honest answer: it widens the filter, and a merge's filter may
        # only ever be wider than the rows it has to find.
        return None
    kind = values.type
    if not (
        pyarrow.types.is_integer(kind)
        or pyarrow.types.is_floating(kind)
        or pyarrow.types.is_temporal(kind)
        or pyarrow.types.is_decimal(kind)
    ):
        # Asked before it is attempted, because a failed cast is not free:
        # `cast(string, int64)` walks the column to find out, and on 400k
        # strings that raise took 115 ms to arrive at "no".
        return whole
    slices = MERGE_RANGE_BANDS * 8
    try:
        if (
            pyarrow.types.is_floating(kind)
            or pyarrow.types.is_decimal(kind)
            or pyarrow.types.is_unsigned_integer(kind)
        ):
            # Straight across: a decimal has no integer cast at all, and an
            # unsigned one past 2**63 has one that raises.
            numeric = _placed(values)
        else:
            # Through the width the type actually stores. Arrow has no
            # `date32 -> int64` cast at all ("Unsupported cast from date32[day]
            # to int64"), and `date32` is what an Iceberg `date` column is --
            # so asking for int64 made every date key fall out of the banding
            # and keep the single range it was supposed to replace.
            physical = "int32" if getattr(kind, "bit_width", 64) == 32 else "int64"
            numeric = _placed(compute.cast(values, physical))
        span = compute.min_max(numeric).as_py()
        low, high = span["min"], span["max"]
        if low is None or high is None or not low < high:
            return whole
        index = compute.max_element_wise(
            compute.min_element_wise(
                compute.cast(
                    compute.floor(
                        compute.divide(compute.subtract(numeric, low), (high - low) / slices)
                    ),
                    "int64",
                ),
                slices - 1,
            ),
            0,
        )
        # Which slices are occupied is asked before what is *in* them: on a
        # 400k-row column that is 1.1 ms against 5.5 for the grouping, and a
        # column with no gap in it -- which is most of them -- is answered
        # without ever paying the second.
        if len(compute.unique(index)) * 10 >= slices * 9:
            return whole
        occupied = (
            pyarrow.table({"value": values, "place": numeric, "band": index})
            .group_by("band")
            .aggregate([("value", "min"), ("value", "max"), ("place", "min"), ("place", "max")])
            .sort_by([("place_min", "ascending")])
        )
    except (
        pyarrow.ArrowInvalid,
        pyarrow.ArrowNotImplementedError,
        pyarrow.ArrowTypeError,
        ZeroDivisionError,
        OverflowError,
    ):
        return whole
    # The literals a band is expressed as, and the numbers its gaps are
    # measured in. They have to be both: a `time` cannot be subtracted from a
    # `time` at all, so measuring a gap in the column's own values crashed
    # every merge on a table keyed by one -- while a range still has to be
    # spelled in the values Iceberg will compare against.
    bands = _fewest(
        list(
            zip(
                occupied.column("value_min").to_pylist(),
                occupied.column("value_max").to_pylist(),
                occupied.column("place_min").to_pylist(),
                occupied.column("place_max").to_pylist(),
                strict=True,
            )
        ),
        MERGE_RANGE_BANDS,
    )
    if len(bands) < 2:
        return whole
    from pyiceberg.expressions import Or

    return Or(*[_between(column, low, high) for low, high, _, _ in bands])


def _placed(values: Any) -> Any:
    """`values` as doubles, **unsafely** -- the number is a place, not a value."""
    return pyarrow.compute.cast(values, "double", safe=False)


def _fewest(
    bands: list[tuple[Any, Any, float, float]], keep: int
) -> list[tuple[Any, Any, float, float]]:
    """`bands` reduced to at most `keep`, always by closing the smallest gap."""
    while len(bands) > keep:
        gap = min(range(len(bands) - 1), key=lambda i: bands[i + 1][2] - bands[i][3])
        bands[gap : gap + 2] = [
            (bands[gap][0], bands[gap + 1][1], bands[gap][2], bands[gap + 1][3])
        ]
    return bands


def _between(column: str, low: Any, high: Any) -> Any:
    """`low <= column <= high`, as one Iceberg expression."""
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

    return And(GreaterThanOrEqual(column, low), LessThanOrEqual(column, high))


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
    """`source` with its columns named the way the table names them *now*."""
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
    """The column's distinct values, or None when there are more than `limit`."""
    head = pyarrow.compute.unique(values.combine_chunks().slice(0, limit + 1))
    if len(head) > limit:
        return None
    distinct = pyarrow.compute.unique(values.combine_chunks())
    return distinct if len(distinct) <= limit else None


def _has_zero(values: Any) -> bool:
    """Whether a float column holds a zero of either sign; False for any other type."""
    if not pyarrow.types.is_floating(values.type):
        return False
    zero = pyarrow.scalar(0.0, values.type)
    return bool(pyarrow.compute.any(pyarrow.compute.equal(values, zero), min_count=0).as_py())


def _match_filter(updates: pyarrow.Table, join: Sequence[str]) -> Any:
    """pyiceberg's exact per-row delete filter, widened where `In` cannot see a zero."""
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual, Or
    from pyiceberg.table import upsert_util

    if len(join) != 1:
        return _factored(updates, join)
    exact = upsert_util.create_match_filter(updates, join)
    if not _has_zero(updates.column(join[0])):
        return exact
    return Or(
        exact,
        And(GreaterThanOrEqual(join[0], 0.0), LessThanOrEqual(join[0], 0.0)),
    )


def _factored(updates: pyarrow.Table, join: Sequence[str]) -> Any:
    """The same filter, with whatever a key column repeats said once."""
    from pyiceberg.expressions import And, EqualTo, In, Or
    from pyiceberg.table import upsert_util

    whole = functools.partial(upsert_util.create_match_filter, updates, join)
    if any(_has_zero(updates.column(name)) for name in join):
        return whole()
    keys = updates.select(list(join))
    try:
        counts = {
            name: len(pyarrow.compute.unique(keys.column(name).combine_chunks())) for name in join
        }
        outer = min(join, key=lambda name: counts[name])
        if counts[outer] == keys.num_rows:
            # One group per row: the terms would be the ones
            # `create_match_filter` builds, so let it build them.
            return whole()
        rest = [name for name in join if name != outer]
        if len(rest) > 1 and counts[outer] * MERGE_GROUP_GAIN > keys.num_rows:
            # Groups too small to be worth *this* way of making them. A key of
            # three columns or more takes each group back through the library
            # to spell what is left of it, and that per-group round trip is
            # what needs the rows to pay for it: measured on 5,000 rows and a
            # two-column key, which does not pay it, groups of 1.6 built in
            # 4.4x the library's time through this path and 0.42x without it.
            return whole()
        if len(rest) == 1:
            # The whole of a two-column key, and the shape worth doing without
            # a round trip through the library: the inner values come out of
            # the same grouping pass, so there is no `take` and no second
            # `group_by` per group. An `In` of one literal is an `EqualTo`
            # again, which is what a group of one would have been anyway.
            grouped = keys.group_by([outer]).aggregate([(rest[0], "list")])
            terms = [
                And(EqualTo(outer, value), In(rest[0], inner))
                for value, inner in zip(
                    grouped.column(outer).to_pylist(),
                    grouped.column(f"{rest[0]}_list").to_pylist(),
                    strict=True,
                )
            ]
        else:
            marked = keys.append_column(SOURCE_INDEX, arrays.sequence(keys.num_rows))
            groups = marked.group_by([outer]).aggregate([(SOURCE_INDEX, "list")])
            terms = [
                And(EqualTo(outer, value), upsert_util.create_match_filter(keys.take(rows), rest))
                for value, rows in zip(
                    groups.column(outer).to_pylist(),
                    groups.column(f"{SOURCE_INDEX}_list").to_pylist(),
                    strict=True,
                )
            ]
    except (
        pyarrow.ArrowInvalid,
        pyarrow.ArrowNotImplementedError,
        pyarrow.ArrowTypeError,
        ValueError,
        TypeError,
    ):
        return whole()
    return Or(*terms) if len(terms) > 1 else terms[0]


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


def _changed(chunk: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """Rows of `chunk` a stored row matches but does not equal."""
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
    pairs = keys_of(chunk, keys, SOURCE_INDEX).join(
        keys_of(matched, keys, TARGET_INDEX), keys=keys, join_type="inner"
    )
    if pairs.num_rows == 0:
        return chunk.schema.empty_table()

    left = chunk.take(pairs.column(SOURCE_INDEX))
    right = matched.take(pairs.column(TARGET_INDEX))
    compute = pyarrow.compute
    differs = None
    for name in compare:
        column = _column_differs(left.column(name), right.column(name))
        # The first column *is* the running answer -- seeding one with a
        # Python list of falses costs 14 ms a million rows, and buys nothing.
        differs = column if differs is None else compute.or_(differs, column)
    return chunk.take(compute.filter(pairs.column(SOURCE_INDEX), differs))


def _column_differs(one: Any, other: Any) -> Any:
    """Which rows of two aligned columns disagree, nulls counted pyiceberg's way."""
    compute = pyarrow.compute
    try:
        unequal = compute.fill_null(compute.not_equal(one, other), False)
    except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, pyarrow.ArrowTypeError):
        return _column_differs_row_by_row(one, other)
    # `not_equal` is null when either side is: a null against a value is a
    # difference, two nulls are not.
    only_one_null = compute.xor(compute.is_null(one), compute.is_null(other))
    return compute.or_(unequal, only_one_null)


def _column_differs_row_by_row(one: Any, other: Any) -> Any:
    """The same answer for a column Arrow will not compare, in Python.

    Two whole columns out of Arrow at once and one `!=` per row, against the
    library's `slice(i, 1)` and `as_py()` per column per row -- and it is the
    library's own comparison either way, because `to_pylist` and `as_py` build
    the same Python objects and `!=` is `!=`. A list of nulls equals a list of
    nulls, a null equals a null, and a NaN equals nothing, all as before.
    """
    return pyarrow.array(
        [left != right for left, right in zip(one.to_pylist(), other.to_pylist(), strict=True)],
        pyarrow.bool_(),
    )


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


def _mark_key(branch: str, partition: Any) -> str:
    """A stable name for one partition of one branch, for `COMPACTION_MARK`.

    No partition at all -- `None`, or the empty mapping an unpartitioned table
    reports -- is the whole branch, which is what a plan that cannot address
    parts of the table separately settles under.
    """
    if not partition:
        return f"{branch}/"
    values = ",".join(f"{name}={partition[name]!r}" for name in sorted(partition))
    return f"{branch}/{values}"


def _stored_files(snapshot: Any) -> int | None:
    """How many data files the state that snapshot heads holds, or None."""
    if snapshot is None:
        return 0
    try:
        return int(snapshot.summary["total-data-files"])
    except (AttributeError, KeyError, TypeError, ValueError):
        return None


def _totals(rows: Sequence[Any]) -> list[int]:
    """What a whole branch holds: `[file count, record count]`, added up.

    `_counts` for one partition; this for all of them at once, which is what a
    whole-table plan has to compare against -- it rewrote every partition, so
    what settles it is every partition's counts.
    """
    return [
        int(sum(row["file_count"] for row in rows)),
        int(sum(row["record_count"] for row in rows)),
    ]


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


def _path_of(location: str) -> str:
    """A file location without its scheme, as one of the spellings to try.

    Never on its own as "the path `pyarrow.fs` would use": it is not.
    `file:/tmp/x` keeps its scheme, `abfss://container@account.dfs.../x`
    resolves to `container/x`, and a Windows drive letter loses a leading
    slash. `_relative` tries this beside the ones `resolve` produces.
    """
    return location.split("://", 1)[-1]


def _relative(path: str, bases: Sequence[str]) -> str | None:
    """`path` under whichever of `bases` it is spelled against, tail only."""
    for base in sorted(bases, key=len, reverse=True):
        if base and path.startswith(base):
            return path[len(base) :].lstrip("/")
    return None


def _cutoff_ms(older_than: datetime.datetime | datetime.timedelta | None) -> int | None:
    """The instant a snapshot must predate to be expirable, in Iceberg's millis."""
    if older_than is None:
        return None
    if isinstance(older_than, datetime.timedelta):
        older_than = datetime.datetime.now(datetime.UTC) - older_than
    return int(older_than.timestamp() * 1000)
