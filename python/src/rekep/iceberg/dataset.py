"""One Iceberg table as a dataset, with the maintenance it needs to stay fast."""

from __future__ import annotations

import dataclasses
import datetime
import functools
import itertools
import json
import logging
import math
import os
import random
import sqlite3
import tempfile
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from functools import cached_property
from typing import Any

import pyarrow
import pyarrow.fs

from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.dataset import (
    SOURCE_INDEX,
    TARGET_INDEX,
    Dataset,
    _positive_int,
    arrow_chunks,
    first_rows,
    keys_of,
    normalised_keys,
    semi_join,
)
from rekep.fields import Field, StructField, arrays
from rekep.iceberg.catalog import IcebergCatalog, _file_location
from rekep.iceberg.fields import metrics_for

LOGGER = logging.getLogger(__name__)

#: The physical root ref PyIceberg stores. Public callers may spell the same
#: state ``root``, ``main``, or ``master`` without creating extra refs.
MAIN = "main"
ROOT_BRANCHES = frozenset({"root", "main", "master"})

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
#: cannot fill a file across commits. `commit_batch_num`, the optional
#: `commit_row_size`, and `compact` are the levers on file count; this one
#: decides how a large commit is sliced.
TARGET_FILE_SIZE = "write.target-file-size-bytes"

#: Table properties that are physical file locations rather than ordinary values.
STORAGE_PATHS = ("write.data.path", "write.metadata.path")

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

#: Maximum sorted runs merged in memory during one external-sort pass. Each
#: run contributes one scan batch, so the fan-in bounds memory independently
#: of how many row groups or files a partition holds.
SORT_MERGE_FAN_IN = 16

#: Source batches a commit carries when nothing says otherwise. Eight amortizes
#: snapshot and file overhead while bounding memory in the units the producer
#: actually controls; a row cap remains available for unusually large batches.
DEFAULT_COMMIT_BATCH_NUM = 8

#: Row target for locally staged partition files when no write row cap is set.
#: This is a file boundary, not a commit boundary: complete partitions remain
#: atomic while their batches spill to disk.
DEFAULT_STAGED_FILE_ROW_SIZE = 1_000_000


#: Snapshot summary key that settles an ambiguous remote acknowledgement.
#: The value stays stable across retries of one bounded operation, so a reload
#: can distinguish "the commit landed" from "build and submit it again".
OPERATION_ID = "rekep.operation-id"

#: Iceberg's table property for the maximum age of an unprotected snapshot.
#: Keeping the protocol name here lets a dataset declaration use it without
#: importing the optional PyIceberg extra during configuration loading.
SNAPSHOT_MAX_AGE = "history.expire.max-snapshot-age-ms"

SnapshotExpiry = datetime.datetime | datetime.timedelta | str | None

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

_InsertSpan = tuple[
    tuple[str, tuple[str, ...], str, str, tuple[Any, ...] | None],
    pyarrow.Scalar,
    pyarrow.Scalar,
]

#: Maintenance defaults safe to retrofit onto an existing table. Explicit
#: retention settings still win; `optimize` only supplies absent declarations.
MAINTENANCE_PROPERTIES = {
    MERGE_MANIFESTS: "true",
    MIN_MANIFESTS_TO_MERGE: "10",
    PREVIOUS_VERSIONS: "20",
    DELETE_OLD_METADATA: "true",
}


@dataclasses.dataclass(eq=False)
class IcebergDataset(Dataset):
    """An Iceberg table, read and written as Arrow through pyiceberg."""

    @classmethod
    @functools.cache
    def into_kind(cls) -> str:
        """Document kind registered with `Dataset`."""
        return "iceberg"

    #: Table coordinates stay outside the schema so catalog identity cannot
    #: change when a field declaration is reused under another namespace.
    name: str
    namespace: str

    #: The declared row shape, named after the unqualified table.
    field: StructField

    #: Catalog loading is explicit; the live catalog stays a lazy property.
    catalog_name: str = "default"
    catalog_properties: dict[str, str] = dataclasses.field(default_factory=dict)

    #: Branch reads and writes use unless a call names another. None, `root`,
    #: `main`, and `master` all mean the table's root state.
    branch: str | None = None

    #: Source batches one commit carries; the producer's batch size bounds the
    #: retained bytes without guessing how wide a row is.
    commit_batch_num: int = DEFAULT_COMMIT_BATCH_NUM

    #: Optional row cap applied with `commit_batch_num`; the first bound reached
    #: commits. None leaves batch count as the default boundary.
    commit_row_size: int | None = None

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

    #: Cutoff applied once after a successful public write. None reads the
    #: table's `history.expire.max-snapshot-age-ms`, including Iceberg's
    #: default when the property is absent.
    snapshot_expiry: SnapshotExpiry = None

    #: Only used when the table is created: where it lives and what it carries.
    location: str | None = None
    table_properties: dict[str, str] = dataclasses.field(default_factory=dict)

    #: Blind retries rebuild one bounded operation against the refreshed branch;
    #: planned keyed writes only settle an acknowledgement before returning the
    #: conflict for a fresh plan. Full jitter keeps parallel remote writers from
    #: colliding in cadence.
    commit_retries: int = 4
    retry_backoff: float = 0.25
    retry_max_backoff: float = 8.0

    #: Target files one streamed rewrite commit may replace.
    rewrite_file_count: int = 16

    def __post_init__(self) -> None:
        """Normalize the declaration and public root spellings once."""
        if not self.name or "." in self.name:
            raise ValueError("an Iceberg dataset name must be non-empty and unqualified")
        if not self.namespace or any(not part for part in self.namespace.split(".")):
            raise ValueError("an Iceberg dataset namespace must be non-empty")
        field = self.field if isinstance(self.field, Field) else Field.from_(self.field)
        if not isinstance(field, StructField):
            raise TypeError("an Iceberg dataset field must be a struct")
        self.field = field.with_name(self.name)
        self.commit_batch_num = _positive_int(self.commit_batch_num, "commit_batch_num")
        if self.commit_row_size is not None:
            self.commit_row_size = _positive_int(self.commit_row_size, "commit_row_size")
        if self.commit_retries < 0:
            raise ValueError("commit_retries cannot be negative")
        if self.rewrite_file_count <= 0:
            raise ValueError("rewrite_file_count must be positive")
        if self.retry_backoff < 0 or self.retry_max_backoff < self.retry_backoff:
            raise ValueError("retry backoff must be non-negative and capped above its start")
        if self.branch in ROOT_BRANCHES:
            self.branch = None
        if self.location is not None:
            self.location = _file_location(self.location)
        self.table_properties = {
            name: _file_location(value) if name in STORAGE_PATHS and value else value
            for name, value in self.table_properties.items()
        }
        configured_expiry = self.table_properties.get(SNAPSHOT_MAX_AGE)
        if configured_expiry is None:
            configured_expiry = self.catalog_properties.get(SNAPSHOT_MAX_AGE)
        if isinstance(self.snapshot_expiry, datetime.timedelta):
            duration = _checked_expiry_delta(self.snapshot_expiry)
            self.table_properties = {
                **self.table_properties,
                SNAPSHOT_MAX_AGE: str(duration // datetime.timedelta(milliseconds=1)),
            }
            self.__dict__["_snapshot_expiry"] = duration
            # Relative retention is an Iceberg table declaration. Keeping its
            # one canonical spelling also makes dataset documents round-trip.
            self.snapshot_expiry = None
        elif self.snapshot_expiry is None and configured_expiry is not None:
            self.__dict__["_snapshot_expiry"] = _expiry_delta(configured_expiry)

    @property
    def identifier(self) -> str:
        """The catalog identifier composed from namespace and table name."""
        return f"{self.namespace}.{self.name}"

    # -- the table ----------------------------------------------------------

    @cached_property
    def store(self) -> IcebergCatalog:
        """The Rekep catalog wrapper that owns the shared live connection."""
        store = IcebergCatalog(
            name=self.catalog_name,
            properties=self.catalog_properties,
        )
        self.__dict__["_owns_store"] = True
        return store

    def close(self) -> None:
        """Release loaded table views without opening lazy resources."""
        self.__dict__.pop("iceberg_table", None)
        self.__dict__.pop("table_field", None)
        self.__dict__.pop("_table_sort_order_id", None)
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
    def catalog(self) -> Any:
        """The live pyiceberg catalog, loaded once when first needed."""
        return self.store.catalog

    @cached_property
    def iceberg_table(self) -> Any:
        """The pyiceberg table this dataset is."""
        return self.store.load_table(self.identifier)

    @property
    def exists(self) -> bool:
        """Whether the table is there yet."""
        return self.store.table_exists(self.identifier)

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
        location = kwargs.pop("location", self.location)
        if location is not None:
            location = _file_location(location)
        creation_properties = dict(kwargs.pop("properties", {}))
        creation_properties = {**self.table_properties, **creation_properties}
        creation_properties = {
            name: _file_location(value) if name in STORAGE_PATHS and value else value
            for name, value in creation_properties.items()
        }
        if self.exists:
            return self
        field = field.with_name(self.name)
        self.store.create_namespace(self.namespace)
        schema = field.into_iceberg_schema()
        defaults = {**(COMMIT_PROPERTIES if self.optimize_commits else {}), **metrics_for(field)}
        table = self.catalog.create_table(
            self.identifier,
            schema=schema,
            location=location,
            partition_spec=field.into_iceberg_partition_spec(schema),
            # Declared at creation, because Iceberg records a sort order on the
            # table and every writer through it honours it -- a shape that says
            # how it is read is a shape that says how it should be laid out.
            sort_order=field.into_iceberg_sort_order(schema, self.sort_by),
            properties={**defaults, **creation_properties},
        )
        self.field = field
        self.__dict__["iceberg_table"] = table
        LOGGER.info(
            "%s created at %s with %d columns, partitioned by %s",
            self.identifier,
            table.location(),
            len(schema.fields),
            field.partition_keys() or "nothing",
        )
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
            self.create_with_field(self.field)
        return self.iceberg_table

    def refresh(self) -> IcebergDataset:
        """Drop what was loaded, so the next call sees other writers' commits."""
        for view in ("iceberg_table", "table_field", "_table_sort_order_id", "_insert_upper"):
            self.__dict__.pop(view, None)
        return self

    # -- what it holds ------------------------------------------------------

    @cached_property
    def table_field(self) -> StructField:
        """The table's own shape: its schema, docs, keys and partitioning."""
        table = self.iceberg_table
        sort_order = table.sort_order()
        self.__dict__["_table_sort_order_id"] = sort_order.order_id
        return StructField.from_iceberg_schema(
            table.schema(),
            self.name,
            spec=table.spec(),
            sort_order=sort_order,
        )

    def into_struct_field(self) -> StructField:
        """The table's declared shape."""
        return self.field

    def derived_columns(self) -> dict[str, tuple[str, ...]]:
        """Columns the declared shape says are a function of other columns.

        Read from the declaration and not from the table: Iceberg records a
        partition spec, not why a column holds what it does, so a shape read
        back from a table says nothing here -- and saying nothing costs a merge
        pruning, never correctness.
        """
        return self.field.derived_keys()

    def merge_columns(self, merge_by: bool | Sequence[str] | None) -> list[str]:
        """Reported merge keys, with partition sources naming their scope."""
        join = self._row_merge_columns(merge_by)
        if not join:
            return join
        shape = self.table_field if "iceberg_table" in self.__dict__ else self.field
        return list(dict.fromkeys([*shape.partition_keys(), *join]))

    def _row_merge_columns(self, merge_by: bool | Sequence[str] | None) -> list[str]:
        """Stored columns compared inside one transformed partition."""
        return super().merge_columns(merge_by)

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
        # The declared shape *is* what writes cast onto, so evolving the table
        # without it would drop the new columns at the next write. Keep its
        # outer name because it is the schema's stable display name.
        self.field = target.with_name(self.name)
        LOGGER.info("%s gained %d columns: %s", self.identifier, len(added), ", ".join(added))
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
        order_by: str | Sequence[str | tuple[str, str]] | None = None,
    ) -> pyarrow.RecordBatchReader:
        """Stream the table, optionally sorting and merging on lexicographic keys.

        A bare `order_by` name is ascending; `(name, "descending")` requests
        the opposite explicitly, so snapshot reads never guess from newer
        table metadata.

        A table that was never written reads as no rows, not as a failure: on
        the first interval of a fresh catalog every stage reads an upstream
        that its own upstream has not created yet, and "nothing there" is the
        true answer to that -- so it is answered once here rather than by an
        `exists` guard at each call site. `parse_messages` refuses a missing
        yggdryl text source instead, because nothing in the pipeline creates one.
        """
        if isinstance(order_by, str):
            requested_order = (order_by,)
        elif (
            isinstance(order_by, tuple)
            and len(order_by) == 2
            and isinstance(order_by[0], str)
            and isinstance(order_by[1], str)
            and order_by[1].lower() in _SORT_DIRECTIONS
        ):
            requested_order = (order_by,)
        else:
            requested_order = tuple(order_by or ())
        ordering_fields = list(_sort_fields(requested_order))
        ordering = tuple(name for name, _ in ordering_fields)
        reference = self._reference(branch, snapshot_id)
        target = None if schema is None else self.target_field(schema)
        if columns and target is not None:
            selected = [target.field(name) for name in columns if name in target.names]
            if not selected:
                raise ValueError(f"columns={list(columns)!r} shares no columns with `schema`")
            target = Field.from_arrow_schema(
                pyarrow.schema(
                    [field.into_arrow_field() for field in selected],
                    metadata=target.into_arrow_schema().metadata,
                )
            )
        if not self.exists:
            return self._empty_reader(target, None if target is not None else columns)
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
        if columns and target is not None:
            found = self._selected(target, scan)
            scan = scan.select(*found)
        elif columns:
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
            _ordered_reader(scan, scan.plan_files(), ordering_fields)
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
            reference = self._branch_name(branch)
            return None if reference == MAIN else reference
        if branch is not None and self._branch_name(branch) != MAIN:
            raise ValueError(
                f"snapshot_id={snapshot_id} and branch={branch!r} name two different states; "
                "a snapshot id is already exact, so pass one or the other"
            )
        return None

    def _branch_name(self, branch: str | None) -> str:
        """The stored ref for a branch argument, with root aliases collapsed."""
        reference = self.branch if branch is None else branch
        return MAIN if reference is None or reference in ROOT_BRANCHES else reference

    @staticmethod
    def _branch_head(table: Any, reference: str) -> Any:
        """A stored ref head; only an unwritten physical root may have none."""
        head = table.refs().get(reference)
        if head is None and reference != MAIN:
            raise ValueError(f"Cannot scan unknown ref={reference}")
        return head

    def _branch_scan(self, table: Any, scan: Any, reference: str) -> Any:
        """Pin a scan to its validated ref, leaving an unwritten root unpinned."""
        return scan if self._branch_head(table, reference) is None else scan.use_ref(reference)

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
        LOGGER.debug(
            "%s projects %d of %d declared columns; unfilled: %s",
            self.identifier,
            len(wanted),
            len(target.names),
            ", ".join(sorted(set(target.names) - set(wanted.values()))) or "none",
        )
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
        commit_batch_num: int | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> None:
        """Upsert a stream, then expire snapshots under the configured cutoff."""
        with self._write(snapshot_expiry):
            self._overwrite_arrow_reader(
                source,
                schema,
                merge_by,
                commit_row_size,
                commit_batch_num=commit_batch_num,
                branch=branch,
                properties=properties,
            )

    def _overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] = True,
        commit_row_size: int | None = None,
        *,
        commit_batch_num: int | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Upsert keyed rows, or stage complete partitions for keyless input."""
        reader: pyarrow.RecordBatchReader | None = None
        delegated = False
        try:
            # An upsert or an unconditional append can put a key beyond an
            # insert-only writer's known maximum. Its cheap monotonic proof is no
            # longer complete after either operation.
            rows, batches = self._commit_limits(commit_row_size, commit_batch_num)
            self.__dict__.pop("_insert_upper", None)
            table = self.get_or_create_table()
            join = self._row_merge_columns(merge_by)
            partitions = _partition_columns(table)
            if partitions and not join:
                # The partition writer owns the source from entry. Do not close
                # it again here: an injected iterator need not make `close`
                # idempotent, and the direct public call must have the same
                # ownership as this dispatch.
                delegated = True
                self.overwrite_partition_arrow_reader(
                    source,
                    schema,
                    merge_by,
                    commit_row_size=rows,
                    commit_batch_num=batches,
                    branch=branch,
                    properties=properties,
                )
                return
            if not join:
                raise ValueError(
                    f"merge_by={merge_by!r} names nothing to match on, and an overwrite "
                    "replaces the rows whose keys match -- pass True for the primary key "
                    "or the columns to match on, or use append_arrow_* to add rows blindly"
                )
            reader = self.target_field(schema).cast_arrow_reader(source)
            reference = self._branch_name(branch)
            self._branch_head(table, reference)
            for chunk in arrow_chunks(reader, rows, batches):
                if self.plan_merges or partitions:
                    self.merge_arrow_table(chunk, join, branch=reference, properties=properties)
                else:
                    chunk = self.sorted(chunk)
                    table = self._commit_with_retry(
                        table,
                        reference,
                        properties or {},
                        lambda current, summary, chunk=chunk: current.upsert(
                            chunk,
                            join_cols=join,
                            branch=reference,
                            snapshot_properties=dict(summary),
                        ),
                    )
                # `arrow_chunks` accumulates the next chunk while this name
                # still holds the last one.
                del chunk
        finally:
            if not delegated:
                _close_write_source(source, reader)

    def overwrite_partition_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = True,
        commit_row_size: int | None = None,
        *,
        commit_batch_num: int | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> None:
        """Replace partition runs, then expire snapshots under the configured cutoff."""
        with self._write(snapshot_expiry):
            self._overwrite_partition_arrow_reader(
                source,
                schema,
                merge_by,
                commit_row_size,
                commit_batch_num=commit_batch_num,
                branch=branch,
                properties=properties,
            )

    def _overwrite_partition_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = True,
        commit_row_size: int | None = None,
        *,
        commit_batch_num: int | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Stage and replace complete partition-contiguous Arrow runs."""
        original = source
        reader: pyarrow.RecordBatchReader | None = None
        delegated = False
        try:
            rows, batches = self._commit_limits(commit_row_size, commit_batch_num)
            self.__dict__.pop("_insert_upper", None)
            table = self.get_or_create_table()
            join = self._row_merge_columns(merge_by)
            if join:
                delegated = True
                self.overwrite_arrow_reader(
                    source,
                    schema,
                    merge_by,
                    commit_row_size=rows,
                    commit_batch_num=batches,
                    branch=branch,
                    properties=properties,
                )
                return
            partitions = _partition_columns(table)
            if not partitions:
                raise ValueError("partition overwrite needs a supported table partition spec")
            required = _requiring_columns(source, [column.source for column in partitions])
            reader = self.target_field(schema).cast_arrow_reader(required)
            reference = self._branch_name(branch)
            self._branch_head(table, reference)
            snapshot = properties or {}
            pending: list[_StagedPartition] = []
            consumed_rows = 0
            pending_batches = 0

            def commit(stager: _PartitionStager) -> None:
                nonlocal pending, consumed_rows, pending_batches, table
                if not pending:
                    consumed_rows = 0
                    pending_batches = 0
                    return
                table = self._overwrite_partitions(table, pending, reference, snapshot, stager)
                pending, consumed_rows, pending_batches = [], 0, 0

            file_rows = rows or DEFAULT_STAGED_FILE_ROW_SIZE
            with _PartitionStager(table, self.sort_fields(), file_rows) as stager:
                for staged in _staged_partition_stream(reader, partitions, stager, rows):
                    if isinstance(staged, _StagedBatch):
                        consumed_rows += staged.rows
                        pending_batches += staged.count
                    else:
                        pending.append(staged)
                    if (rows is not None and consumed_rows >= rows) or (
                        batches is not None and pending_batches >= batches
                    ):
                        commit(stager)
                commit(stager)
        finally:
            if not delegated:
                _close_write_source(original, reader)

    def _overwrite_partitions(
        self,
        table: Any,
        replacements: Sequence[_StagedPartition],
        reference: str,
        properties: Mapping[str, str],
        stager: _PartitionStager,
    ) -> Any:
        """Replace complete staged partitions without loading their Parquet bytes."""
        data_files = [
            data_file for replacement in replacements for data_file in replacement.data_files
        ]
        if not data_files:
            return table

        def commit(current: Any, summary: Mapping[str, str]) -> None:
            with _track_outputs() as generated:
                transaction = current.transaction()
                try:
                    _ensure_name_mapping(transaction)
                    replaced = _partition_data_files(current, replacements, reference)
                    with transaction.update_snapshot(
                        snapshot_properties=dict(summary), branch=reference
                    ).overwrite() as overwrite:
                        for data_file in replaced:
                            overwrite.delete_data_file(data_file)
                        for data_file in data_files:
                            overwrite.append_data_file(data_file)
                except BaseException:
                    _discard_paths(current.io, generated)
                    raise
                _commit_staged(
                    current,
                    transaction,
                    stager,
                    replacements,
                    generated,
                )

        return self._commit_with_retry(table, reference, properties, commit)

    def _append_chunk(
        self,
        table: Any,
        chunk: pyarrow.Table,
        reference: str,
        properties: Mapping[str, str],
        *,
        rebuild: bool = True,
    ) -> Any:
        """Append one bounded chunk through PyIceberg's partition writer.

        A blind append may rebuild after another writer wins a commit. A
        keyed caller passes `rebuild=False`: its scan belongs to the old head,
        so replaying only the final append could duplicate a key added there.
        Lost acknowledgements are still settled by operation id either way.
        """
        if not chunk.num_rows:
            return table
        chunk = self.sorted(chunk)
        return self._commit_with_retry(
            table,
            reference,
            properties,
            lambda current, summary: self._append_chunk_once(current, chunk, reference, summary),
            rebuild=rebuild,
        )

    def _append_chunk_once(
        self,
        table: Any,
        chunk: pyarrow.Table,
        reference: str,
        properties: Mapping[str, str],
    ) -> None:
        """One append attempt through PyIceberg's parallel partition writer."""
        with _track_outputs() as generated:
            transaction = table.transaction()
            try:
                transaction.append(
                    chunk,
                    snapshot_properties=dict(properties),
                    branch=reference,
                )
            except BaseException:
                _discard_paths(table.io, generated)
                raise
            _commit_generated(table, transaction, generated)
            for path in sorted(_settled_paths(generated)):
                LOGGER.debug("%s output %s", self.identifier, path)

    def _commit_merge(
        self,
        table: Any,
        updates: pyarrow.Table,
        inserts: pyarrow.Table,
        predicate: Any,
        reference: str,
        properties: Mapping[str, str],
    ) -> Any:
        """Rewrite matching files in bounded commits, adding values in the last."""
        updates = self.sorted(updates)
        inserts = self.sorted(inserts)
        additions = pyarrow.concat_tables(
            [part for part in (updates, inserts) if part.num_rows],
            promote_options="none",
        )
        scan = table.scan(row_filter=predicate)
        scan = self._branch_scan(table, scan, reference)
        groups = iter(_delete_task_groups(scan.plan_files(), self.rewrite_file_count))
        pending = next(groups, None)
        if pending is None:
            return self._append_chunk(
                table,
                additions,
                reference,
                properties,
                rebuild=False,
            )
        for following in groups:
            table = self._rewrite_delete_tasks(
                table,
                pending,
                predicate,
                reference,
                properties,
                True,
            )
            pending = following
        return self._rewrite_delete_tasks(
            table,
            pending,
            predicate,
            reference,
            properties,
            True,
            additions=additions,
        )

    def merge_arrow_table(
        self,
        chunk: pyarrow.Table,
        merge_by: bool | Sequence[str] | None = True,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> tuple[int, int]:
        """Merge one table, then expire snapshots under the configured cutoff."""
        with self._write(snapshot_expiry):
            return self._merge_arrow_table(
                chunk,
                merge_by,
                branch=branch,
                properties=properties,
            )

    def _merge_arrow_table(
        self,
        chunk: pyarrow.Table,
        merge_by: bool | Sequence[str] | None = True,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> tuple[int, int]:
        """One chunk merged into the table: `(rows updated, rows inserted)`."""
        self.__dict__.pop("_insert_upper", None)
        table = self.get_or_create_table()
        join = self._row_merge_columns(merge_by)
        if not join:
            raise ValueError("merge_arrow_table needs columns to merge on")
        if chunk.num_rows == 0:
            _checked_merge_chunk(table, chunk, join)
            # Nothing to match, and the schema was still worth checking: a scan
            # for it would read the table to discover that, and `_key_ranges`
            # has no bounds to build from.
            return 0, 0
        reference = self._branch_name(branch)
        head = self._branch_head(table, reference)
        partitions = _partition_columns(table)
        partition = None
        if partitions:
            runs = list(_partition_runs(_grouped_partition_chunk(chunk, partitions), partitions))
            if head is None:
                # PyIceberg already writes every partition of one table in
                # parallel. With no stored snapshot there is nothing to match,
                # so the initial merge is one transaction across every part.
                additions = [_checked_merge_chunk(table, run, join) for _, _, run in runs]
                fresh = pyarrow.concat_tables(additions, promote_options="none")
                self._append_chunk(
                    table,
                    fresh,
                    reference,
                    properties or {},
                    rebuild=False,
                )
                return 0, fresh.num_rows
            if len(runs) > 1:
                updated = inserted = 0
                for _, _, run in runs:
                    changed, fresh = self._merge_arrow_table(
                        run,
                        join,
                        branch=branch,
                        properties=properties,
                    )
                    updated += changed
                    inserted += fresh
                return updated, inserted
            _, partition, chunk = runs[0]
        chunk = _checked_merge_chunk(table, chunk, join)
        # The chunk's own shape is the one everything is brought onto: an Arrow
        # join refuses to match a `string` key against the `large_string` a scan
        # hands back, and converting what was *read* costs less than converting
        # what is being written -- a streaming merge reads far fewer rows than
        # it writes.
        shape = Field.from_(chunk.schema)
        derived = self.derived_columns()
        delete_columns = list(
            dict.fromkeys([*(column.source for column in partitions or ()), *join])
        )
        key_shape = Field.from_(
            pyarrow.schema([chunk.schema.field(name) for name in delete_columns])
        )
        scan = table.scan(row_filter=_key_ranges(chunk, join, derived))
        scan = self._branch_scan(table, scan, reference)
        # Range filters are safe supersets. Decode only the keys needed to turn
        # that broad candidate set into exact matches; payload is read in the
        # second phase only when at least one exact key exists.
        selected = self._selected(key_shape, scan)
        if not set(join).issubset(selected.values()):
            # A branch can point at a snapshot from before a merge key was
            # added. No row under that snapshot can carry the key, so every
            # incoming row is new; projecting an arbitrary fallback column
            # would only turn that append into a schema error.
            table = self._append_chunk(
                table,
                chunk,
                reference,
                properties or {},
                rebuild=False,
            )
            return 0, chunk.num_rows
        delete_columns = [name for name in delete_columns if name in selected.values()]
        scan = scan.select(*selected)
        scan = _scoped_partition_scan(scan, table, partition)
        # Planned once and read from that plan: `to_arrow_batch_reader` plans
        # again on its own, and a streaming merge pays planning per chunk. Keep
        # the plan lazy as well as its data: a replay may finish from the first
        # file, so collecting every remaining task would retain metadata it
        # never needs.
        tasks = iter(_tasks_in_partition(table, scan.plan_files(), partition))
        first_task = next(tasks, None)
        if first_task is None:
            # No stored file overlaps the chunk's key ranges, so no stored row
            # can match: the merge *is* an append, with nothing read and
            # nothing to compare. A stream of new keys -- the log-ingest case
            # this exists for -- lands every chunk here.
            table = self._append_chunk(
                table,
                chunk,
                reference,
                properties or {},
                rebuild=False,
            )
            return 0, chunk.num_rows
        # The range scan stays one planned file at a time. Its retained positions
        # and exact key rows are bounded by this source chunk, never by the table.
        matched_positions: list[pyarrow.Array] = []
        matched_rows: list[pyarrow.Table] = []
        matched_count = 0
        with _unordered_reader(
            scan, itertools.chain((first_task,), tasks), group_size=1
        ) as planned:
            for batch in _under_current_names(table, planned):
                candidate = pyarrow.Table.from_batches([batch])
                candidate = _align_keys(candidate, chunk, join)
                candidate = semi_join(candidate, chunk, join)
                if not candidate.num_rows:
                    continue
                positions = _matching_positions(chunk, candidate, join)
                if len(positions) != candidate.num_rows:
                    raise ValueError("Target table has duplicate rows, aborting upsert")
                matched_count += len(positions)
                if matched_count > chunk.num_rows:
                    raise ValueError("Target table has duplicate rows, aborting upsert")
                matched_positions.append(positions)
                matched_rows.append(candidate.select(delete_columns))
        if not matched_positions:
            # The range overlapped stored rows, but the exact keys did not.
            # There is nothing left to compare or anti-join: this is an append.
            table = self._append_chunk(
                table,
                chunk,
                reference,
                properties or {},
                rebuild=False,
            )
            return 0, chunk.num_rows

        matched = pyarrow.concat_arrays(matched_positions)
        if len(pyarrow.compute.unique(matched)) != len(matched):
            raise ValueError("Target table has duplicate rows, aborting upsert")
        keep = pyarrow.compute.invert(
            pyarrow.compute.is_in(arrays.sequence(chunk.num_rows), value_set=matched)
        )
        inserts = chunk.filter(keep)

        exact_rows = pyarrow.concat_tables(matched_rows, promote_options="none")
        exact_scan = table.scan(row_filter=_stored_match_filter(exact_rows, delete_columns))
        exact_scan = self._branch_scan(table, exact_scan, reference)
        exact_scan = exact_scan.select(*self._selected(shape, exact_scan))
        exact_scan = _scoped_partition_scan(exact_scan, table, partition)
        changed_positions: list[pyarrow.Array] = []
        delete_rows: list[pyarrow.Table] = []
        exact_count = 0
        exact_tasks = iter(_tasks_in_partition(table, exact_scan.plan_files(), partition))
        with _unordered_reader(exact_scan, exact_tasks, group_size=1) as planned:
            for batch in _under_current_names(table, planned):
                candidate = pyarrow.Table.from_batches([batch])
                candidate = _align_keys(candidate, chunk, join)
                candidate = semi_join(candidate, chunk, join)
                if not candidate.num_rows:
                    continue
                candidate = shape.cast_arrow_table(candidate)
                positions = _matching_positions(chunk, candidate, join)
                if len(positions) != candidate.num_rows:
                    raise ValueError("Target table has duplicate rows, aborting upsert")
                exact_count += len(positions)
                source = chunk.take(positions)
                changed = _changed(source, candidate, join)
                if changed.num_rows:
                    local = _matching_positions(source, changed, join)
                    changed_positions.append(positions.take(local))
                    delete_rows.append(semi_join(candidate, changed, join).select(delete_columns))
        if exact_count != len(matched):
            raise RuntimeError("exact merge scan disagreed with its key scan")
        updates = (
            chunk.take(pyarrow.compute.sort_indices(pyarrow.concat_arrays(changed_positions)))
            if changed_positions
            else chunk.slice(0, 0)
        )
        if len(updates) == 0 and len(inserts) == 0:
            return 0, 0
        if len(updates) == 0:
            table = self._append_chunk(
                table,
                inserts,
                reference,
                properties or {},
                rebuild=False,
            )
        else:
            from pyiceberg.expressions import And

            deleted = pyarrow.concat_tables(delete_rows)
            # The match filter decides what is *deleted*, so it stays exact;
            # stored partition sources name the transformed partition without
            # becoming row equality keys. The ranges only narrow that exact
            # predicate; they never decide what is removed.
            predicate = And(
                _stored_match_filter(deleted, delete_columns),
                _key_ranges(updates, join, derived),
            )
            self._commit_merge(
                table,
                updates,
                inserts,
                predicate,
                reference,
                properties or {},
            )
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
        commit_batch_num: int | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> int:
        """Append a stream, then expire snapshots under the configured cutoff."""
        with self._write(snapshot_expiry):
            return self._append_arrow_reader(
                source,
                schema,
                merge_by,
                commit_row_size,
                commit_batch_num=commit_batch_num,
                branch=branch,
                properties=properties,
            )

    def _append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        *,
        commit_batch_num: int | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> int:
        """Append a stream, inserting only the keys the table does not hold yet."""
        reader: pyarrow.RecordBatchReader | None = None
        try:
            rows, batches = self._commit_limits(commit_row_size, commit_batch_num)
            table = self.get_or_create_table()
            join = self._row_merge_columns(merge_by)
            reader = self.target_field(schema).cast_arrow_reader(source)
            reference = self._branch_name(branch)
            self._branch_head(table, reference)
            snapshot = properties or {}
            if not join:
                self.__dict__.pop("_insert_upper", None)
                inserted = 0
                for chunk in arrow_chunks(reader, rows, batches):
                    table = self._append_chunk(table, chunk, reference, snapshot)
                    inserted += chunk.num_rows
                    # `arrow_chunks` accumulates the next chunk while this name
                    # still holds the last one.
                    del chunk
                return inserted
            inserted = 0
            for chunk in arrow_chunks(reader, rows, batches):
                inserted += self.insert_arrow_table(
                    chunk, join, branch=reference, properties=properties
                )
                del chunk
            return inserted
        finally:
            _close_write_source(source, reader)

    def insert_arrow_table(
        self,
        chunk: pyarrow.Table,
        merge_by: bool | Sequence[str] | None = True,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> int:
        """Insert one table, then expire snapshots under the configured cutoff."""
        with self._write(snapshot_expiry):
            return self._insert_arrow_table(
                chunk,
                merge_by,
                branch=branch,
                properties=properties,
            )

    def _insert_arrow_table(
        self,
        chunk: pyarrow.Table,
        merge_by: bool | Sequence[str] | None = True,
        *,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> int:
        """One chunk appended where no stored row matches: rows inserted."""
        table = self.get_or_create_table()
        join = self._row_merge_columns(merge_by)
        if not join:
            raise ValueError("insert_arrow_table needs columns to merge on")
        if SOURCE_INDEX in join or TARGET_INDEX in join:
            raise ValueError(
                f"{SOURCE_INDEX} and {TARGET_INDEX} are reserved for joining DataFrames"
            )
        if chunk.num_rows == 0:
            return 0
        reference = self._branch_name(branch)
        head = self._branch_head(table, reference)
        partitions = _partition_columns(table)
        partition = None
        if partitions:
            runs = list(_partition_runs(_grouped_partition_chunk(chunk, partitions), partitions))
            if head is None:
                # An empty table has no keys to scan. Collapse duplicates
                # within each transformed partition, then let PyIceberg land
                # every partition in one append transaction.
                additions = [first_rows(normalised_keys(run, join), join) for _, _, run in runs]
                fresh = pyarrow.concat_tables(additions, promote_options="none")
                _validate_merge_keys(fresh, join)
                table = self._append_chunk(
                    table,
                    fresh,
                    reference,
                    properties or {},
                    rebuild=False,
                )
                span = self._insert_span(fresh, join, reference, None)
                self._remember_inserted(span, table, establish=True)
                return fresh.num_rows
            if len(runs) > 1:
                return sum(
                    self._insert_arrow_table(
                        run,
                        join,
                        branch=branch,
                        properties=properties,
                    )
                    for _, _, run in runs
                )
            _, partition, chunk = runs[0]
        chunk = first_rows(normalised_keys(chunk, join), join)
        # `_key_ranges` raises on a null or NaN key before the scan is even
        # built, which is the same refusal a merge makes.
        row_filter = _key_ranges(chunk, join, self.derived_columns())
        span = self._insert_span(chunk, join, reference, partition)
        empty = head is None
        snapshot_id = head.snapshot_id if head is not None else None
        if span is not None and self._strictly_after_inserted(span, empty, snapshot_id):
            # Once this object has filled an empty table, the upper bound is
            # exact. A later chunk strictly above it cannot match a stored key,
            # so planning manifests and files can only rediscover that fact.
            table = self._append_chunk(
                table,
                chunk,
                reference,
                properties or {},
                rebuild=False,
            )
            self._remember_inserted(span, table, establish=empty)
            return chunk.num_rows
        scan = table.scan(row_filter=row_filter)
        scan = self._branch_scan(table, scan, reference)
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
            table = self._append_chunk(
                table,
                chunk,
                reference,
                properties or {},
                rebuild=False,
            )
            return chunk.num_rows
        scan = scan.select(*wanted)
        scan = _scoped_partition_scan(scan, table, partition)
        # Planned once, like a merge. Each streamed key batch removes the rows
        # it already holds; no stored key table or file-task list is
        # accumulated, and a complete replay stops planning and opening later
        # files as soon as nothing remains.
        tasks = iter(_tasks_in_partition(table, scan.plan_files(), partition))
        first_task = next(tasks, None)
        fresh = keys_of(chunk, join, SOURCE_INDEX)
        if first_task is not None:
            with _unordered_reader(
                scan, itertools.chain((first_task,), tasks), group_size=1
            ) as planned:
                for batch in _under_current_names(table, planned):
                    stored = pyarrow.Table.from_batches([keys.cast_arrow_batch(batch)])
                    fresh = fresh.join(
                        stored.select(list(join)), keys=list(join), join_type="left anti"
                    )
                    if not fresh.num_rows:
                        break
        if fresh.num_rows:
            positions = fresh.column(SOURCE_INDEX).combine_chunks()
            fresh = chunk.take(positions.take(pyarrow.compute.sort_indices(positions)))
        if fresh.num_rows:
            table = self._append_chunk(
                table,
                fresh,
                reference,
                properties or {},
                rebuild=False,
            )
            self._remember_inserted(span, table, establish=empty)
        return fresh.num_rows

    def _insert_span(
        self,
        chunk: pyarrow.Table,
        join: Sequence[str],
        reference: str,
        partition: Mapping[str, Any] | None,
    ) -> _InsertSpan | None:
        """The first declared sort key's `(cache key, minimum, maximum)`."""
        ordered = self.sort_fields()
        if not ordered or ordered[0][0] not in join:
            return None
        name, direction = ordered[0]
        try:
            bounds = pyarrow.compute.min_max(chunk.column(name))
        except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError):
            return None
        lower, upper = bounds["min"], bounds["max"]
        if not lower.is_valid or not upper.is_valid:
            return None
        scope = None
        if partition is not None:
            scope = _partition_identity(_partition_key(self.iceberg_table, partition).partition)
        return (reference, tuple(join), name, direction, scope), lower, upper

    def _strictly_after_inserted(
        self,
        span: _InsertSpan,
        empty: bool,
        snapshot_id: int | None,
    ) -> bool:
        """Whether `span` is provably disjoint from every inserted key."""
        key, lower, upper = span
        remembered = self.__dict__.get("_insert_upper", {}).get(key)
        if remembered is None:
            return empty
        frontier, bounded_snapshot = remembered
        if bounded_snapshot != snapshot_id:
            bounds = self.__dict__["_insert_upper"]
            del bounds[key]
            if not bounds:
                del self.__dict__["_insert_upper"]
            return False
        compare = pyarrow.compute.less if key[3] == "descending" else pyarrow.compute.greater
        value = upper if key[3] == "descending" else lower
        return bool(compare(value, frontier).as_py())

    def _remember_inserted(
        self,
        span: _InsertSpan | None,
        table: Any,
        *,
        establish: bool = False,
    ) -> None:
        """Advance the directional insert frontier; never infer one mid-table."""
        if span is None:
            return
        key, lower, upper = span
        remembered = self.__dict__.get("_insert_upper")
        if remembered is None:
            if not establish:
                return
            remembered = self.__dict__.setdefault("_insert_upper", {})
        previous = remembered.get(key)
        if previous is None and not establish:
            return
        bounded = previous[0] if previous is not None else None
        candidate = lower if key[3] == "descending" else upper
        compare = pyarrow.compute.less if key[3] == "descending" else pyarrow.compute.greater
        if bounded is None or compare(candidate, bounded).as_py():
            bounded = candidate
        head = self._branch_head(table, key[0])
        remembered[key] = (bounded, head.snapshot_id if head is not None else None)

    def sorted(self, chunk: pyarrow.Table) -> pyarrow.Table:
        """`chunk` in `sort_by` order, or exactly as it came when nothing says."""
        fields = self.sort_fields()
        if not fields or chunk.num_rows < 2 or _in_sort_order(chunk, fields):
            return chunk
        return chunk.sort_by(fields)

    def _commit_limits(
        self,
        requested_rows: int | None,
        requested_batches: int | None,
    ) -> tuple[int | None, int]:
        """The row and batch bounds one streaming commit may retain."""
        rows = self.commit_row_size if requested_rows is None else requested_rows
        batches = self.commit_batch_num if requested_batches is None else requested_batches
        if rows is not None:
            rows = _positive_int(rows, "commit_row_size")
        return rows, _positive_int(batches, "commit_batch_num")

    def sort_fields(self) -> list[tuple[str, str]]:
        """Physical Arrow sort fields, with normalized directions."""
        table = self.__dict__.get("iceberg_table")
        if table is not None:
            order_id = table.sort_order().order_id
            if self.__dict__.get("_table_sort_order_id") != order_id:
                self.__dict__.pop("table_field", None)
            shape = self.table_field
        elif self.sort_by is not None:
            return [(name, "ascending") for name in self.sort_by]
        else:
            shape = self.field
            return [
                (name, _sort_direction(direction)) for name, direction in shape.sort_keys().items()
            ]
        return [
            (name, _sort_direction(direction))
            for name, direction in (shape.sort_keys().items() if shape is not None else ())
        ]

    def sort_columns(self) -> list[str]:
        """Columns a chunk is sorted by: what was asked for, or what is declared."""
        return [name for name, _ in self.sort_fields()]

    @contextmanager
    def _write(self, snapshot_expiry: SnapshotExpiry) -> Iterator[None]:
        """Expire once after the outermost successful public write.

        The audit record is here for the same reason the expiry is: this is
        the one place that knows an operation *finished*, whatever it commits
        inside. A write that lands forty chunks is one record, not forty.
        """
        depth = int(self.__dict__.get("_write_depth", 0))
        expiry = (
            self._resolved_snapshot_expiry(snapshot_expiry, self.get_or_create_table())
            if depth == 0
            else snapshot_expiry
        )
        self.__dict__["_write_depth"] = depth + 1
        succeeded = False
        started = time.monotonic()
        try:
            yield
            succeeded = True
        finally:
            remaining = int(self.__dict__["_write_depth"]) - 1
            if remaining:
                self.__dict__["_write_depth"] = remaining
            else:
                self.__dict__.pop("_write_depth", None)
            if succeeded and depth == 0:
                self.expire_snapshots(expiry)
            if depth == 0:
                LOGGER.info(
                    "%s %s branch=%s snapshot=%s in %.0fms",
                    self.identifier,
                    "wrote" if succeeded else "failed",
                    self._branch_name(None),
                    self._logged_snapshot(),
                    (time.monotonic() - started) * 1000,
                )

    def _logged_snapshot(self) -> Any:
        """The snapshot a finished write left, or None if there is not one yet.

        Read defensively: this runs in a `finally`, where the table may not
        exist because the write is what would have created it.
        """
        try:
            table = self.iceberg_table
        except Exception:
            return None
        return getattr(table.metadata, "current_snapshot_id", None)

    def _commit_with_retry(
        self,
        table: Any,
        reference: str,
        properties: Mapping[str, str],
        commit: Callable[[Any, Mapping[str, str]], None],
        *,
        rebuild: bool = True,
    ) -> Any:
        """Settle one bounded commit and rebuild only when its plan remains valid."""
        operation_id = uuid.uuid4().hex
        summary = {**properties, OPERATION_ID: operation_id}
        for attempt in range(self.commit_retries + 1):
            try:
                commit(table, summary)
                return table
            except BaseException as error:
                if not _retryable_commit(error) or attempt >= self.commit_retries:
                    raise
                self.refresh()
                try:
                    table = self.iceberg_table
                    if _operation_committed(table, reference, operation_id):
                        return table
                except BaseException as refresh_error:
                    if not _retryable_commit(refresh_error) or attempt >= self.commit_retries:
                        raise
                if not rebuild:
                    raise
                ceiling = min(self.retry_max_backoff, self.retry_backoff * (2**attempt))
                delay = random.uniform(0.0, ceiling)
                LOGGER.debug(
                    "%s commit retry %d/%d in %.3fs after %s",
                    self.identifier,
                    attempt + 1,
                    self.commit_retries,
                    delay,
                    type(error).__name__,
                )
                time.sleep(delay)
        raise AssertionError("the commit retry loop must return or raise")

    def _resolved_snapshot_expiry(
        self, value: SnapshotExpiry, table: Any
    ) -> datetime.datetime | datetime.timedelta:
        """Validate one write cutoff before the write can commit."""
        if value is None:
            value = self.snapshot_expiry
        if value is None:
            value = self.__dict__.get("_snapshot_expiry")
        return _expiry_value(value, table)

    def expire_snapshots(self, snapshot_expiry: SnapshotExpiry = None) -> int:
        """Expire unprotected snapshots older than one absolute cutoff."""
        table = self.get_or_create_table()
        cutoff = _expiry_cutoff(self._resolved_snapshot_expiry(snapshot_expiry, table))
        expired = self._expirable(0, cutoff)
        if expired:
            table.maintenance.expire_snapshots().older_than(cutoff).commit()
        return len(expired)

    def delete(
        self,
        row_filter: Any = None,
        *,
        branch: str | None = None,
        case_sensitive: bool = True,
        commit_file_count: int = 16,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> int:
        """Delete matching rows through partition-bounded PyIceberg commits.

        Strings use PyIceberg's SQL predicate grammar. A BooleanExpression is
        carried unchanged. Candidate files stay in partition-local bounded
        commits and partial files are filtered one RecordBatch at a time.
        """
        if commit_file_count <= 0:
            raise ValueError("commit_file_count must be positive")
        if not self.exists:
            return 0
        expression = _delete_expression(row_filter)
        with self._write(snapshot_expiry):
            return self._delete_where(
                expression,
                branch=branch,
                case_sensitive=case_sensitive,
                commit_file_count=commit_file_count,
                properties=properties,
            )

    def delete_where(
        self,
        row_filter: Any,
        *,
        branch: str | None = None,
        case_sensitive: bool = True,
        commit_file_count: int = 16,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> int:
        """Delete the rows named by one SQL or PyIceberg expression."""
        if row_filter is None:
            raise ValueError("delete_where needs a row filter; delete() removes every row")
        return self.delete(
            row_filter,
            branch=branch,
            case_sensitive=case_sensitive,
            commit_file_count=commit_file_count,
            properties=properties,
            snapshot_expiry=snapshot_expiry,
        )

    def _delete_where(
        self,
        expression: Any,
        *,
        branch: str | None,
        case_sensitive: bool,
        commit_file_count: int,
        properties: dict[str, str] | None,
    ) -> int:
        """One planned delete, returning the number of rows removed."""
        self.__dict__.pop("_insert_upper", None)
        table = self.iceberg_table
        reference = self._branch_name(branch)
        if self._branch_head(table, reference) is None:
            return 0
        before = _branch_records(table, reference)
        scan = table.scan(row_filter=expression)
        scan = self._branch_scan(table, scan, reference)
        for tasks in _delete_task_groups(scan.plan_files(), commit_file_count):
            table = self._rewrite_delete_tasks(
                table,
                tasks,
                expression,
                reference,
                properties or {},
                case_sensitive,
            )
        after = _branch_records(table, reference)
        return max(before - after, 0)

    def _rewrite_delete_tasks(
        self,
        table: Any,
        tasks: Sequence[Any],
        expression: Any,
        reference: str,
        properties: Mapping[str, str],
        case_sensitive: bool,
        additions: pyarrow.Table | None = None,
    ) -> Any:
        """Rewrite a bounded group of candidate files as streamed batches."""
        from pyiceberg.expressions import AlwaysTrue
        from pyiceberg.expressions.visitors import ROWS_MUST_MATCH, _StrictMetricsEvaluator, bind
        from pyiceberg.io.pyarrow import ArrowScan, _expression_to_complementary_pyarrow
        from pyiceberg.table import FileScanTask

        schema = table.schema()
        bound = bind(schema, expression, case_sensitive)
        preserve = _expression_to_complementary_pyarrow(bound, schema)
        strict = _StrictMetricsEvaluator(schema, expression, case_sensitive).eval
        scanner = ArrowScan(table.metadata, table.io, schema, AlwaysTrue(), case_sensitive)
        file_rows = max(
            *(int(task.file.record_count) for task in tasks),
            additions.num_rows if additions is not None else 0,
            1,
        )
        originals: list[Any] = []
        replacements: list[_StagedPartition] = []
        with _PartitionStager(table, self.sort_fields(), file_rows) as stager:
            for task in tasks:
                if strict(task.file) == ROWS_MUST_MATCH:
                    originals.append(task.file)
                    continue
                partition = _task_partition(table, task)
                stager.start(partition)
                read_rows = kept_rows = 0
                full_task = FileScanTask(task.file, task.delete_files, AlwaysTrue())
                for batch in _task_batches(scanner, table.io, (full_task,)):
                    source = pyarrow.Table.from_batches([batch])
                    retained = source.filter(preserve)
                    read_rows += source.num_rows
                    kept_rows += retained.num_rows
                    if retained.num_rows:
                        stager.write(retained)
                staged = stager.finish()
                if kept_rows == read_rows:
                    continue
                originals.append(task.file)
                replacements.append(staged)
            if additions is not None and additions.num_rows:
                replacements.extend(_stage_chunk(table, additions, stager))
            if not originals and not replacements:
                return table

            def commit(current: Any, summary: Mapping[str, str]) -> None:
                with _track_outputs() as generated:
                    transaction = current.transaction()
                    try:
                        _ensure_name_mapping(transaction)
                        with transaction.update_snapshot(
                            snapshot_properties=dict(summary), branch=reference
                        ).overwrite() as overwrite:
                            for original in originals:
                                overwrite.delete_data_file(original)
                            for replacement in replacements:
                                for data_file in replacement.data_files:
                                    overwrite.append_data_file(data_file)
                    except BaseException:
                        _discard_paths(current.io, generated)
                        raise
                    _commit_staged(
                        current,
                        transaction,
                        stager,
                        replacements,
                        generated,
                    )

            return self._commit_with_retry(
                table,
                reference,
                properties,
                commit,
                rebuild=False,
            )

    # -- snapshots and branches ---------------------------------------------

    def snapshots(self) -> pyarrow.Table:
        """Every snapshot, as Iceberg's own metadata table."""
        return self.iceberg_table.inspect.snapshots()

    def refs(self) -> dict[str, Any]:
        """Branches and tags, by name."""
        return dict(self.iceberg_table.refs())

    def create_branch(self, name: str, snapshot_id: int | None = None) -> IcebergDataset:
        """Branch off the current state, or off `snapshot_id`."""
        if name in ROOT_BRANCHES:
            raise ValueError(f"{name!r} is a reserved spelling for the root branch")
        table = self.iceberg_table
        head = table.current_snapshot()
        current = snapshot_id or (head.snapshot_id if head else None)
        if current is None:
            raise ValueError(
                f"{self.identifier!r} has no snapshot to branch from; write to it first"
            )
        with table.manage_snapshots() as manage:
            manage.create_branch(snapshot_id=current, branch_name=name)
        return self.refresh()

    def remove_branch(self, name: str) -> IcebergDataset:
        """Drop a branch, keeping whatever `main` still references."""
        if name in ROOT_BRANCHES:
            raise ValueError(f"{name!r} is a reserved spelling for the root branch")
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
        planned = {
            "files": len(tasks),
            "rows": sum(task.file.record_count for task in tasks),
            "bytes": sum(task.file.file_size_in_bytes for task in tasks),
        }
        LOGGER.debug(
            "%s planned %d files, %d rows, %d bytes",
            self.identifier,
            planned["files"],
            planned["rows"],
            planned["bytes"],
        )
        return planned

    def compaction_plan(
        self, min_files: int = 2, *, branch: str | None = None
    ) -> list[tuple[Any, int]]:
        """`(row filter, file count)` for every part of the table worth rewriting."""
        return [(part, count) for _, part, count in self._plan_rows(min_files, branch)]

    def _plan_rows(self, min_files: int, branch: str | None) -> list[tuple[str, Any, int]]:
        """`compaction_plan`, with the mark key each part is recorded under."""
        table = self.iceberg_table
        reference = self._branch_name(branch)
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
        head = self._branch_head(table, reference)
        if head is None:
            self.__dict__["_partitions"] = (key, [])
            return []
        found = table.inspect.partitions(snapshot_id=head.snapshot_id)
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
        reference = self._branch_name(branch)
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
        LOGGER.info(
            "%s compacted %d parts into %d rows on %s",
            self.identifier,
            len(touched),
            rewritten,
            reference,
        )
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
            LOGGER.info(
                "%s expired %d snapshots, orphans left alone%s",
                self.identifier,
                report["expired"],
                " (dry run)" if dry_run else "",
            )
            return report
        orphans = self._orphans(orphan_age, metadata=metadata)
        report["deleted"] = len(orphans)
        report["bytes"] = int(sum(size for *_, size in orphans))
        if not dry_run:
            self._sweep(orphans)
        LOGGER.info(
            "%s expired %d snapshots and swept %d files (%d bytes)%s",
            self.identifier,
            report["expired"],
            report["deleted"],
            report["bytes"],
            " (dry run)" if dry_run else "",
        )
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
        # One live set guards every listing. `write.data.path` may overlap the
        # metadata root, so a file is live when anything live names it, never
        # because of the directory listing that happened to find it.
        data, files = self._live(table)
        live = data | files
        directories = [self._data_path(table)]
        if metadata:
            directories.append(self._metadata_path(table))

        found: dict[str, tuple[Any, str, str, int]] = {}
        for directory in directories:
            filesystem, base = _store_of(table, directory)
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
            for info in sorted(filesystem.get_file_info(selector), key=lambda item: item.path):
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
                if older_than > _NO_GRACE:
                    # A missing timestamp cannot prove that another writer's
                    # uncommitted file is old enough to delete.
                    if info.mtime is None or info.mtime > cutoff:
                        continue
                # Keyed by path, because one nested directory inside another is
                # listed under both and a file deleted twice raises the second
                # time -- which would abort the sweep and lose its report.
                found.setdefault(
                    info.path, (filesystem, info.path, f"{directory.rstrip('/')}/{name}", info.size)
                )
        return list(found.values())

    def _sweep(self, orphans: Sequence[tuple[Any, str, str, int]]) -> None:
        """Delete what the sweep found through each listing's exact store."""
        from yggdryl import IOBase

        for filesystem, path, _, _ in orphans:
            # Another sweeper may delete the listed file before this one.
            try:
                IOBase.from_fs(filesystem, path).unlink()
            except FileNotFoundError:
                pass

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
        properties = self.iceberg_table.properties
        updates = {
            name: value for name, value in MAINTENANCE_PROPERTIES.items() if name not in properties
        }
        if properties.get(MERGE_MANIFESTS) != "true":
            updates[MERGE_MANIFESTS] = "true"
        if updates:
            # A no-op commit is still a metadata version, so a scheduled pass
            # only supplies declarations the table does not already carry.
            self.set_properties(updates)
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
    scan: Any,
    tasks: Iterable[Any],
    columns: Sequence[tuple[str, str]],
) -> pyarrow.RecordBatchReader:
    """Read partition paths in order, sorting and merging files on `columns`.

    Iceberg sort orders describe file layout, not result order. Plans may list
    newer manifests first, so a stateful consumer cannot use plan order.
    A file with another recorded layout is externally sorted in bounded runs.
    Partitions are independent storage streams: finish one canonical path
    before opening the next, and merge overlapping file ranges only within it.
    """
    from pyiceberg.conversions import from_bytes
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    from rekep.iceberg.fields import narrowed

    target = narrowed(schema_to_pyarrow(scan.projection()))
    primary, primary_direction = columns[0]
    field = scan.projection().find_field(primary)
    floating_primary = pyarrow.types.is_floating(target.field(primary).type)

    def bound(task: Any, upper: bool) -> Any | None:
        values = task.file.upper_bounds if upper else task.file.lower_bounds
        raw = (values or {}).get(field.field_id)
        return None if raw is None else from_bytes(field.field_type, raw)

    def bounds_cover_every_value(task: Any) -> bool:
        # Iceberg bounds omit nulls and NaNs. Only an explicit zero metric (or
        # a required field for nulls) proves concatenating disjoint ranges is
        # safe; missing metrics are unknown, not zero.
        nulls = (task.file.null_value_counts or {}).get(field.field_id)
        if not field.required and nulls != 0:
            return False
        nans = (task.file.nan_value_counts or {}).get(field.field_id)
        return not floating_primary or nans == 0

    def batches() -> Iterator[pyarrow.RecordBatch]:
        for _, partition in _partition_tasks(scan, tasks):
            ranged = [(bound(task, False), bound(task, True), task) for task in partition]
            if any(
                lower is None or upper is None or not bounds_cover_every_value(task)
                for lower, upper, task in ranged
            ):
                groups = [partition]
            elif primary_direction == "descending":
                ranged.sort(key=lambda item: (item[1], str(item[2].file.file_path)), reverse=True)
                groups = []
                held = []
                low = None
                for lower, upper, task in ranged:
                    if held and upper < low:
                        groups.append(held)
                        held = []
                        low = None
                    held.append(task)
                    low = lower if low is None or lower < low else low
                if held:
                    groups.append(held)
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

    return OwnedRecordBatchReader(target, batches(), lambda: None)


def _sorted_task_batches(
    scan: Any, task: Any, columns: Sequence[tuple[str, str]]
) -> Iterator[pyarrow.RecordBatch]:
    """One file in requested order, externally sorted when its layout differs."""
    if not _task_is_sorted_on(scan, task, columns):
        yield from _externally_sorted_task_batches(scan, task, columns)
        return
    previous = None
    with _planned_reader(scan, [task]) as reader:
        for batch in reader:
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
    scan: Any, tasks: Sequence[Any], columns: Sequence[tuple[str, str]]
) -> Iterator[pyarrow.RecordBatch]:
    """K-way merge overlapping sorted files, moving slices rather than rows."""
    streams = [iter(_sorted_task_batches(scan, task, columns)) for task in tasks]
    yield from _merge_batch_streams(streams, columns)


def _merge_batch_streams(
    streams: Sequence[Iterator[pyarrow.RecordBatch]],
    columns: Sequence[tuple[str, str]],
) -> Iterator[pyarrow.RecordBatch]:
    """K-way merge sorted batch streams without copying their rows."""
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

    try:
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
    finally:
        for stream in streams:
            close = getattr(stream, "close", None)
            if close is not None:
                close()


def _task_is_sorted_on(
    scan: Any,
    task: Any,
    columns: Sequence[tuple[str, str]],
) -> bool:
    """Whether a data file records `columns` as a physical sort prefix."""
    order_id = task.file.sort_order_id
    if order_id is None:
        return False
    order = next(
        (one for one in scan.table_metadata.sort_orders if one.order_id == order_id),
        None,
    )
    if order is None:
        return False
    from pyiceberg.table.sorting import NullOrder, SortDirection
    from pyiceberg.transforms import IdentityTransform

    recorded = []
    schema = scan.projection()
    for field in order.fields:
        name = schema.find_column_name(field.source_id)
        if (
            not name
            or "." in name
            or not isinstance(field.transform, IdentityTransform)
            or field.null_order != NullOrder.NULLS_LAST
        ):
            return False
        direction = "descending" if field.direction == SortDirection.DESC else "ascending"
        recorded.append((name, direction))
        if len(recorded) == len(columns):
            break
    return tuple(recorded) == tuple(columns)


def _externally_sorted_task_batches(
    scan: Any,
    task: Any,
    columns: Sequence[tuple[str, str]],
) -> Iterator[pyarrow.RecordBatch]:
    """Sort one file through bounded Arrow IPC runs on local disk."""
    with tempfile.TemporaryDirectory(prefix="rekep-iceberg-sort-") as directory:
        runs: list[str] = []
        schema = None
        with _planned_reader(scan, [task]) as reader:
            schema = reader.schema
            for index, batch in enumerate(reader):
                if not batch.num_rows:
                    continue
                table = pyarrow.Table.from_batches([batch], schema=reader.schema)
                if not _in_sort_order(table, columns):
                    table = table.sort_by(list(columns))
                path = os.path.join(directory, f"0-{index}.arrow")
                _write_ipc_batches(path, reader.schema, table.to_batches())
                runs.append(path)
        if not runs or schema is None:
            return

        generation = 1
        while len(runs) > 1:
            merged: list[str] = []
            for index in range(0, len(runs), SORT_MERGE_FAN_IN):
                group = runs[index : index + SORT_MERGE_FAN_IN]
                if len(group) == 1:
                    merged.append(group[0])
                    continue
                target = os.path.join(directory, f"{generation}-{index // SORT_MERGE_FAN_IN}.arrow")
                streams = [iter(_ipc_batches(path)) for path in group]
                _write_ipc_batches(target, schema, _merge_batch_streams(streams, columns))
                for path in group:
                    os.unlink(path)
                merged.append(target)
            runs = merged
            generation += 1
        yield from _ipc_batches(runs[0])


def _write_ipc_batches(
    path: str,
    schema: pyarrow.Schema,
    batches: Iterable[pyarrow.RecordBatch],
) -> None:
    """Write one external-sort run without collecting its batches."""
    import pyarrow.ipc

    with pyarrow.ipc.new_file(path, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)


def _ipc_batches(path: str) -> Iterator[pyarrow.RecordBatch]:
    """Stream one Arrow IPC sort run and release its mapping."""
    import pyarrow.ipc

    with pyarrow.memory_map(path, "r") as source:
        reader = pyarrow.ipc.open_file(source)
        for index in range(reader.num_record_batches):
            yield reader.get_batch(index)


@functools.total_ordering
@dataclasses.dataclass(frozen=True)
class _Descending:
    """A scalar whose Python ordering is reversed."""

    value: Any

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, _Descending):
            return NotImplemented
        return bool(self.value > other.value)


_SORT_DIRECTIONS = {
    "asc": "ascending",
    "ascending": "ascending",
    "desc": "descending",
    "descending": "descending",
}


def _sort_direction(direction: Any) -> str:
    """One Arrow direction spelling from a declaration or Iceberg value."""
    value = str(direction).lower()
    try:
        return _SORT_DIRECTIONS[value]
    except KeyError as error:
        raise ValueError(f"unknown sort direction {direction!r}") from error


def _sort_fields(
    columns: Sequence[str] | Sequence[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Normalize name-only ascending keys and explicit direction pairs."""
    return tuple(
        (column, "ascending")
        if isinstance(column, str)
        else (str(column[0]), _sort_direction(column[1]))
        for column in columns
    )


def _row_key(
    batch: pyarrow.RecordBatch,
    columns: Sequence[str] | Sequence[tuple[str, str]],
    index: int,
) -> tuple[Any, ...]:
    """One directional lexicographic key, with nulls kept last."""
    return tuple(
        (
            2 if value is None else 1 if isinstance(value, float) and math.isnan(value) else 0,
            (
                None
                if value is None or isinstance(value, float) and math.isnan(value)
                else _Descending(value)
                if direction == "descending"
                else value
            ),
        )
        for column, direction in _sort_fields(columns)
        for value in (batch.column(column)[index].as_py(),)
    )


def _reader_in_sort_order(
    batch: pyarrow.RecordBatch | pyarrow.Table,
    columns: Sequence[str] | Sequence[tuple[str, str]],
) -> bool:
    """Whether a physical batch follows its directional, null-last ordering."""
    compute = pyarrow.compute
    ordered = None
    for name, direction in reversed(_sort_fields(columns)):
        column = batch.column(name)
        if isinstance(column, pyarrow.ChunkedArray):
            column = column.combine_chunks()
        before, after = column[:-1], column[1:]
        before_null, after_null = compute.is_null(before), compute.is_null(after)
        if pyarrow.types.is_floating(column.type):
            before_nan = compute.fill_null(compute.is_nan(before), False)
            after_nan = compute.fill_null(compute.is_nan(after), False)
        else:
            before_nan = compute.and_(before_null, compute.invert(before_null))
            after_nan = compute.and_(after_null, compute.invert(after_null))
        before_regular = compute.invert(compute.or_(before_null, before_nan))
        after_regular = compute.invert(compute.or_(after_null, after_nan))
        regular_precedes = compute.and_(
            compute.and_(before_regular, after_regular),
            compute.fill_null(
                (compute.greater if direction == "descending" else compute.less)(before, after),
                False,
            ),
        )
        precedes = compute.or_(
            regular_precedes,
            compute.or_(
                compute.and_(before_regular, compute.or_(after_nan, after_null)),
                compute.and_(before_nan, after_null),
            ),
        )
        equal = compute.or_(
            compute.or_(
                compute.and_(before_null, after_null),
                compute.and_(before_nan, after_nan),
            ),
            compute.and_(
                compute.and_(before_regular, after_regular),
                compute.fill_null(compute.equal(before, after), False),
            ),
        )
        ordered = (
            compute.or_(precedes, equal)
            if ordered is None
            else compute.or_(precedes, compute.and_(equal, ordered))
        )
    if ordered is None:
        return True
    return bool(compute.all(ordered, min_count=0).as_py())


def _upper_bound(
    batch: pyarrow.RecordBatch,
    columns: Sequence[str] | Sequence[tuple[str, str]],
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

    return OwnedRecordBatchReader(schema, batches(), reader.close)


def _planned_reader(
    scan: Any, tasks: Iterable[Any], *, group_size: int | None = None
) -> pyarrow.RecordBatchReader:
    """Read a planned stream one partition at a time with bounded read-ahead."""

    def groups() -> Iterator[Sequence[Any]]:
        for _, partition in _partition_tasks(scan, tasks):
            # ArrowScan loads delete files per call. Reopening one shared by
            # several groups costs I/O; handing it the whole partition lets
            # PyIceberg retain every delete array and decoded data file.
            yield from _grouped(partition, group_size or _read_ahead())

    return _scan_reader(scan, groups())


def _unordered_reader(
    scan: Any, tasks: Iterable[Any], *, group_size: int | None = None
) -> pyarrow.RecordBatchReader:
    """Read plan order lazily, with one file in flight while a limit is unresolved."""
    size = group_size or (_read_ahead() if scan.limit is None else 1)
    return _scan_reader(scan, _stream_groups(tasks, size))


def _scan_reader(scan: Any, groups: Iterable[Sequence[Any]]) -> pyarrow.RecordBatchReader:
    """Read bounded task groups under one global scan limit."""
    from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow

    from rekep.iceberg.fields import narrowed

    target = narrowed(schema_to_pyarrow(scan.projection()))

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
        for group in groups:
            batches = _task_batches(
                arrow(None if scan.limit is None else scan.limit - taken),
                scan.io,
                group,
            )
            try:
                for batch in batches:
                    yield batch if batch.schema.equals(target) else _narrow_batch(batch, target)
                    taken += batch.num_rows
            finally:
                close = getattr(batches, "close", None)
                if close is not None:
                    close()
            if scan.limit is not None and taken >= scan.limit:
                return

    return OwnedRecordBatchReader(target, generate(), lambda: None)


def _narrow_batch(batch: pyarrow.RecordBatch, target: pyarrow.Schema) -> pyarrow.RecordBatch:
    """One batch under the reader's declared width.

    A reader promises a schema and a consumer is entitled to it, so the cast
    happens whether or not the store agreed: pyiceberg decodes to
    `large_string` and the declared shape says `string`, and a batch that
    disagreed with its own reader is what a downstream `concat` refuses.
    """
    return pyarrow.RecordBatch.from_struct_array(
        batch.to_struct_array().cast(pyarrow.struct(list(target)))
    )


def _task_batches(scan: Any, io: Any, tasks: Sequence[Any]) -> Iterator[pyarrow.RecordBatch]:
    """Decode planned files synchronously so one task retains one batch."""
    from pyiceberg.io import pyarrow as iceberg_arrow

    decode = getattr(scan, "_record_batches_from_scan_tasks_and_deletes", None)
    read_deletes = getattr(iceberg_arrow, "_read_all_delete_files", None)
    if decode is None or read_deletes is None:
        yield from scan.to_record_batches(tasks)
        return
    deletes = read_deletes(io, tasks)
    yield from decode(tasks, deletes)


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


def _stream_groups(tasks: Iterable[Any], size: int) -> Iterator[tuple[Any, ...]]:
    """A lazy plan in bounded groups, without first collecting or sorting it."""
    planned = iter(tasks)
    while group := tuple(itertools.islice(planned, size)):
        yield group


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
    tasks = iter(scan.plan_files())
    if limit is None:
        return _unordered_reader(scan, tasks)
    exact = _always_true()
    filtered = getattr(scan, "row_filter", exact) != exact
    taken, rows = [], 0
    while rows < limit:
        try:
            task = next(tasks)
        except StopIteration:
            break
        remaining = itertools.chain(taken, (task,), tasks)
        if task.delete_files or (filtered and _null_partition(task.file.partition)):
            # Deletes may be shared by several files in a partition, and a
            # null partition can disagree with Arrow's three-valued filter.
            # Keep both on the partition-aware path whose grouping is exact.
            return _planned_reader(scan, remaining)
        if task.residual != exact:
            # The file has to answer the predicate before its contribution is
            # known. Read one at a time until enough matching rows arrive.
            return _unordered_reader(scan, remaining, group_size=1)
        taken.append(task)
        rows += task.file.record_count
    return _planned_reader(scan, taken)


def _null_partition(partition: Any) -> bool:
    """Whether a file's partition record holds a null in any field."""
    return any(partition[index] is None for index in range(len(partition)))


def _in_sort_order(
    chunk: pyarrow.Table,
    names: Sequence[str] | Sequence[tuple[str, str]],
) -> bool:
    """Whether `chunk` follows the directional lexicographic sort fields."""
    return _reader_in_sort_order(chunk, names)


def _key_ranges(
    chunk: pyarrow.Table,
    join: Sequence[str],
    derived: Mapping[str, Sequence[str]] | None = None,
) -> Any:
    """A predicate every row matching `chunk` on `join` must satisfy."""
    from pyiceberg.expressions import And

    _validate_merge_keys(chunk, join)
    terms = []
    for column in join:
        values = chunk.column(column)
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


def _validate_merge_keys(chunk: pyarrow.Table, join: Sequence[str]) -> None:
    """Refuse merge keys no Iceberg predicate can name exactly."""
    for column in join:
        values = chunk.column(column)
        if values.null_count:
            raise ValueError(
                f"column {column!r} is a merge key and cannot be null; "
                "a null key matches nothing, so merging on it would duplicate rows"
            )
        if _has_nan(values):
            raise ValueError(
                f"column {column!r} is a merge key and cannot be NaN; "
                "no predicate can name a NaN, so merging on it would duplicate rows"
            )


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
    kind = values.type
    if (pyarrow.types.is_timestamp(kind) or pyarrow.types.is_time64(kind)) and kind.unit == "ns":
        # Iceberg temporal values and their predicate literals stop at microseconds.
        # Rounding a nanosecond bound could exclude a row; no term is a safe superset.
        return None
    try:
        bounds = compute.min_max(values).as_py()
        whole = _between(column, bounds["min"], bounds["max"])
    except (ValueError, pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, OverflowError):
        # A bound this column cannot express has no honest term: omission widens
        # the filter, and a merge filter may only be wider than the rows it must find.
        return None
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


def _close_write_source(source: Any, reader: pyarrow.RecordBatchReader | None) -> None:
    """Release the owning cast reader, or a source casting never produced."""
    if reader is not None:
        reader.close()
    elif (close := getattr(source, "close", None)) is not None:
        close()


def _checked_merge_chunk(table: Any, chunk: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """A source chunk validated once for either Iceberg merge implementation."""
    from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible
    from pyiceberg.table import upsert_util

    if SOURCE_INDEX in join or TARGET_INDEX in join:
        # pyiceberg's own message, before an Arrow join fails on the duplicate.
        raise ValueError(f"{SOURCE_INDEX} and {TARGET_INDEX} are reserved for joining DataFrames")
    chunk = normalised_keys(chunk, join)
    _validate_merge_keys(chunk, join)
    if upsert_util.has_duplicate_rows(chunk, join):
        raise ValueError(
            "Duplicate rows found in source dataset based on the key columns. No upsert executed"
        )
    # PyIceberg permits an omitted optional column, but a merge replaces the
    # whole matched row and would turn that stored value into null.
    _check_pyarrow_schema_compatible(
        table.schema(),
        provided_schema=chunk.schema,
        format_version=table.format_version,
        downcast_ns_timestamp_to_us=_downcasts_ns(),
    )
    stored = [member.name for member in table.schema().fields]
    missing = [name for name in stored if name not in chunk.column_names]
    if missing:
        raise ValueError(
            f"chunk is missing {missing}, and a merge writes the row it matches: the stored "
            "values would become nulls. Cast it onto the table's shape before merging"
        )
    return chunk


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


def _stored_match_filter(rows: pyarrow.Table, columns: Sequence[str]) -> Any:
    """Exact stored coordinates, including a transformed partition's null source."""
    from pyiceberg.expressions import And, EqualTo, IsNull, Or

    fixed_null = [name for name in columns if rows.column(name).null_count == rows.num_rows]
    mixed_null = [name for name in columns if 0 < rows.column(name).null_count < rows.num_rows]
    if mixed_null:
        unique = rows.select(list(columns)).group_by(list(columns)).aggregate([])
        terms = [
            And(
                *[
                    IsNull(name) if row[name] is None else EqualTo(name, row[name])
                    for name in columns
                ]
            )
            for row in unique.to_pylist()
        ]
        return Or(*terms) if len(terms) > 1 else terms[0]
    remaining = [name for name in columns if name not in fixed_null]
    terms = [*(IsNull(name) for name in fixed_null)]
    if remaining:
        terms.append(_match_filter(rows, remaining))
    return And(*terms) if len(terms) > 1 else terms[0]


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


def _matching_positions(
    chunk: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]
) -> pyarrow.Array:
    """Positions in `chunk` whose key occurs in `matched`, in source order."""
    found = keys_of(chunk, join, SOURCE_INDEX).join(
        keys_of(matched, join, TARGET_INDEX).select(list(join)),
        keys=list(join),
        join_type="left semi",
    )
    positions = found.column(SOURCE_INDEX).combine_chunks()
    return positions.take(pyarrow.compute.sort_indices(positions))


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


def _renamed(reader: pyarrow.RecordBatchReader, names: dict[str, str]) -> Any:
    """`reader`'s batches under the names the caller asked for them by.

    Batch by batch, so nothing is materialised: a stream stays a stream. The
    mapping is `{what the scan called it: what the caller called it}`, which
    for anything but a pinned read across a rename is the identity.
    """
    if not names or all(stored == asked for stored, asked in names.items()):
        return reader
    schema = pyarrow.schema(
        [field.with_name(names.get(field.name, field.name)) for field in reader.schema],
        metadata=reader.schema.metadata,
    )

    def batches() -> Iterator[pyarrow.RecordBatch]:
        for batch in reader:
            yield batch.rename_columns([names.get(name, name) for name in batch.schema.names])

    return OwnedRecordBatchReader(schema, batches(), reader.close)


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


@dataclasses.dataclass(frozen=True)
class _PartitionColumn:
    """One supported partition field and its Arrow transform."""

    name: str
    source: str
    transform: Any


def _delete_expression(row_filter: Any) -> Any:
    """One PyIceberg BooleanExpression, including its SQL spelling."""
    from pyiceberg.expressions import AlwaysTrue, BooleanExpression
    from pyiceberg.expressions.parser import parse

    if row_filter is None:
        return AlwaysTrue()
    if isinstance(row_filter, str):
        return parse(row_filter)
    if isinstance(row_filter, BooleanExpression):
        return row_filter
    raise TypeError("delete filter must be a SQL string or PyIceberg BooleanExpression")


def _delete_task_groups(tasks: Iterable[Any], commit_file_count: int) -> Iterator[tuple[Any, ...]]:
    """Candidate files in bounded, partition-local commit groups."""
    pending: list[Any] = []
    partition: tuple[Any, ...] | None = None
    for task in tasks:
        spec_id = getattr(task.file, "spec_id", None)
        identity = (spec_id, *_partition_identity(task.file.partition))
        if pending and (identity != partition or len(pending) >= commit_file_count):
            yield tuple(pending)
            pending.clear()
        partition = identity
        pending.append(task)
    if pending:
        yield tuple(pending)


def _task_partition(table: Any, task: Any) -> dict[str, Any]:
    """A planned file's partition under the current compatible spec."""
    current = table.spec()
    spec_id = getattr(task.file, "spec_id", None)
    stored = table.metadata.specs().get(current.spec_id if spec_id is None else int(spec_id))
    if stored is None or not current.compatible_with(stored):
        raise ValueError("streamed delete cannot mix incompatible live partition specs")
    return {field.name: task.file.partition[index] for index, field in enumerate(current.fields)}


def _branch_records(table: Any, reference: str) -> int:
    """Current record count for one branch, from its snapshot summary."""
    head = table.refs().get(reference)
    if head is None:
        return 0
    snapshot = table.metadata.snapshot_by_id(head.snapshot_id)
    try:
        return int(snapshot.summary["total-records"])
    except (AttributeError, KeyError, TypeError, ValueError):
        return 0


def _operation_committed(table: Any, reference: str, operation_id: str) -> bool:
    """Whether a retried operation occurs in the selected branch's ancestry."""
    head = table.refs().get(reference)
    snapshot_id = head.snapshot_id if head is not None else None
    while snapshot_id is not None:
        snapshot = table.metadata.snapshot_by_id(snapshot_id)
        if snapshot is None:
            return False
        if snapshot.summary.get(OPERATION_ID) == operation_id:
            return True
        snapshot_id = snapshot.parent_snapshot_id
    return False


def _retryable_commit(error: BaseException) -> bool:
    """Whether rebuilding one bounded commit can make progress."""
    from pyiceberg.exceptions import (
        CommitFailedException,
        CommitStateUnknownException,
        ConditionalCheckFailedException,
        ServerError,
        ServiceUnavailableError,
        WaitingForLockException,
    )

    if isinstance(error, FileNotFoundError | PermissionError):
        return False
    return isinstance(
        error,
        (
            CommitFailedException,
            CommitStateUnknownException,
            ConditionalCheckFailedException,
            ServerError,
            ServiceUnavailableError,
            WaitingForLockException,
            TimeoutError,
            ConnectionError,
            OSError,
        ),
    )


def _partition_columns(table: Any) -> tuple[_PartitionColumn, ...] | None:
    """The current spec's supported transformed source columns."""
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

    supported = (
        IdentityTransform,
        DayTransform,
        HourTransform,
        MonthTransform,
        YearTransform,
        BucketTransform,
        TruncateTransform,
    )
    schema = table.schema()
    spec = table.spec()
    if spec.is_unpartitioned() or any(
        not isinstance(field.transform, supported) for field in spec.fields
    ):
        return None
    return tuple(
        _PartitionColumn(
            field.name,
            schema.find_column_name(field.source_id),
            field.transform.pyarrow_transform(schema.find_field(field.source_id).field_type),
        )
        for field in spec.fields
    )


def _tasks_in_partition(
    table: Any, tasks: Iterable[Any], partition: Mapping[str, Any] | None
) -> Iterator[Any]:
    """Planned data tasks belonging to one exact transformed partition."""
    if partition is None:
        yield from tasks
        return
    current = table.spec()
    specs = table.metadata.specs()
    target = _partition_identity(_partition_key(table, partition).partition)
    for task in tasks:
        spec_id = int(getattr(task.file, "spec_id", current.spec_id) or 0)
        stored = specs.get(spec_id)
        if stored is None or not current.compatible_with(stored):
            raise ValueError("keyed write cannot mix incompatible live partition specs")
        if _partition_identity(task.file.partition) == target:
            yield task


def _scoped_partition_scan(scan: Any, table: Any, partition: Mapping[str, Any] | None) -> Any:
    """Push one exact transformed partition into local manifest planning."""
    if partition is None:
        return scan
    from pyiceberg.expressions import And

    current = table.spec()
    exact = _partition_value_filter(partition)
    for spec_id, stored in table.metadata.specs().items():
        if current.compatible_with(stored):
            scan.partition_filters[spec_id] = And(scan.partition_filters[spec_id], exact)
    return scan


def _ensure_name_mapping(transaction: Any) -> None:
    """Install the field-name fallback staged Parquet files need when read."""
    if transaction.table_metadata.name_mapping() is not None:
        return
    from pyiceberg.table import TableProperties

    mapping = transaction.table_metadata.schema().name_mapping
    transaction.set_properties(**{TableProperties.DEFAULT_NAME_MAPPING: mapping.model_dump_json()})


def _requiring_columns(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch], columns: Sequence[str]
) -> pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch]:
    """Refuse an omitted partition column before a nullable cast can invent it."""

    def validate(schema: pyarrow.Schema) -> None:
        missing = [name for name in columns if not _schema_has_column(schema, name)]
        if missing:
            raise ValueError(
                f"partition columns {missing} are missing from the source; "
                "partition overwrite cannot infer which partitions to replace"
            )

    if isinstance(source, pyarrow.RecordBatchReader):
        validate(source.schema)
        return source

    def batches() -> Iterator[pyarrow.RecordBatch]:
        try:
            for batch in source:
                validate(batch.schema)
                yield batch
        finally:
            if (close := getattr(source, "close", None)) is not None:
                close()

    return batches()


def _schema_has_column(schema: pyarrow.Schema, name: str) -> bool:
    """Whether an Arrow schema contains an exact or nested column path."""
    if name in schema.names:
        return True
    root, *nested = name.split(".")
    try:
        field = schema.field(root)
        for member in nested:
            field = field.type.field(member)
    except (KeyError, TypeError):
        return False
    return True


@dataclasses.dataclass(frozen=True)
class _StagedPartition:
    """Final data-file paths and row count for one complete partition."""

    partition: Mapping[str, Any]
    paths: tuple[str, ...]
    data_files: tuple[Any, ...]
    rows: int


@dataclasses.dataclass(frozen=True)
class _StagedBatch:
    """Consumed rows and completed source batches while a trailing partition stays open."""

    rows: int
    count: int = 1


class _PartitionHistory:
    """Exact disk-backed identities for partitions whose runs already closed."""

    def __init__(self) -> None:
        self.directory = tempfile.TemporaryDirectory(prefix="rekep-iceberg-partitions-")
        self.database = sqlite3.connect(os.path.join(self.directory.name, "closed.sqlite3"))
        self.database.execute("PRAGMA journal_mode=OFF")
        self.database.execute("PRAGMA synchronous=OFF")
        self.database.execute("PRAGMA cache_size=-1024")
        self.database.execute("CREATE TABLE closed (identity TEXT PRIMARY KEY)")

    @staticmethod
    def _key(identity: tuple[Any, ...]) -> str:
        """One typed partition identity in SQLite's scalar key space."""
        return json.dumps(identity, separators=(",", ":"))

    def __contains__(self, identity: tuple[Any, ...]) -> bool:
        return (
            self.database.execute(
                "SELECT 1 FROM closed WHERE identity = ?", (self._key(identity),)
            ).fetchone()
            is not None
        )

    def add(self, identity: tuple[Any, ...]) -> None:
        """Remember one closed run without retaining its identity in memory."""
        self.database.execute("INSERT INTO closed VALUES (?)", (self._key(identity),))

    def close(self) -> None:
        """Close and remove the temporary exact-set database."""
        self.database.close()
        self.directory.cleanup()


class _PartitionStager:
    """Bounded local Parquet staging for complete partitions."""

    def __init__(
        self,
        table: Any,
        sort_by: Sequence[tuple[str, str]],
        file_row_size: int,
    ) -> None:
        from pyiceberg.io.pyarrow import (
            _get_parquet_writer_kwargs,
            sanitize_column_names,
        )
        from pyiceberg.table import TableProperties
        from pyiceberg.table.locations import load_location_provider
        from pyiceberg.utils.properties import property_as_int

        self.table = table
        self.sort_fields = tuple(sort_by)
        self.sort_by = tuple(name for name, _ in self.sort_fields)
        self.file_row_size = max(int(file_row_size), 1)
        self.directory = tempfile.TemporaryDirectory(prefix="rekep-iceberg-")
        self.location_provider = load_location_provider(
            table_location=table.metadata.location,
            table_properties=table.metadata.properties,
        )
        self.writer_kwargs = _get_parquet_writer_kwargs(table.metadata.properties)
        schema = table.metadata.schema()
        self.requested_schema = sanitize_column_names(schema)
        self.name_mapping = schema.name_mapping
        self.downcast_ns = _downcasts_ns()
        self.row_group_size = property_as_int(
            properties=table.metadata.properties,
            property_name=TableProperties.PARQUET_ROW_GROUP_LIMIT,
            default=TableProperties.PARQUET_ROW_GROUP_LIMIT_DEFAULT,
        )
        self.uploaded: set[str] = set()
        self.partition: Mapping[str, Any] | None = None
        self.paths: list[str] = []
        self.data_files: list[Any] = []
        self.rows = 0
        self._writer: Any | None = None
        self._local: str | None = None
        self._target: str | None = None
        self._file_rows = 0
        self._last_key: tuple[Any, ...] | None = None
        self._incoming_arrow_schema: pyarrow.Schema | None = None
        self._incoming_schema: Any | None = None

    def __enter__(self) -> _PartitionStager:
        return self

    def __exit__(self, exc_type: object, _exc: object, _traceback: object) -> None:
        errors: list[Exception] = []
        try:
            self._close_file(upload=False)
        except Exception as error:
            errors.append(error)
        for path in tuple(self.uploaded):
            try:
                self.table.io.delete(path)
            except FileNotFoundError:
                pass
            except Exception as error:
                errors.append(error)
        try:
            self.directory.cleanup()
        except Exception as error:
            errors.append(error)
        if exc_type is None and errors:
            if len(errors) == 1:
                raise errors[0]
            raise ExceptionGroup("partition staging cleanup failed", errors)

    def start(self, partition: Mapping[str, Any]) -> None:
        if self.partition is not None:
            raise RuntimeError("finish the staged partition before starting another")
        self.partition = dict(partition)
        self.paths = []
        self.data_files = []
        self.rows = 0

    def write(self, chunk: pyarrow.Table) -> None:
        """Write one bounded source chunk without mixing partition values."""
        if self.partition is None:
            raise RuntimeError("start a staged partition before writing it")
        if self.sort_fields and not _in_sort_order(chunk, self.sort_fields):
            chunk = chunk.sort_by(list(self.sort_fields))
        offset = 0
        while offset < chunk.num_rows:
            available = self.file_row_size - self._file_rows
            piece = chunk.slice(offset, min(available, chunk.num_rows - offset))
            piece_batches = piece.to_batches(max_chunksize=piece.num_rows)
            first = _row_key(piece_batches[0], self.sort_fields, 0)
            if self._writer is not None and self._last_key is not None and first < self._last_key:
                self._close_file(upload=True)
                available = self.file_row_size
                piece = chunk.slice(offset, min(available, chunk.num_rows - offset))
                piece_batches = piece.to_batches(max_chunksize=piece.num_rows)
            for batch in piece_batches:
                requested = self._requested_batch(batch)
                self._open_file(requested.schema)
                self._writer.write_batch(requested, row_group_size=self.row_group_size)
            self._file_rows += piece.num_rows
            self.rows += piece.num_rows
            offset += piece.num_rows
            if self.sort_by:
                last_batch = piece_batches[-1]
                self._last_key = _row_key(
                    last_batch,
                    self.sort_fields,
                    last_batch.num_rows - 1,
                )
            if self._file_rows >= self.file_row_size:
                self._close_file(upload=True)

    def finish(self) -> _StagedPartition:
        if self.partition is None:
            raise RuntimeError("no staged partition to finish")
        self._close_file(upload=True)
        staged = _StagedPartition(
            dict(self.partition), tuple(self.paths), tuple(self.data_files), self.rows
        )
        self.partition = None
        self.paths = []
        self.data_files = []
        self.rows = 0
        return staged

    def release(self, partitions: Sequence[_StagedPartition]) -> None:
        """Leave successfully committed targets in place on context exit."""
        for partition in partitions:
            self.uploaded.difference_update(partition.paths)

    def _requested_batch(self, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """One source batch on PyIceberg's sanitized, field-id-bearing file schema."""
        from pyiceberg.io.pyarrow import _to_requested_schema, pyarrow_to_schema

        if self._incoming_arrow_schema is None or not batch.schema.equals(
            self._incoming_arrow_schema, check_metadata=True
        ):
            self._incoming_arrow_schema = batch.schema
            self._incoming_schema = pyarrow_to_schema(
                batch.schema,
                name_mapping=self.name_mapping,
                downcast_ns_timestamp_to_us=self.downcast_ns,
                format_version=self.table.metadata.format_version,
            )
        return _to_requested_schema(
            requested_schema=self.requested_schema,
            file_schema=self._incoming_schema,
            batch=batch,
            downcast_ns_timestamp_to_us=self.downcast_ns,
            include_field_ids=True,
        )

    def _open_file(self, schema: pyarrow.Schema) -> None:
        if self._writer is not None:
            return
        import pyarrow.parquet

        identifier = uuid.uuid4()
        self._local = os.path.join(self.directory.name, f"{identifier}.parquet")
        self._target = self.location_provider.new_data_location(
            data_file_name=f"{identifier}.parquet",
            partition_key=_partition_key(self.table, self.partition or {}),
        )
        self._writer = pyarrow.parquet.ParquetWriter(
            self._local,
            schema=schema,
            store_decimal_as_integer=True,
            **self.writer_kwargs,
        )
        self._file_rows = 0
        self._last_key = None

    def _close_file(self, *, upload: bool) -> None:
        writer, local, target = self._writer, self._local, self._target
        rows = self._file_rows
        self._writer = self._local = self._target = None
        self._file_rows = 0
        self._last_key = None
        if writer is None:
            return
        try:
            writer.close()
            if upload:
                data_file = _staged_data_file(self.table, local, target, self.partition or {})
                # A remote copy can create its object and then lose the
                # acknowledgement. Own the UUID target before starting it so
                # context cleanup retries deletion after either outcome.
                self.uploaded.add(target)
                copier = getattr(self.table.io, "copy_from_local", None)
                if copier is None:
                    _copy_to_output(self.table.io, local, target)
                else:
                    copier(local, target)
                self.paths.append(target)
                self.data_files.append(data_file)
                LOGGER.debug("staged %d rows to %s", rows, target)
        finally:
            try:
                os.unlink(local)
            except FileNotFoundError:
                pass


def _track_outputs() -> Any:
    """One lazy output tracker, keeping PyIceberg an optional import extra."""
    from rekep.iceberg.file_io import track_outputs

    return track_outputs()


def _commit_staged(
    table: Any,
    transaction: Any,
    stager: _PartitionStager,
    staged: Sequence[_StagedPartition],
    generated: Iterable[str],
) -> None:
    """Commit staged files, settling whether a failed acknowledgement landed."""
    staged_paths = {path for partition in staged for path in partition.paths}
    try:
        transaction.commit_transaction()
    except BaseException:
        generated_paths = _settled_paths(generated)
        candidates = staged_paths | generated_paths
        try:
            candidates.update(_transaction_paths(table, transaction))
        except BaseException:
            pass
        if _paths_may_be_live(table, candidates):
            # An unreachable catalog cannot distinguish refusal from a commit
            # whose acknowledgement was lost. Preserve possible live files;
            # orphan maintenance settles them once the catalog is reachable.
            stager.release(staged)
        else:
            # The stager owns its uploads; PyIceberg also writes manifests and
            # preserved rows when a keyed delete splits an existing file.
            _discard_paths(table.io, candidates - staged_paths)
        raise
    else:
        stager.release(staged)


def _commit_generated(table: Any, transaction: Any, generated: Iterable[str]) -> None:
    """Commit tracked outputs and delete them after a definite refusal."""
    try:
        transaction.commit_transaction()
    except BaseException:
        candidates = _settled_paths(generated)
        try:
            candidates.update(_transaction_paths(table, transaction))
        except BaseException:
            pass
        if not _paths_may_be_live(table, candidates):
            _discard_paths(table.io, candidates)
        raise


def _transaction_paths(table: Any, transaction: Any) -> set[str]:
    """Best-effort fallback inventory from completed uncommitted snapshots."""
    from pyiceberg.manifest import ManifestEntryStatus

    existing = {snapshot.snapshot_id for snapshot in table.metadata.snapshots}
    paths: set[str] = set()
    for snapshot in transaction.table_metadata.snapshots:
        if snapshot.snapshot_id in existing:
            continue
        if snapshot.manifest_list:
            paths.add(str(snapshot.manifest_list))
        for manifest in snapshot.manifests(io=table.io):
            if manifest.added_snapshot_id != snapshot.snapshot_id:
                continue
            paths.add(str(manifest.manifest_path))
            for entry in manifest.fetch_manifest_entry(io=table.io, discard_deleted=False):
                if (
                    entry.status == ManifestEntryStatus.ADDED
                    and entry.snapshot_id == snapshot.snapshot_id
                ):
                    paths.add(str(entry.data_file.file_path))
    return paths


def _discard_paths(io: Any, paths: Iterable[str]) -> None:
    """Attempt every orphan deletion without replacing the transaction error."""
    for path in _settled_paths(paths):
        try:
            io.delete(path)
        except Exception:
            pass


def _settled_paths(paths: Iterable[str]) -> set[str]:
    """A stable path snapshot after any tracked workers have finished."""
    settle = getattr(paths, "settle", None)
    if callable(settle):
        settle()
    return set(paths)


def _paths_may_be_live(table: Any, candidates: set[str]) -> bool:
    """Whether a refreshed table references a candidate, or cannot answer safely."""
    if not candidates:
        return False
    try:
        table.refresh()
        if str(table.metadata_location) in candidates:
            return True
        seen: set[str] = set()
        for snapshot in table.metadata.snapshots:
            if str(snapshot.manifest_list) in candidates:
                return True
            for manifest in snapshot.manifests(io=table.io):
                path = str(manifest.manifest_path)
                if path in candidates:
                    return True
                if path in seen:
                    continue
                seen.add(path)
                for entry in manifest.fetch_manifest_entry(
                    io=table.io,
                    discard_deleted=True,
                ):
                    if str(entry.data_file.file_path) in candidates:
                        return True
        return False
    except BaseException:
        return True


def _partition_key(table: Any, partition: Mapping[str, Any]) -> Any:
    """The current spec's path key for one transformed partition."""
    from pyiceberg.partitioning import PartitionFieldValue, PartitionKey

    spec = table.spec()
    return PartitionKey(
        [PartitionFieldValue(field, partition[field.name]) for field in spec.fields],
        spec,
        table.schema(),
    )


def _partition_data_files(
    table: Any, replacements: Sequence[_StagedPartition], reference: str
) -> list[Any]:
    """Live data files from manifests that can contain a replaced partition."""
    from pyiceberg.expressions import Or
    from pyiceberg.expressions.visitors import manifest_evaluator
    from pyiceberg.manifest import DataFileContent, ManifestContent

    current = table.spec()
    targets = {
        _partition_identity(_partition_key(table, replacement.partition).partition)
        for replacement in replacements
    }
    snapshot = table.metadata.snapshot_by_name(reference)
    if snapshot is None:
        return []
    filters = [_partition_value_filter(replacement.partition) for replacement in replacements]
    partition_filter = functools.reduce(Or, filters) if len(filters) > 1 else filters[0]
    specs = table.metadata.specs()
    evaluators: dict[int, Callable[[Any], bool]] = {}
    found = []
    for manifest in snapshot.manifests(io=table.io):
        if manifest.content != ManifestContent.DATA:
            continue
        stored = specs[manifest.partition_spec_id]
        if not current.compatible_with(stored):
            raise ValueError("partition overwrite cannot mix incompatible live partition specs")
        evaluator = evaluators.setdefault(
            stored.spec_id,
            manifest_evaluator(stored, table.schema(), partition_filter),
        )
        if not evaluator(manifest):
            continue
        for entry in manifest.fetch_manifest_entry(io=table.io, discard_deleted=True):
            data_file = entry.data_file
            if data_file.content != DataFileContent.DATA:
                continue
            if _partition_identity(data_file.partition) in targets:
                found.append(data_file)
    return found


def _staged_data_file(table: Any, local: str, target: str, partition: Mapping[str, Any]) -> Any:
    """A staged Parquet footer plus its already computed non-linear partition."""
    import pyarrow.parquet
    from pyiceberg.io.pyarrow import (
        compute_statistics_plan,
        data_file_statistics_from_parquet_metadata,
        parquet_path_to_id_mapping,
        sanitize_column_names,
    )
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat

    metadata = pyarrow.parquet.read_metadata(local)
    schema = sanitize_column_names(table.metadata.schema())
    statistics = data_file_statistics_from_parquet_metadata(
        parquet_metadata=metadata,
        stats_columns=compute_statistics_plan(schema, table.metadata.properties),
        parquet_column_mapping=parquet_path_to_id_mapping(schema),
    )
    return DataFile.from_args(
        _table_format_version=table.metadata.format_version,
        content=DataFileContent.DATA,
        file_path=target,
        file_format=FileFormat.PARQUET,
        partition=_partition_key(table, partition).partition,
        file_size_in_bytes=os.path.getsize(local),
        sort_order_id=table.sort_order().order_id or None,
        spec_id=table.metadata.default_spec_id,
        equality_ids=None,
        key_metadata=None,
        **statistics.to_serialized_dict(),
    )


def _copy_to_output(io: Any, source: str, target: str) -> None:
    """Bounded fallback for a custom PyIceberg FileIO without Arrow copying."""
    try:
        with open(source, "rb") as incoming, io.new_output(target).create(overwrite=True) as output:
            while payload := incoming.read(1 << 22):
                output.write(payload)
    except Exception:
        try:
            io.delete(target)
        except FileNotFoundError:
            pass
        raise


def _staged_partition_stream(
    source: pyarrow.RecordBatchReader,
    partitions: Sequence[_PartitionColumn],
    stager: _PartitionStager,
    row_size: int | None = None,
) -> Iterator[_StagedPartition | _StagedBatch]:
    """Stage adjacent partition runs with one-batch lookahead and bounded metadata."""
    current: tuple[Any, ...] | None = None
    ready: list[_StagedPartition] = []
    columns = [column.source for column in partitions]
    closed = _PartitionHistory()

    def refuse_recurrence(identity: tuple[Any, ...], partition: Mapping[str, Any]) -> None:
        if identity in closed:
            raise ValueError(
                f"partition {partition} recurs after another partition; keep each transformed "
                f"partition contiguous before overwriting source columns {columns}"
            )

    def completed() -> Iterator[_StagedPartition]:
        nonlocal ready
        yield from ready
        ready = []

    def stage(batch: pyarrow.RecordBatch) -> None:
        nonlocal current
        runs = iter(_partition_runs(pyarrow.Table.from_batches([batch]), partitions))
        first = next(runs, None)
        if first is None:
            return
        for identity, partition, run in itertools.chain((first,), runs):
            if identity != current:
                refuse_recurrence(identity, partition)
                if current is not None:
                    closed.add(current)
                    ready.append(stager.finish())
                current = identity
                stager.start(partition)
            stager.write(run)

    def pieces() -> Iterator[tuple[pyarrow.RecordBatch, bool]]:
        """Row-bounded pieces and whether each completes its original source batch."""
        for batch in source:
            if not batch.num_rows:
                continue
            size = batch.num_rows if row_size is None else row_size
            for offset in range(0, batch.num_rows, size):
                length = min(size, batch.num_rows - offset)
                yield batch.slice(offset, length), offset + length == batch.num_rows

    # Keep one row-bounded piece as lookahead. Once another piece exists, every
    # completed partition from the held piece can be released on the commit
    # cadence while only its trailing partition remains open in the stager.
    try:
        incoming = pieces()
        held = next(incoming, None)
        if held is None:
            return
        for following in incoming:
            batch, completes_batch = held
            stage(batch)
            yield from completed()
            yield _StagedBatch(batch.num_rows, int(completes_batch))
            held = following
        batch, completes_batch = held
        stage(batch)
        if current is not None:
            ready.append(stager.finish())
        yield from completed()
        yield _StagedBatch(batch.num_rows, int(completes_batch))
    finally:
        closed.close()


def _staged_partition_chunk(
    chunk: pyarrow.Table,
    partitions: Sequence[_PartitionColumn],
    stager: _PartitionStager,
) -> Iterator[_StagedPartition]:
    """Stage one bounded chunk after grouping equal transformed partitions."""
    chunk = _grouped_partition_chunk(chunk, partitions)
    for _, partition, run in _partition_runs(chunk, partitions):
        stager.start(partition)
        stager.write(run)
        yield stager.finish()


def _stage_chunk(
    table: Any,
    chunk: pyarrow.Table,
    stager: _PartitionStager,
) -> Iterator[_StagedPartition]:
    """Stage one bounded addition under the table's current partition spec."""
    partitions = _partition_columns(table)
    if partitions:
        yield from _staged_partition_chunk(chunk, partitions, stager)
        return
    stager.start({})
    stager.write(chunk)
    yield stager.finish()


def _grouped_partition_chunk(
    chunk: pyarrow.Table, partitions: Sequence[_PartitionColumn]
) -> pyarrow.Table:
    """Put one chunk's equal transformed partitions next to each other."""
    names = [f"partition_{index}" for index in range(len(partitions))]
    values = []
    for partition in partitions:
        transformed = partition.transform(_arrow_source(chunk, partition.source))
        values.append(
            transformed.combine_chunks()
            if isinstance(transformed, pyarrow.ChunkedArray)
            else transformed
        )
    keys = pyarrow.RecordBatch.from_arrays(values, names=names)
    if _reader_in_sort_order(keys, names):
        return chunk
    indices = pyarrow.compute.sort_indices(
        keys,
        sort_keys=[(name, "ascending") for name in names],
    )
    return chunk.take(indices)


def _partition_runs(
    chunk: pyarrow.Table, partitions: Sequence[_PartitionColumn]
) -> Iterator[tuple[tuple[Any, ...], dict[str, Any], pyarrow.Table]]:
    """Contiguous transformed-partition runs, found by Arrow comparisons."""
    if not chunk.num_rows:
        return
    same = None
    values_by_partition: list[Any] = []
    for partition in partitions:
        values = partition.transform(_arrow_source(chunk, partition.source))
        if isinstance(values, pyarrow.ChunkedArray):
            values = values.combine_chunks()
        values_by_partition.append(values)
        if (
            pyarrow.types.is_floating(values.type)
            and pyarrow.compute.any(
                pyarrow.compute.fill_null(pyarrow.compute.is_nan(values), False)
            ).as_py()
        ):
            raise ValueError(
                f"partition column {partition.source!r} contains NaN, which partition staging "
                "does not support"
            )
        before, after = values[:-1], values[1:]
        equal = pyarrow.compute.fill_null(pyarrow.compute.equal(before, after), False)
        both_null = pyarrow.compute.and_(
            pyarrow.compute.is_null(before), pyarrow.compute.is_null(after)
        )
        equal = pyarrow.compute.or_(equal, both_null)
        same = equal if same is None else pyarrow.compute.and_(same, equal)
    boundaries = (
        [int(index) + 1 for index in pyarrow.compute.indices_nonzero(pyarrow.compute.invert(same))]
        if same is not None
        else []
    )
    starts = [0, *boundaries]
    for start, stop in zip(starts, [*boundaries, chunk.num_rows], strict=True):
        partition = {
            field.name: values[start].as_py()
            for field, values in zip(partitions, values_by_partition, strict=True)
        }
        identity = _partition_identity(partition)
        yield identity, partition, chunk.slice(start, stop - start)


def _arrow_source(chunk: pyarrow.Table, source: str) -> Any:
    """An exact or nested Arrow source column."""
    if source in chunk.column_names:
        return chunk.column(source)
    root, *nested = source.split(".")
    return pyarrow.compute.struct_field(chunk.column(root), nested)


def _partition_identity(partition: Any) -> tuple[Any, ...]:
    """A hashable partition identity preserving type and null distinctions."""
    values = (
        partition.values()
        if isinstance(partition, Mapping)
        else (partition[index] for index in range(len(partition)))
    )
    return tuple(
        (
            type(value).__qualname__,
            repr(0.0 if isinstance(value, float) and value == 0 else value),
        )
        for value in values
    )


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


def _partition_value_filter(partition: Mapping[str, Any]) -> Any:
    """An exact predicate over a partition struct's own field names."""
    return _partition_filter(partition, [(name, name) for name in partition])


def _always_true() -> Any:
    from pyiceberg.expressions import AlwaysTrue

    return AlwaysTrue()


def _store_of(table: Any, directory: str) -> tuple[Any, str]:
    """The exact configured Arrow store and path used by this table's FileIO."""
    from rekep.iceberg.file_io import configured_store

    file_io = getattr(table, "io", None)
    if file_io is None:
        raise TypeError("Iceberg maintenance requires a table FileIO")
    return configured_store(file_io, directory)


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


def _expiry_delta(value: Any) -> datetime.timedelta:
    """An Iceberg millisecond retention property as a non-negative duration."""
    try:
        milliseconds = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{SNAPSHOT_MAX_AGE} must be a whole number of milliseconds") from None
    if milliseconds < 0:
        raise ValueError(f"{SNAPSHOT_MAX_AGE} cannot be negative")
    return datetime.timedelta(milliseconds=milliseconds)


def _checked_expiry_delta(value: datetime.timedelta) -> datetime.timedelta:
    """A non-negative relative retention at Iceberg's millisecond precision."""
    if value < datetime.timedelta(0):
        raise ValueError("snapshot_expiry cannot be a negative duration")
    millisecond = datetime.timedelta(milliseconds=1)
    return millisecond * -(-value // millisecond)


def _expiry_value(value: SnapshotExpiry, table: Any) -> datetime.datetime | datetime.timedelta:
    """A relative, absolute, or table-configured expiry declaration."""
    if value is None:
        from pyiceberg.table import TableProperties
        from pyiceberg.utils.properties import property_as_int

        value = datetime.timedelta(
            milliseconds=property_as_int(
                table.properties,
                TableProperties.MAX_SNAPSHOT_AGE_MS,
                TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT,
            )
        )
    if isinstance(value, datetime.timedelta):
        return _checked_expiry_delta(value)
    from rekep.times import datetime_of

    cutoff = datetime_of(value)
    if cutoff is None:
        raise ValueError(f"snapshot_expiry={value!r} is not a datetime")
    return cutoff


def _expiry_cutoff(value: datetime.datetime | datetime.timedelta) -> datetime.datetime:
    """A validated expiry declaration as one absolute UTC cutoff."""
    if isinstance(value, datetime.timedelta):
        return datetime.datetime.now(datetime.UTC) - value
    return value
