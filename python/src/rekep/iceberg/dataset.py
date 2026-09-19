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
import sys
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
    SORT_DIRECTIONS,
    Dataset,
    _positive_int,
    anti_join,
    arrow_chunks,
    comparable,
    first_rows,
    in_sort_order,
    normalised_keys,
    sort_direction,
    sort_order_fields,
)
from rekep.fields import (
    Field,
    field_of,
    replace_field,
)
from rekep.iceberg.catalog import IcebergCatalog, _file_location
from rekep.iceberg.fields import (
    derived_keys,
    iceberg_partition_spec,
    iceberg_schema,
    iceberg_sort_order,
    iceberg_struct_field,
    metrics_for,
    partition_keys,
    sort_keys,
)
from rekep.times import UTC

if sys.version_info < (3, 11):  # pragma: no cover - the builtin is 3.11's
    from exceptiongroup import BaseExceptionGroup

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

#: How big one commit's output files get. Narrower than it sounds: it is read
#: as rows-per-file from the *in-memory* size of the chunk being written -- by
#: `_target_file_rows` here, and by pyiceberg the same way wherever it writes
#: -- and it only ever splits a single commit, because neither has
#: cross-commit state and so neither can fill a file across commits.
#: `commit_batch_num`, the optional `commit_row_size`, and `compact` are the
#: levers on file count; this one decides how a large commit is sliced.
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

#: Rows a staged batch carries before it is worth writing as it came. Putting
#: one batch on the file's schema costs about the same whatever it holds --
#: measured at 0.2 ms, nearly all of it walking the schema -- so a run
#: arriving as many small pieces pays that per piece: 128 source batches over
#: 256 partitions left every run in 128 chunks of 8 rows, 32,768 conversions
#: and 6.4s where one conversion per run is 0.05s. Under this, the run is
#: copied together first, which for pieces that small costs nothing; over it,
#: the copy is the larger of the two and the pieces are written as they are.
STAGE_PIECE_ROW_GAIN = 8192

#: Maximum sorted runs merged in memory during one external-sort pass. Each
#: run contributes one scan batch, so the fan-in bounds memory independently
#: of how many row groups or files a partition holds.
SORT_MERGE_FAN_IN = 16

#: Source batches a commit carries when nothing says otherwise. Eight amortizes
#: snapshot and file overhead while bounding memory in the units the producer
#: actually controls; a row cap remains available for unusually large batches.
DEFAULT_COMMIT_BATCH_NUM = 8


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
    field: Field

    #: Catalog loading is explicit; the live catalog stays a lazy property.
    catalog_name: str = "default"
    catalog_properties: dict[str, str] = dataclasses.field(default_factory=dict)

    #: Branch reads and writes use unless a call names another. None, `root`,
    #: `main`, and `master` all mean the table's root state.
    branch: str | None = None

    #: Add columns declared by a write before its rows land. Existing columns,
    #: partition specs, identifier fields, and sort orders are left unchanged.
    merge_schema: bool = False

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

    #: A blind append is rebuilt against the refreshed branch when another
    #: writer wins the commit; a replacement or a delete only settles whether
    #: its own commit landed, because the files it took out were planned
    #: against the head that moved. Full jitter keeps parallel remote writers
    #: from colliding in cadence.
    commit_retries: int = 4
    retry_backoff: float = 0.25
    retry_max_backoff: float = 8.0

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> IcebergDataset:
        """Build a dataset document containing a native field declaration."""
        declared = dict(mapping)
        field = declared.get("field")
        if isinstance(field, Mapping):
            declared["field"] = Field.from_dict(field)
        return super().from_dict(declared)

    def into_dict(self) -> dict[str, Any]:
        """Serialize the native field as its portable mapping."""
        declared = super().into_dict()
        declared["field"] = self.field.into_dict()
        return declared

    def __post_init__(self) -> None:
        """Normalize the declaration and public root spellings once."""
        if not self.name or "." in self.name:
            raise ValueError("an Iceberg dataset name must be non-empty and unqualified")
        if not self.namespace or any(not part for part in self.namespace.split(".")):
            raise ValueError("an Iceberg dataset namespace must be non-empty")
        field = field_of(self.field)
        if field.dtype.id != "struct":
            raise TypeError("an Iceberg dataset field must be a struct")
        self.field = replace_field(field, name=self.name)
        self.commit_batch_num = _positive_int(self.commit_batch_num, "commit_batch_num")
        if self.commit_row_size is not None:
            self.commit_row_size = _positive_int(self.commit_row_size, "commit_row_size")
        if self.commit_retries < 0:
            raise ValueError("commit_retries cannot be negative")
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

    def create_with_field(self, field: Field, **kwargs: Any) -> IcebergDataset:
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
        field = field_of(field, self.name)
        self.store.create_namespace(self.namespace)
        schema = iceberg_schema(field)
        defaults = {**(COMMIT_PROPERTIES if self.optimize_commits else {}), **metrics_for(field)}
        table = self.catalog.create_table(
            self.identifier,
            schema=schema,
            location=location,
            partition_spec=iceberg_partition_spec(field, schema),
            # Declared at creation, because Iceberg records a sort order on the
            # table and every writer through it honours it -- a shape that says
            # how it is read is a shape that says how it should be laid out.
            sort_order=iceberg_sort_order(field, schema, self.sort_by),
            properties={**defaults, **creation_properties},
        )
        self.field = field
        self.__dict__["iceberg_table"] = table
        LOGGER.info(
            "%s created at %s with %d columns, partitioned by %s",
            self.identifier,
            table.location(),
            len(schema.fields),
            partition_keys(field) or "nothing",
        )
        return self

    def get_or_create_table(self, field: Any = None) -> Any:
        """The pyiceberg table, created from `field` or the declaration when absent.

        A table already loaded is handed straight back: `exists` is a catalog
        round trip, which is free on SQLite and a network hop on a REST or Glue
        catalog -- and this is called once per write.
        """
        if "iceberg_table" in self.__dict__:
            return self.iceberg_table
        if not self.exists:
            self.create_with_field(self.target_field(field))
        return self.iceberg_table

    def refresh(self) -> IcebergDataset:
        """Drop what was loaded, so the next call sees other writers' commits."""
        for view in ("iceberg_table", "table_field", "_table_sort_order_id"):
            self.__dict__.pop(view, None)
        return self

    # -- what it holds ------------------------------------------------------

    @cached_property
    def table_field(self) -> Field:
        """The table's own shape: its schema, docs, keys and partitioning."""
        table = self.iceberg_table
        sort_order = table.sort_order()
        self.__dict__["_table_sort_order_id"] = sort_order.order_id
        return iceberg_struct_field(
            table.schema(),
            self.name,
            spec=table.spec(),
            sort_order=sort_order,
        )

    def into_struct_field(self) -> Field:
        """The table's declared shape."""
        return self.field

    def add_fields(self, source: Any = None, *, dry_run: bool = False) -> list[str]:
        """Add the columns `source` has and the table lacks; skip when there are none."""
        target = self.target_field(source)
        incoming = iceberg_schema(target)
        held = set(self.iceberg_table.schema().column_names)
        missing = [name for name in incoming.column_names if name not in held]
        added = [
            name for name in missing if not (parent := name.rpartition(".")[0]) or parent in held
        ]
        if not added or dry_run:
            return added
        table = self.iceberg_table
        with table.update_schema() as update:
            for name in added:
                field = incoming.find_field(name)
                update.add_column(
                    tuple(name.split(".")),
                    field.field_type,
                    doc=field.doc,
                    required=field.required,
                )
        self.refresh()
        # Keep the write declaration's Yggdryl protocols and add the physical
        # ids Iceberg assigned to the columns it accepted.
        self.field = target.merge_with(self.table_field, upscale=False)
        LOGGER.info("%s gained %d columns: %s", self.identifier, len(added), ", ".join(added))
        return added

    def _write_field(
        self,
        schema: Any,
        merge_schema: bool | None,
    ) -> Field:
        """The applied write field, after its opt-in additive table evolution."""
        target = self.target_field(schema)
        if self._merge_schema_enabled(merge_schema):
            self.add_fields(target)
            self.field = target.merge_with(self.table_field, upscale=False)
            return self.field
        return target

    def _merge_schema_enabled(self, merge_schema: bool | None) -> bool:
        """Resolve a write override against the dataset default."""
        return self.merge_schema if merge_schema is None else merge_schema

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
        text source instead, because nothing in the pipeline creates one.
        """
        if isinstance(order_by, str):
            requested_order = (order_by,)
        elif (
            isinstance(order_by, tuple)
            and len(order_by) == 2
            and isinstance(order_by[0], str)
            and isinstance(order_by[1], str)
            and order_by[1].lower() in SORT_DIRECTIONS
        ):
            requested_order = (order_by,)
        else:
            requested_order = tuple(order_by or ())
        ordering_fields = list(sort_order_fields(requested_order))
        ordering = tuple(name for name, _ in ordering_fields)
        reference = self._reference(branch, snapshot_id)
        target = None if schema is None else self.target_field(schema)
        requested: tuple[str, ...] | None = None
        if columns and target is not None:
            target_names = {member.name for member in target}
            requested = tuple(name for name in columns if name in target_names)
            if not requested:
                raise ValueError(f"columns={list(columns)!r} shares no columns with `schema`")
            result_target = _field_projection(target, requested)
            target = _applied_projection(target, requested)
        else:
            result_target = target
        if not self.exists:
            return self._empty_reader(result_target, None if result_target is not None else columns)
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
        visible = set(requested) if requested is not None else projected
        missing = [name for name in ordering if name not in visible]
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
        applied = target.apply_arrow_reader(
            _renamed(reader, found),
            safe=False,
            nullability="strict",
        )
        return _projected(applied, requested) if requested is not None else applied

    def _empty_reader(
        self, target: Field | None, columns: Sequence[str] | None
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

    def _selected(self, target: Field, scan: Any) -> dict[str, str]:
        """`{the scan's name: the target's name}` for every column it can fill."""
        current = {field.name: field.field_id for field in self.iceberg_table.schema().fields}
        pinned = {field.field_id: field.name for field in scan.projection().fields}
        by_name = set(pinned.values())
        wanted = {}
        target_names = [member.name for member in target]
        for name in target_names:
            stored = pinned.get(current.get(name, -1)) or (name if name in by_name else None)
            if stored is not None:
                wanted[stored] = name
        LOGGER.debug(
            "%s projects %d of %d declared columns; unfilled: %s",
            self.identifier,
            len(wanted),
            len(target_names),
            ", ".join(sorted(set(target_names) - set(wanted.values()))) or "none",
        )
        # Nothing in common: one column is named because a scan must project
        # something, and what comes back is a table of no columns and no rows
        # -- which is pyiceberg's own answer to an empty projection too.
        return wanted or {next(iter(pinned.values())): next(iter(pinned.values()))}

    # -- writing ------------------------------------------------------------

    def overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = True,
        commit_row_size: int | None = None,
        *,
        commit_batch_num: int | None = None,
        merge_schema: bool | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> int:
        """Replace what a stream carries, then expire snapshots under the configured cutoff.

        One commit per bounded chunk, and every commit is the same three
        steps: the chunk is written to the table's store as Parquet, one
        transformed partition at a time; the stored rows it replaces are
        taken out -- the rows carrying its keys under `merge_by`, or every
        row of the partitions it touches when `merge_by` names nothing; and
        the written files are appended. A replay of the same rows leaves the
        table holding them once.

        Under keys a stored row is the one in the same transformed partition
        carrying the same key -- the same symbol on two days is two rows, and
        a null partition value is a partition of its own. The stored files of
        the partitions the chunk touches whose key bounds overlap the chunk's
        are read and written back without those keys. A key that recurs within
        a chunk's partition keeps its first row, which is what a stream that
        carries a line twice means; one that recurs in a later chunk replaces
        the row the earlier chunk landed. Keyless, a partition is emptied once
        per write and only added to after that, so a partition split across
        two chunks is not emptied again by the second of them.

        Returns the rows the stream carried into the table.
        """
        create_with = schema if self._merge_schema_enabled(merge_schema) else None
        with self._write(snapshot_expiry, create_with=create_with):
            return self._overwrite_arrow_reader(
                source,
                schema,
                merge_by,
                commit_row_size,
                commit_batch_num=commit_batch_num,
                merge_schema=merge_schema,
                branch=branch,
                properties=properties,
            )

    def _overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = True,
        commit_row_size: int | None = None,
        *,
        commit_batch_num: int | None = None,
        merge_schema: bool | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> int:
        """Stage, take out, append: one commit per bounded chunk."""
        reader: pyarrow.RecordBatchReader | None = None
        try:
            rows, batches = self._commit_limits(commit_row_size, commit_batch_num)
            table = self.get_or_create_table()
            reference = self._branch_name(branch)
            self._branch_head(table, reference)
            join = self.merge_columns(merge_by)
            target = self._write_field(schema, merge_schema)
            table = self.iceberg_table
            partitions = _partition_columns(table)
            if not join and not partitions:
                raise ValueError(
                    f"merge_by={merge_by!r} names nothing to match on and the table is not "
                    "partitioned, so nothing says which stored rows this replaces -- pass True "
                    "for the primary key or the columns to match on, or use append_arrow_* to "
                    "add rows blindly"
                )
            if not join:
                source = _requiring_columns(
                    source, [column.source for column in partitions], derived_keys(target)
                )
            reader = target.apply_arrow_reader(
                source,
                safe=False,
                nullability="strict",
            )
            snapshot = properties or {}
            replaced: set[tuple[Any, ...]] = set()
            written = 0
            for chunk in arrow_chunks(reader, rows, batches):
                table, landed = self._replace_chunk(
                    table, chunk, join, reference, snapshot, replaced
                )
                written += landed
                # `arrow_chunks` accumulates the next chunk while this name
                # still holds the last one.
                del chunk
            return written
        finally:
            _close_write_source(source, reader)

    def _replace_chunk(
        self,
        table: Any,
        chunk: pyarrow.Table,
        join: Sequence[str],
        reference: str,
        properties: Mapping[str, str],
        replaced: set[tuple[Any, ...]],
    ) -> tuple[Any, int]:
        """One chunk staged, its stored rows taken out, and both committed.

        `(the table after the commit, the rows landed)`. `replaced` is what
        this write has already emptied, so a keyless chunk empties a partition
        the first time it touches it and adds to it after that.
        """
        if not chunk.num_rows:
            return table, 0
        with _PartitionStager(table, self.sort_fields(), _target_file_rows(table, chunk)) as stager:
            if join:
                staged, originals, rewritten, landed = self._replace_keys(
                    table, chunk, join, reference, stager
                )
            else:
                staged = list(_stage_chunk(table, chunk, stager))
                fresh = [
                    part for part in staged if _partition_identity(part.partition) not in replaced
                ]
                originals = _partition_data_files(table, fresh, reference) if fresh else []
                replaced.update(_partition_identity(part.partition) for part in fresh)
                rewritten = []
                landed = chunk.num_rows
            table = self._commit_replacement(
                table,
                stager,
                originals,
                [*rewritten, *staged],
                reference,
                properties,
                rebuild=False,
            )
        return table, landed

    def _replace_keys(
        self,
        table: Any,
        chunk: pyarrow.Table,
        join: Sequence[str],
        reference: str,
        stager: _PartitionStager,
    ) -> tuple[list[_StagedPartition], list[Any], list[_StagedPartition], int]:
        """Stage a keyed chunk one partition at a time, taking its keys out as it goes.

        `(staged, originals, rewritten, landed)`: the chunk's files, the
        stored files to delete, the files that stand in for them, and the rows
        the chunk lands. A key is scoped to its transformed partition, so the
        files a partition's keys are taken out of are that partition's own,
        planned once for the chunk: only files whose key bounds overlap the
        chunk's are planned, and only the partitions the chunk carries are
        read. What this holds at once is one partition of the chunk and one
        stored file of it, never the table.
        """
        partitions = _partition_columns(table)
        if partitions:
            runs = _partition_run_tables(chunk, partitions)
        else:
            if not table.spec().is_unpartitioned():
                raise ValueError(
                    f"{table.spec()} names a transform with no Arrow form, so this write "
                    "cannot tell which partition a row belongs to"
                )
            runs = iter([({}, chunk)])
        # Refused before anything is planned: a null or NaN key has no bound
        # and no match, and each partition's rows are checked again as they
        # are staged, where a key that recurs keeps its first row.
        _validate_merge_keys(chunk, join)
        sources = _partition_sources(table, chunk)
        widen = {column: unit for column, unit in sources.items() if unit}
        bounds = _key_bounds(chunk, list(dict.fromkeys([*sources, *join])), widen)
        scan = self._branch_scan(table, table.scan(row_filter=bounds), reference)
        stored = _tasks_by_partition(table, scan.plan_files())
        staged: list[_StagedPartition] = []
        originals: list[Any] = []
        rewritten: list[_StagedPartition] = []
        landed = 0
        for partition, run in runs:
            run = _checked_keys(run, join)
            if not run.num_rows:
                continue
            staged.append(_stage_partition(stager, partition, run))
            landed += run.num_rows
            identity = _partition_identity(_partition_key(table, partition).partition)
            tasks = stored.pop(identity, ())
            if tasks:
                keys = run.select(list(join))
                deleted, replaced = self._rewritten_without(
                    table,
                    tasks,
                    stager,
                    lambda rows, keys=keys: anti_join(rows, keys, join),
                    needs=join,
                )
                originals.extend(deleted)
                rewritten.extend(replaced)
            del run
        return staged, originals, rewritten, landed

    def _rewritten_without(
        self,
        table: Any,
        tasks: Iterable[Any],
        stager: _PartitionStager,
        keep: Callable[[pyarrow.Table], pyarrow.Table],
        *,
        needs: Sequence[str] | None = None,
        doomed: Callable[[Any], bool] | None = None,
        case_sensitive: bool = True,
    ) -> tuple[list[Any], list[_StagedPartition]]:
        """The files `tasks` plan, written back without the rows `keep` drops.

        `(originals, replacements)`: the data files to delete and the staged
        files that stand in for them. A file `doomed` says holds only such rows
        is deleted whole and never read; a file `keep` keeps every row of
        stands. Files are read and written back one at a time through
        `stager`, so what this holds is one file's replacement and never the
        table's.

        `needs` names the columns `keep` reads. Given, a file is first read by
        those columns alone, which settles the two answers a replace mostly
        gets without decoding the rest of it: a file that keeps every row
        stands and is never written, and one that keeps none is deleted and
        never decoded whole. Only a file that keeps some of its rows is read
        whole, and written back without the others.
        """
        from pyiceberg.expressions import AlwaysTrue
        from pyiceberg.io.pyarrow import ArrowScan
        from pyiceberg.table import FileScanTask

        schema = table.schema()
        scanner = ArrowScan(table.metadata, table.io, schema, AlwaysTrue(), case_sensitive)
        sampler = None
        if needs:
            narrow = schema.select(*needs, case_sensitive=case_sensitive)
            sampler = ArrowScan(table.metadata, table.io, narrow, AlwaysTrue(), case_sensitive)
        originals: list[Any] = []
        replacements: list[_StagedPartition] = []
        for task in tasks:
            if doomed is not None and doomed(task.file):
                originals.append(task.file)
                continue
            whole = FileScanTask(task.file, task.delete_files, AlwaysTrue())
            if sampler is not None:
                read, kept = _kept_of(sampler, table.io, whole, keep)
                if kept == read:
                    continue
                if not kept:
                    originals.append(task.file)
                    LOGGER.debug(
                        "%s deleted %s: none of its %d rows survive",
                        self.identifier,
                        task.file.file_path,
                        read,
                    )
                    continue
            stager.start(_task_partition(table, task))
            read = kept = 0
            for batch in _task_batches(scanner, table.io, (whole,)):
                rows = pyarrow.Table.from_batches([batch])
                remaining = keep(rows)
                read += rows.num_rows
                kept += remaining.num_rows
                if remaining.num_rows:
                    stager.write(remaining)
            staged = stager.finish()
            if kept == read:
                # Every row survived, so the file it came from stands and the
                # copy just written is not wanted.
                stager.discard([staged])
                continue
            originals.append(task.file)
            replacements.append(staged)
            LOGGER.debug(
                "%s rewrote %s keeping %d of %d rows",
                self.identifier,
                task.file.file_path,
                kept,
                read,
            )
        return originals, replacements

    def _commit_replacement(
        self,
        table: Any,
        stager: _PartitionStager,
        originals: Sequence[Any],
        additions: Sequence[_StagedPartition],
        reference: str,
        properties: Mapping[str, str],
        *,
        rebuild: bool,
    ) -> Any:
        """One snapshot: `originals` deleted and `additions` appended.

        An append snapshot when nothing is deleted, which is the cheaper
        metadata and says in the log what happened; an overwrite otherwise. A
        blind append may be rebuilt after another writer wins a commit -- its
        staged files are still there to commit again. A replacement passes
        `rebuild=False`: the files it takes out were planned against this head,
        and a head that moved has to be planned again.
        """
        if not originals and not any(part.data_files for part in additions):
            return table

        def commit(current: Any, summary: Mapping[str, str]) -> None:
            with _track_outputs() as generated:
                transaction = current.transaction()
                try:
                    _ensure_name_mapping(transaction)
                    if originals:
                        with transaction.update_snapshot(
                            snapshot_properties=dict(summary), branch=reference
                        ).overwrite() as overwrite:
                            for original in originals:
                                overwrite.delete_data_file(original)
                            for part in additions:
                                for data_file in part.data_files:
                                    overwrite.append_data_file(data_file)
                    else:
                        with _append_files(transaction, reference, summary) as append:
                            for part in additions:
                                for data_file in part.data_files:
                                    append.append_data_file(data_file)
                except BaseException:
                    _discard_paths(current.io, generated)
                    raise
                _commit_staged(current, transaction, stager, additions, generated)
            for part in additions:
                for path in part.paths:
                    LOGGER.debug("%s output %s", self.identifier, path)

        return self._commit_with_retry(table, reference, properties, commit, rebuild=rebuild)

    def _append_chunk(
        self,
        table: Any,
        chunk: pyarrow.Table,
        reference: str,
        properties: Mapping[str, str],
    ) -> Any:
        """Append one bounded chunk, staged one partition at a time.

        Not PyIceberg's `Transaction.append`, which takes the whole chunk and
        copies it twice on the way to Parquet -- once to filter each partition
        out of it and once to give that copy fresh buffers -- with every
        partition's copy alive at once, because it submits them all to its
        pool before the first file is written. Measured on a 70 MiB chunk of
        524,288 rows, peak Arrow memory was 2.01x the chunk over one partition
        and 1.78x over four; staged, taking one partition out of the chunk at
        a time and casting one batch of it at a time, it is 1.07x and 1.52x.
        The staged files also record the order they were written in, which
        PyIceberg's writer leaves null, and an ordered read of a file that
        does not say it is sorted has to sort it again.
        """
        if not chunk.num_rows:
            return table
        with _PartitionStager(table, self.sort_fields(), _target_file_rows(table, chunk)) as stager:
            staged = list(_stage_chunk(table, chunk, stager))
            return self._commit_replacement(
                table, stager, [], staged, reference, properties, rebuild=True
            )

    def append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        commit_row_size: int | None = None,
        *,
        commit_batch_num: int | None = None,
        merge_schema: bool | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> int:
        """Append a stream, then expire snapshots under the configured cutoff.

        Blind: every row lands, whatever the table already holds. A write that
        has to replace what it carries is `overwrite_arrow_reader`.
        """
        create_with = schema if self._merge_schema_enabled(merge_schema) else None
        with self._write(snapshot_expiry, create_with=create_with):
            return self._append_arrow_reader(
                source,
                schema,
                commit_row_size,
                commit_batch_num=commit_batch_num,
                merge_schema=merge_schema,
                branch=branch,
                properties=properties,
            )

    def _append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        commit_row_size: int | None = None,
        *,
        commit_batch_num: int | None = None,
        merge_schema: bool | None = None,
        branch: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> int:
        """Append a stream, one commit per bounded chunk: the rows it added."""
        reader: pyarrow.RecordBatchReader | None = None
        try:
            rows, batches = self._commit_limits(commit_row_size, commit_batch_num)
            table = self.get_or_create_table()
            reference = self._branch_name(branch)
            self._branch_head(table, reference)
            target = self._write_field(schema, merge_schema)
            table = self.iceberg_table
            reader = target.apply_arrow_reader(
                source,
                safe=False,
                nullability="strict",
            )
            snapshot = properties or {}
            inserted = 0
            for chunk in arrow_chunks(reader, rows, batches):
                table = self._append_chunk(table, chunk, reference, snapshot)
                inserted += chunk.num_rows
                # `arrow_chunks` accumulates the next chunk while this name
                # still holds the last one.
                del chunk
            return inserted
        finally:
            _close_write_source(source, reader)

    def sorted(self, chunk: pyarrow.Table) -> pyarrow.Table:
        """`chunk` in `sort_by` order, or exactly as it came when nothing says."""
        fields = self.sort_fields()
        if not fields or chunk.num_rows < 2 or in_sort_order(chunk, fields):
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
                (name, sort_direction(direction)) for name, direction in sort_keys(shape).items()
            ]
        return [
            (name, sort_direction(direction))
            for name, direction in (sort_keys(shape).items() if shape is not None else ())
        ]

    def sort_columns(self) -> list[str]:
        """Columns a chunk is sorted by: what was asked for, or what is declared."""
        return [name for name, _ in self.sort_fields()]

    @contextmanager
    def _write(
        self,
        snapshot_expiry: SnapshotExpiry,
        *,
        create_with: Any = None,
    ) -> Iterator[None]:
        """Expire once after the outermost successful public write.

        The audit record is here for the same reason the expiry is: this is
        the one place that knows an operation *finished*, whatever it commits
        inside. A write that lands forty chunks is one record, not forty.
        """
        depth = int(self.__dict__.get("_write_depth", 0))
        expiry = (
            self._resolved_snapshot_expiry(
                snapshot_expiry,
                self.get_or_create_table(create_with),
            )
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
        from pyiceberg.exceptions import CommitFailedException, ValidationException

        operation_id = uuid.uuid4().hex
        summary = {**properties, OPERATION_ID: operation_id}
        for attempt in range(self.commit_retries + 1):
            try:
                commit(table, summary)
                return table
            except ValidationException as error:
                # PyIceberg retries a commit whose head moved on its own, and
                # lands it when the plan still holds; it refuses one a
                # concurrent commit invalidated -- rows landed this write never
                # saw, or a file it deletes is gone. That is the stale head
                # the caller plans again against, whatever it is called.
                raise CommitFailedException(
                    f"branch {reference} has changed since this write was planned "
                    f"({error}); refresh and write again"
                ) from error
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
        properties: dict[str, str] | None = None,
        snapshot_expiry: SnapshotExpiry = None,
    ) -> int:
        """Delete the matching rows in one commit, and say how many went.

        Strings use PyIceberg's SQL predicate grammar; a BooleanExpression is
        carried unchanged. A file the filter provably empties is deleted
        unread, and one it may only partly empty is read and written back
        without the rows it names, one file at a time.
        """
        if not self.exists:
            return 0
        expression = _delete_expression(row_filter)
        with self._write(snapshot_expiry):
            return self._delete_where(
                expression,
                branch=branch,
                case_sensitive=case_sensitive,
                properties=properties,
            )

    def delete_where(
        self,
        row_filter: Any,
        *,
        branch: str | None = None,
        case_sensitive: bool = True,
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
            properties=properties,
            snapshot_expiry=snapshot_expiry,
        )

    def _delete_where(
        self,
        expression: Any,
        *,
        branch: str | None,
        case_sensitive: bool,
        properties: dict[str, str] | None,
    ) -> int:
        """One planned delete, returning the number of rows removed."""
        from pyiceberg.expressions.visitors import ROWS_MUST_MATCH, _StrictMetricsEvaluator, bind
        from pyiceberg.io.pyarrow import _expression_to_complementary_pyarrow

        table = self.iceberg_table
        reference = self._branch_name(branch)
        if self._branch_head(table, reference) is None:
            return 0
        before = _branch_records(table, reference)
        scan = self._branch_scan(table, table.scan(row_filter=expression), reference)
        tasks = list(scan.plan_files())
        if not tasks:
            return 0
        schema = table.schema()
        preserve = _expression_to_complementary_pyarrow(
            bind(schema, expression, case_sensitive), schema
        )
        strict = _StrictMetricsEvaluator(schema, expression, case_sensitive).eval
        file_rows = max(int(task.file.record_count) for task in tasks)
        with _PartitionStager(table, self.sort_fields(), file_rows) as stager:
            originals, replacements = self._rewritten_without(
                table,
                tasks,
                stager,
                lambda rows: rows.filter(preserve),
                doomed=lambda data_file: strict(data_file) == ROWS_MUST_MATCH,
                case_sensitive=case_sensitive,
            )
            table = self._commit_replacement(
                table,
                stager,
                originals,
                replacements,
                reference,
                properties or {},
                rebuild=False,
            )
        after = _branch_records(table, reference)
        return max(before - after, 0)

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
        cutoff = datetime.datetime.now(UTC) - older_than
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
        return table.location_provider()

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
            if not in_sort_order(batch, columns):
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
                if not in_sort_order(table, columns):
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
        for column, direction in sort_order_fields(columns)
        for value in (batch.column(column)[index].as_py(),)
    )


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


def _kept_of(
    scan: Any, io: Any, task: Any, keep: Callable[[pyarrow.Table], pyarrow.Table]
) -> tuple[int, int]:
    """`(read, kept)` over one planned file, by the columns `scan` projects.

    A function of its own so that the last batch it read dies with it: a
    loop's variables would hold that batch beside the whole file read next.
    """
    read = kept = 0
    for batch in _task_batches(scan, io, (task,)):
        rows = pyarrow.Table.from_batches([batch])
        read += rows.num_rows
        kept += keep(rows).num_rows
    return read, kept


def _task_batches(scan: Any, io: Any, tasks: Sequence[Any]) -> Iterator[pyarrow.RecordBatch]:
    """Decode planned files synchronously so one task retains one batch.

    `ArrowScan.to_record_batches` hands every task of the group to the pool
    and collects each file's batches whole before yielding the first, so a
    group holds every decoded file at once. The generator under it decodes one
    task at a time and yields as it goes; the group's delete files are still
    read once, up front, which is what the group is for.
    """
    from pyiceberg.io.pyarrow import _read_all_delete_files

    deletes = _read_all_delete_files(io, tasks)
    yield from scan._record_batches_from_scan_tasks_and_deletes(tasks, deletes)


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
    specs = scan.table_metadata.specs()
    schema = scan.table_metadata.schema()

    def identity(task: Any) -> tuple[str, int]:
        data = task.file
        spec_id = int(data.spec_id or 0)
        spec = specs.get(spec_id)
        if spec is not None:
            try:
                return spec.partition_to_path(data.partition, schema), spec_id
            except (KeyError, TypeError, ValueError):
                pass
        return str(data.partition or ""), spec_id

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
    reading further ahead than that fills memory without filling the pool.
    `ExecutorFactory.max_workers()` is the width its configuration names; a
    pool left to size itself is asked for the width it settled on, and if it
    stops saying, one file at a time is the answer that cannot be wrong about
    memory.
    """
    from pyiceberg.utils.concurrent import ExecutorFactory

    configured = ExecutorFactory.max_workers()
    if configured:
        return max(int(configured), 1)
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


def window_filter(column: str, window: tuple[datetime.datetime, datetime.datetime]) -> Any:
    """The rows a window covers, as the predicate a scan prunes by.

    The reading `rekep.times.within` gives an Arrow column, for a stored one:
    `start <= column < end`, and the rows carrying no value in it, which
    belong to every window. Over a partition source, Iceberg projects the
    bounds through the transform and opens only the partitions the window
    touches.
    """
    from pyiceberg.expressions import And, GreaterThanOrEqual, IsNull, LessThan, Or

    lower, upper = window
    return Or(And(GreaterThanOrEqual(column, lower), LessThan(column, upper)), IsNull(column))


def _key_bounds(
    chunk: pyarrow.Table, join: Sequence[str], widen: Mapping[str, str] | None = None
) -> Any:
    """A predicate every stored row carrying one of `chunk`'s keys satisfies.

    Between each key column's least and greatest value, and nothing finer: a
    file whose bounds fall outside is never opened, and one whose bounds
    overlap is read and told apart row by row. A column `widen` names is a
    time-partitioned source, and its range is the hours or days the chunk
    touches rather than its instants, because a file of that partition holds
    any instant in them. Wider is the direction this may be wrong in, so a
    column no kernel can bound contributes no term; neither does a nanosecond
    instant, whose literal Iceberg would round, nor a column with a null in
    it, which no range holds -- a partition column may be null, and its null
    partition is then still planned.
    """
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan, LessThanOrEqual

    compute = pyarrow.compute
    unbounded = (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, pyarrow.ArrowTypeError)
    terms = []
    for column in join:
        values = comparable(chunk.column(column))
        kind = values.type
        if (
            values.null_count
            or pyarrow.types.is_boolean(kind)
            or (
                (pyarrow.types.is_timestamp(kind) or pyarrow.types.is_time64(kind))
                and kind.unit == "ns"
            )
        ):
            continue
        try:
            bounds = compute.min_max(values)
            lower, upper = bounds["min"], bounds["max"]
            if unit := (widen or {}).get(column):
                lower = compute.floor_temporal(lower, unit=unit)
                upper = compute.ceil_temporal(upper, unit=unit, ceil_is_strictly_greater=True)
        except unbounded:
            continue
        lower, upper = lower.as_py(), upper.as_py()
        if lower is None or upper is None:
            continue
        above = GreaterThanOrEqual(column, lower)
        below = LessThan(column, upper) if unit else LessThanOrEqual(column, upper)
        terms.append(And(above, below))
    if not terms:
        return _always_true()
    return And(*terms) if len(terms) > 1 else terms[0]


#: The Arrow rounding unit of each Iceberg time transform, for a bound on the
#: transform's source that covers the whole partition.
_TRANSFORM_UNITS = {"hour": "hour", "day": "day", "month": "month", "year": "year"}


def _partition_sources(table: Any, chunk: pyarrow.Table) -> dict[str, str | None]:
    """`chunk`'s partition source columns, with the unit a bound on each widens to.

    An identity partition's source bounds the partition exactly, and widens to
    nothing; a time transform's source widens to the transform's unit; a
    bucket or truncate has no range on its source at all, and is left out.
    """
    schema = table.schema()
    sources: dict[str, str | None] = {}
    for field in table.spec().fields:
        column = schema.find_column_name(field.source_id)
        if column not in chunk.column_names:
            continue
        transform = str(field.transform)
        if transform == "identity":
            sources[column] = None
        elif transform in _TRANSFORM_UNITS:
            sources[column] = _TRANSFORM_UNITS[transform]
    return sources


def _checked_keys(chunk: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """`chunk` as the rows a keyed write lands: keys it can name, one row each.

    A null or NaN key is refused: no join finds the stored row it would
    replace, so writing it would duplicate rather than replace. A negative
    zero in a float key is the zero it equals. A key that recurs keeps its
    first row, which is what a stream that carries a line twice means.
    """
    missing = [column for column in join if column not in chunk.column_names]
    if missing:
        raise ValueError(f"chunk is missing the merge key columns {missing}")
    chunk = normalised_keys(chunk, join)
    _validate_merge_keys(chunk, join)
    return first_rows(chunk, join)


def _validate_merge_keys(chunk: pyarrow.Table, join: Sequence[str]) -> None:
    """Refuse merge keys no join can match."""
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
                "a NaN matches nothing, so merging on it would duplicate rows"
            )


def _has_nan(values: Any) -> bool:
    """Whether a floating column holds a NaN, which no key can name."""
    return (
        pyarrow.types.is_floating(values.type)
        and pyarrow.compute.any(pyarrow.compute.is_nan(values)).as_py()
    )


def _close_write_source(source: Any, reader: pyarrow.RecordBatchReader | None) -> None:
    """Release the owning cast reader, or a source casting never produced."""
    if reader is not None:
        reader.close()
    elif (close := getattr(source, "close", None)) is not None:
        close()


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


def _field_projection(source: Field, names: Sequence[str]) -> Field:
    """One top-level Field projection, retaining its root metadata."""
    return field_of(
        pyarrow.schema(
            [source.field(name).into_arrow() for name in names],
            metadata=source.into_arrow_schema().metadata,
        )
    )


def _applied_projection(source: Field, requested: Sequence[str]) -> Field:
    """A projection widened only for native apply dependencies."""
    available = [member.name for member in source]
    present = set(requested)
    dependencies = derived_keys(source)
    pending = list(requested)
    while pending:
        for path in dependencies.get(pending.pop(), ()):
            root = path.split(".", 1)[0]
            if root in available and root not in present:
                present.add(root)
                pending.append(root)
    if any(source.field(name).digest.get("role") == "holder" for name in present):
        # A retained holder may have been reached through a partition
        # dependency. An absent or `*` digest source means every non-holder
        # sibling, so keep the complete input and let Yggdryl own selection.
        return source
    return _field_projection(source, [name for name in available if name in present])


def _projected(
    reader: pyarrow.RecordBatchReader, names: Sequence[str]
) -> pyarrow.RecordBatchReader:
    """Select streamed output after hidden apply dependencies have run."""
    if list(names) == reader.schema.names:
        return reader
    schema = pyarrow.schema(
        [reader.schema.field(name) for name in names],
        metadata=reader.schema.metadata,
    )

    def batches() -> Iterator[pyarrow.RecordBatch]:
        for batch in reader:
            yield batch.select(names)

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


def _task_partition(table: Any, task: Any) -> dict[str, Any]:
    """A planned file's partition under the current compatible spec."""
    current = table.spec()
    spec_id = getattr(task.file, "spec_id", None)
    stored = table.metadata.specs().get(current.spec_id if spec_id is None else int(spec_id))
    if stored is None or not current.compatible_with(stored):
        raise ValueError("streamed delete cannot mix incompatible live partition specs")
    return {field.name: task.file.partition[index] for index, field in enumerate(current.fields)}


def _tasks_by_partition(table: Any, tasks: Iterable[Any]) -> dict[tuple[Any, ...], list[Any]]:
    """Planned files by the identity of their partition under the current spec."""
    current = table.spec()
    specs = table.metadata.specs()
    grouped: dict[tuple[Any, ...], list[Any]] = {}
    for task in tasks:
        spec_id = getattr(task.file, "spec_id", None)
        stored = specs.get(current.spec_id if spec_id is None else int(spec_id))
        if stored is None or not current.compatible_with(stored):
            raise ValueError("keyed replace cannot mix incompatible live partition specs")
        grouped.setdefault(_partition_identity(task.file.partition), []).append(task)
    return grouped


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
        VoidTransform,
        YearTransform,
    )

    # Every transform Iceberg names, except the one that stands for a
    # transform this PyIceberg does not know -- and a table partitioned by
    # that cannot be written through PyIceberg either, because its own writer
    # asks the transform for the same Arrow form and is refused.
    supported = (
        IdentityTransform,
        DayTransform,
        HourTransform,
        MonthTransform,
        YearTransform,
        BucketTransform,
        TruncateTransform,
        VoidTransform,
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


def _ensure_name_mapping(transaction: Any) -> None:
    """Install the field-name fallback staged Parquet files need when read."""
    if transaction.table_metadata.name_mapping() is not None:
        return
    from pyiceberg.table import TableProperties

    mapping = transaction.table_metadata.schema().name_mapping
    transaction.set_properties(**{TableProperties.DEFAULT_NAME_MAPPING: mapping.model_dump_json()})


def _requiring_columns(
    source: pyarrow.RecordBatchReader,
    columns: Sequence[str],
    derived: Mapping[str, Sequence[str]] | None = None,
) -> pyarrow.RecordBatchReader:
    """Refuse partition columns neither supplied nor derivable by the target."""
    missing = []
    for name in columns:
        if _schema_has_column(source.schema, name):
            continue
        sources = None if derived is None else derived.get(name)
        if sources and all(_schema_has_column(source.schema, value) for value in sources):
            continue
        missing.append(name)
    if missing:
        raise ValueError(
            f"partition columns {missing} are missing from the source; "
            "partition overwrite cannot infer which partitions to replace"
        )
    return source


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


class _PartitionStager:
    """Bounded staging of complete partitions, streamed into the table's store.

    One partition at a time and one file of it open at a time, written through
    pyiceberg's own Parquet writer on the table's configured `FileIO`: the
    writer opens the store's output stream, applies the options the table
    properties declare, and answers the file's statistics when it closes. So
    nothing is written to local disk first, and nothing is read back to
    describe what was written. Every location opened here is owned until the
    commit that references it lands, and deleted on the way out otherwise.
    """

    def __init__(
        self,
        table: Any,
        sort_by: Sequence[tuple[str, str]],
        file_row_size: int,
    ) -> None:
        from pyiceberg.io.fileformat import FileFormatFactory
        from pyiceberg.io.pyarrow import sanitize_column_names
        from pyiceberg.manifest import FileFormat
        from pyiceberg.table import TableProperties
        from pyiceberg.utils.properties import property_as_int

        self.table = table
        self.sort_fields = tuple(sort_by)
        self.sort_by = tuple(name for name, _ in self.sort_fields)
        #: What a partition uses when it cannot say: a stream staging one
        #: partition over many calls has no rows to measure until it is done.
        self.default_file_row_size = max(int(file_row_size), 1)
        self.file_row_size = self.default_file_row_size
        self.location_provider = table.location_provider()
        #: pyiceberg's writer for the table's file format: it names the
        #: extension, stamps each column with its Iceberg field id, and writes
        #: one file per `create_writer`.
        self.format_model = FileFormatFactory.get(FileFormat.PARQUET)
        schema = table.metadata.schema()
        #: The schema a file is written under: column names sanitized the way
        #: pyiceberg's own writer sanitizes them, ids and all.
        self.file_schema = sanitize_column_names(schema)
        self.name_mapping = schema.name_mapping
        self.downcast_ns = _downcasts_ns()
        self.row_group_size = property_as_int(
            properties=table.metadata.properties,
            property_name=TableProperties.PARQUET_ROW_GROUP_LIMIT,
            default=TableProperties.PARQUET_ROW_GROUP_LIMIT_DEFAULT,
        )
        #: Store locations this stager still owns: opened here, and not yet
        #: handed over to a commit that landed.
        self.outputs: set[str] = set()
        self.partition: Mapping[str, Any] | None = None
        self.paths: list[str] = []
        self.data_files: list[Any] = []
        self.rows = 0
        self._writer: Any | None = None
        self._output: Any | None = None
        self._target: str | None = None
        self._file_rows = 0
        self._last_key: tuple[Any, ...] | None = None
        self._pending: list[pyarrow.RecordBatch] = []
        self._pending_rows = 0
        self._incoming_arrow_schema: pyarrow.Schema | None = None
        self._incoming_schema: Any | None = None

    def __enter__(self) -> _PartitionStager:
        return self

    def __exit__(self, exc_type: object, _exc: object, _traceback: object) -> None:
        errors: list[BaseException] = []
        try:
            self._close_file(keep=False)
        except BaseException as error:
            # Whatever stopped the open writer, the files this already wrote
            # are still this object's to delete, and an interrupt escaping
            # here left every one of them behind.
            errors.append(error)
        leftover = tuple(self.outputs)
        # The store these were written through, held before the question is
        # asked: answering it reloads the table, and a reloaded table carries
        # a fresh `FileIO`.
        io = self.table.io
        # Asked before deleting, because a commit that landed and was then
        # interrupted before its files were handed over leaves them here,
        # live and referenced. `_paths_may_be_live` answers True for an
        # unreachable catalog too, which is the answer that keeps rows.
        if leftover and not _paths_may_be_live(self.table, set(leftover)):
            for path in leftover:
                try:
                    io.delete(path)
                except FileNotFoundError:
                    pass
                except Exception as error:
                    errors.append(error)
        if exc_type is None and errors:
            if len(errors) == 1:
                raise errors[0]
            # `BaseExceptionGroup` and not `ExceptionGroup`, which refuses to
            # carry an interrupt; it still builds the narrower group when every
            # error it holds is an ordinary one.
            raise BaseExceptionGroup("partition staging cleanup failed", errors)

    def start(self, partition: Mapping[str, Any], file_row_size: int | None = None) -> None:
        """Open a partition, sized by its own rows when the caller knows them.

        Its own, because `TARGET_FILE_SIZE` is a size in bytes and rows are
        not all the same width: one bound taken from a whole chunk's average
        splits a partition of short rows into files a fraction of the target
        and packs one of wide rows past it.
        """
        if self.partition is not None:
            raise RuntimeError("finish the staged partition before starting another")
        self.file_row_size = (
            max(int(file_row_size), 1) if file_row_size else self.default_file_row_size
        )
        self.partition = dict(partition)
        self.paths = []
        self.data_files = []
        self.rows = 0

    def write(self, chunk: pyarrow.Table) -> None:
        """Write one bounded source chunk without mixing partition values."""
        if self.partition is None:
            raise RuntimeError("start a staged partition before writing it")
        if self.sort_fields and not in_sort_order(chunk, self.sort_fields):
            chunk = chunk.sort_by(list(self.sort_fields))
        offset = 0
        while offset < chunk.num_rows:
            available = self.file_row_size - self._file_rows
            piece = chunk.slice(offset, min(available, chunk.num_rows - offset))
            piece_batches = piece.to_batches(max_chunksize=piece.num_rows)
            first = _row_key(piece_batches[0], self.sort_fields, 0)
            if self._writer is not None and self._last_key is not None and first < self._last_key:
                self._close_file(keep=True)
                available = self.file_row_size
                piece = chunk.slice(offset, min(available, chunk.num_rows - offset))
                piece_batches = piece.to_batches(max_chunksize=piece.num_rows)
            for batch in piece_batches:
                self._open_file()
                self._hold(self._requested_batch(batch))
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
                self._close_file(keep=True)

    def _hold(self, batch: pyarrow.RecordBatch) -> None:
        """Keep a batch back until it fills a row group.

        A row group per source batch is what writing each one as it arrives
        makes, and a source that hands over small batches then writes a file
        of small row groups: 2,000 batches of 8 rows measured 2,000 row groups
        and twice the bytes of the same rows in one. Held batches are the
        chunk's own, so what this keeps is a reference to rows already there.
        """
        self._pending.append(batch)
        self._pending_rows += batch.num_rows
        if self._pending_rows >= self.row_group_size:
            self._flush()

    def _flush(self, writer: Any = None, *, whole: bool = False) -> None:
        """Write held rows as full row groups, keeping any remainder held.

        The remainder stays because a batch boundary is not a row group
        boundary: flushing whatever had arrived would size every row group by
        the source's batch size instead of the table's declared limit, and a
        70,000-row batch under a 131,072-row limit would fill barely half of
        each. Only closing the file writes a short one.
        """
        if not self._pending:
            return
        held = pyarrow.Table.from_batches(self._pending)
        groups = held.num_rows // self.row_group_size
        full = held.num_rows if whole else groups * self.row_group_size
        remainder = held.slice(full)
        self._pending = remainder.to_batches() if remainder.num_rows else []
        self._pending_rows = remainder.num_rows
        if full:
            # pyiceberg's writer cuts what it is handed into row groups of the
            # table's declared limit, so a multiple of it lands as full ones.
            (self._writer if writer is None else writer).write(held.slice(0, full))

    def finish(self) -> _StagedPartition:
        if self.partition is None:
            raise RuntimeError("no staged partition to finish")
        self._close_file(keep=True)
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
            self.outputs.difference_update(partition.paths)

    def discard(self, partitions: Sequence[_StagedPartition]) -> None:
        """Remove staged files this write turned out not to need.

        Now rather than on the way out: they were never committed, so nothing
        has to be asked about whether they are live, and a delete that keeps
        most of the files it read would otherwise carry every rewritten copy
        of them to the end of the operation.
        """
        paths = [path for partition in partitions for path in partition.paths]
        _discard_paths(self.table.io, paths)
        self.outputs.difference_update(paths)

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
            requested_schema=self.file_schema,
            file_schema=self._incoming_schema,
            batch=batch,
            downcast_ns_timestamp_to_us=self.downcast_ns,
            include_field_ids=True,
            format_model=self.format_model,
        )

    def _open_file(self) -> None:
        """Open the next file of this partition in the store, once."""
        if self._writer is not None:
            return
        identifier = uuid.uuid4()
        target = self.location_provider.new_data_location(
            data_file_name=f"{identifier}.{self.format_model.file_extension()}",
            partition_key=_partition_key(self.table, self.partition or {}),
        )
        # Owned before the store sees a byte of it: a stream can create its
        # object and then fail, and what this owns on the way out is deleted
        # whichever way the write ends.
        self.outputs.add(target)
        output = self.table.io.new_output(target)
        self._writer = self.format_model.create_writer(
            output, self.file_schema, self.table.metadata.properties
        )
        self._output, self._target = output, target
        self._file_rows = 0
        self._last_key = None

    def _close_file(self, *, keep: bool) -> None:
        """Close the open file: recorded as a data file, or left for the sweep."""
        writer, output, target = self._writer, self._output, self._target
        rows = self._file_rows
        self._writer = self._output = self._target = None
        self._file_rows = 0
        self._last_key = None
        if writer is None:
            self._pending, self._pending_rows = [], 0
            return
        if not keep:
            # Whatever stopped this file, what it wrote is this object's to
            # delete on the way out -- and a stream still open would refuse
            # that on Windows, replacing the error that got here with its own.
            self._pending, self._pending_rows = [], 0
            _close_quietly(writer)
            return
        try:
            self._flush(writer, whole=True)
        except BaseException:
            _close_quietly(writer)
            raise
        # The footer the writer just wrote is what describes the file: its
        # statistics come back from the close, and nothing reads it again.
        statistics = writer.close()
        self.paths.append(target)
        self.data_files.append(
            _data_file(
                self.table,
                target,
                len(output),
                statistics,
                self.partition or {},
                ordered=bool(self.sort_fields),
            )
        )
        LOGGER.debug("staged %d rows to %s", rows, target)


def _close_quietly(writer: Any) -> None:
    """Release a format writer's stream without replacing the error that stopped it."""
    try:
        writer.close()
    except Exception:
        pass


def _track_outputs() -> Any:
    """One lazy output tracker, keeping PyIceberg an optional import extra."""
    from rekep.iceberg.file_io import track_outputs

    return track_outputs()


def _append_files(transaction: Any, reference: str, properties: Mapping[str, str]) -> Any:
    """The append producer PyIceberg would pick for this table's properties.

    Read from the table and not chosen here, because `MERGE_MANIFESTS` is a
    declaration about how the table commits and every writer through it
    honours the same one.
    """
    from pyiceberg.table import TableProperties
    from pyiceberg.utils.properties import property_as_bool

    update = transaction.update_snapshot(snapshot_properties=dict(properties), branch=reference)
    merged = property_as_bool(
        transaction.table_metadata.properties,
        TableProperties.MANIFEST_MERGE_ENABLED,
        TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT,
    )
    return update.merge_append() if merged else update.fast_append()


def _target_file_rows(table: Any, rows: pyarrow.Table) -> int:
    """Rows per staged file, from `TARGET_FILE_SIZE` and these rows' width.

    Sized from in-memory bytes because that is how Iceberg's own writer reads
    the property, so a table keeps one answer to how big its files are
    whoever wrote them. Still a bound per commit and not across commits: a
    file closes when the rows it is packing run out.
    """
    from pyiceberg.table import TableProperties
    from pyiceberg.utils.properties import property_as_int

    target = property_as_int(
        properties=table.metadata.properties,
        property_name=TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        default=TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
    )
    if not rows.num_rows or not target:
        return max(rows.num_rows, 1)
    return max(1, int(target / max(rows.nbytes / rows.num_rows, 1)))


def _stage_partition(
    stager: _PartitionStager,
    partition: Mapping[str, Any] | None,
    rows: pyarrow.Table,
) -> _StagedPartition:
    """Stage one resolved partition's rows, uncommitted."""
    stager.start(partition or {}, _target_file_rows(stager.table, rows))
    stager.write(rows)
    return stager.finish()


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
            # The stager keeps its uploads: a retry commits these same files,
            # and only giving up for good -- the stager's own way out --
            # deletes them. PyIceberg's manifests and the rows it preserved
            # when a keyed delete split an existing file are this attempt's,
            # and go now.
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
    """Whether the stored table references a candidate, or cannot answer safely.

    Loaded beside the caller's table rather than through `refresh()`, which
    swaps that table's `FileIO` for the catalog's newest -- and the caller is
    about to delete files through the one it wrote them with.
    """
    if not candidates:
        return False
    try:
        stored = table.catalog.load_table(table.name())
        if str(stored.metadata_location) in candidates:
            return True
        seen: set[str] = set()
        for snapshot in stored.metadata.snapshots:
            if str(snapshot.manifest_list) in candidates:
                return True
            for manifest in snapshot.manifests(io=stored.io):
                path = str(manifest.manifest_path)
                if path in candidates:
                    return True
                if path in seen:
                    continue
                seen.add(path)
                for entry in manifest.fetch_manifest_entry(
                    io=stored.io,
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


def _data_file(
    table: Any,
    target: str,
    size: int,
    statistics: Any,
    partition: Mapping[str, Any],
    *,
    ordered: bool,
) -> Any:
    """One written file as the `DataFile` a commit appends.

    `statistics` is what pyiceberg's writer answered on closing the file, and
    `partition` the already computed transformed partition, which a bucket's
    bounds could not give back. `ordered` is whether the writer laid these
    rows out in the table's recorded order, and only then does the file say
    so. A shape cannot hold every order Iceberg can record -- a transformed
    sort field, a nulls-first one, a nested column -- and for those the writer
    has nothing to sort by, so a file stamped with the order id would be
    claiming one it was not written in. A reader takes that claim at its word.
    """
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat

    return DataFile.from_args(
        _table_format_version=table.metadata.format_version,
        content=DataFileContent.DATA,
        file_path=target,
        file_format=FileFormat.PARQUET,
        partition=_partition_key(table, partition).partition,
        file_size_in_bytes=size,
        sort_order_id=(table.sort_order().order_id or None) if ordered else None,
        spec_id=table.metadata.default_spec_id,
        equality_ids=None,
        key_metadata=None,
        **statistics.to_serialized_dict(),
    )


def _staged_partition_chunk(
    chunk: pyarrow.Table,
    partitions: Sequence[_PartitionColumn],
    stager: _PartitionStager,
) -> Iterator[_StagedPartition]:
    """Stage one bounded chunk, one transformed partition at a time."""
    for partition, run in _partition_run_tables(chunk, partitions):
        yield _stage_partition(stager, partition, run)


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
    if not table.spec().is_unpartitioned():
        raise ValueError(
            f"{table.spec()} names a transform with no Arrow form, so this write cannot tell "
            "which partition a row belongs to"
        )
    yield _stage_partition(stager, {}, chunk)


def _partition_run_tables(
    chunk: pyarrow.Table, partitions: Sequence[_PartitionColumn]
) -> Iterator[tuple[dict[str, Any], pyarrow.Table]]:
    """One transformed partition of `chunk` at a time, taken out one at a time.

    Batch by batch, rather than by sorting the chunk once. Sorting the chunk
    is a copy of it, held beside it while every run is written; and taking
    rows out of a table whose columns are chunked concatenates each column
    first, which is another copy of the chunk per run. Ordering a batch and
    slicing it costs the batch, and a batch whose partitions are already
    next to each other -- an hour of a chronological stream -- costs nothing
    at all, because its runs are views.
    """
    if not chunk.num_rows:
        return
    names = [f"partition_{index}" for index in range(len(partitions))]
    found: dict[tuple[Any, ...], dict[str, Any]] = {}
    planned: list[tuple[pyarrow.RecordBatch, Any, dict[tuple[Any, ...], tuple[int, int]]]] = []
    for batch in chunk.to_batches():
        if not batch.num_rows:
            continue
        values = _partition_values(batch, partitions)
        keys = pyarrow.RecordBatch.from_arrays(values, names=names)
        indices = None
        if not in_sort_order(keys, names):
            indices = pyarrow.compute.sort_indices(
                keys, sort_keys=[(name, "ascending") for name in names]
            )
            # Held until every partition has taken its rows, so held at the
            # width this batch needs rather than the 64 bits Arrow sorts with:
            # on rows narrow enough to make it matter -- two int64 columns --
            # the difference was 1.51x of the chunk against 1.29x.
            indices = indices.cast(_index_type(batch.num_rows))
            values = keys.take(indices).columns
        # A list and not one span apiece: ordering the keys is what puts a
        # partition's rows together, and this does not have to be the only
        # thing that knows it. Runs that arrive apart are carried apart.
        spans: dict[tuple[Any, ...], list[tuple[int, int]]] = {}
        for identity, partition, start, stop in _partition_spans(partitions, values):
            found.setdefault(identity, partition)
            spans.setdefault(identity, []).append((start, stop))
        planned.append((batch, indices, spans))
    for identity, partition in found.items():
        pieces = []
        for batch, indices, spans in planned:
            for start, stop in spans.get(identity, ()):
                pieces.append(
                    batch.slice(start, stop - start)
                    if indices is None
                    else batch.take(indices.slice(start, stop - start))
                )
        run = pyarrow.Table.from_batches(pieces, chunk.schema)
        if len(pieces) > 1 and run.num_rows < STAGE_PIECE_ROW_GAIN * len(pieces):
            run = run.combine_chunks()
        yield partition, run


def _index_type(rows: int) -> Any:
    """The narrowest unsigned Arrow type that can address `rows` of them."""
    if rows <= 1 << 16:
        return pyarrow.uint16()
    if rows <= 1 << 32:
        return pyarrow.uint32()
    return pyarrow.uint64()


def _partition_values(rows: Any, partitions: Sequence[_PartitionColumn]) -> list[Any]:
    """Each partition field's transformed values over a table or batch."""
    values_by_partition: list[Any] = []
    for partition in partitions:
        values = partition.transform(_arrow_source(rows, partition.source))
        if isinstance(values, pyarrow.ChunkedArray):
            values = values.combine_chunks()
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
        values_by_partition.append(values)
    return values_by_partition


def _partition_spans(
    partitions: Sequence[_PartitionColumn], values_by_partition: Sequence[Any]
) -> Iterator[tuple[tuple[Any, ...], dict[str, Any], int, int]]:
    """Where each run of equal transformed values starts and stops."""
    same = None
    for values in values_by_partition:
        before, after = values[:-1], values[1:]
        equal = pyarrow.compute.fill_null(pyarrow.compute.equal(before, after), False)
        both_null = pyarrow.compute.and_(
            pyarrow.compute.is_null(before), pyarrow.compute.is_null(after)
        )
        equal = pyarrow.compute.or_(equal, both_null)
        same = equal if same is None else pyarrow.compute.and_(same, equal)
    if same is None:
        return
    boundaries = [
        int(index) + 1 for index in pyarrow.compute.indices_nonzero(pyarrow.compute.invert(same))
    ]
    rows = len(values_by_partition[0])
    for start, stop in zip([0, *boundaries], [*boundaries, rows], strict=True):
        partition = {
            field.name: values[start].as_py()
            for field, values in zip(partitions, values_by_partition, strict=True)
        }
        yield _partition_identity(partition), partition, start, stop


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
        older_than = datetime.datetime.now(UTC) - older_than
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
        return datetime.datetime.now(UTC) - value
    return value
