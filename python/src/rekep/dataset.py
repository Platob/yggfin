"""Dataset: the OpenLineage resource for one namespace-qualified data product
(https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md).

`record` names the schema helper -- a `Record` subclass whose Arrow
projection *is* this dataset's schema facet. `Record` itself carries no
resource identity; it is purely the schema `Dataset` (and the Iceberg/Doris
table records) project through.

Location is layered the same way field metadata already is
(`rekep.records.arrow.Arrow`'s shared vs. protocol-prefixed keys): `properties`
are shared by every protocol that writes this dataset, `direct` names a
single physical location shared the same way, and `protocols` carries
per-protocol overrides (`iceberg`, `doris`, ...) merged over the shared ones.

Deploying is autonomous: `into_iceberg_table()`/`into_doris_table()` build an
ad hoc `IcebergTable`/`DorisTable` straight from this dataset's own fields,
and `deploy_iceberg`/`deploy_doris` hand it to `Iceberg.deploy_one`/
`Doris.deploy_one` -- catalog and namespace still come from the deployment's
`stacks/iceberg`/`stacks/doris` (`catalogs/`, `namespaces/` only), but the
table itself needs no side file of its own.

Reading and writing are both generic at the top and protocol-specific
underneath: `read_arrow_reader`/`write_arrow_reader` dispatch by `format` to
the `{format}_read_arrow_reader`/`{format}_write_arrow_reader` hook, which is
the customisation point a deployment overrides to say how the I/O actually
happens. Nothing wraps those calls: a dataset moves data, and what a run of
it *was* is `rekep.run`'s shape to describe, built by whoever wants the
record rather than emitted from inside every write.

Both directions leverage pyiceberg's own table API rather than
reimplementing any of it:

- `iceberg_read_arrow_reader` scans with **filter pushdown** -- `row_filter`
  and `columns` reach the scan planner, so partitions and files that cannot
  match are never opened -- and reads a branch, a tag or a snapshot id
  through `DataScan.use_ref`/`snapshot_id`.
- `iceberg_write_arrow_reader` takes one `merge_by`: `True` upserts on the
  record's own primary key (`Arrow(key=True)` -> Iceberg identifier fields),
  a list of names upserts on those, anything falsy appends. A table with no
  snapshot yet has nothing to merge against, so the merge is skipped and the
  first write simply appends.
- `iceberg_compact`/`iceberg_expire_snapshots`/`iceberg_publish` are the
  maintenance side of the same table: rewrite the partitions that grew too
  many small files, drop snapshots older than a cutoff, fast-forward `main`
  onto a branch that turned out good.

`file_write_arrow_reader`/`file_read_arrow_reader` are the same shape for
any `pyarrow.fs` filesystem: a URI maps to a `(filesystem, path)` pair
through `rekep.filesystems`, cached per URL, and the reader streams straight
into `pyarrow.dataset.write_dataset`, **hive-partitioned by whatever the
record declares** `Arrow(partition=...)` on -- the same declaration Iceberg's
partition spec is built from, so both protocols partition a dataset the same
way without being told twice.

Every write is reshaped onto the target schema on the way in
(`records.arrow.cast_reader`, unsafe by default): a transform that produces
a wider integer, a missing nullable column or its columns in another order
still writes, because the target -- the record, then the table -- is the
authority on what the data is.
"""

from __future__ import annotations

import dataclasses
import datetime
import logging
import os
import pathlib
import uuid
from collections.abc import Iterator
from typing import Any

import pyarrow
import pyarrow.dataset
import pyarrow.fs

from rekep import config
from rekep.filesystems import resolve as resolve_filesystem
from rekep.imports import locate
from rekep.namespace import ResourceUri
from rekep.records import registry
from rekep.records.arrow import FIELD_ID_KEY, cast_reader
from rekep.records.record import Record, record
from rekep.run import InputDataset, OutputDataset

logger = logging.getLogger("rekep.dataset")

#: pyiceberg's own default ref; passed explicitly rather than relied on as a
#: parameter default, since `iceberg_branch()` may resolve to None.
ICEBERG_MAIN_BRANCH = "main"

#: Rows accumulated before a write commits. A merge needs to compare against
#: existing data, so some materialising is unavoidable; committing in chunks
#: bounds memory and turns many small commits into few large ones, which is
#: what lets Iceberg's partition pruning actually pay off. Named for its unit
#: and dimension, like every other size parameter here.
COMMIT_ROW_SIZE = 100_000

#: A partition with fewer data files than this is left alone by
#: `iceberg_compact`: rewriting two files into one costs a whole scan and a
#: commit to save one file open, which is not a trade worth making.
ICEBERG_COMPACT_MIN_FILES = 8

#: How long a file must have been sitting unreferenced before `iceberg_cleanup`
#: will delete it. A write in flight has files on disk that no committed
#: snapshot points at yet; deleting those destroys a concurrent writer's work,
#: so the grace period is deliberately generous.
ICEBERG_ORPHAN_GRACE = datetime.timedelta(days=3)

#: Table properties that make Iceberg prune its own `metadata.json` trail.
#: Set rather than emulated: once these are on, every commit does the work,
#: and the first one does it retroactively.
ICEBERG_METADATA_RETENTION = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}

#: Merging manifests on commit; off by default in pyiceberg, which is why an
#: untuned table grows one manifest per write forever.
ICEBERG_MANIFEST_MERGE = "commit.manifest-merge.enabled"

#: The table's own bin-packing target for a written data file. Read rather
#: than guessed at: a table that wants 128MB files says so once, here.
ICEBERG_TARGET_FILE_BYTES = "write.target-file-size-bytes"
ICEBERG_TARGET_FILE_BYTES_DEFAULT = 512 * 1024 * 1024

#: `protocols[<protocol>]` keys that route a write, a read or a maintenance
#: pass rather than describe the table -- excluded from `table_properties()`.
_PROTOCOL_ROUTING_KEYS = frozenset(
    {
        "location",
        "branch",
        "merge_by",
        "merge_schema",
        "retain",
        "compact_min_files",
        "commit_row_size",
    }
)

#: Suffixes `iceberg_retention()` accepts on a retention window.
_RETENTION_UNITS = {
    "s": "seconds",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "w": "weeks",
}

#: Spellings of `merge_by` that mean "yes, on the primary key" and "no".
_TRUE_WORDS = frozenset({"true", "yes", "on", "1"})
_FALSE_WORDS = frozenset({"false", "no", "off", "0", ""})

#: Where dataset side files live when nothing says otherwise: the checkout's
#: `stacks/datasets` if it has one, else the user's `~/.config/rekep/datasets`
#: -- see `rekep.config.folder`. `REKEP_DATASETS_ROOT` overrides both.
DATASETS_ROOT = os.environ.get("REKEP_DATASETS_ROOT")


@record
class Dataset(Record):
    """A namespace-qualified data product: schema, location, identity.

    `properties` are shared by every protocol that writes this dataset;
    `direct` is a single physical location shared the same way; `protocols`
    carries per-protocol overrides (its own `location`, its own properties),
    each merged over the shared ones -- `protocol_properties("iceberg")`
    resolves exactly what an Iceberg write should see.
    """

    schema: str
    """Dotted path of the `Record` class whose Arrow projection is this schema.

    A path rather than an inline field list because a declaration has to
    survive a round trip through a file, and only a name can: the class is
    the schema, and pointing at it keeps one definition instead of two that
    can disagree. `arrow_schema()` is the Arrow view of it, which is what
    everything downstream actually uses."""

    uri: str | None = None
    """This dataset's identity, as a path: `ds:/catalog/namespace/name#branch`.

    One string instead of separate name/namespace/catalog fields, because
    they are one identity -- and a path, because a catalog contains
    namespaces and a namespace contains tables, which a dot cannot say
    without ambiguity. Undeclared, it is built from the record's own
    snake_case name in `default`."""

    direct: str | None = None
    """A single physical location (a path or URI), shared by every protocol."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Properties shared by every protocol that writes this dataset."""

    protocols: dict[str, dict[str, str]] = dataclasses.field(default_factory=dict)
    """Per-protocol overrides (`iceberg`, `doris`, ...), merged over `properties`."""

    # -- identity / schema ----------------------------------------------

    @classmethod
    def load_all(cls, root: str | os.PathLike[str] | None = None, **context: Any) -> list[Dataset]:
        """Every dataset declared under `root`, one file each, and registered.

        The same registry-of-one-folder shape `IcebergDeployment`'s
        `catalogs/`/`namespaces/` use -- no `tables/` folder anywhere
        declares a table directly; a `Dataset` here deploys autonomously
        against whichever catalog/namespace registry `deploy_iceberg`/
        `deploy_doris` is handed.

        `root` defaults through `rekep.config.folder`: the checkout's
        `stacks/datasets` when it has one, the user's config home when it
        does not -- so a repository's own declarations win, and a bare
        install still has somewhere to keep them. Everything loaded lands in
        the process-wide registry, so a `ds:/...` reference resolves without
        reading the directory again.
        """
        folder = config.folder("datasets", root if root is not None else DATASETS_ROOT)
        return [config.register(entry) for entry in registry.entries(folder, cls, context)]

    @classmethod
    def load(cls, uri: str, root: str | os.PathLike[str] | None = None) -> Dataset:
        """The dataset `uri` names: from the registry, or by loading the folder."""
        found = config.lookup(uri, service="datasets")
        if found is None:
            cls.load_all(root)
            found = config.lookup(uri, service="datasets")
        if found is None:
            raise KeyError(f"no dataset {uri!r} declared under {config.folder('datasets', root)}")
        return found

    def dump(self, root: str | os.PathLike[str] | None = None) -> pathlib.Path:
        """Write this dataset's declaration where `load_all` will find it."""
        folder = config.folder("datasets", root if root is not None else DATASETS_ROOT, create=True)
        path = folder / f"{self.dataset_name()}.yaml"
        self.into_yaml(path)
        config.register(self)
        return path

    def record_class(self) -> type[Record]:
        """The `Record` class `schema` names."""
        cls = locate(self.schema)
        if not (isinstance(cls, type) and issubclass(cls, Record)):
            raise TypeError(f"{self.schema} is not a Record class")
        return cls

    def arrow_schema(self) -> pyarrow.Schema:
        """This dataset's schema, as Arrow -- the view everything else uses."""
        return self.record_class().into_arrow_schema()

    def dataset_name(self) -> str:
        """This dataset's name: the URI's last level, else the record's own."""
        if self.uri:
            return self.resource_uri().name()
        return self.record_class().doris_table_name()

    def dataset_namespace(self) -> str:
        """The namespace this dataset is identified under."""
        return self.resource_uri().namespace() if self.uri else "default"

    def resource_uri(self) -> ResourceUri:
        """This dataset's identity: `ds:/catalog/namespace/name#branch`.

        A `ResourceUri`, the one place a job's and a dataset's identifiers
        are built and parsed -- so the two can never collide even when they
        share a namespace and a name, and every spelling resolves to one
        identity. The branch rides along as the fragment, because a branch
        is not a different dataset; a declared `uri` without one picks up
        whatever `protocols.iceberg.branch` says.
        """
        if self.uri:
            parsed = ResourceUri.parse(self.uri, service="datasets")
            return parsed if parsed.branch else parsed.at(self.iceberg_branch())
        return ResourceUri.of(
            "datasets", self.record_class().doris_table_name(), branch=self.iceberg_branch()
        )

    def schema_facet(self) -> dict[str, Any]:
        """OpenLineage `SchemaDatasetFacet`: the record's fields, by name."""
        return {"fields": self.record_class().into_dict()["fields"]}

    def facets(self) -> dict[str, Any]:
        """Every static facet this dataset carries; `schema` and `dataSource` always included."""
        return {"schema": self.schema_facet(), "dataSource": {"uri": str(self.resource_uri())}}

    # -- location -------------------------------------------------------

    def location(self, protocol: str | None = None) -> str | None:
        """The physical location for `protocol`, falling back to `direct`."""
        if protocol:
            override = self.protocols.get(protocol, {}).get("location")
            if override:
                return override
        return self.direct

    def protocol_properties(self, protocol: str) -> dict[str, str]:
        """`properties` merged with `protocol`'s own, the protocol winning.

        Every per-protocol setting resolves through here, the routing keys
        (`branch`, `merge_by`, `merge_schema`, `retain`,
        `compact_min_files`) included -- so a dataset that wants one policy
        everywhere declares it once in `properties`, and only the exceptions
        go under `protocols`.
        """
        return {**self.properties, **self.protocols.get(protocol, {})}

    def table_properties(self, protocol: str) -> dict[str, str]:
        """`protocol_properties(protocol)`, minus the keys that route a write
        (`location`, `branch`) rather than describe the table itself.

        `into_iceberg_table`/`into_doris_table` use this, not
        `protocol_properties` directly -- `protocols["iceberg"]["branch"]`
        picks which Iceberg branch a write targets, it is not a property to
        persist on the table.
        """
        return {
            key: value
            for key, value in self.protocol_properties(protocol).items()
            if key not in _PROTOCOL_ROUTING_KEYS
        }

    def iceberg_branch(self) -> str | None:
        """This dataset's declared Iceberg branch (`protocols["iceberg"]["branch"]`).

        None means pyiceberg's own default (`main`) -- a dataset need not
        declare one to be written or deployed.
        """
        return self.protocol_properties("iceberg").get("branch")

    def iceberg_merge_by(self) -> bool | list[str] | None:
        """This dataset's declared merge key (`protocols["iceberg"]["merge_by"]`).

        Side files hold strings, so the spelling is a string too: `"true"`
        (merge on the record's primary key), `"false"` (append), or a
        comma-separated list of column names (merge on those). None means
        the dataset declares nothing and the call site decides.
        """
        declared = self.protocol_properties("iceberg").get("merge_by")
        if declared is None or isinstance(declared, bool | list):
            return declared
        text = str(declared).strip()
        if text.lower() in _TRUE_WORDS:
            return True
        if text.lower() in _FALSE_WORDS:
            return False
        return [name.strip() for name in text.split(",") if name.strip()]

    def merge_schema(self, protocol: str = "iceberg") -> bool:
        """Whether a write may widen this dataset (`protocols.<protocol>.merge_schema`).

        Off unless declared: silently growing a table because a source grew
        a column is the kind of thing that should be a decision, made once,
        in the file that describes the dataset.
        """
        declared = self.protocol_properties(protocol).get("merge_schema")
        if isinstance(declared, bool):
            return declared
        return str(declared or "").strip().lower() in _TRUE_WORDS

    def merge_columns(self, merge_by: bool | list[str] | str | None = None) -> list[str] | None:
        """The columns a write should merge on -- None meaning "append instead".

        One argument decides between the two write shapes, because for a
        caller they are one decision: `True` merges on the record's own
        primary key (the `Arrow(key=True)` fields that already became
        Iceberg's identifier fields), a list merges on exactly those
        columns, and anything falsy appends. `None` defers to
        `iceberg_merge_by()`, so a dataset that declares its merge key in
        its side file needs nothing at the call site.

        A record with no primary key cannot answer `True`, and guessing a
        join key is how a merge silently corrupts a table -- so that case is
        refused by name, with both ways out.
        """
        if merge_by is None:
            merge_by = self.iceberg_merge_by()
        if isinstance(merge_by, str):
            merge_by = [name.strip() for name in merge_by.split(",") if name.strip()]
        if not merge_by:
            return None
        if merge_by is True:
            keys = self.record_class().primary_keys()
            if not keys:
                raise ValueError(
                    f"dataset {self.dataset_name()!r}: merge_by=True needs a primary key, but "
                    f"{self.schema} declares no Arrow(key=True) field; declare one or pass the "
                    "columns to merge on"
                )
            return keys
        return list(merge_by)

    def partition_columns(self) -> dict[str, str]:
        """The record's declared partition fields, mapped to their transform.

        The one declaration both protocols partition from: Iceberg builds
        its `PartitionSpec` from it, the file writer builds hive directories
        from it.
        """
        return self.record_class().partition_keys()

    def hive_partitioning(self) -> Any:
        """A `pyarrow.dataset` hive partitioning for the record's partition fields.

        Only `identity` transforms become directories: a `day`, `hour` or
        `bucket[16]` partition is a *computed* value Iceberg derives at write
        time, and computing it here would mean inventing a column the record
        never declared. Those are skipped (logged), so a record partitioned
        only by transforms writes flat rather than wrongly.

        None when nothing is left to partition on -- which is what
        `pyarrow.dataset.write_dataset` wants for an unpartitioned write.
        """
        schema = self.record_class().into_arrow_schema()
        fields = []
        for name, transform in self.partition_columns().items():
            if transform == "identity":
                fields.append(schema.field(name))
            else:
                logger.debug(
                    "dataset %s: partition %s=%s is a computed transform, not a directory",
                    self.dataset_name(),
                    name,
                    transform,
                )
        if not fields:
            return None
        return pyarrow.dataset.partitioning(pyarrow.schema(fields), flavor="hive")

    def iceberg_retention(self) -> datetime.timedelta | None:
        """How long this dataset keeps snapshot history (`protocols.iceberg.retain`).

        Spelled as a window rather than a cutoff, because that is what a
        retention policy is: `"7d"`, `"12h"`, `"90m"`, `"2w"`, or bare
        seconds. None means the dataset declares no policy and
        `iceberg_cleanup` leaves its history alone.
        """
        declared = self.protocol_properties("iceberg").get("retain")
        if declared in (None, ""):
            return None
        if isinstance(declared, datetime.timedelta):
            return declared
        text = str(declared).strip().lower()
        unit = _RETENTION_UNITS.get(text[-1:])
        if unit is None:
            return datetime.timedelta(seconds=float(text))
        return datetime.timedelta(**{unit: float(text[:-1])})

    def commit_row_size(self, protocol: str = "iceberg") -> int | None:
        """How many rows a write accumulates before it commits, when declared.

        `protocols.<protocol>.commit_row_size`. Declared per dataset because
        the right answer is a property of the *data*, not of the call site: a
        table written once a minute in tiny batches wants a large one, a wide
        table whose chunk has to fit in memory wants a small one, and neither
        should have to be remembered at every write.

        None means the dataset declares nothing and the protocol's own answer
        stands -- `COMMIT_ROW_SIZE` for Iceberg, where every write commits a
        snapshot whether or not anyone chose a size, and Arrow's own file
        sizing for a plain file write, where there is no commit to bound.
        """
        declared = self.protocol_properties(protocol).get("commit_row_size")
        return None if declared in (None, "") else int(declared)

    def iceberg_compact_min_files(self) -> int:
        """File count that makes a partition worth rewriting.

        `protocols.iceberg.compact_min_files`, defaulting to
        `ICEBERG_COMPACT_MIN_FILES`.
        """
        declared = self.protocol_properties("iceberg").get("compact_min_files")
        return ICEBERG_COMPACT_MIN_FILES if declared in (None, "") else int(declared)

    # -- deploy: autonomous, no side file needed -------------------------

    def into_iceberg_table(self) -> Any:
        """This dataset as an ad hoc `IcebergTable`, ready for `Iceberg.deploy_one`."""
        from rekep.records.iceberg import IcebergTable

        return IcebergTable(
            record=self.schema,
            name=self.dataset_name(),
            namespace=self.dataset_namespace(),
            location=self.location("iceberg"),
            properties=self.table_properties("iceberg"),
        )

    def into_doris_table(self) -> Any:
        """This dataset as an ad hoc `DorisTable`, ready for `Doris.deploy_one`."""
        from rekep.records.doris import DorisTable

        return DorisTable(
            record=self.schema,
            name=self.dataset_name(),
            namespace=self.dataset_namespace(),
            properties=self.table_properties("doris"),
        )

    def deploy_iceberg(self, stack: Any, dry_run: bool = False) -> Any:
        """Converge this dataset into `stack` (an `Iceberg`) -- no side file needed."""
        return stack.deploy_one(self.into_iceberg_table(), dry_run=dry_run)

    def deploy_doris(self, stack: Any, dry_run: bool = False) -> Any:
        """Converge this dataset into `stack` (a `Doris`) -- no side file needed."""
        return stack.deploy_one(self.into_doris_table(), dry_run=dry_run)

    def deploy(self, target: str, stack: Any, dry_run: bool = False) -> Any:
        """Converge this dataset into `stack`, dispatching by `target`.

        `target="iceberg"` calls `deploy_iceberg`, `target="doris"` calls
        `deploy_doris` -- the same generic-dispatch shape `write_arrow_reader`
        uses for `format`, deploy's own protocol name.
        """
        method = getattr(self, f"deploy_{target}", None)
        if not callable(method):
            raise ValueError(f"dataset {self.dataset_name()!r}: no {target!r} deploy target")
        return method(stack, dry_run=dry_run)

    # -- lineage refs -----------------------------------------------------

    def as_input(self, **input_facets: Any) -> InputDataset:
        """This dataset as a run's `InputDataset` reference."""
        return InputDataset(
            namespace=self.dataset_namespace(),
            name=self.dataset_name(),
            facets=self.facets(),
            input_facets=input_facets,
        )

    def as_output(self, **output_facets: Any) -> OutputDataset:
        """This dataset as a run's `OutputDataset` reference."""
        return OutputDataset(
            namespace=self.dataset_namespace(),
            name=self.dataset_name(),
            facets=self.facets(),
            output_facets=output_facets,
        )

    # -- reading ----------------------------------------------------------

    def read_arrow_reader(self, format: str, **options: Any) -> pyarrow.RecordBatchReader:
        """Read this dataset through the protocol named `format`.

        The mirror of `write_arrow_reader`: dispatches to
        `{format}_read_arrow_reader` and returns a **lazy**
        `pyarrow.RecordBatchReader` -- nothing is read until the reader is
        iterated, and nothing is ever materialised whole.
        """
        reader = getattr(self, f"{format}_read_arrow_reader", None)
        if not callable(reader):
            raise ValueError(f"dataset {self.dataset_name()!r}: no {format!r} reader")
        return reader(**options)

    def iceberg_read_arrow_reader(
        self,
        *,
        table: Any = None,
        row_filter: Any = None,
        columns: list[str] | None = None,
        snapshot_id: int | None = None,
        branch: str | None = None,
        limit: int | None = None,
        case_sensitive: bool = True,
        scan_options: dict[str, str] | None = None,
    ) -> pyarrow.RecordBatchReader:
        """Scan an Iceberg table into a batch reader, filters pushed down.

        `row_filter` and `columns` are not applied to the result -- they are
        handed to the *scan planner*, which is the whole point: Iceberg
        prunes partitions from the filter, then files from their column
        statistics, then row groups inside the files that survive, and only
        then reads. A filter that matches one day of a date-partitioned
        table opens one day's files. Both spellings pyiceberg accepts work:
        a string (`"date >= '2026-08-01'"`) or a built
        `pyiceberg.expressions` tree.

        Which snapshot is read is resolved in the same order the write side
        resolves where to write: an explicit `snapshot_id` wins, then
        `branch` (or this dataset's declared `protocols["iceberg"]["branch"]`),
        then the table's current state. A declared branch that does not
        exist yet reads `main` instead of failing -- the same bootstrap
        asymmetry the writer has, since a branch only appears once
        something has been written to it.
        """
        if table is None:
            raise NotImplementedError(
                f"{type(self).__name__}.iceberg_read_arrow_reader needs table=<pyiceberg "
                "Table>; override to resolve one from protocol_properties('iceberg')"
            )
        arguments: dict[str, Any] = {
            "selected_fields": tuple(columns) if columns else ("*",),
            "case_sensitive": case_sensitive,
            "limit": limit,
            "options": scan_options or {},
        }
        if row_filter is not None:
            arguments["row_filter"] = row_filter
        if snapshot_id is not None:
            arguments["snapshot_id"] = snapshot_id
        scan = table.scan(**arguments)
        if snapshot_id is None:
            reference = self._read_ref(table, branch)
            if reference:
                scan = scan.use_ref(reference)
        return scan.to_arrow_batch_reader()

    def file_read_arrow_reader(
        self,
        *,
        uri: str | None = None,
        filesystem: pyarrow.fs.FileSystem | None = None,
        row_filter: Any = None,
        columns: list[str] | None = None,
        batch_size: int | None = None,
        partitioning: Any = None,
        **options: Any,
    ) -> pyarrow.RecordBatchReader:
        """Scan a file location into a batch reader, filters pushed down.

        The same pushdown story as Iceberg's, through Arrow's own dataset
        scanner: `row_filter` (a `pyarrow.compute.Expression`) prunes hive
        partition directories before any file is opened, and parquet row
        groups by their statistics after. `partitioning` defaults to
        `hive_partitioning()` -- the record's own declaration -- so a
        dataset written by `file_write_arrow_reader` reads back with its
        partition columns intact, without being told the layout twice.
        """
        target = uri or self.location("file")
        if not target:
            raise NotImplementedError(
                f"{type(self).__name__}.file_read_arrow_reader needs a location: pass uri=, "
                "set direct=, or protocols['file']['location']"
            )
        if filesystem is None:
            filesystem, target = resolve_filesystem(target)
        read_format = options.pop("format", "parquet")
        source = pyarrow.dataset.dataset(
            target,
            filesystem=filesystem,
            format=read_format,
            partitioning=self.hive_partitioning() if partitioning is None else partitioning,
            **options,
        )
        scan: dict[str, Any] = {"columns": columns, "filter": row_filter}
        if batch_size is not None:
            scan["batch_size"] = batch_size
        return source.scanner(**scan).to_reader()

    # -- writing ----------------------------------------------------------

    def _aligned(
        self,
        reader: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        *,
        merge_schema: bool = False,
    ) -> pyarrow.RecordBatchReader:
        """`reader` reshaped onto this dataset's record schema, batch by batch.

        Every write hook starts here rather than the dispatcher doing it
        once, so each hook is self-sufficient when called directly: the
        reshape belongs to the write, not to the dispatch.
        It is `records.arrow.cast_reader`, so it also accepts a plain
        iterator of batches and casts unsafely -- the record is the
        authority on what this dataset's data is, and a narrower target
        type is a declaration, not an accident.

        `merge_schema=True` keeps the columns the stream has and the record
        does not, appended nullable after the declared ones instead of
        dropped. The returned reader's `schema` is the merged one, which is
        what the protocol then evolves its table to.

        The Iceberg table's own schema is deliberately *not* the target:
        pyiceberg spells strings `large_string` in `Schema.as_arrow()` and
        accepts either on write, so casting to it would double every string
        column's memory to satisfy a difference the writer does not care
        about.
        """
        return cast_reader(
            reader, self.record_class().into_arrow_schema(), merge_schema=merge_schema
        )

    def write_arrow_reader(
        self,
        reader: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        format: str,
        **options: Any,
    ) -> int:
        """Write `reader`'s batches through the protocol named `format`.

        Dispatches to `{format}_write_arrow_reader` -- e.g. `format="iceberg"`
        calls `iceberg_write_arrow_reader`, the hook a deployment overrides.

        Every hook reshapes what it is handed onto the record's Arrow
        schema first (`_aligned`), so a plain iterator of batches -- what
        `Job.arrow_transform` yields -- pipes straight in with no ceremony
        and no exact-shape requirement at the call site:
        `dataset.write_arrow_reader(job.arrow_transform(job.extract()), "iceberg", table=t)`.
        """
        writer = getattr(self, f"{format}_write_arrow_reader", None)
        if not callable(writer):
            raise ValueError(f"dataset {self.dataset_name()!r}: no {format!r} writer")
        return writer(reader, **options)

    def iceberg_write_arrow_reader(
        self,
        reader: pyarrow.RecordBatchReader,
        *,
        table: Any = None,
        merge_by: bool | list[str] | str | None = None,
        merge_schema: bool | None = None,
        overwrite: bool | str | Any = False,
        branch: str | None = None,
        commit_row_size: int | None = None,
        **options: Any,
    ) -> int:
        """Write `reader`'s batches to an Iceberg table: merge, append or overwrite.

        The write itself: abstract in spirit, not by enforcement --
        override it to change how the table is resolved (a catalog lookup
        against `protocol_properties("iceberg")`, a cached connection) or how
        the write happens. The default here expects `table=`, a live
        `pyiceberg.table.Table`, and calls straight into its own API rather
        than reimplementing any of it.

        **`merge_by` picks the write shape**, because for a caller that is
        one decision rather than a mode plus a key:

        - falsy (the default, unless the side file declares otherwise) --
          `append`.
        - `True` -- `upsert` on the record's primary key, the
          `Arrow(key=True)` fields that already are the table's identifier
          fields. `ParsedMessage.hash64` needs no extra wiring.
        - a list of column names -- `upsert` on exactly those.

        A merge into a table that **has no snapshot yet** has nothing to
        merge against: every row is an insert by definition, and the merge's
        anti-join would only cost a scan of nothing. That case skips the
        merge and appends, logged rather than silent.

        The same reasoning prunes each chunk. Iceberg already records the
        min and max of every column in every data file, so before merging a
        chunk this compares its own key range against those bounds
        (`_key_bounds`): if no existing file's range can overlap on even one
        join column, no row in the chunk can match anything, and the merge
        is an anti-join guaranteed to find nothing. That chunk appends
        instead. It costs one manifest read for the whole write and one
        `min_max` kernel per key column per chunk, against a merge that
        would otherwise scan and join -- and for the common shape, a stream
        of *new* data keyed on something time-ordered, it prunes every
        chunk. Bounds are only ever widened by Iceberg's own truncation of
        long strings, so the comparison can say "cannot match" but never
        wrongly say "does not match".

        Both shapes accumulate `commit_row_size` rows per commit rather than
        writing batch by batch, because in Iceberg **a batch is not a unit
        of work**: every call commits a snapshot and lands at least one data
        file per partition it touches, so appending a reader of ten thousand
        small batches leaves ten thousand snapshots and as many tiny files
        for every later scan to open. Accumulating first keeps memory
        bounded by `commit_row_size` -- the parameter, not the input -- and
        turns that into a handful of full-sized files. It defaults to
        `protocols.iceberg.commit_row_size`, so how much a dataset commits at
        once is declared with the dataset rather than at every call site.
        (`iceberg_compact` exists because the same thing happens across
        *runs*, which no single write can batch away.)

        **`merge_schema`** is the other half of the same idea, for columns
        rather than rows: on, a column the stream has and the table does
        not is *added* to the table (pyiceberg's own `union_by_name`,
        nullable, since rows already written have nothing to put in it)
        instead of being dropped on the way in. Columns both sides have are
        cast to the table's declared types either way -- widening a column
        because a source spelled it differently is not schema evolution,
        it is losing the declaration. Defaults to
        `protocols.iceberg.merge_schema`, which defaults to off: a table
        growing a column should be a decision, made once, in the file that
        describes the dataset. Whether or not it is on, the branch is first
        moved onto a snapshot using the table's current schema
        (`_align_iceberg_ref`) -- a scan projects the schema its snapshot
        was written under, so a ref that predates *any* schema change reads
        back the old column set.

        `overwrite=True` replaces the whole table with the reader;
        `overwrite=<filter>` replaces only what the filter matches. Both
        need the reader to fit in memory, same as `Table.overwrite` itself,
        and neither combines with `merge_by`.

        `branch` defaults to `iceberg_branch()` -- this dataset's own
        declared branch -- falling back to `main`. A branch that does not
        exist yet is created from `main`'s current snapshot first
        (`_ensure_iceberg_branch`), because pyiceberg's own auto-creation on
        first write gives the branch no parent at all: an independent, empty
        lineage that merely shares the table, which is not what a dev/WAP
        branch means. A table with no snapshot at all has nothing to fork
        from either way, and Iceberg allows only `main` there, so that very
        first write lands on `main` regardless -- logged, never silent.
        """
        if table is None:
            raise NotImplementedError(
                f"{type(self).__name__}.iceberg_write_arrow_reader needs table=<pyiceberg "
                "Table>; override to resolve one from protocol_properties('iceberg')"
            )
        if overwrite is not False and merge_by:
            raise ValueError(
                f"dataset {self.dataset_name()!r}: overwrite and merge_by are two different "
                "writes; pick one"
            )
        if merge_schema is None:
            merge_schema = self.merge_schema("iceberg")
        reader = self._aligned(reader, merge_schema=merge_schema)
        bootstrapping = table.current_snapshot() is None
        branch = self._write_ref(table, branch, bootstrapping=bootstrapping)
        if merge_schema:
            _evolve_iceberg_table(table, reader.schema, self.dataset_name())
        _align_iceberg_ref(table, branch)

        join_cols = None if overwrite is not False else self.merge_columns(merge_by)
        if join_cols and bootstrapping:
            logger.info(
                "dataset %s: table has no snapshot to merge into, appending on %s instead of "
                "merging by %s",
                self.dataset_name(),
                branch,
                ", ".join(join_cols),
            )
            join_cols = None
        reader = cast_reader(
            reader, _iceberg_write_schema(table, reader.schema, complete=join_cols is not None)
        )

        if overwrite is not False:
            whole = reader.read_all()
            if overwrite is not True:
                options["overwrite_filter"] = overwrite
            table.overwrite(whole, branch=branch, **options)
            return whole.num_rows

        bounds = _key_bounds(table, branch, join_cols) if join_cols else None
        if commit_row_size is None:
            commit_row_size = self.commit_row_size("iceberg") or COMMIT_ROW_SIZE
        written = 0
        for chunk in _chunked(reader, commit_row_size):
            if join_cols is None or _outside(chunk, join_cols, bounds):
                table.append(chunk, branch=branch, **options)
                written += chunk.num_rows
            else:
                result = table.upsert(chunk, join_cols=join_cols, branch=branch, **options)
                written += result.rows_updated + result.rows_inserted
        return written

    def file_write_arrow_reader(
        self,
        reader: pyarrow.RecordBatchReader,
        *,
        uri: str | None = None,
        filesystem: pyarrow.fs.FileSystem | None = None,
        partitioning: Any = None,
        merge_schema: bool | None = None,
        commit_row_size: int | None = None,
        **options: Any,
    ) -> int:
        """Write `reader`'s batches to a file location, any `pyarrow.fs` filesystem.

        The write itself: generic across filesystems the way
        `iceberg_write_arrow_reader` is generic across catalogs. `uri`
        defaults to `location("file")` (falling back to `direct`) and maps
        to a `(filesystem, path)` pair through `rekep.filesystems.resolve`,
        cached per URL; pass `filesystem=` explicitly to skip that mapping --
        the escape hatch for a filesystem built from `protocol_properties`
        (credentials, region) rather than parsed off the URI.

        `partitioning` defaults to `hive_partitioning()`, so the record's
        own `Arrow(partition=...)` declaration lays out the directories --
        the same declaration Iceberg's partition spec is built from. Pass
        `partitioning=False` for a flat write.

        Each write also gets its own file basename prefix and
        `existing_data_behavior="overwrite_or_ignore"`, so writing twice
        into one location appends instead of colliding on
        `part-0.parquet` -- which is what a partitioned dataset written
        daily needs, and what `write_dataset`'s own defaults refuse. Both
        are plain `options`, so a caller who wants replace-the-directory
        semantics passes `existing_data_behavior="delete_matching"`.

        `commit_row_size` means here what it means on the Iceberg side, one
        layer down: a file layout has no commit, so what a write lands per
        unit is a **file**, and the parameter caps its rows (a row group
        cannot exceed a file, so it is capped with it). The default is
        `protocols.file.commit_row_size` -- undeclared, Arrow decides, which
        is the right answer for a write that is not streaming.

        `merge_schema` means the same thing here as on the Iceberg side --
        keep the columns the stream has and the record does not -- and needs
        no evolution step, since a file layout has no schema to migrate;
        the widened columns simply land in the parquet the write produces.

        `options` otherwise reaches `pyarrow.dataset.write_dataset` as-is
        (`format="parquet"` by default); the reader streams into it one
        batch at a time, never materialising the whole thing.
        """
        target = uri or self.location("file")
        if not target:
            raise NotImplementedError(
                f"{type(self).__name__}.file_write_arrow_reader needs a location: pass uri=, "
                "set direct=, or protocols['file']['location']"
            )
        if filesystem is None:
            filesystem, target = resolve_filesystem(target)
        if merge_schema is None:
            merge_schema = self.merge_schema("file")
        counted, count = _counting(self._aligned(reader, merge_schema=merge_schema))
        write_format = options.pop("format", "parquet")
        if commit_row_size is None:
            commit_row_size = self.commit_row_size("file")
        if commit_row_size:
            options.setdefault("max_rows_per_file", commit_row_size)
            options.setdefault("max_rows_per_group", commit_row_size)
        options.setdefault("existing_data_behavior", "overwrite_or_ignore")
        options.setdefault(
            "basename_template", f"part-{uuid.uuid4().hex[:12]}-{{i}}.{write_format}"
        )
        pyarrow.dataset.write_dataset(
            counted,
            target,
            filesystem=filesystem,
            format=write_format,
            partitioning=self.hive_partitioning() if partitioning is None else partitioning or None,
            **options,
        )
        return count[0]

    # -- maintenance: compact, cleanup, optimize ---------------------------

    def compact(self, protocol: str = "iceberg", **options: Any) -> dict[str, Any]:
        """Rewrite the partitions that grew too many small files."""
        return self._maintenance(protocol, "compact", options)

    def cleanup(self, protocol: str = "iceberg", **options: Any) -> dict[str, Any]:
        """Reclaim what the table no longer needs: old snapshots, dead files."""
        return self._maintenance(protocol, "cleanup", options)

    def optimize(self, protocol: str = "iceberg", **options: Any) -> dict[str, Any]:
        """Do whatever this table actually needs, in the order it needs it."""
        return self._maintenance(protocol, "optimize", options)

    def _maintenance(self, protocol: str, verb: str, options: dict[str, Any]) -> dict[str, Any]:
        """Dispatch a maintenance verb the same way I/O dispatches a format."""
        method = getattr(self, f"{protocol}_{verb}", None)
        if not callable(method):
            raise ValueError(f"dataset {self.dataset_name()!r}: no {protocol!r} {verb}")
        return method(**options)

    def iceberg_compact(
        self,
        *,
        table: Any = None,
        branch: str | None = None,
        min_input_files: int | None = None,
        row_filter: Any = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Rewrite the partitions that grew too many small data files.

        Streaming writes are what make this necessary: every commit lands at
        least one file per partition it touched, so a table written once a
        minute has a thousand files a day and a scan pays a thousand file
        opens to read them. Compaction reads those rows back and writes them
        out as few large files -- the data is unchanged, only its layout.

        pyiceberg has no `rewrite_data_files` procedure, so this is built
        from what it does have, and leans on it rather than reinventing it:

        - `inspect.partitions()` says how many files and how many bytes each
          partition holds. That is manifest metadata, so choosing what to
          rewrite costs no scan at all.
        - The output size is **not decided here**. `write.target-file-size-bytes`
          is the table's own property and pyiceberg's writer already
          bin-packs to it, so a table that wants 128MB files says so once, on
          the table, and every writer agrees -- including this one.
        - `dynamic_partition_overwrite` replaces exactly the partitions
          present in what is written back. One commit, no other partition
          touched.

        A partition is worth rewriting when it holds `min_input_files` or
        more files *and* they average under the target size -- the second
        half matters, because a partition of eight full-sized files is not
        fragmented, it is just big. `min_input_files` defaults to
        `protocols.iceberg.compact_min_files`.

        Only `identity` partitions can be targeted, because only those have a
        partition value that is also a column value to filter on; a table
        partitioned by a computed transform (`day`, `bucket`) is refused by
        name rather than half-compacted, with `row_filter=` as the way to say
        what to rewrite instead. An unpartitioned table is the simple case:
        too many files means rewrite all of them.
        """
        if table is None:
            raise NotImplementedError(
                f"{type(self).__name__}.iceberg_compact needs table=<pyiceberg Table>"
            )
        reference = self._read_ref(table, branch)
        snapshot = table.snapshot_by_name(reference) if reference else table.current_snapshot()
        empty: dict[str, Any] = {"partitions": [], "files": 0, "rows": 0, "compacted": False}
        if snapshot is None:
            return empty

        crowded = _crowded_partitions(
            table,
            snapshot.snapshot_id,
            self.iceberg_compact_min_files() if min_input_files is None else min_input_files,
            _target_file_bytes(table),
        )
        if not crowded:
            return empty
        files = sum(count for _, count in crowded)
        partitions = [dict(values) for values, _ in crowded]
        report: dict[str, Any] = {"partitions": partitions, "files": files, "rows": 0}
        if row_filter is None and partitions:
            row_filter = _partition_filter(table, partitions)
        if dry_run:
            report["compacted"] = False
            return report

        scan = table.scan(row_filter=row_filter) if row_filter is not None else table.scan()
        if reference:
            scan = scan.use_ref(reference)
        data = scan.to_arrow()
        report["rows"] = data.num_rows
        target = reference or ICEBERG_MAIN_BRANCH
        if partitions == [{}]:  # unpartitioned: there is nothing to overwrite dynamically
            table.overwrite(data, branch=target)
        else:
            table.dynamic_partition_overwrite(data, branch=target)
        logger.info(
            "dataset %s: compacted %d files across %d partitions into %d rows on %s",
            self.dataset_name(),
            files,
            len(partitions),
            report["rows"],
            target,
        )
        report["compacted"] = True
        return report

    def iceberg_expire_snapshots(
        self,
        *,
        table: Any = None,
        older_than: datetime.datetime | datetime.timedelta | None = None,
        snapshot_ids: list[int] | None = None,
        dry_run: bool = False,
    ) -> list[int]:
        """Drop snapshots this table no longer needs.

        `older_than` takes a cutoff or a `timedelta` back from now -- a
        retention window, which is how this is actually configured -- and
        pyiceberg keeps whatever any ref still points at, so a branch or tag
        protects its own history.

        **This frees no disk on its own.** pyiceberg's `expire_snapshots` is
        metadata-only: it drops the snapshots and leaves every data file they
        alone referenced sitting in the warehouse, unreachable. That is what
        `iceberg_cleanup` is for, and why it runs this and then goes looking
        for what it stranded.

        Returns the snapshot ids expired, or with `dry_run` the ones that
        would be; an empty list means nothing was old enough.
        """
        if table is None:
            raise NotImplementedError(
                f"{type(self).__name__}.iceberg_expire_snapshots needs table=<pyiceberg Table>"
            )
        if isinstance(older_than, datetime.timedelta):
            older_than = datetime.datetime.now(datetime.UTC) - older_than
        chosen = list(snapshot_ids or ())
        if older_than is not None:
            cutoff = older_than.timestamp() * 1000
            protected = {reference.snapshot_id for reference in table.refs().values()}
            chosen += [
                snapshot.snapshot_id
                for snapshot in table.snapshots()
                if snapshot.timestamp_ms < cutoff and snapshot.snapshot_id not in protected
            ]
        chosen = sorted(set(chosen))
        if chosen and not dry_run:
            table.maintenance.expire_snapshots().by_ids(chosen).commit()
            logger.info("dataset %s: expired %d snapshots", self.dataset_name(), len(chosen))
        return chosen

    def iceberg_cleanup(
        self,
        *,
        table: Any = None,
        older_than: datetime.datetime | datetime.timedelta | None = None,
        orphan_grace: datetime.timedelta = ICEBERG_ORPHAN_GRACE,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Reclaim the space the table is no longer using. Three steps.

        1. **Metadata files** are pyiceberg's own job, once the table says so:
           `write.metadata.delete-after-commit.enabled` and
           `write.metadata.previous-versions-max` make every commit prune the
           `metadata.json` trail behind it, retroactively on the first commit
           after they are set. So this sets them rather than deleting
           anything itself -- a table that keeps itself tidy needs no
           maintenance pass at all.
        2. **Snapshots** older than `older_than` (default:
           `protocols.iceberg.retain`) are expired.
        3. **Orphans** -- and this is the step nothing else does. Expiring a
           snapshot in pyiceberg drops metadata and *nothing else*: every
           data file only that snapshot referenced stays on disk, unreachable
           and unaccounted for. So the reachable set is computed from
           `inspect.all_files()`/`all_manifests()` across every surviving
           snapshot, the warehouse under the table's location is listed, and
           what is in the second and not the first is deleted.

           `orphan_grace` is why that is safe: a file younger than the grace
           period is left alone, because a write in flight has files on disk
           that no committed snapshot references yet. Deleting those would
           destroy a concurrent writer's work. Three days by default, which
           is the same conservative default the JVM implementation uses.
        """
        if table is None:
            raise NotImplementedError(
                f"{type(self).__name__}.iceberg_cleanup needs table=<pyiceberg Table>"
            )
        report: dict[str, Any] = {"properties": [], "expired": [], "orphans": []}

        missing = {
            key: value
            for key, value in ICEBERG_METADATA_RETENTION.items()
            if key not in table.properties
        }
        if missing and not dry_run:
            with table.transaction() as transaction:
                transaction.set_properties(**missing)
            table.refresh()
        report["properties"] = sorted(missing)

        report["expired"] = self.iceberg_expire_snapshots(
            table=table,
            older_than=self.iceberg_retention() if older_than is None else older_than,
            dry_run=dry_run,
        )
        if report["expired"] and not dry_run:
            table.refresh()

        report["orphans"] = _orphan_files(table, orphan_grace)
        if report["orphans"] and not dry_run:
            for path in report["orphans"]:
                table.io.delete(path)
            logger.info(
                "dataset %s: deleted %d orphaned files",
                self.dataset_name(),
                len(report["orphans"]),
            )
        return report

    def iceberg_optimize(
        self, *, table: Any = None, branch: str | None = None, dry_run: bool = False
    ) -> dict[str, Any]:
        """Compact, then clean up -- both driven by this dataset's own config.

        The order is not a preference. Compaction *creates* garbage: the
        files it replaced become unreachable the moment the new ones commit,
        so cleaning first would only have to be redone. And enabling manifest
        merging comes before either, because it is the one that stops the
        problem recurring: pyiceberg writes a manifest per commit and does
        **not** merge them by default, so a streaming table accumulates
        manifests as fast as it accumulates files. Turning
        `commit.manifest-merge.enabled` on costs nothing per commit and means
        the next thousand writes do not need this pass.

        `protocols.iceberg.compact_min_files` and `protocols.iceberg.retain`
        are the whole policy, so a scheduled `rekep dataset optimize` needs
        no arguments and no code -- the side file already says what this
        dataset wants.
        """
        if table is None:
            raise NotImplementedError(
                f"{type(self).__name__}.iceberg_optimize needs table=<pyiceberg Table>"
            )
        report: dict[str, Any] = {"manifest_merge": False}
        if ICEBERG_MANIFEST_MERGE not in table.properties:
            report["manifest_merge"] = True
            if not dry_run:
                with table.transaction() as transaction:
                    transaction.set_properties(**{ICEBERG_MANIFEST_MERGE: "true"})
                table.refresh()

        report["compaction"] = self.iceberg_compact(table=table, branch=branch, dry_run=dry_run)
        if report["compaction"].get("compacted"):
            table.refresh()
        report["cleanup"] = self.iceberg_cleanup(table=table, dry_run=dry_run)
        return report

    def iceberg_publish(self, *, table: Any = None, branch: str | None = None) -> int | None:
        """Fast-forward `main` onto `branch`: the publish half of write-audit-publish.

        A branch write leaves `main` untouched on purpose -- that is what
        makes it safe to iterate against real data. Publishing is the moment
        that stops being true, so it is its own explicit call, never
        something a write does on its own. Returns the snapshot id `main`
        now points at, or None when the branch has nothing to publish.
        """
        if table is None:
            raise NotImplementedError(
                f"{type(self).__name__}.iceberg_publish needs table=<pyiceberg Table>"
            )
        reference = branch or self.iceberg_branch()
        if not reference or reference == ICEBERG_MAIN_BRANCH:
            raise ValueError(
                f"dataset {self.dataset_name()!r}: publish needs a branch other than "
                f"{ICEBERG_MAIN_BRANCH!r}; pass branch= or declare protocols.iceberg.branch"
            )
        snapshot = table.snapshot_by_name(reference)
        if snapshot is None:
            return None
        table.manage_snapshots().set_current_snapshot(ref_name=reference).commit()
        logger.info(
            "dataset %s: published %s (snapshot %s) onto %s",
            self.dataset_name(),
            reference,
            snapshot.snapshot_id,
            ICEBERG_MAIN_BRANCH,
        )
        return snapshot.snapshot_id

    # -- iceberg ref resolution -------------------------------------------

    def _read_ref(self, table: Any, branch: str | None) -> str | None:
        """The ref a read should use: `branch`, else the declared one, else none.

        A declared branch that does not exist on the table yet resolves to
        None -- read the table's current state -- rather than failing: a
        branch only appears once something has been written to it, so a
        dataset declaring one is describing where writes go, not asserting
        that reads already have somewhere to look.
        """
        reference = branch or self.iceberg_branch()
        if not reference:
            return None
        if reference in table.refs():
            return reference
        logger.info(
            "dataset %s: no %r ref on the table yet, reading its current state instead",
            self.dataset_name(),
            reference,
        )
        return None

    def _write_ref(self, table: Any, branch: str | None, *, bootstrapping: bool) -> str:
        """The branch a write should target, forking it from `main` if it is new."""
        reference = branch or self.iceberg_branch() or ICEBERG_MAIN_BRANCH
        if reference == ICEBERG_MAIN_BRANCH:
            return reference
        if bootstrapping:
            logger.info(
                "dataset %s: table has no snapshot yet, bootstrapping on %s instead of %r",
                self.dataset_name(),
                ICEBERG_MAIN_BRANCH,
                reference,
            )
            return ICEBERG_MAIN_BRANCH
        _ensure_iceberg_branch(table, reference)
        return reference


def _counting(reader: pyarrow.RecordBatchReader) -> tuple[pyarrow.RecordBatchReader, list[int]]:
    """A `RecordBatchReader` that counts the rows passing through it.

    `pyarrow.dataset.write_dataset` streams its input and reports nothing
    back, so the row count `file_write_arrow_reader` returns has to be
    tallied on the way through -- one pass, no materialising.
    """
    count = [0]

    def generate() -> Any:
        for batch in reader:
            count[0] += batch.num_rows
            yield batch

    return pyarrow.RecordBatchReader.from_batches(reader.schema, generate()), count


def _chunked(reader: pyarrow.RecordBatchReader, commit_row_size: int) -> Iterator[pyarrow.Table]:
    """Group `reader`'s batches into `pyarrow.Table`s of about `commit_row_size` each.

    The last chunk may be smaller; a `commit_row_size` larger than the reader
    yields exactly one chunk, the whole thing -- the same shape a caller
    doing one big `upsert` would reach for by hand.
    """
    pending: list[pyarrow.RecordBatch] = []
    pending_rows = 0
    for batch in reader:
        pending.append(batch)
        pending_rows += batch.num_rows
        if pending_rows >= commit_row_size:
            yield pyarrow.Table.from_batches(pending, schema=reader.schema)
            pending, pending_rows = [], 0
    if pending:
        yield pyarrow.Table.from_batches(pending, schema=reader.schema)


def _evolve_iceberg_table(table: Any, schema: pyarrow.Schema, dataset: str) -> None:
    """Add the columns `schema` has and `table` does not, in one commit.

    pyiceberg's own `union_by_name` does the adding. It is handed **only the
    new fields**, never the whole union: a column both sides have is already
    exactly what the table declared, and union_by_name maps Arrow
    nullability onto Iceberg's `required` verbatim, so re-stating an
    existing column can silently relax a NOT NULL one to optional. Nothing
    is restated, so nothing can be relaxed.

    New columns are nullable, which is not a preference: `union_by_name`
    refuses a required addition outright ("cannot add required column"),
    because rows already written have nothing to put in it.

    Nothing happens when there is nothing to add, so a `merge_schema` write
    over a table that has already caught up costs one name comparison.
    """
    existing = {field.name for field in table.schema().fields}
    additions = [field for field in schema if field.name not in existing]
    if not additions:
        return
    with table.update_schema() as update:
        update.union_by_name(pyarrow.schema(additions))
    table.refresh()
    logger.info(
        "dataset %s: added columns %s", dataset, ", ".join(field.name for field in additions)
    )


def _iceberg_write_schema(table: Any, schema: pyarrow.Schema, *, complete: bool) -> pyarrow.Schema:
    """`schema`, in the table's column order, carrying the table's field ids.

    Identity is the whole point. Iceberg matches columns **by field id**, and
    `union_by_name` does not keep the ids an Arrow schema arrives with -- it
    assigns its own, counting on from the table's `last-column-id`. So the
    ids `merge_schemas` stamped (counted on from the *record*, which never
    changes) are only accidentally right, and diverge the moment two
    widening writes carry different extra columns: the second write's data
    would be filed under the first write's column, silently, with no error
    anywhere. Taking every id back from the table closes that.

    Only the ids are taken. The *types* stay the reader's, because pyiceberg
    spells strings `large_string` in `Schema.as_arrow()` and accepts either
    on write -- adopting them wholesale would re-encode every string column
    on every batch to satisfy a difference the writer does not care about.

    `complete=True` also carries the columns the table has and the stream
    does not, as nulls. An append does not need them (Iceberg fills an
    absent optional column itself) but a **merge** does: pyiceberg's
    `upsert` compares the incoming frame against the rows it scanned back,
    column for column, and refuses a frame that is merely narrower.
    """
    live = {field.name: field for field in table.schema().as_arrow()}
    fields = []
    for field in schema:
        source = live.pop(field.name, None)
        identifier = (source.metadata or {}).get(FIELD_ID_KEY) if source is not None else None
        if identifier is None:
            # A column the table does not have is left exactly as it came:
            # dropping it would hide the mismatch, and pyiceberg's own
            # "contains more columns" error says it far better than a
            # silently narrower write would.
            fields.append(field)
        else:
            fields.append(field.with_metadata({**(field.metadata or {}), FIELD_ID_KEY: identifier}))
    if complete:
        fields.extend(live.values())
    return pyarrow.schema(fields, metadata=schema.metadata)


def _align_iceberg_ref(table: Any, branch: str) -> None:
    """Move `branch` onto a snapshot that uses the table's current schema.

    **A snapshot records the schema it was written under, and a scan
    projects that schema, not the table's current one.** So after any schema
    change -- this write's own `merge_schema`, a `deploy` that converged new
    columns, someone else's evolution -- reading the branch back still
    yields the old column set, and pyiceberg's `upsert` fails outright
    comparing that against the widened data it was handed.

    An empty append moves the ref forward: no rows, so no data file, one
    metadata-only commit. It is guarded on the ref's own snapshot rather
    than on "did I just add a column", because those are different
    questions -- a branch forked before an evolution is stale even when the
    table itself is perfectly up to date.
    """
    snapshot = table.snapshot_by_name(branch)
    if snapshot is None:
        return
    current = table.schema()
    if getattr(snapshot, "schema_id", None) == current.schema_id:
        return
    table.append(current.as_arrow().empty_table(), branch=branch)
    table.refresh()


def _ensure_iceberg_branch(table: Any, branch: str) -> None:
    """Create `branch` from `main`'s current snapshot, if it does not exist yet.

    Without this, pyiceberg's own `append(branch=...)` auto-creates a branch
    with no parent on first write to it -- an independent, empty lineage
    that merely shares the table, not a fork of what `main` already has,
    which is what a dev/WAP branch means. A `main` with no snapshot yet
    (a brand new table) has nothing to fork from; the first write there
    must still go to `main` itself, same as pyiceberg's own requirement.
    """
    if branch in table.refs():
        return
    source = table.snapshot_by_name(ICEBERG_MAIN_BRANCH)
    if source is not None:
        table.manage_snapshots().create_branch(source.snapshot_id, branch).commit()


def _target_file_bytes(table: Any) -> int:
    """The table's own bin-packing target, or Iceberg's default."""
    declared = table.properties.get(ICEBERG_TARGET_FILE_BYTES)
    return int(declared) if declared else ICEBERG_TARGET_FILE_BYTES_DEFAULT


def _crowded_partitions(
    table: Any, snapshot_id: int, min_input_files: int, target_bytes: int
) -> list[tuple[tuple[tuple[str, Any], ...], int]]:
    """Partitions worth rewriting: many files, and small ones.

    Read from `inspect.data_files()`, which is the manifest list rather than
    the data, so deciding what to compact costs no scan at all. An
    unpartitioned table reports one group with no values -- the whole table
    -- which is exactly how it should be treated.

    Both halves of the test matter. A partition of eight files is not
    fragmented if each is already the target size; it is just a big
    partition, and rewriting it would read and write everything to achieve
    nothing.
    """
    files = table.inspect.data_files(snapshot_id=snapshot_id)
    if files.num_rows == 0:
        return []
    counts: dict[tuple[tuple[str, Any], ...], list[int]] = {}
    for values, size in zip(
        files.column("partition").to_pylist(),
        files.column("file_size_in_bytes").to_pylist(),
        strict=True,
    ):
        key = tuple(sorted((values or {}).items()))
        tally = counts.setdefault(key, [0, 0])
        tally[0] += 1
        tally[1] += size or 0
    return [
        (key, count)
        for key, (count, total) in counts.items()
        if count >= min_input_files and total / count < target_bytes
    ]


def _key_bounds(table: Any, branch: str, join_cols: list[str]) -> dict[str, tuple[Any, Any]] | None:
    """The min and max each join column spans across the table's data files.

    Iceberg writes these into every manifest, so this is a metadata read --
    no scan, one call for a whole write. None means "no bounds to reason
    with": an empty branch, or a column whose statistics were not collected
    (`write.metadata.metrics.*` can turn them off), in which case the merge
    must go ahead as normal.
    """
    snapshot = table.snapshot_by_name(branch)
    if snapshot is None:
        return None
    files = table.inspect.data_files(snapshot_id=snapshot.snapshot_id)
    if files.num_rows == 0:
        return None
    metrics = files.column("readable_metrics").to_pylist()
    spans: dict[str, tuple[Any, Any]] = {}
    for column in join_cols:
        lows = [row[column]["lower_bound"] for row in metrics if row.get(column)]
        highs = [row[column]["upper_bound"] for row in metrics if row.get(column)]
        known = [(low, high) for low, high in zip(lows, highs, strict=True) if low is not None]
        if len(known) != len(metrics):
            return None
        spans[column] = (min(low for low, _ in known), max(high for _, high in known))
    return spans


def _outside(chunk: pyarrow.Table, join_cols: list[str], bounds: Any) -> bool:
    """True when nothing in `chunk` can possibly match what the table holds.

    One column is enough: a row matches only if it matches on *every* join
    column, so a chunk whose range on any single one misses the table's
    range entirely cannot contain a match at all.
    """
    if not bounds:
        return False
    for column, (low, high) in bounds.items():
        span = pyarrow.compute.min_max(chunk.column(column)).as_py()
        if span["min"] is None:
            return False
        if span["max"] < low or span["min"] > high:
            return True
    return False


def _orphan_files(table: Any, grace: datetime.timedelta) -> list[str]:
    """Files under the table's location that nothing reachable references.

    The reachable set is every surviving snapshot's data, delete and manifest
    files, plus the manifest lists and the `metadata.json` trail -- so a file
    is an orphan only if no snapshot the table still has can reach it.

    Anything younger than `grace` is spared regardless: a writer mid-commit
    has files on disk that no snapshot references *yet*, and they are
    indistinguishable from garbage by reachability alone. Age is what tells
    them apart.
    """
    location = table.location()
    try:
        filesystem, root = resolve_filesystem(_listable(location))
    except Exception as error:  # a location no pyarrow filesystem can enumerate
        logger.warning("cannot list %s to look for orphans (%s); nothing freed", location, error)
        return []
    cutoff = datetime.datetime.now(datetime.UTC) - grace

    reachable = {
        _bare_path(path) for path in table.inspect.all_files().column("file_path").to_pylist()
    }
    reachable |= {
        _bare_path(path) for path in table.inspect.all_manifests().column("path").to_pylist()
    }
    reachable |= {
        _bare_path(snapshot.manifest_list)
        for snapshot in table.snapshots()
        if snapshot.manifest_list
    }
    reachable |= {
        _bare_path(entry["file"])
        for entry in table.inspect.metadata_log_entries().to_pylist()
        if entry.get("file")
    }
    reachable |= {_bare_path(table.metadata_location)}

    selector = pyarrow.fs.FileSelector(root, recursive=True, allow_not_found=True)
    orphans = []
    for info in filesystem.get_file_info(selector):
        if info.type is not pyarrow.fs.FileType.File:
            continue
        if _bare_path(info.path) in reachable:
            continue
        modified = info.mtime
        if modified is not None and modified.astimezone(datetime.UTC) > cutoff:
            continue
        orphans.append(f"{location.rstrip('/')}/{info.path[len(root) :].lstrip('/')}")
    return sorted(orphans)


def _listable(location: str) -> str:
    """A location `pyarrow.fs` can enumerate.

    `file://` with a *relative* path -- `file://stacks/iceberg/warehouse` --
    is a spelling catalogs accept and pyarrow refuses, because the first
    segment reads as a hostname. It is unambiguous in practice (there is no
    such host), so it is resolved as the path it plainly means rather than
    failing a maintenance pass over a URI's punctuation.
    """
    if location.startswith("file://") and not location.startswith("file:///"):
        return location[len("file://") :]
    return location


def _bare_path(uri: str | None) -> str:
    """A path with any scheme and authority stripped, for comparing sets.

    The same file is spelled `file:///wh/t/data/x.parquet` in a manifest and
    `/wh/t/data/x.parquet` by the filesystem that lists it. Comparing those
    as strings finds no overlap at all -- and an orphan sweep that thinks
    nothing is reachable deletes the table.
    """
    if not uri:
        return ""
    _, _, rest = str(uri).partition("://")
    path = rest or str(uri)
    return "/" + path.partition("/")[2].strip("/") if rest else "/" + path.strip("/")


def _partition_filter(table: Any, partitions: list[dict[str, Any]]) -> Any:
    """A row filter matching exactly `partitions`, or None for the whole table.

    Only works for `identity` partitions: their partition value *is* the
    column value, so `EqualTo(column, value)` selects the partition. Any
    other transform stores something derived (a day number, a bucket
    ordinal) that no predicate on the source column can name, so it is
    refused rather than guessed at.
    """
    from pyiceberg.expressions import And, EqualTo, Or

    if partitions == [{}]:
        return None
    columns = {}
    for field in table.spec().fields:
        if str(field.transform) != "identity":
            raise ValueError(
                f"partition {field.name!r} uses the {field.transform} transform, whose value is "
                "not a column value to filter on; pass row_filter= to say what to rewrite"
            )
        columns[field.name] = table.schema().find_field(field.source_id).name
    matches = []
    for values in partitions:
        predicates = [EqualTo(columns[name], value) for name, value in values.items()]
        matches.append(predicates[0] if len(predicates) == 1 else And(*predicates))
    return matches[0] if len(matches) == 1 else Or(*matches)
