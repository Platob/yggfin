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

Writing is generic at the top and protocol-specific underneath:
`write_arrow_reader` dispatches by `format` to `_{format}_write_arrow_reader`
-- a private method that opens a `Run`, calls the *public*
`{format}_write_arrow_reader` hook, and closes the run on the way out.
`iceberg_write_arrow_reader` is that hook for Iceberg: public because it is
the customisation point a deployment overrides to say how the write actually
happens, abstract in spirit because the base implementation only knows how
to write to a table it is handed. It leverages pyiceberg's own table-level
API directly -- `append`/`upsert`/`overwrite`, each `branch`-aware -- rather
than reimplementing any of it; `upsert`'s `join_cols` falls back to the
table's Iceberg identifier fields, which is exactly what a record's
`Arrow(key=True)` fields already become. The private method is the only
thing between the generic dispatcher and that hook, and lineage tracking is
the whole reason it exists rather than calling the hook directly.
`file_write_arrow_reader` is the same shape for any `pyarrow.fs` filesystem:
a URI maps to a `(filesystem, path)` pair through `rekep.filesystems`,
cached per URL, and the reader streams straight into `pyarrow.dataset.write_dataset`.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
from collections.abc import Iterator
from typing import Any

import pyarrow
import pyarrow.dataset
import pyarrow.fs

from rekep.filesystems import resolve as resolve_filesystem
from rekep.imports import locate
from rekep.job import Job
from rekep.namespace import unique_uri
from rekep.records import registry
from rekep.records.record import Record, record
from rekep.run import InputDataset, OutputDataset, Run, RunEvent, RunState
from rekep.run import now as _now

logger = logging.getLogger("rekep.dataset")

#: pyiceberg's own default ref; passed explicitly rather than relied on as a
#: parameter default, since `iceberg_branch()` may resolve to None.
ICEBERG_MAIN_BRANCH = "main"

#: Rows accumulated per `upsert` call. A merge needs to compare against
#: existing data, so some materialising is unavoidable; chunking bounds
#: memory and turns many small merges into few large ones, which is what
#: lets Iceberg's partition pruning actually pay off.
ICEBERG_UPSERT_CHUNK_ROWS = 100_000

#: `protocols[<protocol>]` keys that route a write/deploy rather than
#: describe the table -- excluded from `table_properties()`.
_PROTOCOL_ROUTING_KEYS = frozenset({"location", "branch"})

#: Where dataset side files live, relative to the deployment root. Overridable
#: per call and by environment, so a datasets folder can point anywhere.
DATASETS_ROOT = pathlib.Path(os.environ.get("REKEP_DATASETS_ROOT", "stacks/datasets"))


@record
class Dataset(Record):
    """A namespace-qualified data product: schema, location, lineage.

    `properties` are shared by every protocol that writes this dataset;
    `direct` is a single physical location shared the same way; `protocols`
    carries per-protocol overrides (its own `location`, its own properties),
    each merged over the shared ones -- `protocol_properties("iceberg")`
    resolves exactly what an Iceberg write should see.
    """

    record: str
    """Dotted path of the `Record` class that is this dataset's schema."""

    name: str | None = None
    """Dataset name; defaults to the record's snake_case name."""

    namespace: str = "default"
    """OpenLineage namespace this dataset is identified under."""

    direct: str | None = None
    """A single physical location (a path or URI), shared by every protocol."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Properties shared by every protocol that writes this dataset."""

    protocols: dict[str, dict[str, str]] = dataclasses.field(default_factory=dict)
    """Per-protocol overrides (`iceberg`, `doris`, ...), merged over `properties`."""

    # -- identity / schema ----------------------------------------------

    @classmethod
    def load_all(
        cls, root: str | os.PathLike[str] = DATASETS_ROOT, **context: Any
    ) -> list[Dataset]:
        """Every dataset declared under `root`, one file each, stem defaults `name`.

        The same registry-of-one-folder shape `IcebergDeployment`'s
        `catalogs/`/`namespaces/` use -- no `tables/` folder anywhere
        declares a table directly; a `Dataset` here deploys autonomously
        against whichever catalog/namespace registry `deploy_iceberg`/
        `deploy_doris` is handed.
        """
        return registry.entries(pathlib.Path(root), cls, context)

    def record_class(self) -> type[Record]:
        cls = locate(self.record)
        if not (isinstance(cls, type) and issubclass(cls, Record)):
            raise TypeError(f"{self.record} is not a Record class")
        return cls

    def dataset_name(self) -> str:
        """This dataset's name; the record's snake_case name when undeclared."""
        return self.name or self.record_class().doris_table_name()

    def uri(self) -> str:
        """This dataset's globally unique id: `dataset://namespace/name`.

        Built by `rekep.namespace.unique_uri`, the one place a job and a
        dataset's identifiers come from -- so the two can never collide even
        when they share a namespace and a name.
        """
        return unique_uri("dataset", self.namespace, self.dataset_name())

    def schema_facet(self) -> dict[str, Any]:
        """OpenLineage `SchemaDatasetFacet`: the record's fields, by name."""
        return {"fields": self.record_class().into_dict()["fields"]}

    def facets(self) -> dict[str, Any]:
        """Every static facet this dataset carries; `schema` and `dataSource` always included."""
        return {"schema": self.schema_facet(), "dataSource": {"uri": self.uri()}}

    # -- location -------------------------------------------------------

    def location(self, protocol: str | None = None) -> str | None:
        """The physical location for `protocol`, falling back to `direct`."""
        if protocol:
            override = self.protocols.get(protocol, {}).get("location")
            if override:
                return override
        return self.direct

    def protocol_properties(self, protocol: str) -> dict[str, str]:
        """`properties` merged with `protocol`'s own, the protocol winning."""
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
        return self.protocols.get("iceberg", {}).get("branch")

    # -- deploy: autonomous, no side file needed -------------------------

    def into_iceberg_table(self) -> Any:
        """This dataset as an ad hoc `IcebergTable`, ready for `Iceberg.deploy_one`."""
        from rekep.records.iceberg import IcebergTable

        return IcebergTable(
            record=self.record,
            name=self.dataset_name(),
            namespace=self.namespace,
            location=self.location("iceberg"),
            properties=self.table_properties("iceberg"),
        )

    def into_doris_table(self) -> Any:
        """This dataset as an ad hoc `DorisTable`, ready for `Doris.deploy_one`."""
        from rekep.records.doris import DorisTable

        return DorisTable(
            record=self.record,
            name=self.dataset_name(),
            namespace=self.namespace,
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
            namespace=self.namespace,
            name=self.dataset_name(),
            facets=self.facets(),
            input_facets=input_facets,
        )

    def as_output(self, **output_facets: Any) -> OutputDataset:
        """This dataset as a run's `OutputDataset` reference."""
        return OutputDataset(
            namespace=self.namespace,
            name=self.dataset_name(),
            facets=self.facets(),
            output_facets=output_facets,
        )

    def events(self) -> list[RunEvent]:
        """This dataset's own lineage log: every run this instance tracked.

        Internal bookkeeping, not an emission -- events accumulate on the
        instance (lazily, so a fresh `Dataset` costs nothing extra) rather
        than going anywhere until something else reads them.
        """
        return list(self.__dict__.get("_Dataset__events", ()))

    # -- writing ----------------------------------------------------------

    def write_arrow_reader(
        self,
        reader: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        format: str,
        **options: Any,
    ) -> int:
        """Write `reader`'s batches through the protocol named `format`.

        Dispatches to `_{format}_write_arrow_reader` -- e.g. `format="iceberg"`
        calls `_iceberg_write_arrow_reader`. Every protocol's private method
        wraps its own public write in the same lineage tracking, so the
        dispatch itself carries none.

        `reader` need not already be a `pyarrow.RecordBatchReader` -- a plain
        iterator of batches (what `Job.arrow_transform` yields) is wrapped in
        one against `record_class().into_arrow_schema()`, so a job's output
        pipes straight into a dataset's write with no ceremony at the call
        site: `dataset.write_arrow_reader(job.arrow_transform(job.extract()), ...)`.
        """
        if not isinstance(reader, pyarrow.RecordBatchReader):
            reader = pyarrow.RecordBatchReader.from_batches(
                self.record_class().into_arrow_schema(), reader
            )
        private = getattr(self, f"_{format}_write_arrow_reader", None)
        if not callable(private):
            raise ValueError(f"dataset {self.dataset_name()!r}: no {format!r} writer")
        return private(reader, **options)

    def iceberg_write_arrow_reader(
        self,
        reader: pyarrow.RecordBatchReader,
        *,
        table: Any = None,
        mode: str = "append",
        branch: str | None = None,
        join_cols: list[str] | None = None,
        chunk_rows: int = ICEBERG_UPSERT_CHUNK_ROWS,
        **options: Any,
    ) -> int:
        """Write `reader`'s batches to an Iceberg table -- append, upsert or overwrite.

        The public write itself: abstract in spirit, not by enforcement --
        override it to change how the table is resolved (a catalog lookup
        against `protocol_properties("iceberg")`, a cached connection) or how
        the write happens. The default here expects `table=`, a live
        `pyiceberg.table.Table`, and calls straight into its own API rather
        than reimplementing any of it. `branch` defaults to `iceberg_branch()`
        -- this dataset's own declared branch -- falling back to `main`.

        `mode`:

        - `"append"` (default) streams one batch at a time, never
          materialising the whole reader.
        - `"upsert"` merges `chunk_rows` rows at a time via pyiceberg's own
          `Table.upsert`, joined on `join_cols` when given, else the
          identifier fields a record's `Arrow(key=True)` columns already
          become in the Iceberg schema. Chunked, not streamed one batch at a
          time: a merge needs to compare against existing data, so some
          materialising is unavoidable, but accumulating first bounds memory
          and turns many small merges into few large ones -- fewer,
          partition-aligned commits instead of one per batch.
        - `"overwrite"` replaces the table (or `options["overwrite_filter"]`'s
          match) with the whole reader; needs it to fit in memory, same as
          `Table.overwrite` itself.

        A `branch` that does not exist yet is created from `main`'s current
        snapshot first (`_ensure_iceberg_branch`) -- pyiceberg's own
        auto-creation on first write gives the branch no parent at all
        (an independent, empty lineage that merely shares the table), which
        is not what a dev/WAP branch means: it should start as a fork of
        what `main` already has. A table with no snapshot at all yet -- the
        very first write to it, ever -- has nothing to fork from either way;
        Iceberg allows only `main` there, so that first write always lands
        on `main` regardless of `branch`, logged so the redirect is never
        silent. Every write after it is free to target the declared branch.
        """
        if table is None:
            raise NotImplementedError(
                f"{type(self).__name__}.iceberg_write_arrow_reader needs table=<pyiceberg "
                "Table>; override to resolve one from protocol_properties('iceberg')"
            )
        branch = branch or self.iceberg_branch() or ICEBERG_MAIN_BRANCH
        if branch != ICEBERG_MAIN_BRANCH:
            if table.current_snapshot() is None:
                logger.info(
                    "dataset %s: table has no snapshot yet, bootstrapping on %s instead of %r",
                    self.dataset_name(),
                    ICEBERG_MAIN_BRANCH,
                    branch,
                )
                branch = ICEBERG_MAIN_BRANCH
            else:
                _ensure_iceberg_branch(table, branch)

        if mode == "append":
            written = 0
            for batch in reader:
                chunk = pyarrow.Table.from_batches([batch], schema=reader.schema)
                table.append(chunk, branch=branch, **options)
                written += batch.num_rows
            return written

        if mode == "overwrite":
            whole = reader.read_all()
            table.overwrite(whole, branch=branch, **options)
            return whole.num_rows

        if mode == "upsert":
            written = 0
            for chunk in _chunked(reader, chunk_rows):
                result = table.upsert(chunk, join_cols=join_cols, branch=branch, **options)
                written += result.rows_updated + result.rows_inserted
            return written

        raise ValueError(f"dataset {self.dataset_name()!r}: no iceberg {mode!r} write mode")

    def _iceberg_write_arrow_reader(self, reader: pyarrow.RecordBatchReader, **options: Any) -> int:
        """Lineage-wrapped call to the public `iceberg_write_arrow_reader`.

        The write itself is the public method's job, kept overridable on its
        own; this private method is only the boundary around it -- a `Run`
        opens before the call and closes after, whatever the public hook
        actually does to move the data.
        """
        return self._tracked_write("iceberg", self.iceberg_write_arrow_reader, reader, options)

    def file_write_arrow_reader(
        self,
        reader: pyarrow.RecordBatchReader,
        *,
        uri: str | None = None,
        filesystem: pyarrow.fs.FileSystem | None = None,
        **options: Any,
    ) -> int:
        """Write `reader`'s batches to a file location, any `pyarrow.fs` filesystem.

        The public write itself: generic across filesystems the way
        `iceberg_write_arrow_reader` is generic across catalogs. `uri`
        defaults to `location("file")` (falling back to `direct`) and maps
        to a `(filesystem, path)` pair through `rekep.filesystems.resolve`,
        cached per URL; pass `filesystem=` explicitly to skip that mapping --
        the escape hatch for a filesystem built from `protocol_properties`
        (credentials, region) rather than parsed off the URI. `options`
        reaches `pyarrow.dataset.write_dataset` as-is (`format="parquet"` by
        default); the reader streams into it one batch at a time, never
        materialising the whole thing.
        """
        target = uri or self.location("file")
        if not target:
            raise NotImplementedError(
                f"{type(self).__name__}.file_write_arrow_reader needs a location: pass uri=, "
                "set direct=, or protocols['file']['location']"
            )
        if filesystem is None:
            filesystem, target = resolve_filesystem(target)
        counted, count = _counting(reader)
        write_format = options.pop("format", "parquet")
        pyarrow.dataset.write_dataset(
            counted, target, filesystem=filesystem, format=write_format, **options
        )
        return count[0]

    def _file_write_arrow_reader(self, reader: pyarrow.RecordBatchReader, **options: Any) -> int:
        """Lineage-wrapped call to the public `file_write_arrow_reader`."""
        return self._tracked_write("file", self.file_write_arrow_reader, reader, options)

    # -- internal lineage tracking ---------------------------------------

    def _tracked_write(
        self,
        protocol: str,
        writer: Any,
        reader: pyarrow.RecordBatchReader,
        options: dict[str, Any],
    ) -> int:
        """START a `Run`, call `writer`, COMPLETE or FAIL it -- return what it wrote."""
        run = Run()
        job = Job(name=f"{self.dataset_name()}.write.{protocol}", namespace=self.namespace)
        output = self.as_output()
        self._emit(
            RunEvent(
                event_type=RunState.START,
                event_time=_now(),
                run=run,
                job=job,
                outputs=[output],
            )
        )
        try:
            written = writer(reader, **options)
        except Exception:
            self._emit(
                RunEvent(
                    event_type=RunState.FAIL,
                    event_time=_now(),
                    run=run,
                    job=job,
                    outputs=[output],
                )
            )
            raise
        completed = dataclasses.replace(
            output, output_facets={"outputStatistics": {"rowCount": written}}
        )
        self._emit(
            RunEvent(
                event_type=RunState.COMPLETE,
                event_time=_now(),
                run=run,
                job=job,
                outputs=[completed],
            )
        )
        return written

    def _emit(self, event: RunEvent) -> RunEvent:
        self.__dict__.setdefault("_Dataset__events", []).append(event)
        return event


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


def _chunked(reader: pyarrow.RecordBatchReader, chunk_rows: int) -> Iterator[pyarrow.Table]:
    """Group `reader`'s batches into `pyarrow.Table`s of about `chunk_rows` each.

    The last chunk may be smaller; a `chunk_rows` larger than the reader
    yields exactly one chunk, the whole thing -- the same shape a caller
    doing one big `upsert` would reach for by hand.
    """
    pending: list[pyarrow.RecordBatch] = []
    pending_rows = 0
    for batch in reader:
        pending.append(batch)
        pending_rows += batch.num_rows
        if pending_rows >= chunk_rows:
            yield pyarrow.Table.from_batches(pending, schema=reader.schema)
            pending, pending_rows = [], 0
    if pending:
        yield pyarrow.Table.from_batches(pending, schema=reader.schema)


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
