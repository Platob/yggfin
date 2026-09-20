"""dbt's DuckDB adapter, reading and committing through this package's datasets.

dbt-duckdb reaches anything that is not DuckDB through a plugin module it
imports by name, and this is that module: a source is one Iceberg read, a model
is one Iceberg commit, and DuckDB owns only the SQL in between. Nothing in
`rekep` imports it -- dbt does, under the `runner` group a task executes in --
so the package itself declares no dbt dependency.

A model's own configuration is the declaration: `table` names the Iceberg table,
`primary_key`, `partition_by`, `sort_by` and `arrow_types` say what its rows
are keyed, laid out, ordered and typed by, and `mode` picks the verb. The shape
is otherwise the Arrow schema DuckDB staged, so a column a model stops selecting
stops being written.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from typing import Any

import pyarrow
import pyarrow.parquet
from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig

from rekep.fields import Field, field_of, partition_key, primary_key, replace_field, sort_key
from rekep.iceberg import IcebergCatalog

LOGGER = logging.getLogger(__name__)

#: The catalog an operator names without editing the profile. It holds the same
#: mapping every task document spells, as JSON, and wins over the profile's own
#: so one deployment configures dbt the way it configures every other task.
CATALOG = "REKEP_DBT_CATALOG"

#: What a source or a model takes for a table it does not name itself.
TABLE = "{schema}.{identifier}"

#: Rows one staged batch carries back out of the file dbt wrote. A commit holds
#: this much of a model at a time, not the whole of it.
BATCH_ROW_SIZE = 65_536

#: Rows each model committed, by the table it committed them to. A dbt build and
#: this module share one process, so what a run wrote is read back here rather
#: than guessed from a run result; `rekep.dbt.committed()` clears it for the
#: next build.
COMMITTED: dict[str, int] = {}

#: Every plugin in this process holding a catalog open. dbt builds a plugin per
#: run, keeps it for the life of the process and hands it no teardown, so the
#: catalog a build read and committed through outlives the build unless a task
#: closes it; `rekep.dbt.released()` is what closes them.
OPENED: list[Plugin] = []

#: What `mode` may say, and what the dataset verb each one names did. An append
#: adds every row the model built; an overwrite replaces the rows whose keys
#: match -- or, for a model with no key, the partitions it touches -- and adds
#: the rest. Both state the rows they carried.
MODES = {"append": "appended", "overwrite": "overwrote"}


def catalog_settings(declared: Any) -> dict[str, Any]:
    """One catalog mapping, from the profile's YAML or the environment's JSON."""
    if isinstance(declared, str):
        declared = json.loads(declared)
    if not isinstance(declared, Mapping):
        raise TypeError(
            "the rekep dbt plugin takes `catalog` as a mapping of name and properties, "
            f"not {type(declared).__name__}"
        )
    return dict(declared)


def committed() -> dict[str, int]:
    """What has been committed since the last call, and clear the record."""
    written = dict(COMMITTED)
    COMMITTED.clear()
    return written


def released() -> int:
    """Close the catalog every plugin opened, and say how many were holding one.

    An Iceberg catalog is a live SQLite or REST connection, and dbt hands a
    plugin nothing that says the build is over -- so a run that ends leaves one
    open, and on Windows an open file is one its caller cannot delete. A task
    calls this once its build is done, the way it calls `committed()`.
    """
    held = list(OPENED)
    OPENED.clear()
    for plugin in held:
        plugin.close()
    return len(held)


def declared_field(
    schema: pyarrow.Schema,
    table: str,
    *,
    arrow_types: Mapping[str, str] | None = None,
    keys: Sequence[str] = (),
    not_null: Sequence[str] = (),
    partition_by: Mapping[str, str] | Sequence[str] | None = None,
    sort_by: Mapping[str, str] | Sequence[str] | None = None,
) -> Field:
    """The staged Arrow schema as the table's declaration.

    A key is non-null because Iceberg identifier fields are, so naming one in
    `primary_key` is what makes it required; `arrow_types` is the one place a
    model states a storage type SQL cannot spell, such as the sixteen bytes an
    identity is. It is spelled `arrow_types` and not `column_types` because dbt
    owns that name for a seed's own casts.
    """
    field = Field.from_arrow_schema(schema)
    names = {member.name for member in field}
    marks: dict[str, dict[str, str]] = {}
    for column in keys:
        marks.setdefault(column, {}).update(primary_key()["metadata"])
    for column, transform in _named(partition_by, "identity").items():
        marks.setdefault(column, {}).update(partition_key(transform)["metadata"])
    for column, direction in _named(sort_by, "asc").items():
        marks.setdefault(column, {}).update(sort_key(direction)["metadata"])
    required = {*keys, *not_null}
    spelled = dict(arrow_types or {})
    unknown = sorted({*marks, *required, *spelled} - names)
    if unknown:
        raise ValueError(f"{table} declares {', '.join(unknown)}, which the model does not select")
    for member in list(field):
        metadata = {**dict(member.metadata), **marks.get(member.name, {})}
        dtype = spelled.get(member.name, member.dtype)
        field.set_field(
            member.name,
            replace_field(
                member,
                dtype=dtype,
                nullable=member.nullable and member.name not in required,
                metadata=metadata,
            ),
        )
    return field_of(field, table)


def _named(declared: Mapping[str, str] | Sequence[str] | None, default: str) -> dict[str, str]:
    """A column list or a column mapping as one mapping of column to reading."""
    if declared is None:
        return {}
    if isinstance(declared, Mapping):
        return {str(column): str(value) for column, value in declared.items()}
    if isinstance(declared, str):
        return {declared: default}
    return {str(column): default for column in declared}


class Plugin(BasePlugin):
    """The `rekep` plugin: dbt's sources and models, as Iceberg reads and commits."""

    def initialize(self, plugin_config: dict[str, Any]) -> None:
        """Hold the catalog this profile names; open it on first use."""
        self._settings = catalog_settings(os.environ.get(CATALOG) or plugin_config.get("catalog"))
        self._opened: IcebergCatalog | None = None

    def default_materialization(self) -> str:
        """A source is read once into DuckDB, not re-read per cursor."""
        return "table"

    def catalog(self) -> IcebergCatalog:
        """The one catalog handle this build reads and commits through."""
        if self._opened is None:
            self._opened = IcebergCatalog.from_dict(self._settings)
            OPENED.append(self)
        return self._opened

    def close(self) -> None:
        """Release the catalog this plugin opened, if it opened one."""
        opened, self._opened = self._opened, None
        if opened is not None:
            opened.close()

    def load(self, source_config: SourceConfig) -> pyarrow.Table:
        """One Iceberg table, under the projection, filter and limit it names.

        Memory-sized by construction: DuckDB takes a table, so a source that
        does not fit is narrowed by `columns`, `row_filter` and `limit` rather
        than read whole.
        """
        table = str(source_config.get("table", TABLE)).format(**source_config.as_dict())
        # An unset projection or filter is spelled as nothing, so a profile may
        # leave either as the empty value an environment override renders to.
        columns = source_config.get("columns") or None
        dataset = self.catalog().dataset(table)
        try:
            reader = dataset.read_arrow_reader(
                dataset.table_field if columns else None,
                row_filter=source_config.get("row_filter") or None,
                columns=columns,
                snapshot_id=source_config.get("snapshot_id"),
                limit=source_config.get("limit"),
                branch=source_config.get("branch"),
            )
            try:
                loaded = reader.read_all()
            finally:
                reader.close()
        finally:
            dataset.close()
        LOGGER.debug("%s read %d rows for %s", table, loaded.num_rows, source_config.name)
        return loaded

    def store(self, target_config: TargetConfig) -> None:
        """One staged model, committed as the table its configuration names."""
        config = target_config.config
        relation = target_config.relation
        table = config.get("table") or TABLE.format(
            schema=relation.schema, identifier=relation.identifier
        )
        location = target_config.location
        if location is None or location.format != "parquet":
            raise ValueError(
                f"{table} is committed from a staged Parquet file; "
                f"dbt offered {location.format if location else 'nothing'}"
            )
        mode = str(config.get("mode", "append")).casefold()
        if mode not in MODES:
            raise ValueError(f"{table} declares mode={mode!r}; it is one of {', '.join(MODES)}")
        keys = list(config.get("primary_key") or ())
        with ExitStack() as opened:
            # The staged file outlives the reader over it: a batch is read out
            # of it for every commit the write makes, not once up front.
            staged = pyarrow.parquet.ParquetFile(location.path)
            opened.callback(staged.close)
            field = declared_field(
                staged.schema_arrow,
                table,
                arrow_types=config.get("arrow_types"),
                keys=keys,
                not_null=config.get("not_null") or (),
                partition_by=config.get("partition_by"),
                sort_by=config.get("sort_by"),
            )
            merge_by = config.get("merge_by", bool(keys))
            dataset = self.catalog().dataset(
                table,
                field=field,
                merge_schema=bool(config.get("merge_schema", True)),
                branch=config.get("branch"),
            )
            opened.callback(dataset.close)
            reader = pyarrow.RecordBatchReader.from_batches(
                staged.schema_arrow,
                staged.iter_batches(batch_size=int(config.get("batch_row_size", BATCH_ROW_SIZE))),
            )
            applied = field.apply_arrow_reader(reader, safe=False, nullability="strict")
            opened.callback(applied.close)
            if mode == "append":
                written = dataset.append_arrow_reader(applied, field)
            else:
                written = dataset.overwrite_arrow_reader(applied, field, merge_by=merge_by)
        COMMITTED[table] = COMMITTED.get(table, 0) + written
        LOGGER.info("%s %s %d rows into %s", relation.identifier, MODES[mode], written, table)


__all__ = [
    "BATCH_ROW_SIZE",
    "CATALOG",
    "COMMITTED",
    "MODES",
    "OPENED",
    "TABLE",
    "Plugin",
    "catalog_settings",
    "committed",
    "declared_field",
    "released",
]
