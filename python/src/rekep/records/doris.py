"""Projecting a record onto Apache Doris DDL."""

from __future__ import annotations

import dataclasses
import os
import pathlib
from collections.abc import Mapping
from typing import Any, ClassVar

import pyarrow

from rekep.records import registry
from rekep.records.arrow import ArrowFieldBuilder, partition_keys, primary_keys
from rekep.records.record import Record, record

#: Where the Doris deployment lives, relative to the deployment root.
DORIS_ROOT = pathlib.Path(os.environ.get("REKEP_DORIS_ROOT", "stacks/doris"))

#: The registry folders: one entry per file, the file stem defaulting `name`.
#: Tables are deliberately not one of these -- `rekep.dataset.Dataset` deploys
#: autonomously against these two, no `tables/` side file needed.
CATALOGS_DIR = "catalogs"
NAMESPACES_DIR = "namespaces"

#: Metadata written by the Arrow projection that the DDL reads back.
DESCRIPTION = b"description"

#: Doris `date_trunc` granularities for the Iceberg transforms that have one.
TRUNC_SQL = {"identity": "day", "true": "day", "day": "day", "month": "month", "hour": "hour"}

#: Best-practice defaults for a Doris catalog over Iceberg, from
#: https://doris.apache.org/docs/4.x/lakehouse/best-practices/doris-iceberg
#: -- a REST catalog; deployment properties override any of them.
DORIS_ICEBERG_DEFAULTS = {
    "type": "iceberg",
    "iceberg.catalog.type": "rest",
}


@record
class DorisCatalog(Record):
    """One Doris catalog, with the defaulting keys tables in it inherit."""

    name: str = "iceberg"
    """Catalog name; the default is the Iceberg lakehouse catalog."""

    type: str = "iceberg"
    """iceberg for a lakehouse catalog, internal for Doris's own OLAP storage."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """CREATE CATALOG properties for external types, table defaults otherwise."""


@record
class DorisNamespace(Record):
    """One namespace -- a Doris database -- inside a catalog."""

    name: str = "default"
    """Database name."""

    catalog: str = "iceberg"
    """Name of the catalog this namespace belongs to."""

    replication_num: int = 1
    """Replicas per tablet; production typically wants 3."""

    buckets: str = "AUTO"
    """Bucket count for HASH distribution, or AUTO to let Doris size it."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Table PROPERTIES every table in this namespace defaults to."""


@record
class DorisTable(Record):
    """One table: a record bound to a namespace, one side file each."""

    record: str
    """The record this table stores: `rekep:///records/<name>`."""

    name: str | None = None
    """Table name; defaults to the record's snake_case name."""

    namespace: str = "default"
    """Name of the namespace the table is created in."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Table PROPERTIES on top of the namespace and catalog defaults."""

    fields: list[dict] = dataclasses.field(default_factory=list)
    """The record's fields in Doris terms; generated, verified on use."""

    def record_class(self) -> type[Record]:
        return Record.locate(self.record)

    def materialized(self) -> DorisTable:
        """This entry with `fields` freshly derived, key columns leading."""
        cls = self.record_class()
        builder = DorisDdlBuilder()
        schema = builder.ARROW_BUILDER().schema(cls)
        keys = primary_keys(schema)
        fields = []
        for field in builder.ordered_fields(schema, keys):
            entry: dict[str, Any] = {"name": field.name, "type": builder.sql_type(field.type)}
            if not field.nullable:
                entry["not_null"] = True
            if field.name in keys:
                entry["key"] = True
            description = (field.metadata or {}).get(DESCRIPTION)
            if description:
                entry["comment"] = description.decode()
            fields.append(entry)
        return dataclasses.replace(self, fields=fields)

    def verify(self) -> DorisTable:
        """Refuse a side file whose protocol view drifted from its record."""
        fresh = self.materialized()
        if self.fields and self.fields != fresh.fields:
            raise ValueError(
                f"table {self.name or self.record}: `fields` drifted from the record; "
                "regenerate with: rekep doris sync"
            )
        return fresh


@record
class DorisDeployment(Record):
    """Everything one Doris deployment declares, loaded from `stacks/doris`.

    The folder is a registry of folders, one entry per file:
    `catalogs/internal.yaml` (one catalog each), `namespaces/yggfin.yaml`
    (one database each, bound to a catalog). The file stem defaults the
    entry's `name`, so most files only say what differs. No `tables/`
    folder: `rekep.dataset.Dataset` builds a table autonomously from its own
    config (`into_doris_table()`) and converges it straight into the catalog
    and namespace this deployment declares; `tables` stays a plain field so
    a caller can still populate it directly in Python. Each level defaults
    entirely, so an empty folder still renders working DDL; Jinja is
    rendered before parsing throughout, so replication and names can come
    from the environment.
    """

    catalogs: list[DorisCatalog] = dataclasses.field(default_factory=lambda: [DorisCatalog()])
    namespaces: list[DorisNamespace] = dataclasses.field(default_factory=lambda: [DorisNamespace()])
    tables: list[DorisTable] = dataclasses.field(default_factory=list)

    # -- loading ------------------------------------------------------------

    @classmethod
    def load(cls, root: str | os.PathLike[str] = DORIS_ROOT, **context: Any) -> DorisDeployment:
        """The deployment the folder declares, or pure defaults without one."""
        directory = pathlib.Path(root)
        return cls(
            catalogs=registry.entries(directory / CATALOGS_DIR, DorisCatalog, context)
            or [DorisCatalog()],
            namespaces=registry.entries(directory / NAMESPACES_DIR, DorisNamespace, context)
            or [DorisNamespace()],
        )

    # -- lookups ------------------------------------------------------------

    def catalog(self, name: str) -> DorisCatalog:
        return registry.named(self.catalogs, name, "catalog")

    def namespace(self, name: str) -> DorisNamespace:
        namespace = registry.named(self.namespaces, name, "namespace")
        self.catalog(namespace.catalog)  # a dangling catalog fails here, not in SQL
        return namespace

    def table(self, record_class: type[Record]) -> DorisTable | None:
        """The table entry declaring `record_class`, when one exists.

        Matched by resolving each entry's path, not by comparing strings: the
        side file may name the class through a re-export (`rekep.models.X`)
        while the class's own module is the deeper one.
        """
        for table in self.tables:
            if table.record_class() is record_class:
                return table
        return None

    # -- ddl ----------------------------------------------------------------

    def catalog_ddl(self) -> list[str]:
        """CREATE CATALOG statements for every external catalog declared."""
        builder = DorisDdlBuilder()
        return [
            statement
            for catalog in self.catalogs
            if (statement := builder.create_catalog(catalog)) is not None
        ]

    def ddl(self, table: DorisTable, **kwargs: Any) -> str:
        """CREATE TABLE for one declared table, defaults resolved.

        A `namespace=` in the call overrides the table's own -- the call is
        the most specific level of the defaulting chain.
        """
        cls = table.record_class()
        return cls.into_doris_ddl(
            table.name or cls.record_name(),
            deployment=self,
            namespace=kwargs.pop("namespace", None) or table.namespace,
            properties={**table.properties, **(kwargs.pop("properties", None) or {})},
            **kwargs,
        )

    def ddl_for(self, record_class: type[Record], **kwargs: Any) -> str:
        """CREATE TABLE for a record: its table entry when declared, else defaults."""
        table = self.table(record_class) or DorisTable(
            record=str(record_class.record_uri()),
            namespace=self.namespaces[0].name,
        )
        if kwargs.get("table_name"):
            table = dataclasses.replace(table, name=kwargs.pop("table_name"))
        else:
            kwargs.pop("table_name", None)
        return self.ddl(table, **kwargs)


class DorisDdlBuilder:
    """Renders a record's Arrow fields as a Doris `CREATE TABLE`.

    The same declarations that drive the Iceberg DDL drive this one, bent to
    Doris's model:

    - **Key columns lead.** Doris requires `UNIQUE KEY` columns to be the
      leading columns, so fields marked `Arrow(key=True)` are moved to the
      front, declaration order preserved. No keys means no key clause -- Doris
      falls back to its duplicate model.
    - **Partitions become `AUTO PARTITION BY RANGE(date_trunc(...))`** for
      date-shaped transforms; a `bucket[N]` partition becomes the HASH
      distribution instead, which is what bucketing is in Doris.
    - **`time` has no Doris type** and is stored as STRING; override
      `sql_type` to choose differently.

    Defaults flow catalog -> namespace -> table -> call, later wins.
    """

    ARROW_BUILDER: ClassVar[type[ArrowFieldBuilder]] = ArrowFieldBuilder

    # -- entry points -------------------------------------------------------

    def create_table(
        self,
        cls: type,
        table_name: str,
        *,
        deployment: DorisDeployment | None = None,
        namespace: str | None = None,
        if_not_exists: bool = True,
        properties: Mapping[str, str] | None = None,
    ) -> str:
        """The CREATE TABLE statement for `cls` on one Doris deployment."""
        deployment = deployment or DorisDeployment()
        space = deployment.namespace(namespace or deployment.namespaces[0].name)
        catalog = deployment.catalog(space.catalog)
        schema = self.ARROW_BUILDER().schema(cls)
        keys = primary_keys(schema)
        ordered = self.ordered_fields(schema, keys)

        columns = ",\n".join(f"    {self.column(field)}" for field in ordered)
        exists = "IF NOT EXISTS " if if_not_exists else ""
        qualified = f"{catalog.name}.{space.name}.{table_name}"
        lines = [f"CREATE TABLE {exists}{qualified} (", columns, ")", "ENGINE=OLAP"]
        if keys:
            lines.append(f"UNIQUE KEY({', '.join(f'`{name}`' for name in keys)})")
        comment = (schema.metadata or {}).get(DESCRIPTION)
        if comment:
            lines.append(f'COMMENT "{self.quote(comment.decode())}"')
        partition = self.partition_sql(schema)
        if partition:
            lines.append(partition)
        lines.append(self.distribution_sql(schema, keys, space))
        lines.append(self.properties_sql(catalog, space, properties))
        return "\n".join(lines) + ";\n"

    def create_catalog(self, catalog: DorisCatalog) -> str | None:
        """CREATE CATALOG for an external catalog; None for internal ones.

        For `type: iceberg` the properties start from the documented REST
        best practices (`DORIS_ICEBERG_DEFAULTS`) and the deployment's
        properties override them -- a minimal file gets a working lakehouse
        wiring, a full one keeps every word it wrote.
        """
        if catalog.type == "internal":
            return None
        defaults = DORIS_ICEBERG_DEFAULTS if catalog.type == "iceberg" else {"type": catalog.type}
        merged = {**defaults, **catalog.properties}
        rendered = ",\n".join(
            f'    "{self.quote(str(key))}" = "{self.quote(str(value))}"'
            for key, value in merged.items()
        )
        return f"CREATE CATALOG IF NOT EXISTS `{catalog.name}` PROPERTIES (\n{rendered}\n);\n"

    # -- pieces -------------------------------------------------------------

    def ordered_fields(self, schema: pyarrow.Schema, keys: list[str]) -> list[pyarrow.Field]:
        """Key columns first -- Doris requires it -- then the rest, in order."""
        named = {field.name: field for field in schema}
        rest = [field for field in schema if field.name not in keys]
        return [named[name] for name in keys] + rest

    def column(self, field: pyarrow.Field) -> str:
        parts = [f"`{field.name}`", self.sql_type(field.type)]
        if not field.nullable:
            parts.append("NOT NULL")
        description = (field.metadata or {}).get(DESCRIPTION)
        if description:
            parts.append(f'COMMENT "{self.quote(description.decode())}"')
        return " ".join(parts)

    def partition_sql(self, schema: pyarrow.Schema) -> str | None:
        """AUTO RANGE partition on the first date-shaped partition field."""
        for name, transform in partition_keys(schema).items():
            granularity = TRUNC_SQL.get(transform)
            if granularity:
                return f"AUTO PARTITION BY RANGE (date_trunc(`{name}`, '{granularity}')) ()"
        return None

    def distribution_sql(
        self, schema: pyarrow.Schema, keys: list[str], namespace: DorisNamespace
    ) -> str:
        """HASH distribution: a bucket[] partition wins, then keys, then first."""
        buckets = namespace.buckets
        columns = keys
        for name, transform in partition_keys(schema).items():
            if transform.startswith("bucket["):
                columns = [name]
                buckets = transform.removeprefix("bucket[").rstrip("]")
                break
        if not columns:
            columns = [schema.field(0).name]
        named = ", ".join(f"`{name}`" for name in columns)
        return f"DISTRIBUTED BY HASH({named}) BUCKETS {buckets}"

    def properties_sql(
        self,
        catalog: DorisCatalog,
        namespace: DorisNamespace,
        extra: Mapping[str, str] | None,
    ) -> str:
        merged = {
            "replication_num": str(namespace.replication_num),
            **catalog.properties,
            **namespace.properties,
            **(extra or {}),
        }
        rendered = ",\n".join(
            f'    "{self.quote(str(key))}" = "{self.quote(str(value))}"'
            for key, value in merged.items()
        )
        return f"PROPERTIES (\n{rendered}\n)"

    def sql_type(self, data_type: pyarrow.DataType) -> str:
        """Doris SQL spelling of one Arrow type, recursing into containers."""
        types = pyarrow.types
        if types.is_boolean(data_type):
            return "BOOLEAN"
        if types.is_integer(data_type):
            return "BIGINT" if data_type.bit_width > 32 else "INT"
        if types.is_float32(data_type):
            return "FLOAT"
        if types.is_float64(data_type):
            return "DOUBLE"
        if types.is_decimal(data_type):
            return f"DECIMAL({data_type.precision}, {data_type.scale})"
        if types.is_string(data_type) or types.is_large_string(data_type):
            return "STRING"
        if types.is_binary(data_type) or types.is_large_binary(data_type):
            return "STRING"  # Doris has no BINARY column type
        if types.is_date(data_type):
            return "DATE"
        if types.is_timestamp(data_type):
            return "DATETIME(6)"
        if types.is_time(data_type):
            return "STRING"  # Doris has no TIME column type
        if types.is_list(data_type) or types.is_large_list(data_type):
            return f"ARRAY<{self.sql_type(data_type.value_type)}>"
        if types.is_map(data_type):
            return f"MAP<{self.sql_type(data_type.key_type)}, {self.sql_type(data_type.item_type)}>"
        if types.is_struct(data_type):
            fields = ", ".join(
                f"{data_type.field(i).name}: {self.sql_type(data_type.field(i).type)}"
                for i in range(data_type.num_fields)
            )
            return f"STRUCT<{fields}>"
        raise TypeError(f"no Doris SQL type for Arrow {data_type}; override sql_type to map it")

    def quote(self, text: str) -> str:
        """Escape a value for a double-quoted Doris string."""
        return text.replace('"', '""')
