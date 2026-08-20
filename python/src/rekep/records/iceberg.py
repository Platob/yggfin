"""Projecting a record onto Iceberg."""

from __future__ import annotations

import dataclasses
import os
import pathlib
from typing import TYPE_CHECKING, Any, ClassVar

import pyarrow

from rekep.imports import locate
from rekep.records import registry
from rekep.records.annotations import docstring_summary
from rekep.records.arrow import ArrowFieldBuilder, partition_keys, primary_keys
from rekep.records.record import Record, record

if TYPE_CHECKING:  # pragma: no cover - pyiceberg is imported at the point of use
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import IcebergType, NestedField

#: Arrow metadata key the Arrow projection writes descriptions under.
DESCRIPTION = b"description"


class IcebergFieldBuilder:
    """Projects a record onto Iceberg, by way of the Arrow schema it already has.

    pyiceberg ships the Arrow-to-Iceberg conversion, so a record is projected
    once onto Arrow and handed over, rather than walked a second time against a
    parallel table of types. Two things the conversion does not carry are added
    back afterwards:

    - **Descriptions.** Arrow keeps them in field metadata, Iceberg keeps them
      in `NestedField.doc`, and nothing maps one to the other -- so they are
      copied across, recursing into nested structs.
    - **Field ids.** The no-ids converter leaves every id at -1, which is not a
      valid schema; `assign_fresh_schema_ids` numbers them in one pass, which is
      also the only way to keep ids unique across nesting.

    Nullability needs no translation: an Arrow `not null` field is an Iceberg
    `required` one, so the record's declarations carry straight through.

    Subclass and point `Record.ICEBERG_BUILDER` at it to change any of this;
    `ARROW_BUILDER` selects the Arrow projection it starts from.
    """

    ARROW_BUILDER: ClassVar[type[ArrowFieldBuilder]] = ArrowFieldBuilder

    # -- entry points -------------------------------------------------------

    def schema(self, cls: type) -> IcebergSchema:
        """Iceberg schema for `cls`, ids and docs carried from the Arrow one.

        The Arrow projection stamps Iceberg-style field ids into
        `PARQUET:field_id` metadata, so the *public* pyiceberg conversion reads
        them straight off -- Arrow stays the single authority on ids. A
        builder that switched stamping off falls back to fresh assignment.
        """
        from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids, pyarrow_to_schema
        from pyiceberg.schema import assign_fresh_schema_ids

        arrow = self.arrow_schema(cls)
        try:
            converted = pyarrow_to_schema(arrow)
        except ValueError:  # no ids in the metadata: number afresh
            converted = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(arrow))
        documented = [self._document(field, arrow.field(field.name)) for field in converted.fields]
        declared = set(primary_keys(arrow))
        keys = [field.field_id for field in converted.fields if field.name in declared]
        return converted.__class__(*documented, identifier_field_ids=keys)

    def struct(self, cls: type) -> IcebergType:
        """`cls` as an Iceberg struct, ids numbered from one."""
        return self.schema(cls).as_struct()

    def field(
        self, cls: type, name: str | None = None, *, field_id: int = 1, required: bool = True
    ) -> NestedField:
        """`cls` as one Iceberg field, required unless asked otherwise.

        The struct inside is numbered from one, so a caller embedding this in a
        larger schema should renumber the result -- ids have to be unique across
        a whole schema, and only the schema knows what is already taken.
        """
        from pyiceberg.types import NestedField

        return NestedField(
            field_id=field_id,
            name=name or cls.__name__,
            field_type=self.struct(cls),
            required=required,
            doc=docstring_summary(cls) or None,
        )

    def partition_spec(self, cls: type) -> PartitionSpec:
        """The partition spec the record's `partition` declarations imply.

        Partition field ids start at 1000, per the Iceberg spec; sources are
        matched by field id, so this must be built against the same schema
        `schema()` returns.

        A partition field is named after its source column, except when the
        transform *computes* a different value from it (`day`, `bucket[16]`):
        Iceberg refuses a partition field that shadows a schema column with a
        value that is not that column's, so those get Iceberg's own
        `<column>_<transform>` spelling -- `at_day`, `account_bucket_16`.
        """
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import parse_transform

        schema = self.schema(cls)
        arrow = self.arrow_schema(cls)
        declared = partition_keys(arrow)
        fields = []
        for field in schema.fields:
            transform = declared.get(field.name)
            if not transform:
                continue
            fields.append(
                PartitionField(
                    source_id=field.field_id,
                    field_id=1000 + len(fields),
                    transform=parse_transform(transform),
                    name=_partition_field_name(field.name, transform),
                )
            )
        return PartitionSpec(*fields)

    def arrow_schema(self, cls: type) -> pyarrow.Schema:
        """The Arrow schema this projection starts from."""
        return self.ARROW_BUILDER().schema(cls)

    # -- documenting --------------------------------------------------------

    def _document(self, field: NestedField, arrow_field: pyarrow.Field) -> NestedField:
        """Copy `arrow_field`'s description onto `field`, and into its struct."""
        updates: dict[str, Any] = {}
        description = (arrow_field.metadata or {}).get(DESCRIPTION)
        if description:
            updates["doc"] = description.decode()
        nested = self._document_type(field.field_type, arrow_field.type)
        if nested is not None:
            updates["field_type"] = nested
        return field.model_copy(update=updates) if updates else field

    def _document_type(self, field_type: Any, arrow_type: pyarrow.DataType) -> Any | None:
        """A documented copy of `field_type`, or None when there is nothing to do.

        Descriptions live on struct members, but the struct itself may sit
        inside a list or a map, so those are stepped through to reach it. The
        `item`/`value` wrapper fields themselves never carry a description --
        the Arrow projection names them, not the author.
        """
        from pyiceberg.types import ListType, MapType, StructType

        if isinstance(field_type, StructType) and pyarrow.types.is_struct(arrow_type):
            children = {
                arrow_type.field(i).name: arrow_type.field(i) for i in range(arrow_type.num_fields)
            }
            return StructType(
                *[
                    self._document(field, children[field.name]) if field.name in children else field
                    for field in field_type.fields
                ]
            )
        if isinstance(field_type, ListType) and pyarrow.types.is_list(arrow_type):
            element = self._document_type(field_type.element_type, arrow_type.value_type)
            if element is not None:
                return field_type.model_copy(update={"element_type": element})
        if isinstance(field_type, MapType) and pyarrow.types.is_map(arrow_type):
            value = self._document_type(field_type.value_type, arrow_type.item_type)
            if value is not None:
                return field_type.model_copy(update={"value_type": value})
        return None


# -- deployment -------------------------------------------------------------

ICEBERG_ROOT = pathlib.Path(os.environ.get("REKEP_ICEBERG_ROOT", "stacks/iceberg"))

#: The registry folders: one entry per file, the file stem defaulting `name`.
#: Tables are deliberately not one of these -- `rekep.dataset.Dataset` deploys
#: autonomously against these two, no `tables/` side file needed.
CATALOGS_DIR = "catalogs"
NAMESPACES_DIR = "namespaces"


@record
class IcebergCatalog(Record):
    """One Iceberg catalog; the defaults are a fully local development setup."""

    name: str = "iceberg"
    """Catalog name, as engines address it."""

    type: str = "sql"
    """pyiceberg catalog implementation: sql, rest, hive or glue."""

    uri: str = "sqlite:///stacks/iceberg/catalog.db"
    """Catalog endpoint; a local SQLite file by default."""

    warehouse: str = "file://stacks/iceberg/warehouse"
    """Where table data lands; a local folder by default."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Extra catalog properties (credentials, endpoints, cache settings)."""

    def pyiceberg_properties(self) -> dict[str, str]:
        """This catalog as `pyiceberg.catalog.load_catalog` wants it."""
        return {"type": self.type, "uri": self.uri, "warehouse": self.warehouse, **self.properties}


@record
class IcebergNamespace(Record):
    """One Iceberg namespace inside a catalog."""

    name: str = "default"
    """Namespace name."""

    catalog: str = "iceberg"
    """Name of the catalog this namespace belongs to."""

    location: str | None = None
    """Base location for tables; defaults under the catalog warehouse."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Table properties every table in this namespace defaults to."""


@record
class IcebergTable(Record):
    """One table: a record bound to a namespace, one side file each.

    `fields` and `partition` are the record's schema *in Iceberg terms* --
    Iceberg type spellings, field ids, docs, identifier flags -- written by
    `materialized` so the side file is reviewable without opening Python.
    They are strictly derived: `verify` refuses a file that drifted from its
    record, so the protocol view can never quietly disagree with the code.
    """

    record: str
    """Dotted path of the Record class this table stores."""

    name: str | None = None
    """Table name; defaults to the record's snake_case name."""

    namespace: str = "default"
    """Name of the namespace the table is created in."""

    location: str | None = None
    """Explicit table location, overriding the derived one."""

    partitioned_by: list[str] = dataclasses.field(default_factory=list)
    """Partition clauses, overriding the record's field metadata."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Table properties on top of the namespace and catalog defaults."""

    fields: list[dict] = dataclasses.field(default_factory=list)
    """The record's fields in Iceberg terms; generated, verified on use."""

    partition: list[dict] = dataclasses.field(default_factory=list)
    """The partition spec in Iceberg terms; generated, verified on use."""

    def record_class(self) -> type[Record]:
        cls = locate(self.record)
        if not (isinstance(cls, type) and issubclass(cls, Record)):
            raise TypeError(f"{self.record} is not a Record class")
        return cls

    def materialized(self) -> IcebergTable:
        """This entry with `fields` and `partition` freshly derived."""
        cls = self.record_class()
        schema = cls.into_iceberg_schema()
        identifiers = schema.identifier_field_names()
        fields = []
        for field in schema.fields:
            entry: dict[str, Any] = {
                "name": field.name,
                "type": str(field.field_type),
                "field_id": field.field_id,
            }
            if field.required:
                entry["required"] = True
            if field.name in identifiers:
                entry["primary_key"] = True
            if field.doc:
                entry["doc"] = field.doc
            fields.append(entry)
        partition = [
            {
                "name": field.name,
                "transform": str(field.transform),
                "source_id": field.source_id,
                "field_id": field.field_id,
            }
            for field in cls.into_iceberg_partition_spec().fields
        ]
        return dataclasses.replace(self, fields=fields, partition=partition)

    def verify(self) -> IcebergTable:
        """Refuse a side file whose protocol view drifted from its record."""
        fresh = self.materialized()
        if self.fields and self.fields != fresh.fields:
            raise ValueError(
                f"table {self.name or self.record}: `fields` drifted from the record; "
                "regenerate with: rekep service iceberg tables sync"
            )
        if self.partition and self.partition != fresh.partition:
            raise ValueError(
                f"table {self.name or self.record}: `partition` drifted from the record; "
                "regenerate with: rekep service iceberg tables sync"
            )
        return fresh


@record
class IcebergDeployment(Record):
    """Everything one Iceberg deployment declares, loaded from `stacks/iceberg`.

    The same registry-of-folders shape as the Doris deployment: `catalogs/`,
    `namespaces/`, one entry per file, the stem defaulting `name`. No
    `tables/` folder: tables are never loaded from this deployment's own
    disk registry -- `rekep.dataset.Dataset` builds one autonomously from its
    own config (`into_iceberg_table()`) and converges it straight into the
    catalog and namespace this deployment declares. `tables` stays a plain
    field so a caller can still populate it directly in Python, e.g. for a
    one-off deploy that skips side files entirely.
    The default catalog is fully local -- SQLite catalog, file warehouse --
    so a laptop renders and runs without any service.
    """

    catalogs: list[IcebergCatalog] = dataclasses.field(default_factory=lambda: [IcebergCatalog()])
    namespaces: list[IcebergNamespace] = dataclasses.field(
        default_factory=lambda: [IcebergNamespace()]
    )
    tables: list[IcebergTable] = dataclasses.field(default_factory=list)

    @classmethod
    def load(cls, root: str | os.PathLike[str] = ICEBERG_ROOT, **context: Any) -> IcebergDeployment:
        """The deployment the folder declares, or pure defaults without one."""
        directory = pathlib.Path(root)
        return cls(
            catalogs=registry.entries(directory / CATALOGS_DIR, IcebergCatalog, context)
            or [IcebergCatalog()],
            namespaces=registry.entries(directory / NAMESPACES_DIR, IcebergNamespace, context)
            or [IcebergNamespace()],
        )

    def catalog(self, name: str) -> IcebergCatalog:
        return registry.named(self.catalogs, name, "catalog")

    def namespace(self, name: str) -> IcebergNamespace:
        namespace = registry.named(self.namespaces, name, "namespace")
        self.catalog(namespace.catalog)  # a dangling catalog fails here, not in SQL
        return namespace

    def table(self, record_class: type[Record]) -> IcebergTable | None:
        """The table entry declaring `record_class`, matched by resolved class."""
        for table in self.tables:
            if table.record_class() is record_class:
                return table
        return None

    def ddl(self, table: IcebergTable, **kwargs: Any) -> str:
        """CREATE TABLE for one declared table, defaults resolved."""
        cls = table.record_class()
        space = self.namespace(kwargs.pop("namespace", None) or table.namespace)
        catalog = self.catalog(space.catalog)
        name = table.name or cls.doris_table_name()
        root = space.location or f"{catalog.warehouse.rstrip('/')}/{space.name}"
        return cls.into_iceberg_ddl(
            table_name=f"{catalog.name}.{space.name}.{name}",
            location=kwargs.pop("location", None) or table.location or f"{root}/{name}",
            partitioned_by=kwargs.pop("partitioned_by", None) or table.partitioned_by,
            properties={
                **catalog.properties,
                **space.properties,
                **table.properties,
                **(kwargs.pop("properties", None) or {}),
            }
            or None,
            **kwargs,
        )

    def ddl_for(self, record_class: type[Record], **kwargs: Any) -> str:
        """CREATE TABLE for a record: its table entry when declared, else defaults."""
        table = self.table(record_class) or IcebergTable(
            record=f"{record_class.__module__}.{record_class.__qualname__}",
            namespace=self.namespaces[0].name,
        )
        if kwargs.get("table_name"):
            table = dataclasses.replace(table, name=kwargs.pop("table_name"))
        else:
            kwargs.pop("table_name", None)
        return self.ddl(table, **kwargs)


def _partition_field_name(column: str, transform: str) -> str:
    """The partition field's name: the column's, unless the transform computes.

    Iceberg refuses a partition field that shadows a schema column while
    holding a *different* value, so only `identity` may keep the plain name.
    The rest take Iceberg's own convention -- source column, then transform,
    the width folded in with an underscore rather than brackets, which are
    not legal in a field name.
    """
    if transform in ("identity", "true", "1", "yes"):
        return column
    return f"{column}_{transform.replace('[', '_').replace(']', '').replace(',', '_')}"
