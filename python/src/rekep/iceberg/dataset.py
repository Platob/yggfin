"""One Iceberg table as a dataset."""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator, Sequence
from functools import cached_property
from typing import Any

import pyarrow

from rekep.dataset import Dataset, arrow_chunks
from rekep.fields import StructField
from rekep.require import require

#: The branch a write lands on when the dataset names none -- pyiceberg's own
#: default, spelled out here so reads and writes cannot disagree about it.
MAIN = "main"


@dataclasses.dataclass(eq=False)
class IcebergDataset(Dataset):
    """An Iceberg table, read and written as Arrow through pyiceberg.

    Nothing about Iceberg is reimplemented here: pyiceberg plans the scan,
    writes the files and commits the snapshots. What this adds is the two ends
    -- the shape (`StructField`) the data is cast onto, and the streaming that
    keeps a commit from happening once per batch::

        logs = IcebergDataset(
            name="trading.logs",
            catalog="local",
            properties={"uri": "sqlite:///catalog.db", "warehouse": "file:///tmp/wh"},
            struct=Log.FIELD,
        )
        logs.write_arrow_reader(log.into_arrow_reader(), merge_by=True)
        logs.read_arrow_table(row_filter="date = '2026-08-14'")

    `struct` is optional: with one, the table is created from it (schema,
    documentation, identifier fields and partition spec included) the first
    time it is written; without one, the table's own schema is the shape.

    Reads push what they can down to the scan planner -- the row filter, the
    columns, the snapshot -- so a filtered read never materialises the files it
    can skip, and the cast onto a target shape happens batch by batch on the
    way out.
    """

    #: Table identifier, `namespace.name`.
    name: str

    #: Catalog name pyiceberg loads, with `properties` as its configuration.
    catalog: str = "default"
    properties: dict[str, str] = dataclasses.field(default_factory=dict)

    #: The declared shape. None means "whatever the table says".
    struct: StructField | None = None

    #: Branch reads and writes use; None is Iceberg's `main`.
    branch: str | None = None

    #: Only used when the table is created: where it lives and what it carries.
    location: str | None = None
    table_properties: dict[str, str] = dataclasses.field(default_factory=dict)

    # -- the table ----------------------------------------------------------

    @cached_property
    def iceberg_catalog(self) -> Any:
        """The pyiceberg catalog, built once: loading one reads config and may
        open a connection, and every table here lives in the same one."""
        require("pyiceberg", "iceberg")
        from pyiceberg.catalog import load_catalog

        return load_catalog(self.catalog, **self.properties)

    @cached_property
    def iceberg_table(self) -> Any:
        """The pyiceberg table this dataset is."""
        return self.iceberg_catalog.load_table(self.name)

    @property
    def exists(self) -> bool:
        """Whether the table is there yet."""
        return bool(self.iceberg_catalog.table_exists(self.name))

    def get_or_create_table(self) -> Any:
        """The table, created from the declared shape when it is not there yet.

        Idempotent, and the only place a table is created: a write that lands
        on a fresh catalog builds the namespace, the schema, the identifier
        fields and the partition spec from what was declared, and a second one
        finds them.
        """
        if self.exists:
            return self.iceberg_table
        if self.struct is None:
            raise ValueError(
                f"{self.name!r} does not exist and this dataset declares no shape; "
                "give it `struct=` or create the table first"
            )
        namespace = self.name.rpartition(".")[0]
        if namespace:
            self.iceberg_catalog.create_namespace_if_not_exists(namespace)
        schema = self.struct.into_iceberg_schema()
        table = self.iceberg_catalog.create_table(
            self.name,
            schema=schema,
            location=self.location,
            partition_spec=self.struct.into_iceberg_partition_spec(schema),
            properties=self.table_properties,
        )
        self.__dict__["iceberg_table"] = table
        return table

    def refresh(self) -> None:
        """Drop the loaded table, so the next call sees other writers' commits."""
        self.__dict__.pop("iceberg_table", None)
        self.__dict__.pop("table_field", None)

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

    # -- reading ------------------------------------------------------------

    def read_arrow_reader(
        self,
        schema: pyarrow.Schema | StructField | None = None,
        *,
        row_filter: Any = None,
        columns: Sequence[str] | None = None,
        snapshot_id: int | None = None,
        limit: int | None = None,
    ) -> pyarrow.RecordBatchReader:
        """Stream the table, pushing what it can down to the scan planner.

        `row_filter` (a pyiceberg expression or its string form), `columns` and
        `limit` are the planner's business, not ours: handing them over is what
        lets Iceberg skip whole files on partition and column statistics rather
        than reading them to throw the rows away.

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
        if self.branch and snapshot_id is None:
            scan = scan.use_ref(self.branch)
        reader = scan.to_arrow_batch_reader()
        if schema is None:
            return reader
        return self.target_field(schema).cast_arrow_reader(reader)

    # -- writing ------------------------------------------------------------

    def write_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: pyarrow.Schema | StructField | None = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
    ) -> None:
        """Write a stream into the table, one commit per chunk.

        The stream is cast onto `schema` -- this dataset's shape by default --
        so a nearly-right batch lands instead of failing pyiceberg's own schema
        check, and a column the source never produced is filled when it may be.

        `merge_by=True` merges on the primary key the shape declares, a list of
        names merges on those, and falsy appends. A merge is pyiceberg's own
        upsert: it plans the matching rows itself, which is a job for the engine
        that holds the statistics, not for this code.
        """
        table = self.get_or_create_table()
        reader = self.target_field(schema).cast_arrow_reader(source)
        join = self.merge_columns(merge_by)
        branch = self.branch or MAIN
        for chunk in arrow_chunks(reader, commit_row_size):
            if join:
                table.upsert(chunk, join_cols=join, branch=branch)
            else:
                table.append(chunk, branch=branch)
