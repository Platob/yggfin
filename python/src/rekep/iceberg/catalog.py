"""Catalogs and namespaces: the CRUD around the tables."""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from functools import cached_property
from typing import Any

from rekep.convert import Convertible
from rekep.fields import Field, StructField
from rekep.require import require

#: FileIO pyiceberg is pointed at unless the caller names another. Arrow is the
#: hub here, so the store's reads and writes go through the same filesystem
#: implementations everything else does -- one credential chain, one set of
#: URI rules, and `pyarrow.fs` handles for anything that has to be listed or
#: deleted during maintenance. Ours rather than pyiceberg's own, for the one
#: parsing fix `rekep.arrow_file_io` explains: Windows drive letters.
PYARROW_FILE_IO = "rekep.arrow_file_io.ArrowFileIO"


@dataclasses.dataclass(eq=False)
class IcebergCatalog(Convertible):
    """One pyiceberg catalog, with the verbs a stack needs."""

    name: str = "default"
    properties: dict[str, str] = dataclasses.field(default_factory=dict)

    # -- the catalog --------------------------------------------------------

    @cached_property
    def catalog(self) -> Any:
        """The pyiceberg catalog, loaded once.

        Loading reads configuration and may open a connection, and every table
        here lives in the same one. `py-io-impl` defaults to Arrow's FileIO;
        naming another in `properties` wins.
        """
        require("pyiceberg", "iceberg")
        from pyiceberg.catalog import load_catalog

        from rekep.arrow_file_io import inferred_properties

        properties = inferred_properties(self.properties)
        return load_catalog(self.name, **{"py-io-impl": PYARROW_FILE_IO, **properties})

    def close(self) -> None:
        """Release a loaded catalog without opening an unused one."""
        catalog = self.__dict__.pop("catalog", None)
        if catalog is not None:
            catalog.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    # -- namespaces ---------------------------------------------------------

    def namespaces(self, under: str | None = None, *, recursive: bool = False) -> list[str]:
        """Namespace names, dotted, under `under` or at the top.

        `list_namespaces` is one level deep -- `trading` does not bring back
        `trading.eu` -- so `recursive=True` walks down. It costs one call per
        namespace found, which is free on SQLite and a round trip each on REST
        or Glue; the default stays the single call.
        """
        found = self.catalog.list_namespaces(*((under,) if under else ()))
        names = [".".join(levels) for levels in found]
        if not recursive:
            return names
        below = [name for parent in names for name in self.namespaces(parent, recursive=True)]
        return list(dict.fromkeys(names + below))

    def namespace(self, name: str) -> IcebergNamespace:
        """A handle on one namespace, whether or not it exists yet."""
        return IcebergNamespace(catalog=self, name=name)

    def create_namespace(
        self, name: str, properties: dict[str, str] | None = None, *, exists_ok: bool = True
    ) -> IcebergNamespace:
        """Create a namespace, or leave the one that is there alone."""
        if exists_ok:
            self.catalog.create_namespace_if_not_exists(name, properties or {})
        else:
            self.catalog.create_namespace(name, properties or {})
        return self.namespace(name)

    def drop_namespace(self, name: str, *, missing_ok: bool = True) -> None:
        """Drop a namespace; it has to be empty, as Iceberg requires."""
        if missing_ok and not self.namespace_exists(name):
            return
        self.catalog.drop_namespace(name)

    def namespace_exists(self, name: str) -> bool:
        return bool(self.catalog.namespace_exists(name))

    def namespace_properties(self, name: str) -> dict[str, str]:
        return dict(self.catalog.load_namespace_properties(name))

    def update_namespace_properties(
        self, name: str, updates: dict[str, str] | None = None, removals: set[str] | None = None
    ) -> Any:
        return self.catalog.update_namespace_properties(name, removals or set(), updates or {})

    # -- tables -------------------------------------------------------------

    def tables(self, namespace: str | None = None) -> list[str]:
        """Table identifiers, dotted: one namespace's, or every namespace's.

        *Every* namespace means nested ones too. A `list_namespaces` with no
        argument returns only the top level, so `trading.eu.paris` was silently
        missing -- and a sweep written as `for dataset in catalog.datasets()`
        never touched those tables, without reporting a skip.
        """
        spaces = [namespace] if namespace else self.namespaces(recursive=True)
        return [
            ".".join(identifier)
            for space in spaces
            for identifier in self.catalog.list_tables(space)
        ]

    def table_exists(self, name: str) -> bool:
        return bool(self.catalog.table_exists(name))

    def load_table(self, name: str) -> Any:
        """The pyiceberg table, for what this package does not wrap."""
        return self.catalog.load_table(name)

    def drop_table(self, name: str, *, purge: bool = False, missing_ok: bool = True) -> None:
        """Drop a table, optionally deleting its files with it."""
        if missing_ok and not self.table_exists(name):
            return
        if purge:
            self.catalog.purge_table(name)
        else:
            self.catalog.drop_table(name)

    def rename_table(self, name: str, to: str) -> Any:
        return self.catalog.rename_table(name, to)

    def dataset(self, name: str, **kwargs: Any) -> Any:
        """A dataset on this catalog: the way to read and write a table here.

        `name` becomes the supplied field's outer name. Without a field, the
        existing table is loaded once and its declaration is read back.

        Handed *this* catalog rather than left to build its own, the way
        `create_with_field` hands over the table it just made: loading a
        pyiceberg catalog builds a SQLAlchemy engine or asks a REST server for
        its config, and `datasets()` was paying that per table.
        """
        from rekep.iceberg.dataset import IcebergDataset

        field = kwargs.pop("field", None)
        table = None
        if field is None:
            table = self.load_table(name)
            field = StructField.from_iceberg_schema(table.schema(), name, spec=table.spec())
        else:
            field = Field.from_(field).with_name(name)
        built = IcebergDataset(field=field, catalog=self.name, properties=self.properties, **kwargs)
        built.__dict__["store"] = self
        built.__dict__["_owns_store"] = False
        if table is not None:
            built.__dict__["iceberg_table"] = table
        return built

    def datasets(self, namespace: str | None = None) -> Iterator[Any]:
        """One dataset per table, for a sweep over a whole namespace."""
        for identifier in self.tables(namespace):
            yield self.dataset(identifier)


@dataclasses.dataclass(eq=False)
class IcebergNamespace(Convertible):
    """One namespace in a catalog: its properties, its tables, its datasets."""

    catalog: IcebergCatalog
    name: str

    @property
    def exists(self) -> bool:
        return self.catalog.namespace_exists(self.name)

    def create(self, properties: dict[str, str] | None = None) -> IcebergNamespace:
        """Create it if it is not there; hand it back either way."""
        self.catalog.create_namespace(self.name, properties)
        return self

    def drop(self, *, missing_ok: bool = True) -> None:
        self.catalog.drop_namespace(self.name, missing_ok=missing_ok)

    @property
    def properties(self) -> dict[str, str]:
        return self.catalog.namespace_properties(self.name)

    def update_properties(
        self, updates: dict[str, str] | None = None, removals: set[str] | None = None
    ) -> Any:
        return self.catalog.update_namespace_properties(self.name, updates, removals)

    def tables(self) -> list[str]:
        return self.catalog.tables(self.name)

    def dataset(self, name: str, **kwargs: Any) -> Any:
        """A dataset for a table in this namespace, named without the prefix."""
        return self.catalog.dataset(f"{self.name}.{name}", **kwargs)
