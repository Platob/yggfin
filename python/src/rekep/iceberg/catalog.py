"""Catalogs and namespaces: the CRUD around the tables."""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from functools import cached_property
from typing import Any

from rekep.convert import Convertible
from rekep.require import require

#: FileIO pyiceberg is pointed at unless the caller names another. Arrow is the
#: hub here, so the store's reads and writes go through the same filesystem
#: implementations everything else does -- one credential chain, one set of
#: URI rules, and `pyarrow.fs` handles for anything that has to be listed or
#: deleted during maintenance.
PYARROW_FILE_IO = "pyiceberg.io.pyarrow.PyArrowFileIO"


@dataclasses.dataclass(eq=False)
class IcebergCatalog(Convertible):
    """One pyiceberg catalog, with the verbs a stack needs.

    Configuration is data -- a name and the properties pyiceberg loads it with
    -- so a catalog is a document too::

        catalog = IcebergCatalog(
            name="local",
            properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///data"},
        )
        catalog.create_namespace("trading")
        catalog.dataset("trading.quotes", struct=Quote.FIELD).create_with()

    Every verb is idempotent in the direction that matters: creating what is
    there is not an error, dropping what is not there is not either, unless
    the caller asks for the opposite.
    """

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

        return load_catalog(self.name, **{"py-io-impl": PYARROW_FILE_IO, **self.properties})

    # -- namespaces ---------------------------------------------------------

    def namespaces(self, under: str | None = None) -> list[str]:
        """Namespace names, dotted, under `under` or at the top."""
        found = self.catalog.list_namespaces(*((under,) if under else ()))
        return [".".join(levels) for levels in found]

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
        """Table identifiers, dotted: one namespace's, or every namespace's."""
        spaces = [namespace] if namespace else self.namespaces()
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
        """A dataset on this catalog: the way to read and write a table here."""
        from rekep.iceberg.dataset import IcebergDataset

        return IcebergDataset(name=name, catalog=self.name, properties=self.properties, **kwargs)

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
