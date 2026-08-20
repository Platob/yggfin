"""Tables: the Iceberg table resource, CRUD over pyiceberg."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from rekep.iceberg.namespaces import Namespaces
from rekep.records.iceberg import IcebergTable

if TYPE_CHECKING:  # pragma: no cover - pyiceberg is imported at the point of use
    from pyiceberg.table import Table

logger = logging.getLogger("rekep.iceberg")


class Tables:
    """CRUD over the tables a deployment declares.

    `create_or_update` is the deploy verb: create when absent, otherwise
    union the record's schema in by name (new columns appear, existing ones
    are never broken) and settle the declared properties. Every entry is
    `verify`-ed first, so a side file that drifted from its record stops the
    deploy instead of shipping a stale schema.
    """

    def __init__(self, namespaces: Namespaces) -> None:
        self.namespaces = namespaces
        self.catalogs = namespaces.catalogs

    def list(self) -> list[IcebergTable]:
        return list(self.catalogs.deployment.tables)

    def identifier(self, table: IcebergTable) -> str:
        name = table.name or table.record_class().record_name()
        return f"{table.namespace}.{name}"

    def exists(self, table: IcebergTable) -> bool:
        catalog = self._catalog(table)
        return bool(catalog.table_exists(self.identifier(table)))

    def get(self, table: IcebergTable) -> Table:
        return self._catalog(table).load_table(self.identifier(table))

    def create(self, table: IcebergTable, dry_run: bool = False) -> Table | None:
        """Create the table; on a dry run, say what would be created instead."""
        table.verify()
        cls = table.record_class()
        catalog = self._catalog(table)
        if dry_run:
            schema = cls.into_iceberg_schema()
            logger.info(
                "table %s: would create (%d fields)",
                self.identifier(table),
                len(schema.fields),
            )
            return None
        schema = cls.into_iceberg_schema()
        spec = cls.into_iceberg_partition_spec()
        properties = self._properties(table)
        created = catalog.create_table(
            self.identifier(table), schema=schema, partition_spec=spec, properties=properties
        )
        logger.info("table %s: created", self.identifier(table))
        logger.debug(
            "table %s: fields=%s",
            self.identifier(table),
            [(f.field_id, f.name, str(f.field_type), f.required) for f in schema.fields],
        )
        logger.debug(
            "table %s: identifiers=%s partition=%s properties=%s",
            self.identifier(table),
            sorted(schema.identifier_field_names()),
            [(f.name, str(f.transform)) for f in spec.fields],
            properties,
        )
        return created

    def get_or_create(self, table: IcebergTable, dry_run: bool = False) -> Table | None:
        if self.exists(table):
            logger.info("table %s: exists, nothing to do", self.identifier(table))
            return self.get(table)
        return self.create(table, dry_run=dry_run)

    def create_or_update(self, table: IcebergTable, dry_run: bool = False) -> Table | None:
        """Create when absent; otherwise converge schema and properties.

        Updates go through pyiceberg's own evolution: `union_by_name` adds
        what the record gained and refuses incompatible changes, and property
        updates only touch keys whose value actually differs -- both no-ops
        when nothing changed.
        """
        if not self.exists(table):
            return self.create(table, dry_run=dry_run)
        table.verify()
        live = self.get(table)
        cls = table.record_class()

        changed = {
            key: value
            for key, value in self._properties(table).items()
            if live.properties.get(key) != value
        }
        if dry_run:
            existing = {field.name for field in live.schema().fields}
            additions = [
                field.name
                for field in cls.into_iceberg_schema().fields
                if field.name not in existing
            ]
            plans = [
                part
                for part in (
                    f"add columns [{', '.join(additions)}]" if additions else "",
                    f"set properties [{', '.join(sorted(changed))}]" if changed else "",
                )
                if part
            ]
            logger.info(
                "table %s: would %s", self.identifier(table), ", ".join(plans) or "change nothing"
            )
            return live

        with live.update_schema() as update:
            update.union_by_name(cls.into_iceberg_schema())
        logger.debug(
            "table %s: live fields=%s",
            self.identifier(table),
            [(f.field_id, f.name, str(f.field_type)) for f in self.get(table).schema().fields],
        )
        if changed:
            with live.transaction() as transaction:
                transaction.set_properties(**changed)
            logger.info(
                "table %s: updated (%s)", self.identifier(table), ", ".join(sorted(changed))
            )
        else:
            logger.info("table %s: schema converged, properties unchanged", self.identifier(table))
        return self.get(table)

    def delete(self, table: IcebergTable, dry_run: bool = False) -> None:
        if dry_run:
            logger.info("table %s: would drop", self.identifier(table))
            return
        self._catalog(table).drop_table(self.identifier(table))
        logger.info("table %s: dropped", self.identifier(table))

    def _catalog(self, table: IcebergTable) -> Any:
        namespace = self.namespaces.get(table.namespace)
        return self.catalogs.connect(namespace.catalog)

    def _properties(self, table: IcebergTable) -> dict[str, str]:
        namespace = self.namespaces.get(table.namespace)
        catalog = self.catalogs.get(namespace.catalog)
        return {**catalog.properties, **namespace.properties, **table.properties}
