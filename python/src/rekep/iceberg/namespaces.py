"""Namespaces: the Iceberg namespace resource, CRUD over pyiceberg."""

from __future__ import annotations

import logging

from rekep.iceberg.catalogs import Catalogs
from rekep.records.iceberg import IcebergNamespace

logger = logging.getLogger("rekep.iceberg")


class Namespaces:
    """CRUD over the namespaces a deployment declares.

    Every operation takes the *declared* entry, so properties and catalog
    binding travel with it; `get_or_create` is the deploy verb -- idempotent,
    logged either way.
    """

    def __init__(self, catalogs: Catalogs) -> None:
        self.catalogs = catalogs

    def list(self) -> list[IcebergNamespace]:
        return list(self.catalogs.deployment.namespaces)

    def get(self, name: str) -> IcebergNamespace:
        return self.catalogs.deployment.namespace(name)

    def exists(self, namespace: IcebergNamespace) -> bool:
        return bool(self.catalogs.connect(namespace.catalog).namespace_exists(namespace.name))

    def create(self, namespace: IcebergNamespace, dry_run: bool = False) -> IcebergNamespace:
        catalog = self.catalogs.connect(namespace.catalog)
        if dry_run:
            logger.info("namespace %s.%s: would create", namespace.catalog, namespace.name)
            return namespace
        catalog.create_namespace(namespace.name, properties=namespace.properties)
        logger.info("namespace %s.%s: created", namespace.catalog, namespace.name)
        logger.debug(
            "namespace %s.%s: properties=%s",
            namespace.catalog,
            namespace.name,
            namespace.properties,
        )
        return namespace

    def get_or_create(self, namespace: IcebergNamespace, dry_run: bool = False) -> IcebergNamespace:
        """Create when absent, no-op when present -- and say which happened."""
        catalog = self.catalogs.connect(namespace.catalog)
        if catalog.namespace_exists(namespace.name):
            logger.info("namespace %s.%s: exists, nothing to do", namespace.catalog, namespace.name)
            return namespace
        return self.create(namespace, dry_run=dry_run)

    def delete(self, namespace: IcebergNamespace, dry_run: bool = False) -> None:
        if dry_run:
            logger.info("namespace %s.%s: would drop", namespace.catalog, namespace.name)
            return
        self.catalogs.connect(namespace.catalog).drop_namespace(namespace.name)
        logger.info("namespace %s.%s: dropped", namespace.catalog, namespace.name)
