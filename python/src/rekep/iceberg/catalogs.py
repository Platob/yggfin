"""Catalogs: the Iceberg catalog resource, CRUD over pyiceberg."""

from __future__ import annotations

import functools
import logging
from typing import TYPE_CHECKING

from rekep.records.iceberg import IcebergCatalog, IcebergDeployment

if TYPE_CHECKING:  # pragma: no cover - pyiceberg is imported at the point of use
    from pyiceberg.catalog import Catalog

logger = logging.getLogger("rekep.iceberg")


class Catalogs:
    """The declared catalogs, and live pyiceberg connections to them.

    A catalog is not created through pyiceberg -- it *is* the endpoint -- so
    the CRUD here is declarative: `list` and `get` read the deployment,
    `connect` proves a catalog reachable, and `check` is the deploy-time
    no-op gate the stack runs before touching namespaces.
    """

    def __init__(self, deployment: IcebergDeployment) -> None:
        self.deployment = deployment

    def list(self) -> list[IcebergCatalog]:
        return list(self.deployment.catalogs)

    def get(self, name: str) -> IcebergCatalog:
        return self.deployment.catalog(name)

    @functools.lru_cache(maxsize=16)  # noqa: B019 - one live connection per catalog
    def connect(self, name: str) -> Catalog:
        """A live pyiceberg catalog, one connection per name."""
        from pyiceberg.catalog import load_catalog

        declared = self.get(name)
        logger.info("catalog %s: connecting (%s)", name, declared.type)
        return load_catalog(declared.name, **declared.pyiceberg_properties())

    def check(self, name: str) -> bool:
        """Prove the catalog reachable; the no-op 'create' of this resource."""
        catalog = self.connect(name)
        catalog.list_namespaces()
        logger.info("catalog %s: reachable", name)
        return True
