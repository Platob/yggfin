"""The Iceberg service: resources with CRUD, and the deploy that orders them.

Resource-oriented -- `Catalogs`, `Namespaces`, `Tables` -- each speaking
pyiceberg, each with the idempotent verbs a deploy needs (`get_or_create`,
`create_or_update`). `Iceberg.deploy` runs them in dependency order:
catalogs are checked first, then namespaces in parallel, then tables in
parallel -- a level starts only when the level it depends on has finished.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from rekep.iceberg.catalogs import Catalogs
from rekep.iceberg.namespaces import Namespaces
from rekep.iceberg.tables import Tables
from rekep.records.iceberg import ICEBERG_ROOT, IcebergDeployment

__all__ = ["Catalogs", "Iceberg", "Namespaces", "Tables"]

logger = logging.getLogger("rekep.iceberg")


class Iceberg:
    """One deployment's Iceberg stack, resource by resource."""

    def __init__(self, deployment: IcebergDeployment | None = None) -> None:
        self.deployment = deployment or IcebergDeployment()
        self.catalogs = Catalogs(self.deployment)
        self.namespaces = Namespaces(self.catalogs)
        self.tables = Tables(self.namespaces)

    @classmethod
    def load(cls, root: Any = ICEBERG_ROOT, **context: Any) -> Iceberg:
        return cls(IcebergDeployment.load(root, **context))

    @classmethod
    def deploy_folder(
        cls,
        root: Any = ICEBERG_ROOT,
        *,
        parallel: bool = True,
        max_workers: int = 4,
        dry_run: bool = False,
        **context: Any,
    ) -> dict[str, list[str]]:
        """Load the registry at `root` and converge it, one call.

        Priority planning is `deploy`'s: catalog -> namespace -> table, each
        level gated on the last. `parallel` widens each level to
        `max_workers`; off, everything runs one at a time in declaration
        order, which is the debuggable mode.
        """
        return cls.load(root, **context).deploy(
            max_workers=max_workers if parallel else 1, dry_run=dry_run
        )

    def deploy(self, max_workers: int = 4, dry_run: bool = False) -> dict[str, list[str]]:
        """Converge the whole stack, dependency-ordered, parallel within a level.

        catalog -> namespace -> table: each level waits for the previous one,
        entries inside a level run concurrently. Every action logs what it
        did -- created, updated, or nothing -- so a deploy reads as a plan
        that happened. With `dry_run` nothing mutates and every log line says
        what *would* happen; catalogs are still connected, because a plan
        against an unreachable catalog is fiction.
        """
        done: dict[str, list[str]] = {"catalogs": [], "namespaces": [], "tables": []}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for catalog in pool.map(
                lambda c: (self.catalogs.check(c.name), c.name)[1], self.catalogs.list()
            ):
                done["catalogs"].append(catalog)
            for namespace in pool.map(
                lambda n: self.namespaces.get_or_create(n, dry_run=dry_run),
                self.namespaces.list(),
            ):
                done["namespaces"].append(f"{namespace.catalog}.{namespace.name}")
            for identifier in pool.map(
                lambda t: (
                    self.tables.create_or_update(t, dry_run=dry_run),
                    self.tables.identifier(t),
                )[1],
                self.tables.list(),
            ):
                done["tables"].append(identifier)
        logger.info(
            "deploy complete: %d catalog(s), %d namespace(s), %d table(s)",
            len(done["catalogs"]),
            len(done["namespaces"]),
            len(done["tables"]),
        )
        return done
