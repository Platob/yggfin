"""The Doris service: resources with CRUD, and the deploy that orders them.

Same shape as `rekep.iceberg` -- `Catalogs`, `Namespaces`, `Tables`, each
with the idempotent verbs -- but Doris speaks SQL over a connection this
package does not own. Every verb therefore *renders* its statement and hands
it to the service's `execute` callable: pass a real cursor's execute to run
against a cluster, or leave the default to collect a reviewable plan. All
statements are `IF NOT EXISTS`-shaped, so running the plan twice is a no-op
on the server exactly as `get_or_create` is in code.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from rekep.records.doris import DORIS_ROOT, DorisDdlBuilder, DorisDeployment, DorisTable

__all__ = ["Catalogs", "Doris", "Namespaces", "Tables"]

logger = logging.getLogger("rekep.doris")

#: What every verb hands its SQL to. Returns are ignored.
Executor = Callable[[str], Any]


class Resource:
    """Shared shape: statements rendered, logged, and handed to `execute`."""

    def __init__(self, deployment: DorisDeployment, execute: Executor | None = None) -> None:
        self.deployment = deployment
        self.builder = DorisDdlBuilder()
        self.plan: list[str] = []
        self._execute = execute

    def run(self, kind: str, name: str, statement: str | None, dry_run: bool = False) -> str | None:
        if statement is None:
            logger.info("%s %s: internal, nothing to create", kind, name)
            return None
        logger.info("%s %s: %s", kind, name, "would apply" if dry_run else "apply")
        logger.debug("%s %s: statement:\n%s", kind, name, statement.rstrip())
        self.plan.append(statement)
        if self._execute is not None and not dry_run:
            self._execute(statement)
        return statement


class Catalogs(Resource):
    """The catalog resource: external catalogs become CREATE CATALOG."""

    def list(self) -> list[Any]:
        return list(self.deployment.catalogs)

    def get(self, name: str) -> Any:
        return self.deployment.catalog(name)

    def get_or_create(self, name: str, dry_run: bool = False) -> str | None:
        """IF NOT EXISTS in the statement is the no-op check on the server."""
        return self.run("catalog", name, self.builder.create_catalog(self.get(name)), dry_run)


class Namespaces(Resource):
    """The namespace resource: databases become CREATE DATABASE."""

    def list(self) -> list[Any]:
        return list(self.deployment.namespaces)

    def get(self, name: str) -> Any:
        return self.deployment.namespace(name)

    def get_or_create(self, name: str, dry_run: bool = False) -> str | None:
        namespace = self.get(name)
        catalog = self.deployment.catalog(namespace.catalog)
        statement = f"CREATE DATABASE IF NOT EXISTS {catalog.name}.{namespace.name};\n"
        return self.run("namespace", f"{catalog.name}.{namespace.name}", statement, dry_run)


class Tables(Resource):
    """The table resource: records become CREATE TABLE IF NOT EXISTS."""

    def list(self) -> list[DorisTable]:
        return list(self.deployment.tables)

    def identifier(self, table: DorisTable) -> str:
        return table.name or table.record_class().record_name()

    def get_or_create(self, table: DorisTable, dry_run: bool = False) -> str | None:
        """Verified against the record first, so stale side files stop here."""
        table.verify()
        return self.run("table", self.identifier(table), self.deployment.ddl(table), dry_run)


class Doris:
    """One deployment's Doris stack, resource by resource."""

    def __init__(
        self, deployment: DorisDeployment | None = None, execute: Executor | None = None
    ) -> None:
        self.deployment = deployment or DorisDeployment()
        self.catalogs = Catalogs(self.deployment, execute)
        self.namespaces = Namespaces(self.deployment, execute)
        self.tables = Tables(self.deployment, execute)

    @classmethod
    def load(cls, root: Any = DORIS_ROOT, execute: Executor | None = None, **context: Any) -> Doris:
        return cls(DorisDeployment.load(root, **context), execute)

    def deploy_one(self, table: DorisTable, dry_run: bool = False) -> str | None:
        """Converge one table, autonomous: catalog -> namespace -> table.

        `table` need not be declared in this deployment's own registry --
        `rekep.dataset.Dataset.into_doris_table()` builds one ad hoc and
        hands it here. Its `namespace` must still name an entry this
        deployment's `catalogs`/`namespaces` declare.
        """
        namespace = self.deployment.namespace(table.namespace)
        self.catalogs.get_or_create(namespace.catalog, dry_run=dry_run)
        self.namespaces.get_or_create(table.namespace, dry_run=dry_run)
        return self.tables.get_or_create(table, dry_run=dry_run)

    @classmethod
    def deploy_folder(
        cls,
        root: Any = DORIS_ROOT,
        *,
        parallel: bool = False,
        dry_run: bool = False,
        execute: Executor | None = None,
        **context: Any,
    ) -> list[str]:
        """Load the registry at `root` and converge it, one call.

        Levels stay strictly ordered; `parallel` widens *within* a level,
        which only matters against a live cluster -- the default keeps the
        plan deterministic.
        """
        return cls.load(root, execute=execute, **context).deploy(parallel=parallel, dry_run=dry_run)

    def deploy(self, dry_run: bool = False, parallel: bool = False) -> list[str]:
        """The whole stack in dependency order: catalog -> namespace -> table.

        Levels are strictly sequential -- ordering is the entire point --
        and `parallel` widens within a level for live clusters; off (the
        default) the plan is deterministic. `dry_run` renders and logs the
        plan without handing anything to the executor.
        """
        from concurrent.futures import ThreadPoolExecutor

        levels = (
            (self.catalogs.list(), lambda c: self.catalogs.get_or_create(c.name, dry_run=dry_run)),
            (
                self.namespaces.list(),
                lambda n: self.namespaces.get_or_create(n.name, dry_run=dry_run),
            ),
            (self.tables.list(), lambda t: self.tables.get_or_create(t, dry_run=dry_run)),
        )
        if parallel:
            with ThreadPoolExecutor(max_workers=4) as pool:
                for entries, verb in levels:
                    list(pool.map(verb, entries))
        else:
            for entries, verb in levels:
                for entry in entries:
                    verb(entry)
        plan = [*self.catalogs.plan, *self.namespaces.plan, *self.tables.plan]
        logger.info("deploy plan: %d statement(s)", len(plan))
        return plan
