"""The pipeline tasks this package bundles, and the one way each one runs."""

from __future__ import annotations

import dataclasses
import importlib
import json
from collections.abc import Mapping
from importlib import resources
from types import ModuleType
from typing import Any

#: Every bundled task, in the order the supported graph runs them. A name is
#: both its module under `rekep.tasks` and the JSON document of defaults
#: shipped beside that module.
NAMES = (
    "parse_messages",
    "parse_fix_raw",
    "parse_fix_refined",
    "parse_books",
    "parse_orders",
    "parse_quotes",
    "parse_executions",
    "build_dbt",
    "optimize_iceberg",
)


@dataclasses.dataclass(frozen=True)
class Task:
    """One bundled task: its shipped defaults and the module that runs them.

    `rekep.tasks.<name>` defines `run`, whose keyword-only parameters are
    exactly the keys of `<name>.json`, and `TARGETS`, the tables it writes.
    Reading the defaults imports nothing, so a scheduler can declare a task
    without loading what running it needs.
    """

    name: str

    def __post_init__(self) -> None:
        if self.name not in NAMES:
            raise ValueError(f"no task is named {self.name!r}; the tasks are {', '.join(NAMES)}")

    @property
    def parameters(self) -> dict[str, Any]:
        """The shipped defaults, as a fresh mapping on every read."""
        document = resources.files(__package__).joinpath(f"{self.name}.json")
        return json.loads(document.read_text(encoding="utf-8"))

    @property
    def module(self) -> ModuleType:
        """The module whose `run` does the work."""
        return importlib.import_module(f"{__package__}.{self.name}")

    @property
    def summary(self) -> str:
        """The first line of the module's docstring."""
        return (self.module.__doc__ or "").strip().splitlines()[0]

    @property
    def targets(self) -> tuple[str, ...]:
        """The tables a run writes."""
        return tuple(self.module.TARGETS)

    def resolved(self, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """The defaults under `overrides`, refusing a name the task does not take."""
        parameters = self.parameters
        undeclared = sorted(set(overrides or {}) - set(parameters))
        if undeclared:
            raise TypeError(
                f"{self.name} takes no {', '.join(undeclared)}; it takes {', '.join(parameters)}"
            )
        parameters.update(overrides or {})
        return parameters

    def run(self, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Run once and return the validated `rekep.logs.Stage` result."""
        from rekep.logs import Stage

        result = Stage.validated(self.module.run(**self.resolved(overrides)))
        named = result["task"]
        if named != self.name and not named.startswith(f"{self.name}_"):
            raise ValueError(f"{self.name} returned {named!r}, not a {self.name} run")
        return result

    def deploy(
        self,
        overrides: Mapping[str, Any] | None = None,
        *,
        table_properties: Mapping[str, str] | None = None,
        branch: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Create each table this task writes that its catalog lacks.

        Answers the catalog it used and `created`, `present` or, under
        `dry_run`, `missing` per table. A table no `rekep.deploy.TABLES` entry
        declares -- a dbt product, whose model declares it -- is created by
        the run that first commits it, so it is not among them.
        """
        from rekep.deploy import TABLES, deploy
        from rekep.fix import fix_codec, fix_registry
        from rekep.iceberg import IcebergCatalog

        parameters = self.resolved(overrides)
        declared = {shape.table for shape in TABLES}
        tables = [table for table in self.targets if table in declared]
        configured = parameters.get("catalog")
        if not tables:
            return {"catalog": configured, "tables": {}}
        codec = None
        if "registry" in parameters:
            # The dictionary a run parses with is what types the FIX tables
            # it creates, and one the run would refuse is refused here,
            # before any table is.
            codec = fix_codec(
                fix_registry(parameters["registry"]), **(parameters.get("codec_options") or {})
            )
        if not isinstance(configured, Mapping):
            raise TypeError("a task catalog is a mapping of name and properties")
        unexpected = sorted(set(configured) - {"name", "properties"})
        if unexpected:
            raise TypeError(
                "a task catalog accepts only name and properties; unexpected "
                + ", ".join(unexpected)
            )
        catalog = IcebergCatalog.from_dict(configured)
        try:
            done = deploy(
                catalog,
                table_properties=dict(table_properties or {}),
                branch=branch,
                tables=tables,
                dry_run=dry_run,
                codec=codec,
            )
            return {"catalog": catalog.into_dict(), "tables": done}
        finally:
            catalog.close()


#: Every bundled task, in the order the supported graph runs them.
TASKS = tuple(Task(name) for name in NAMES)
