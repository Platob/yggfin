"""Build the dbt products from `fix.refined` and commit each one back into Iceberg."""

from __future__ import annotations

import collections
import json
import logging
import os
import pathlib
from collections.abc import Mapping, Sequence
from typing import Any

from rekep.logs import Stage, configure

#: The tables the shipped project's models commit.
TARGETS = ("orders.events", "orders.current", "executions.fills")

#: What dbt calls a node that did not settle.
FAILURES = ("error", "fail", "runtime error")

#: dbt's own severities, as the levels this package records them at. A node
#: finishing is dbt's `info` and this package's DEBUG: one INFO record per
#: verb is the policy, and the verb here is the whole build.
LEVELS = {
    "debug": logging.DEBUG,
    "test": logging.DEBUG,
    "info": logging.DEBUG,
    "warn": logging.WARNING,
    "error": logging.ERROR,
}


def run(
    *,
    project: str,
    profiles: str | None,
    target: str | None,
    select: str | Sequence[str] | None,
    catalog: Mapping[str, Any] | None,
    log_level: str,
) -> dict[str, Any]:
    """Run `dbt build` over `project` and answer what each model committed.

    dbt and the `rekep.dbt` plugin it loads are the `runner` group's, not the
    package's, so they are imported here rather than with the module.
    """
    from dbt.cli.main import dbtRunner

    from rekep.dbt import CATALOG, committed, released

    # Records go to stderr from here on, dbt's own among them.
    configure(log_level)
    logger = logging.getLogger("rekep.dbt")
    directory = pathlib.Path(project)
    if not (directory / "dbt_project.yml").is_file():
        raise FileNotFoundError(directory / "dbt_project.yml")
    stage = Stage("build_dbt", sources={"project": str(directory)})

    def relayed(event: Any) -> None:
        # dbt reports through callbacks and this reports through logging, so
        # the two streams are one stream and stdout stays the result's alone.
        logger.log(LEVELS.get(event.info.level, logging.DEBUG), "%s", event.info.msg)

    if catalog is not None:
        # The one mapping every task spells, handed to the profile rather
        # than repeated in it.
        os.environ[CATALOG] = json.dumps(catalog)
    # Whatever an earlier build in this process committed is not this one's.
    committed()
    argv = [
        "build",
        # dbt's console is silent: the callback above is what an operator
        # reads, and `stdout` carries one task result and nothing else.
        "--log-level",
        "none",
        "--project-dir",
        str(directory),
        "--profiles-dir",
        str(profiles or directory),
    ]
    if target:
        argv += ["--target", str(target)]
    if select:
        argv += ["--select", *([select] if isinstance(select, str) else list(select))]
    invoked = dbtRunner(callbacks=[relayed]).invoke(argv)
    # dbt says nothing to a plugin when a build ends, so the catalog it read
    # and committed through is this task's to close: it is a live connection,
    # and over SQLite it is a file the caller of a run cannot delete until the
    # handle goes. Nothing below reads it -- the counts come off the results.
    released()

    nodes = list(getattr(invoked.result, "results", None) or ())
    statuses = collections.Counter(str(node.status) for node in nodes)
    failed = [node.node.name for node in nodes if str(node.status) in FAILURES]
    if invoked.exception is not None:
        raise invoked.exception
    if not invoked.success or failed:
        raise RuntimeError(f"dbt build failed: {', '.join(failed) or 'no node ran'}")
    # What each published model committed to, read off the model's own
    # configuration, and what it wrote, read off the plugin that wrote it.
    for node in nodes:
        table = getattr(node.node.config, "extra", {}).get("table")
        if table:
            stage.targets[node.node.name] = str(table)
    written = committed()
    kinds = collections.Counter(str(node.node.resource_type) for node in nodes)
    # A test the project declares as a warning is a quality signal and not a
    # failure, so it is counted rather than raised on.
    warned = [node.node.name for node in nodes if str(node.status) == "warn"]
    stage.says(
        "%d nodes ran: %d models, %d tests, %d warned",
        len(nodes),
        kinds.get("model", 0),
        kinds.get("test", 0),
        len(warned),
    )
    return stage.finished(
        read=len(nodes),
        written=sum(written.values()),
        skipped=statuses.get("skipped", 0),
        models=kinds.get("model", 0),
        tests=kinds.get("test", 0),
        warned=warned,
        rows=written,
    )
