import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import collections
    import json
    import logging
    import os
    import pathlib

    import marimo as mo
    from dbt.cli.main import dbtRunner

    from rekep.dbt import CATALOG, committed
    from rekep.logs import Stage, configure
    from rekep.tasks import Task

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


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Build dbt

    Derive the order and execution products from the stored FIX rows, and
    commit each one back into Iceberg.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    project = _defaults["project"]
    profiles = _defaults["profiles"]
    target = _defaults["target"]
    select = _defaults["select"]
    catalog = _defaults["catalog"]
    log_level = _defaults["log_level"]
    return catalog, log_level, profiles, project, select, target


@app.cell
def _(log_level):
    # Records go to stderr from here on, dbt's own among them. Every cell that
    # can emit one reads `records` back, and marimo builds a cell's edges from
    # its body -- so the level is in force before the first of them runs.
    records = configure(log_level)
    return (records,)


@app.cell
def _(catalog, profiles, project, records, select, target):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    logger = logging.getLogger("rekep.dbt")
    directory = pathlib.Path(project)
    if not (directory / "dbt_project.yml").is_file():
        raise FileNotFoundError(directory / "dbt_project.yml")
    stage = Stage("build_dbt", sources={"project": str(directory)})

    def _relayed(event) -> None:
        # dbt reports through callbacks and this reports through logging, so
        # the two streams are one stream and stdout stays the result's alone.
        logger.log(LEVELS.get(event.info.level, logging.DEBUG), "%s", event.info.msg)

    if catalog is not None:
        # The one mapping every task document spells, handed to the profile
        # rather than repeated in it.
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
    invoked = dbtRunner(callbacks=[_relayed]).invoke(argv)
    return invoked, stage


@app.cell
def _(invoked, stage):
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
    result = stage.finished(
        read=len(nodes),
        written=sum(written.values()),
        skipped=statuses.get("skipped", 0),
        models=kinds.get("model", 0),
        tests=kinds.get("test", 0),
        warned=warned,
        rows=written,
    )
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
