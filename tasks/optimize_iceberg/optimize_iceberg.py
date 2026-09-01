import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import datetime
    import pathlib

    import marimo as mo

    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Optimize Iceberg

    Compact tables, retain recent snapshots, and sweep unreachable files.
    """)
    return


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_yaml(str(pathlib.Path(__file__).with_suffix(".yml"))).parameters
    catalog = _defaults["catalog"]
    namespace = _defaults["namespace"]
    branch = _defaults["branch"]
    min_files = _defaults["min_files"]
    retain = _defaults["retain"]
    snapshot_age_days = _defaults["snapshot_age_days"]
    orphan_age_days = _defaults["orphan_age_days"]
    remove_orphans = _defaults["remove_orphans"]
    metadata = _defaults["metadata"]
    log_level = _defaults["log_level"]
    return (
        branch,
        catalog,
        log_level,
        metadata,
        min_files,
        namespace,
        orphan_age_days,
        remove_orphans,
        retain,
        snapshot_age_days,
    )


@app.cell
def _(log_level):
    # Records go to stderr from here on. Every cell that can emit one reads
    # `records` back, and marimo builds a cell's edges from its body -- so the
    # level is in force before the first of them runs.
    records = configure(log_level)
    return (records,)


@app.cell
def _(catalog, min_files, orphan_age_days, records, retain, snapshot_age_days):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    if type(retain) is not int or retain < 1:
        raise ValueError("retain must be a positive integer")
    if type(min_files) is not int or min_files < 2:
        raise ValueError("min_files must be at least 2")
    if snapshot_age_days is not None and snapshot_age_days < 0:
        raise ValueError("snapshot_age_days must be non-negative or null")
    if orphan_age_days < 0:
        raise ValueError("orphan_age_days must be non-negative")
    store = IcebergCatalog.from_dict(catalog)
    snapshot_age = None if snapshot_age_days is None else datetime.timedelta(days=snapshot_age_days)
    orphan_age = datetime.timedelta(days=orphan_age_days)
    return orphan_age, snapshot_age, store


@app.cell
def _(store):
    # The catalog is open before the run is announced, so the stage names it
    # from the handle rather than from the parameter.
    stage = Stage("optimize_iceberg", sources={"catalog": store.name})
    return (stage,)


@app.cell
def _(
    branch,
    metadata,
    min_files,
    namespace,
    orphan_age,
    remove_orphans,
    retain,
    snapshot_age,
    stage,
    store,
):
    reports = {}
    # `stage` is a ref so the opening record precedes the per-table ones.
    for _dataset in store.datasets(namespace):
        reports[_dataset.name] = _dataset.optimize(
            branch=branch,
            min_files=min_files,
            retain=retain,
            older_than=snapshot_age,
            remove_orphans=remove_orphans,
            orphan_age=orphan_age,
            metadata=metadata,
        )

    rewritten = sum(report["rewritten"] for report in reports.values())
    deleted = sum(report["deleted"] for report in reports.values())
    byte_size = sum(report["bytes"] for report in reports.values())
    stage.says(
        "visited %d tables: %d parts compacted, %d snapshots expired, %d files swept (%d bytes)",
        len(reports),
        rewritten,
        sum(report["expired"] for report in reports.values()),
        deleted,
        byte_size,
    )
    return byte_size, deleted, reports, rewritten


@app.cell
def _(byte_size, deleted, reports, rewritten, stage):
    # A maintenance pass reads every table it visits and writes the parts it
    # compacted, which is what `read` and `written` mean everywhere else.
    result = stage.finished(
        read=len(reports),
        written=rewritten,
        skipped=len(reports) - sum(1 for report in reports.values() if report["rewritten"]),
        tables=len(reports),
        expired=sum(report["expired"] for report in reports.values()),
        deleted=deleted,
        byte_size=byte_size,
        reports=reports,
    )
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
