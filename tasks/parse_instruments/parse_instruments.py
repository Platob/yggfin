import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib

    import marimo as mo
    from pyiceberg.expressions import And, GreaterThanOrEqual, In, IsNull, LessThan

    from rekep.fix.registry import FixRegistry
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.market import InstUpdate
    from rekep.resources import resource
    from rekep.tasks import Task
    from rekep.text import FixMsg
    from rekep.times import unix_of


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse instruments

    Version the reference data carried by checked FIX market messages.
    """)
    return


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_yaml(str(pathlib.Path(__file__).with_suffix(".yml"))).parameters
    project_root = _defaults["project_root"]
    source = _defaults["source"]
    start = _defaults["start"]
    end = _defaults["end"]
    fix_dictionary = _defaults["fix_dictionary"]
    catalog = _defaults["catalog"]
    table_properties = _defaults["table_properties"]
    branch = _defaults["branch"]
    target = _defaults["target"]
    batch_row_size = _defaults["batch_row_size"]
    commit_batch_num = _defaults["commit_batch_num"]
    commit_row_size = _defaults["commit_row_size"]
    log_level = _defaults["log_level"]
    return (
        batch_row_size,
        branch,
        catalog,
        commit_batch_num,
        commit_row_size,
        end,
        fix_dictionary,
        log_level,
        project_root,
        source,
        start,
        table_properties,
        target,
    )


@app.cell
def _(log_level):
    # Records go to stderr from here on. Every cell that can emit one reads
    # `records` back, and marimo builds a cell's edges from its body -- so the
    # level is in force before the first of them runs.
    records = configure(log_level)
    return (records,)


@app.cell
def _(end, start):
    # The FIX stage resolved the transaction clock and wrote it as `unix`, which is
    # what this table is partitioned from. The read requests event order explicitly;
    # pipeline writes do not add a physical sort.
    lower, upper = unix_of(start), unix_of(end, upper=True)
    return lower, upper


@app.cell
def _(fix_dictionary, project_root, records):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    registry = (
        FixRegistry()
        if fix_dictionary is None
        else FixRegistry(cache_dir=str(resource(str(fix_dictionary), root=project_root).url))
    )
    return (registry,)


@app.cell
def _():
    field = FixMsg.into_field()
    return (field,)


@app.cell
def _(lower, records, source, target, upper):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    stage = Stage(
        "parse_instruments",
        sources={"market": source},
        targets={"instruments": target},
        window=(lower, upper),
    )
    return (stage,)


@app.cell
def _(
    batch_row_size,
    branch,
    catalog,
    commit_batch_num,
    commit_row_size,
    field,
    stage,
    table_properties,
):
    if isinstance(batch_row_size, bool) or not isinstance(batch_row_size, int):
        raise TypeError("batch_row_size must be an integer")
    if batch_row_size <= 0:
        raise ValueError("batch_row_size must be positive")
    if isinstance(commit_batch_num, bool) or not isinstance(commit_batch_num, int):
        raise TypeError("commit_batch_num must be an integer")
    if commit_batch_num <= 0:
        raise ValueError("commit_batch_num must be positive")
    if commit_row_size is not None and (
        isinstance(commit_row_size, bool) or not isinstance(commit_row_size, int)
    ):
        raise TypeError("commit_row_size must be an integer or null")
    if commit_row_size is not None and commit_row_size <= 0:
        raise ValueError("commit_row_size must be positive")
    store = IcebergCatalog.from_dict(catalog)
    # The stage named both tables, and reading them back off it is what orders
    # this cell after the record that opened the run.
    messages = store.dataset(stage.sources["market"], field=field, branch=branch)
    instruments = store.dataset(
        stage.targets["instruments"],
        field=InstUpdate.into_field(),
        table_properties=dict(table_properties),
        branch=branch,
        commit_batch_num=commit_batch_num,
        commit_row_size=commit_row_size,
    )
    return instruments, messages


@app.cell
def _(
    batch_row_size,
    commit_batch_num,
    commit_row_size,
    field,
    instruments,
    lower,
    messages,
    registry,
    upper,
):
    counts = {"read": 0, "written": 0}

    def _window(lower, upper, column="unix"):
        predicates = []
        if lower is not None:
            predicates.append(GreaterThanOrEqual(column, lower))
        if upper is not None:
            predicates.append(LessThan(column, upper))
        return (
            None if not predicates else predicates[0] if len(predicates) == 1 else And(*predicates)
        )

    def _observed():
        """Every update the window's messages describe, enriched per ticker."""
        if not messages.exists:
            return iter(())
        window = _window(lower, upper)
        # Failed rows remain in fix.market for audit; an incomplete reading cannot
        # become reference data merely because its raw message carried a symbol.
        clean = IsNull("error")
        row_filter = clean if window is None else And(window, clean)
        reader = messages.read_arrow_reader(
            field, row_filter=row_filter, order_by=("unix", "msgseqnum", "hash")
        )
        return InstUpdate.from_fixmsgs(FixMsg.from_arrow_reader(reader), registry=registry)

    def _stored(codes):
        """Current rows keyed by their canonical component ticker."""
        if not instruments.exists or not codes:
            return {}
        reader = instruments.read_arrow_reader(
            InstUpdate.into_field(), row_filter=In("code", codes)
        )
        return {row.instrument.symbolticker: row for row in InstUpdate.from_arrow_reader(reader)}

    def _versions():
        """Only what changes the table, one bounded lookup per batch."""
        for batch in InstUpdate.into_arrow_reader(_observed(), batch_row_size=batch_row_size):
            observed = list(InstUpdate.from_arrow_reader(iter((batch,))))
            counts["read"] += len(observed)
            stored = _stored(tuple(row.code for row in observed))
            changed = list(InstUpdate.versioned(observed, stored))
            if changed:
                counts["written"] += len(changed)
                yield InstUpdate.into_arrow_batch(changed)

    def _with_first(first, rest):
        yield first
        yield from rest

    # One lifecycle has one current row under the declared xhash primary key, so
    # enrichment overwrites it. Nothing is written when no batch changed, so a
    # replay of an unchanged window commits no snapshot.
    _changes = iter(_versions())
    _first = next(_changes, None)
    if _first is not None:
        instruments.overwrite_arrow_reader(
            _with_first(_first, _changes),
            InstUpdate.into_field(),
            merge_by=True,
            commit_row_size=commit_row_size,
            commit_batch_num=commit_batch_num,
        )
    return (counts,)


@app.cell
def _(counts, stage):
    stage.says(
        "observed %d instruments, of which %d are new versions",
        counts["read"],
        counts["written"],
    )
    result = stage.finished(read=counts["read"], written=counts["written"])
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
