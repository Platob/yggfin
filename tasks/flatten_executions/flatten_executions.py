import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib

    import marimo as mo
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.market import Book, Execution
    from rekep.tasks import Task
    from rekep.times import unix_of


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Flatten executions

    Project nested book deltas into the canonical `Execution` table with Arrow kernels.
    """)
    return


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_yaml(str(pathlib.Path(__file__).with_suffix(".yml"))).parameters
    source = _defaults["source"]
    target = _defaults["target"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    table_properties = _defaults["table_properties"]
    branch = _defaults["branch"]
    merge_by = _defaults["merge_by"]
    commit_batch_num = _defaults["commit_batch_num"]
    commit_row_size = _defaults["commit_row_size"]
    log_level = _defaults["log_level"]
    return (
        branch,
        catalog,
        commit_batch_num,
        commit_row_size,
        end,
        log_level,
        merge_by,
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
    lower, upper = unix_of(start), unix_of(end, upper=True)
    return lower, upper


@app.cell
def _(lower, records, source, target, upper):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    stage = Stage(
        "flatten_executions",
        sources={"books": source},
        targets={"executions": target},
        window=(lower, upper),
    )
    return (stage,)


@app.cell
def _(branch, catalog, commit_batch_num, commit_row_size, stage, table_properties):
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
    # The stage named both tables, and reading them back is what orders this
    # cell after the record that opened the run.
    books = store.dataset(
        stage.sources["books"],
        field=Book.into_field(),
        branch=branch,
    )
    executions = store.dataset(
        stage.targets["executions"],
        field=Execution.into_field(),
        table_properties=dict(table_properties),
        branch=branch,
        commit_batch_num=commit_batch_num,
        commit_row_size=commit_row_size,
    )
    return books, executions


@app.cell
def _(books, commit_batch_num, commit_row_size, executions, lower, merge_by, upper):
    counts = {"read": 0}

    def _filter(column="unix"):
        predicates = []
        if lower is not None:
            predicates.append(GreaterThanOrEqual(column, lower))
        if upper is not None:
            predicates.append(LessThan(column, upper))
        return (
            None if not predicates else predicates[0] if len(predicates) == 1 else And(*predicates)
        )

    def _batches():
        reader = books.read_arrow_reader(
            Book.into_field(), row_filter=_filter(), order_by=("unix", "hash")
        )
        for batch in reader:
            flattened = Execution.from_books_arrow_batch(batch)
            counts["read"] += flattened.num_rows
            if flattened.num_rows:
                yield flattened

    written = executions.append_arrow_reader(
        _batches(),
        Execution.into_field(),
        merge_by=merge_by,
        commit_row_size=commit_row_size,
        commit_batch_num=commit_batch_num,
    )
    return counts, written


@app.cell
def _(counts, stage, written):
    stage.says("projected %d executions out of the books in the window", counts["read"])
    result = stage.finished(read=counts["read"], written=written)
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
