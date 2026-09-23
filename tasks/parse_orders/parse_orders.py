import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow

    from rekep.fields import stored_arrow_reader
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.market import (
        book_event_arrow_reader,
        book_field,
        market_event_field,
        market_window_filter,
        market_window_reader,
    )
    from rekep.tasks import Task
    from rekep.times import window_of

    TARGET = "market.orders"
    COLUMNS = ("bid.deltas", "ask.deltas")


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse orders

    Read this window from one committed book snapshot and store its native
    orders. The book already owns the FIX fold and lifecycle facts.
    """)


@app.cell
def parameters():
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    books = _defaults["books"]
    snapshot_id = _defaults["snapshot_id"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    return books, catalog, end, snapshot_id, start


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(books, catalog, end, records, snapshot_id, start):
    _ = records
    if snapshot_id is not None and (
        isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or snapshot_id < 0
    ):
        raise ValueError(f"expected a nonnegative book snapshot_id or None, got {snapshot_id!r}")
    with ExitStack() as opened:
        window = window_of(start, end)
        selected = market_window_filter(window)
        stage = Stage(
            "parse_orders",
            sources={"books": books},
            targets={"orders": TARGET},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        declared = book_field()
        bookset = store.dataset(books, field=declared)
        opened.callback(bookset.close)
        _source_snapshot_id = snapshot_id
        if _source_snapshot_id is None:
            _snapshot = bookset.iceberg_table.current_snapshot() if bookset.exists else None
            _source_snapshot_id = _snapshot.snapshot_id if _snapshot is not None else 0
        if _source_snapshot_id == 0:
            # Zero is a pinned absence, never permission to follow a newer head.
            source = pyarrow.RecordBatchReader.from_batches(declared.into_arrow_schema(), [])
        else:
            if not bookset.exists:
                raise ValueError(f"{books} has no snapshot {_source_snapshot_id}: table is missing")
            source = bookset.read_arrow_reader(
                row_filter=selected, columns=COLUMNS, snapshot_id=_source_snapshot_id
            )
        opened.callback(source.close)
        counts = {"read": 0, "events": 0}

        def _batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(source.schema, _batches())
        opened.callback(counted.close)
        flattened = book_event_arrow_reader(counted, "orders")
        opened.callback(flattened.close)
        bounded = market_window_reader(flattened, window)
        opened.callback(bounded.close)

        def _events():
            for batch in bounded:
                counts["events"] += batch.num_rows
                yield batch

        answered = pyarrow.RecordBatchReader.from_batches(bounded.schema, _events())
        opened.callback(answered.close)
        field = market_event_field()
        applied = stored_arrow_reader(answered, field)
        opened.callback(applied.close)
        target = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(target.close)
        written = target.overwrite_arrow_reader(applied, field, row_filter=selected)
        _outcome = stage.finished(
            read=counts["read"],
            written=written,
            skipped=max(0, counts["events"] - written),
            source_snapshot_id=_source_snapshot_id,
        )
    outcome = _outcome
    return (outcome,)


@app.cell
def _(outcome):
    result = outcome
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
