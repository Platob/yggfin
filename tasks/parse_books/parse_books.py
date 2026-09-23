import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    import uuid
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow

    from rekep.fields import stored_arrow_reader
    from rekep.fix import SORT_COLUMNS, fix_codec, fix_message_field, fix_registry
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.market import (
        book_arrow_reader,
        book_field,
        market_window_filter,
        market_window_reader,
    )
    from rekep.tasks import Task
    from rekep.times import window_of

    TARGET = "market.books"


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse books

    Fold ordered refined FIX events into native book continuations for this
    window, then publish the committed snapshot for the three event readers.
    """)


@app.cell
def parameters():
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    refined = _defaults["refined"]
    registry = _defaults["registry"]
    codec_options = _defaults["codec_options"]
    snapshot_millis = _defaults["snapshot_millis"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    return catalog, codec_options, end, refined, registry, snapshot_millis, start


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(catalog, codec_options, end, records, refined, registry, snapshot_millis, start):
    _ = records
    with ExitStack() as opened:
        window = window_of(start, end)
        selected = market_window_filter(window)
        stage = Stage(
            "parse_books",
            sources={"refined": refined},
            targets={"books": TARGET},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        codec = fix_codec(fix_registry(registry), **(codec_options or {}))
        fixed = fix_message_field(codec)
        messages = store.dataset(refined, field=fixed)
        opened.callback(messages.close)
        source = messages.read_arrow_reader(fixed, row_filter=selected, order_by=SORT_COLUMNS)
        opened.callback(source.close)
        counts = {"read": 0}

        def _batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(source.schema, _batches())
        opened.callback(counted.close)
        folded = book_arrow_reader(codec, counted, snapshot_millis=snapshot_millis)
        opened.callback(folded.close)
        bounded = market_window_reader(folded, window)
        opened.callback(bounded.close)
        field = book_field()
        applied = stored_arrow_reader(bounded, field)
        opened.callback(applied.close)
        bookset = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(bookset.close)
        _run_id = uuid.uuid4().hex
        written = bookset.overwrite_arrow_reader(
            applied, field, row_filter=selected, properties={"rekep.books-run-id": _run_id}
        )
        # A recovered commit acknowledgement may refresh to a newer head.
        # Identify this write by its snapshot summary, never by that head.
        _snapshots = bookset.iceberg_table.metadata.snapshots
        _committed = [
            snapshot.snapshot_id
            for snapshot in _snapshots
            if snapshot.summary is not None
            and snapshot.summary.get("rekep.books-run-id") == _run_id
        ]
        if len(_committed) == 1:
            _snapshot_id = _committed[0]
        elif not _snapshots and written == 0:
            _snapshot_id = 0
        else:
            raise ValueError(f"{TARGET} has no unique committed snapshot for run {_run_id}")
        _outcome = stage.finished(
            read=counts["read"],
            written=written,
            skipped=0,
            snapshot_id=_snapshot_id,
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
