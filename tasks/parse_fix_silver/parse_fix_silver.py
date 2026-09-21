import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import datetime
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow
    from rekep.fields import stored_arrow_reader
    from rekep.fix import (
        EVENT_CLOCK,
        SORT_COLUMNS,
        UNDATED,
        fix_codec,
        fix_lifecycle_arrow_reader,
        fix_message_field,
        fix_registry,
        fix_window_filter,
    )
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.times import window_of, within

    TARGET = "fix.silver"


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse FIX: silver

    Read the previous hour and this job's window in event order, walk their
    lifecycle, and land only the events belonging to this job's window.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    bronze = _defaults["bronze"]
    registry = _defaults["registry"]
    codec_options = _defaults["codec_options"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    return bronze, catalog, codec_options, end, registry, start


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(bronze, catalog, codec_options, end, records, registry, start):
    _ = records
    with ExitStack() as opened:
        window = window_of(start, end)
        history = (window[0] - datetime.timedelta(hours=1), window[1])
        stage = Stage(
            "parse_fix_silver",
            sources={"bronze": bronze},
            targets={"silver": TARGET},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        # The same codec `parse_fix_bronze` pinned: the walk reads each row
        # back as the message that wrote it, and the dictionary is what wrote
        # it. The same field too, because a walked row is the parsed row
        # restated: same columns, same key, same layout.
        codec = fix_codec(fix_registry(registry), **(codec_options or {}))
        field = fix_message_field(codec)
        parsed = store.dataset(bronze, field=field)
        opened.callback(parsed.close)
        # The hour transform prunes partitions, then the ordered reader
        # concatenates disjoint file ranges and merges only overlapping ones.
        # Undated rows use the epoch partition and TransactTime file bounds.
        source = parsed.read_arrow_reader(
            field, row_filter=fix_window_filter(history), order_by=SORT_COLUMNS
        )
        opened.callback(source.close)
        counts = {"read": 0, "events": 0, "outside_window": 0}

        def _batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(
            source.schema,
            _batches(),
        )
        opened.callback(counted.close)
        # The native lifecycle owns dating, delivery deduplication, state and
        # expiry. Its finite capture is limited to this input window; there
        # is no second collected Arrow table or union before the walk.
        walked = fix_lifecycle_arrow_reader(codec, counted)
        opened.callback(walked.close)

        def _events():
            for batch in walked:
                clock = batch.column(EVENT_CLOCK)
                selected = batch.filter(
                    pyarrow.compute.or_(
                        within(clock, window), pyarrow.compute.equal(clock, UNDATED)
                    )
                )
                counts["events"] += selected.num_rows
                counts["outside_window"] += batch.num_rows - selected.num_rows
                if selected.num_rows:
                    yield selected

        events = pyarrow.RecordBatchReader.from_batches(walked.schema, _events())
        opened.callback(events.close)
        # The storage boundary, the same one bronze crossed: the content codes
        # read as the signed integers Iceberg stores, then the field applied
        # in its native order.
        applied = stored_arrow_reader(events, field)
        opened.callback(applied.close)
        silver = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(silver.close)
        # Only this window is written. Previous-hour rows warm the lifecycle
        # without replacing the historical chain with a truncated replay.
        written = silver.overwrite_arrow_reader(applied, field, merge_by=True)
        _outcome = stage.finished(
            read=counts["read"],
            written=written,
            skipped=counts["events"] - written,
            events=counts["events"],
            outside_window=counts["outside_window"],
        )
    outcome = _outcome
    return (outcome,)


@app.cell
def _(outcome):
    result = outcome
    mo.tree(result)
    return (result,)


@app.cell
def datasets(bronze, catalog, end, result, start):
    # What the two datasets look like after the run: how each is laid out, and
    # one chain read in the order the walk gave it. A presentation cell -- the
    # runner publishes `result` and nothing else -- and it runs headless, so a
    # layout that stopped being what this task declares fails the run here
    # rather than in a table nobody opened.
    with ExitStack() as shown:
        _store = IcebergCatalog.from_dict(catalog)
        shown.callback(_store.close)
        _layout = {}
        for _name in (bronze, TARGET):
            if not _store.table_exists(_name):
                continue
            _dataset = _store.dataset(_name)
            shown.callback(_dataset.close)
            _table = _dataset.iceberg_table
            _layout[_name] = {
                "rows": _dataset.records,
                "partitions": sorted(
                    {str(_file["partition"]) for _file in _dataset.data_files().to_pylist()}
                ),
                "spec": [str(_field) for _field in _table.spec().fields],
                "key": sorted(
                    _table.schema().find_column_name(_held) or ""
                    for _held in _table.schema().identifier_field_ids
                ),
            }
        _chain = None
        if TARGET in _layout and _layout[TARGET]["rows"]:
            _silver = _store.dataset(TARGET)
            shown.callback(_silver.close)
            # The interactive view is a bounded sample of this window; it
            # never scans the full warehouse after an otherwise pruned job.
            _sample = _silver.read_arrow_reader(
                columns=("crosscode", "currunix", "seqnum", "curruuid", "prevuuid", "state"),
                row_filter=fix_window_filter(window_of(start, end)),
                order_by=SORT_COLUMNS,
                limit=64,
            )
            shown.callback(_sample.close)
            _chain = _sample.read_all().to_pylist()
    layout = _layout
    chain = _chain
    mo.vstack([mo.tree(layout), mo.ui.table(chain or [], selection=None)])
    return chain, layout


if __name__ == "__main__":
    app.run()
