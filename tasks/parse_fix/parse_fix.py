import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow

    from rekep.fix import (
        fix_arrow_reader,
        fix_codec,
        fix_message_field,
        fix_registry,
        fix_stored_reader,
    )
    from rekep.iceberg import IcebergCatalog, window_filter
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import Message
    from rekep.times import window_of

    SOURCE = "logs.messages"
    TARGET = "fix.messages"


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse FIX

    Read one window of stored capture lines into settled FIX rows: parse every
    frame a line carried -- which settles what it implied where it is read --
    and then name the chains those messages belong to.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    registry = _defaults["registry"]
    lifecycle = _defaults["lifecycle"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    return catalog, end, lifecycle, registry, start


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(catalog, end, lifecycle, records, registry, start):
    _ = records
    with ExitStack() as opened:
        # The same window `parse_messages` wrote, read back off the stored
        # capture: `[start, end)` over the capture clock itself, with the lines
        # that carry no clock beside them, because a line the reader could not
        # stamp belongs to every window. It prunes: a file holds one hour of
        # that clock, so its own stored bounds answer the window without the
        # predicate having to name the partition column -- which it must not,
        # because a capture line with no clock lands in the null partition and
        # PyIceberg 0.12 cannot plan a comparison against one.
        window = window_of(start, end)
        stage = Stage(
            "parse_fix",
            sources={"messages": SOURCE},
            targets={"fix": TARGET},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        carrier = Message.into_field()
        messages = store.dataset(SOURCE, field=carrier)
        opened.callback(messages.close)
        source = messages.read_arrow_reader(
            carrier, row_filter=window_filter("timestamp", window)
        )
        opened.callback(source.close)
        counts = {"read": 0, "messages": 0}

        def _batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(
            source.schema,
            _batches(),
        )
        opened.callback(counted.close)
        # The codec is the whole parse surface: the dictionary and the instant
        # an undated message takes are pinned on it once, and each of the two
        # stages after it is a call. No capture order is among them -- this
        # door reads stored rows, where a column named after a field fills it
        # by that name, and a position is what the line door resolves. Pinning
        # one here would be a reading of a header this task never sees, stale
        # the moment the capture is read under one of its own.
        codec = fix_codec(fix_registry(registry))
        # The published field is what the whole pipeline answers, read from the
        # dictionary alone rather than from the first batch -- so an empty
        # capture creates the same table a full one does.
        field = fix_message_field(codec, carrier)
        # parse -> lifecycle, over the stored capture's batches. A row is a
        # message, so one line carrying two frames answers two -- and one
        # message logged at three hops answers three rows of one identity,
        # which is what the key below folds.
        parsed = fix_arrow_reader(codec, counted, lifecycle=lifecycle)
        opened.callback(parsed.close)

        def _parsed():
            for batch in parsed:
                counts["messages"] += batch.num_rows
                yield batch

        answered = pyarrow.RecordBatchReader.from_batches(
            parsed.schema,
            _parsed(),
        )
        opened.callback(answered.close)
        # The storage boundary: the content codes read as the signed integers
        # Iceberg stores, then the field applied in its native order.
        applied = fix_stored_reader(answered, field)
        opened.callback(applied.close)
        fixes = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(fixes.close)
        # What the window answers replaces what the table held under the same
        # `curruuid`, so a replay lands the same events again and a message
        # logged at every hop it passed lands once. A key is scoped to its
        # partition and the partition is the hour of `unix`, which is the
        # event's own instant: every restatement of one event carries the same
        # one, so they meet. An event whose `unix` moves between two runs --
        # a dictionary that reads its clock differently -- lands in a second
        # hour, where the first copy's key is not in scope and is not replaced.
        written = fixes.overwrite_arrow_reader(applied, field, merge_by=True)
        # A row is a message, not a line: one line carrying two frames answers
        # two and one carrying none answers nothing, so what the write left
        # out -- a restatement of an identity already landed -- is counted
        # against the messages the codec answered rather than the lines read.
        _outcome = stage.finished(
            read=counts["read"],
            written=written,
            skipped=counts["messages"] - written,
            messages=counts["messages"],
        )
    outcome = _outcome
    return (outcome,)


@app.cell
def _(outcome):
    result = outcome
    mo.tree(result)
    return (result,)


@app.cell
def datasets(catalog, result):
    # What the two datasets look like after the run: how each is laid out, and
    # one chain read in the order the walk gave it. A presentation cell -- the
    # runner publishes `result` and nothing else -- and it runs headless, so a
    # layout that stopped being what this task declares fails the run here
    # rather than in a table nobody opened.
    with ExitStack() as shown:
        _store = IcebergCatalog.from_dict(catalog)
        shown.callback(_store.close)
        _layout = {}
        for _name in (SOURCE, TARGET):
            _dataset = _store.dataset(_name)
            shown.callback(_dataset.close)
            if not _dataset.exists:
                continue
            _table = _dataset.iceberg_table
            _layout[_name] = {
                "rows": _dataset.read_arrow_table().num_rows,
                "partitions": sorted(
                    {
                        str(_file["partition"])
                        for _file in _dataset.data_files().to_pylist()
                    }
                ),
                "spec": [str(_field) for _field in _table.spec().fields],
                "key": sorted(
                    _table.schema().find_column_name(_held) or ""
                    for _held in _table.schema().identifier_field_ids
                ),
            }
        _chain = None
        if TARGET in _layout and _layout[TARGET]["rows"]:
            _fixes = _store.dataset(TARGET)
            shown.callback(_fixes.close)
            # `unix, seqnum` is the order the walk gave a chain, and the widest
            # chain is the one worth showing.
            _rows = _fixes.read_arrow_table().select(
                ("crosscode", "unix", "seqnum", "curruuid", "prevuuid", "px", "prevpx")
            )
            _codes = _rows.column("crosscode").to_pylist()
            _widest = max(set(_codes), key=_codes.count)
            _chain = (
                _rows.filter(pyarrow.compute.equal(_rows.column("crosscode"), _widest))
                .sort_by([("unix", "ascending"), ("seqnum", "ascending")])
                .to_pylist()
            )
    layout = _layout
    chain = _chain
    mo.vstack([mo.tree(layout), mo.ui.table(chain or [], selection=None)])
    return chain, layout


if __name__ == "__main__":
    app.run()
