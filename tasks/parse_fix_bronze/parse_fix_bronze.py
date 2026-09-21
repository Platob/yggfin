import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow
    from rekep.fields import stored_arrow_reader
    from rekep.fix import (
        fix_codec,
        fix_message_field,
        fix_parse_arrow_reader,
        fix_registry,
    )
    from rekep.iceberg import IcebergCatalog, window_filter
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import Message
    from rekep.times import window_of

    TARGET = "fix.bronze"


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse FIX: bronze

    Read one window of stored capture lines into parsed FIX rows: every frame
    a line carried, settled where it is read, and nothing walked.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    messages = _defaults["messages"]
    registry = _defaults["registry"]
    codec_options = _defaults["codec_options"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    return catalog, codec_options, end, messages, registry, start


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(catalog, codec_options, end, messages, records, registry, start):
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
            "parse_fix_bronze",
            sources={"messages": messages},
            targets={"bronze": TARGET},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        carrier = Message.into_field()
        lines = store.dataset(messages, field=carrier)
        opened.callback(lines.close)
        source = lines.read_arrow_reader(carrier, row_filter=window_filter("timestamp", window))
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
        # an undated message takes are pinned on it once, and the stage after
        # it is a call. No capture order is among them -- this door reads
        # stored rows, where a column named after a field fills it by that
        # name, and a position is what the line door resolves. Pinning one
        # here would be a reading of a header this task never sees, stale the
        # moment the capture is read under one of its own.
        codec = fix_codec(fix_registry(registry), **(codec_options or {}))
        # The published field is what both FIX tables are declared with, read
        # from the dictionary alone rather than from the first batch -- so an
        # empty window creates the same table a full one does.
        field = fix_message_field(codec)
        # The parse alone, over the stored capture's batches, and no walk: a
        # row is a message, so one line carrying two frames answers two -- and
        # one message logged at three hops answers three rows of one identity,
        # which is what the key below folds. `seqnum` and `prevuuid` are empty
        # on every row, because nothing has placed a message in its chain yet;
        # that is `parse_fix_silver`'s reading of this table.
        parsed = fix_parse_arrow_reader(codec, counted)
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
        applied = stored_arrow_reader(answered, field)
        opened.callback(applied.close)
        bronze = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(bronze.close)
        # What the window answers replaces what the table held under the same
        # `curruuid`, so a replay lands the same events again and a message
        # logged at every hop it passed lands once. A key is scoped to its
        # partition and the partition is the hour of `currunix`, which is the
        # event's own instant: every restatement of one event carries the same
        # one, so they meet. A message the parse could not date sits at the
        # codec's pin -- one hour, one partition -- until the walk dates it.
        written = bronze.overwrite_arrow_reader(applied, field, merge_by=True)
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
def datasets(catalog, messages, result):
    # What the two datasets look like after the run: how each is laid out. A
    # presentation cell -- the runner publishes `result` and nothing else --
    # and it runs headless, so a layout that stopped being what this task
    # declares fails the run here rather than in a table nobody opened.
    with ExitStack() as shown:
        _store = IcebergCatalog.from_dict(catalog)
        shown.callback(_store.close)
        _layout = {}
        for _name in (messages, TARGET):
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
    layout = _layout
    mo.tree(layout)
    return (layout,)


if __name__ == "__main__":
    app.run()
