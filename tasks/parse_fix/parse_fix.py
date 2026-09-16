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
    frame a line carried, enrich what a message implied, and name the chains
    it belongs to.
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
        # capture: `[start, end)` over `timepartition`, which prunes to the
        # hours it covers, with the lines that carry no clock beside them.
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
            carrier, row_filter=window_filter("timepartition", window)
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
        # an undated message takes are pinned on it once, and each of the three
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
        # parse -> enrich -> lifecycle, over the stored capture's batches. A
        # row is a message, so one line carrying two frames answers two.
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
        applied = field.apply_arrow_reader(
            answered,
            safe=False,
            nullability="strict",
        )
        opened.callback(applied.close)
        fixes = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(fixes.close)
        # What the window answers replaces what the table held for the same
        # `(sourceurl, rownum, msghash)`, so a replay lands the same messages
        # again and a dictionary change lands their new reading.
        written = fixes.overwrite_arrow_reader(applied, field, merge_by=True)
        # A row is a message, not a line: one line carrying two frames answers
        # two and one carrying none answers nothing, so what the write left
        # out -- a second message of one identity -- is counted against the
        # messages the codec answered rather than against the lines read.
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


if __name__ == "__main__":
    app.run()
