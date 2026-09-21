import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow

    from rekep import IOBase
    from rekep.fields import stored_arrow_reader
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import Message
    from rekep.times import window_of, within

    TARGET = "logs.messages"


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse messages

    Read physical text records into raw message rows, for one window of
    `currunix`, the event the native read settles over each line.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    filesystem = _defaults["filesystem"]
    rowheader = _defaults["rowheader"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    return catalog, end, filesystem, rowheader, start


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(catalog, end, filesystem, records, rowheader, start):
    _ = records
    with ExitStack() as opened:
        # The window a run covers, `[start, end)`: the last day when the
        # document names neither bound, and exactly the scheduler's interval
        # when it names both. Read before anything is opened, so a bound that
        # is not an instant is refused before a capture is.
        window = window_of(start, end)
        source = IOBase.from_uri(filesystem)
        opened.callback(source.close)
        source_location = source.masked_uri or str(source.url)
        if not source.exists():
            raise FileNotFoundError(source_location)
        stage = Stage(
            "parse_messages",
            sources={"capture": source_location},
            targets={"messages": TARGET},
            window=window,
        )
        field = Message.into_field()
        # A bridge writing these same facts in a layout of its own is read by
        # naming its header here; the names it captures are still the ones
        # the read fills from -- a column each, and `currunix` for `mtime` --
        # and a header that renames one is refused rather than stored as a
        # column of nulls under a clock that settled nothing.
        options = Message.text_options(rowheader)
        counts = {"read": 0}
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        messages = store.dataset(
            stage.targets["messages"],
            field=field,
        )
        opened.callback(messages.close)
        reader = source.read_arrow_reader(options=options)
        opened.callback(reader.close)

        def _batches():
            # Every line is read and counted; the ones the window covers go
            # on. `currunix` is the event the read settled over the line and
            # the column the table is laid out by, and a line the header
            # could not date sits at the epoch pin, which is in every window,
            # so a header that did not match never loses a line.
            for batch in reader:
                counts["read"] += batch.num_rows
                yield batch.filter(within(batch.column("currunix"), window))

        read = pyarrow.RecordBatchReader.from_batches(
            Message.read_field().into_arrow_schema(),
            _batches(),
        )
        opened.callback(read.close)
        # The read states a line's content code unsigned and Iceberg's only
        # sixty-four-bit integer is signed, so the same eight bytes are viewed
        # rather than converted on the way to the table.
        parsed = stored_arrow_reader(read, field)
        opened.callback(parsed.close)
        # What the window carries replaces what the table held under the same
        # `currhashcode`, the key the field declares, within the hour of
        # `currunix` it falls in: a replay of the window lands the same rows
        # again, so the table holds each line once however often it runs.
        written = messages.overwrite_arrow_reader(parsed, field, merge_by=True)
        _outcome = stage.finished(read=counts["read"], written=written)
    outcome = _outcome
    return (outcome,)


@app.cell
def _(outcome):
    result = outcome
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
