import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    from yggdryl import IOBase, TextOptions

    from rekep.fields import strict_cast_batch
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import Message
    from rekep.times import MESSAGE_HEADER

    SOURCE_NAMES = {"url": "sourceurl", "rownum": "sourcerownum"}
    TARGET = "logs.messages"


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse messages

    Read physical text records into raw message rows.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    filesystem = _defaults["filesystem"]
    catalog = _defaults["catalog"]
    return catalog, filesystem


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(catalog, filesystem, records):
    _ = records
    with ExitStack() as opened:
        source = IOBase.from_uri(filesystem)
        opened.callback(source.close)
        source_location = source.masked_uri or str(source.url)
        if not source.exists():
            raise FileNotFoundError(source_location)
        stage = Stage(
            "parse_messages",
            sources={"capture": source_location},
            targets={"messages": TARGET},
        )
        field = Message.field()
        options = TextOptions()
        options.with_rownum = 1
        options.rowheader = MESSAGE_HEADER
        options.autotype = False
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
            for batch in reader:
                names = [SOURCE_NAMES.get(name, name) for name in batch.schema.names]
                parsed = strict_cast_batch(field, batch.rename_columns(names))
                counts["read"] += parsed.num_rows
                yield parsed

        written = messages.append_arrow_reader(_batches(), field, merge_by=True)
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
