import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow

    from rekep.fix import dated_arrow_reader, fix_codec, fix_registry, iceberg_fix_field
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import Message

    SOURCE = "logs.messages"
    TARGET = "fix.messages"


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse FIX

    Parse raw message bodies into the fixed rekep FIX Arrow schema.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    registry = _defaults["registry"]
    version = _defaults["version"]
    catalog = _defaults["catalog"]
    return catalog, registry, version


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(catalog, records, registry, version):
    _ = records
    with ExitStack() as opened:
        stage = Stage(
            "parse_fix",
            sources={"messages": SOURCE},
            targets={"fix": TARGET},
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        messages = store.dataset(SOURCE, field=Message.into_field())
        opened.callback(messages.close)
        source = messages.read_arrow_reader(Message.into_field())
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
        # The codec is the whole parse surface: the dictionary and what holds
        # for the run are pinned on it once, and the read is one call.
        codec = fix_codec(fix_registry(registry), version=version)
        # A message stating no clock of its own is dated by the line's capture
        # instant, so the identity the codec computes is the same on a replay.
        dated = dated_arrow_reader(counted)
        opened.callback(dated.close)
        parsed = codec.parse_text_arrow_reader(dated)
        opened.callback(parsed.close)
        field = iceberg_fix_field(parsed.schema)

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
        written = fixes.append_arrow_reader(applied, field, merge_by=True)
        # A row is a message, not a line: one line carrying two frames answers
        # two and one carrying none answers nothing, so what the read skipped
        # is counted against the messages the codec answered rather than
        # against the lines it was handed.
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
