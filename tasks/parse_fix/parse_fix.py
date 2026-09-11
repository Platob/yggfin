import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow

    from rekep.fix import PAYLOAD_COLUMN, FixCodec, fix_registry, iceberg_fix_field
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
    branch = _defaults["branch"]
    version = _defaults["version"]
    catalog = _defaults["catalog"]
    return branch, catalog, registry, version


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(branch, catalog, records, registry, version):
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
        counts = {"read": 0, "parsed": 0}

        def _batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(
            source.schema,
            _batches(),
        )
        opened.callback(counted.close)
        codec = FixCodec(
            fix_registry(registry),
            branch=branch,
            version=version,
            payload_column=PAYLOAD_COLUMN,
        )
        parsed = codec.parse_text_arrow_reader(counted)
        opened.callback(parsed.close)
        field = iceberg_fix_field(parsed.schema)
        applied = field.apply_arrow_reader(
            parsed,
            safe=False,
            nullability="strict",
        )
        opened.callback(applied.close)

        def _messages():
            for batch in applied:
                counts["parsed"] += batch.num_rows
                yield batch

        # A bridge configuration line states several messages, so the rows the
        # codec answers are counted separately from the lines that were read:
        # `skipped` is what the merge already held, never a line-to-row gap.
        measured = pyarrow.RecordBatchReader.from_batches(
            applied.schema,
            _messages(),
        )
        opened.callback(measured.close)
        fixes = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(fixes.close)
        written = fixes.append_arrow_reader(measured, field, merge_by=True)
        _outcome = stage.finished(
            read=counts["read"],
            written=written,
            skipped=counts["parsed"] - written,
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
