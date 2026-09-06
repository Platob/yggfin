import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow
    from yggdryl import Field
    from yggdryl.fix import FixRegistry, global_registry, parse_arrow_reader

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

    Parse raw message bodies into the fixed Yggdryl FIX Arrow schema.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    registry = _defaults["registry"]
    catalog = _defaults["catalog"]
    return catalog, registry


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(catalog, records, registry):
    _ = records
    with ExitStack() as opened:
        stage = Stage(
            "parse_fix",
            sources={"messages": SOURCE},
            targets={"fix": TARGET},
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        messages = store.dataset(SOURCE, field=Message.field())
        opened.callback(messages.close)
        source = messages.read_arrow_reader(Message.field())
        opened.callback(source.close)
        counts = {"read": 0}

        def _batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(source.schema, _batches())
        opened.callback(counted.close)
        dictionary = global_registry() if registry is None else FixRegistry.from_handle(registry)
        if not dictionary:
            raise ValueError(
                "parse_fix requires a non-empty Yggdryl FIX registry; "
                "set registry or YGGDRYL_FIX_REGISTRY"
            )
        parsed = parse_arrow_reader(counted, registry=dictionary, column="body")
        opened.callback(parsed.close)
        field = Field.from_arrow_schema(parsed.schema, name="FixMessage")
        applied = field.apply_arrow_reader(
            parsed,
            safe=False,
            nullability="strict",
        )
        opened.callback(applied.close)
        fixes = store.dataset(TARGET, field=field)
        opened.callback(fixes.close)
        written = fixes.append_arrow_reader(applied, field, merge_by=True)
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
