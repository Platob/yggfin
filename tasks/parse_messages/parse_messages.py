import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow.compute
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

    Read physical text records into raw message rows, resuming each source
    above the line it was last read to.
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
        counts = {"read": 0, "settled": 0}
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        messages = store.dataset(
            stage.targets["messages"],
            field=field,
        )
        opened.callback(messages.close)
        # The physical line each source was last read to. A run then costs what
        # arrived rather than what the target already holds: without it every
        # run re-reads the whole capture and leans on the write to discard the
        # duplicates, which grows with history instead of with new data.
        marks = messages.watermarks("sourcerownum", by="sourceurl")

        def _leaves():
            # A capture is usually a tree of objects, and may be one object.
            # The selection is the one a folder read makes for itself: the
            # leaves whose media type is what these options read.
            entries = source.ls(recursive=True) if source.is_dir() else (source,)
            for entry in entries:
                if not entry.is_dir() and entry.media_type.base == options.mime_type:
                    yield entry

        def _batches():
            for leaf in _leaves():
                mark = marks.get(str(leaf.url), 0)
                # Counting physical lines builds no batches, so a source that
                # has not grown settles without being parsed. A source that
                # shrank has nothing an insert would take either.
                if mark and leaf.row_size <= mark:
                    counts["settled"] += 1
                    continue
                reader = leaf.read_arrow_reader(options=options)
                try:
                    for batch in reader:
                        if mark:
                            batch = batch.filter(
                                pyarrow.compute.greater(batch.column("rownum"), mark)
                            )
                            if not batch.num_rows:
                                continue
                        names = [SOURCE_NAMES.get(name, name) for name in batch.schema.names]
                        parsed = strict_cast_batch(field, batch.rename_columns(names))
                        counts["read"] += parsed.num_rows
                        yield parsed
                finally:
                    reader.close()

        written = messages.append_arrow_reader(_batches(), field, merge_by=True)
        _outcome = stage.finished(
            read=counts["read"],
            written=written,
            settled=counts["settled"],
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
