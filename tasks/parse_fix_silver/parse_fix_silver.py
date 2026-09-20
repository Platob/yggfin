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
        fix_arrival_reader,
        fix_codec,
        fix_lifecycle_arrow_reader,
        fix_message_field,
        fix_registry,
        fix_window_filter,
    )
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import Message
    from rekep.times import window_of

    TARGET = "fix.silver"


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse FIX: silver

    Read one window of parsed FIX rows back as the messages that wrote them,
    walk them as the chains they belong to, and land the walked rows.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    bronze = _defaults["bronze"]
    registry = _defaults["registry"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    return bronze, catalog, end, registry, start


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(bronze, catalog, end, records, registry, start):
    _ = records
    with ExitStack() as opened:
        # The window, `[start, end)`, read off the event clock and not the
        # capture's: a bronze row is already an event, dated by what its
        # message stated. A message that stated no sending clock sits at the
        # codec's pin until this walk dates it by its transaction time, so the
        # predicate reads those rows by that clock instead -- one partition,
        # pruned by the transaction times its files hold.
        window = window_of(start, end)
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
        codec = fix_codec(fix_registry(registry))
        carrier = Message.into_field()
        field = fix_message_field(codec, carrier)
        parsed = store.dataset(bronze, field=field)
        opened.callback(parsed.close)
        source = parsed.read_arrow_reader(field, row_filter=fix_window_filter(window))
        opened.callback(source.close)
        counts = {"read": 0}

        def _batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(
            source.schema,
            _batches(),
        )
        opened.callback(counted.close)
        # A chain is read off a stream, and a table's layout is not one: the
        # window is put back in the order the capture logged its lines --
        # the object each row names, then the line's place in it -- which is
        # the order the parse handed the walk. Then the walk, over the
        # dictionary's columns alone: the capture's own columns are held back
        # and put in front again by the line each walked row names, because a
        # capture column read as content would give every hop that logged a
        # message its own identity. A stored row is widened back to the
        # dictionary's own types on the way in.
        walked = fix_lifecycle_arrow_reader(codec, fix_arrival_reader(counted))
        opened.callback(walked.close)
        # The storage boundary, the same one bronze crossed: the content codes
        # read as the signed integers Iceberg stores, then the field applied
        # in its native order.
        applied = stored_arrow_reader(walked, field)
        opened.callback(applied.close)
        silver = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(silver.close)
        # What the walk answers replaces what the table held under the same
        # `curruuid`: a replay of a window lands the same walked rows again,
        # and every copy of one message logged at several hops -- which the
        # walk gives the same place, the same lineage and the same state --
        # lands once. A message the walk dated re-settles its identity, so a
        # silver key is the bronze key only where the parse could date the
        # message; where it could not, the row moves from the pin's hour to
        # the hour its transaction happened in.
        written = silver.overwrite_arrow_reader(applied, field, merge_by=True)
        # The walk answers one row per row, so what the write left out is a
        # copy of an identity already landed.
        _outcome = stage.finished(read=counts["read"], written=written)
    outcome = _outcome
    return (outcome,)


@app.cell
def _(outcome):
    result = outcome
    mo.tree(result)
    return (result,)


@app.cell
def datasets(bronze, catalog, result):
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
                "rows": _dataset.read_arrow_table().num_rows,
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
            # `currunix, seqnum` is the order the walk gave a chain, and the
            # widest chain is the one worth showing.
            _rows = _silver.read_arrow_table().select(
                ("crosscode", "currunix", "seqnum", "curruuid", "prevuuid", "state")
            )
            _codes = _rows.column("crosscode").to_pylist()
            _widest = max(set(_codes), key=_codes.count)
            _chain = (
                _rows.filter(pyarrow.compute.equal(_rows.column("crosscode"), _widest))
                .sort_by([("currunix", "ascending"), ("seqnum", "ascending")])
                .to_pylist()
            )
    layout = _layout
    chain = _chain
    mo.vstack([mo.tree(layout), mo.ui.table(chain or [], selection=None)])
    return chain, layout


if __name__ == "__main__":
    app.run()
