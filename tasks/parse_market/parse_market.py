import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib

    import marimo as mo
    import pyarrow.compute as pc
    from pyiceberg.expressions import (
        And,
        GreaterThanOrEqual,
        IsNull,
        LessThan,
        LessThanOrEqual,
        NotEqualTo,
        NotNull,
    )

    from rekep.enums import EventType
    from rekep.fix.registry import FixRegistry
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.market import Book, BookIterator, Execution, Order
    from rekep.resources import resource
    from rekep.tasks import Task
    from rekep.text import FixMsg
    from rekep.times import unix_of


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse market

    Fold sorted parsed FIX messages into books, or write their orders and executions directly.
    """)
    return


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_yaml(str(pathlib.Path(__file__).with_suffix(".yml"))).parameters
    project_root = _defaults["project_root"]
    fix_dictionary = _defaults["fix_dictionary"]
    source = _defaults["source"]
    books = _defaults["books"]
    target = _defaults["target"]
    order_target = _defaults["order_target"]
    execution_target = _defaults["execution_target"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    table_properties = _defaults["table_properties"]
    branch = _defaults["branch"]
    snapshot_every = _defaults["snapshot_every"]
    max_lateness_ns = _defaults["max_lateness_ns"]
    max_order_age_ns = _defaults["max_order_age_ns"]
    max_side_alive = _defaults["max_side_alive"]
    merge_by = _defaults["merge_by"]
    batch_row_size = _defaults["batch_row_size"]
    commit_batch_num = _defaults["commit_batch_num"]
    commit_row_size = _defaults["commit_row_size"]
    log_level = _defaults["log_level"]
    return (
        batch_row_size,
        books,
        branch,
        catalog,
        commit_batch_num,
        commit_row_size,
        end,
        execution_target,
        fix_dictionary,
        log_level,
        max_lateness_ns,
        max_order_age_ns,
        max_side_alive,
        merge_by,
        order_target,
        project_root,
        snapshot_every,
        source,
        start,
        table_properties,
        target,
    )


@app.cell
def _(log_level):
    # Records go to stderr from here on. Every cell that can emit one reads
    # `records` back, and marimo builds a cell's edges from its body -- so the
    # level is in force before the first of them runs.
    records = configure(log_level)
    return (records,)


@app.cell
def _(batch_row_size, commit_batch_num, commit_row_size, records):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    # A size the storage layer cannot use fails here, ahead of the stage and
    # the catalog: every later cell reads a constant this one defines, so the
    # run stops before it opens a record or creates a table.
    if isinstance(batch_row_size, bool) or not isinstance(batch_row_size, int):
        raise TypeError("batch_row_size must be an integer")
    if batch_row_size <= 0:
        raise ValueError("batch_row_size must be positive")
    if isinstance(commit_batch_num, bool) or not isinstance(commit_batch_num, int):
        raise TypeError("commit_batch_num must be an integer")
    if commit_batch_num <= 0:
        raise ValueError("commit_batch_num must be positive")
    if commit_row_size is not None and (
        isinstance(commit_row_size, bool) or not isinstance(commit_row_size, int)
    ):
        raise TypeError("commit_row_size must be an integer or null")
    if commit_row_size is not None and commit_row_size <= 0:
        raise ValueError("commit_row_size must be positive")
    HOUR = 3_600_000_000_000
    DAY = 86_400_000_000_000
    return DAY, HOUR


@app.cell
def _(HOUR, end, max_lateness_ns, start):
    lower, upper = unix_of(start), unix_of(end, upper=True)
    read_lower = None if lower is None else lower - lower % HOUR - HOUR
    read_upper = None if upper is None else ((upper + HOUR - 1) // HOUR) * HOUR + max_lateness_ns
    return lower, read_lower, read_upper, upper


@app.cell
def _(books, execution_target, lower, order_target, source, target, upper):
    _configured_targets = (
        (target,)
        if books
        else tuple(name for name in (order_target, execution_target) if name is not None)
    )
    if not _configured_targets or _configured_targets[0] is None:
        raise ValueError("book mode needs target; direct mode needs an event target")
    if len(_configured_targets) != len(set(_configured_targets)):
        raise ValueError("market targets must be distinct")
    # The targets a run actually writes, keyed by what each holds. A role this
    # mode does not write is left out rather than stored as null.
    stage = Stage(
        "parse_market",
        sources={"market": source},
        targets=(
            {"books": target}
            if books
            else {
                role: name
                for role, name in (("orders", order_target), ("executions", execution_target))
                if name is not None
            }
        ),
        window=(lower, upper),
    )
    return (stage,)


@app.cell
def _(fix_dictionary, project_root, records):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    registry = (
        FixRegistry()
        if fix_dictionary is None
        else FixRegistry(cache_dir=str(resource(str(fix_dictionary), root=project_root).url))
    )
    return (registry,)


@app.cell
def _(branch, catalog, stage):
    store = IcebergCatalog.from_dict(catalog)
    # The stage already named every table this run touches, and reading the
    # names back off it is what orders the catalog after the record that
    # opened the run.
    logs_table = store.dataset(
        stage.sources["market"],
        field=FixMsg.into_field(),
        branch=branch,
    )
    return logs_table, store


@app.cell
def _(
    books,
    branch,
    commit_batch_num,
    commit_row_size,
    execution_target,
    order_target,
    stage,
    store,
    table_properties,
):
    book_table = (
        None
        if not books
        else store.dataset(
            stage.targets["books"],
            field=Book.into_field(),
            table_properties=dict(table_properties),
            branch=branch,
            commit_batch_num=commit_batch_num,
            commit_row_size=commit_row_size,
        )
    )

    def _event_table(name, event_type):
        if name is None:
            return None
        return store.dataset(
            name,
            field=event_type.into_field(),
            table_properties=dict(table_properties),
            branch=branch,
            commit_batch_num=commit_batch_num,
            commit_row_size=commit_row_size,
        )

    order_table = None if books else _event_table(order_target, Order)
    execution_table = None if books else _event_table(execution_target, Execution)
    return book_table, execution_table, order_table


@app.cell
def _(
    DAY,
    batch_row_size,
    book_table,
    commit_batch_num,
    commit_row_size,
    execution_table,
    logs_table,
    lower,
    max_order_age_ns,
    max_side_alive,
    merge_by,
    order_table,
    read_lower,
    read_upper,
    registry,
    snapshot_every,
    upper,
):
    # The two modes are one cell because they are one write: exactly one of
    # them runs, and both settle the same four counters.
    def _window(lower, upper, column="unix"):
        predicates = []
        if lower is not None:
            predicates.append(GreaterThanOrEqual(column, lower))
        if upper is not None:
            predicates.append(LessThan(column, upper))
        return (
            None if not predicates else predicates[0] if len(predicates) == 1 else And(*predicates)
        )

    def _book_seeds():
        if book_table is None or read_lower is None:
            return ()
        recent = And(
            GreaterThanOrEqual("unix", read_lower - DAY),
            LessThanOrEqual("unix", read_lower),
            NotNull("snapunix"),
        )
        reader = book_table.read_arrow_reader(
            Book.into_field(), row_filter=recent, order_by=("unix", "hash")
        )
        return Book.from_arrow_reader(reader)

    def _log_reader():
        row_filter = _window(read_lower, read_upper)
        if book_table is None:
            market_events = NotEqualTo("eventtype", int(EventType.INSTRUMENT))
            row_filter = market_events if row_filter is None else And(row_filter, market_events)
        # parse_fix_market retains failed rows in its source category for
        # audit. They cannot mutate a book or emit a partial order from an
        # incomplete reading.
        clean = IsNull("error")
        row_filter = clean if row_filter is None else And(row_filter, clean)
        reader = logs_table.read_arrow_reader(
            FixMsg.into_field(),
            row_filter=row_filter,
            order_by=("unix", "msgseqnum", "hash"),
        )
        return reader

    def _inside(event):
        return (lower is None or event.unix >= lower) and (upper is None or event.unix < upper)

    def _write_books():
        snapshots = _book_seeds()
        read = {"books": 0, "orders": 0, "executions": 0}
        events = FixMsg.into_ordered_market_events(_log_reader(), registry=registry)
        iterating = BookIterator.from_events(
            events,
            snapshots=snapshots,
            registry=registry,
            snapshot_every=snapshot_every,
            snapshot_until=upper,
            max_order_age_ns=max_order_age_ns,
            max_side_alive=max_side_alive,
        )

        def selected():
            nonlocal read
            for book in iterating:
                if _inside(book):
                    read["books"] += 1
                    read["orders"] += len(book.deltas)
                    read["executions"] += len(book.executions)
                    yield book

        written = book_table.append_arrow_reader(
            Book.into_arrow_reader(selected(), batch_row_size=batch_row_size),
            Book.into_field(),
            merge_by=merge_by,
            commit_row_size=commit_row_size,
            commit_batch_num=commit_batch_num,
        )
        checkpoint = min((book.unix for book in iterating.snapshots), default=None)
        return read, written, checkpoint

    def _write_events():
        tables = {Order: order_table, Execution: execution_table}
        read = {Order: 0, Execution: 0}
        written = {Order: 0, Execution: 0}
        buffers = {event_type: [] for event_type in tables}
        held_rows = {event_type: 0 for event_type in tables}

        def flush(event_type):
            batches = buffers[event_type]
            table = tables[event_type]
            if not batches or table is None:
                return
            written[event_type] += table.append_arrow_reader(
                iter(batches),
                event_type.into_field(),
                merge_by=merge_by,
                commit_row_size=commit_row_size,
                commit_batch_num=commit_batch_num,
            )
            buffers[event_type] = []
            held_rows[event_type] = 0

        for event_type, batch in FixMsg.into_market_arrow_batches(
            _log_reader(), batch_row_size=batch_row_size, registry=registry
        ):
            mask = None
            if lower is not None:
                mask = pc.greater_equal(batch.column("unix"), lower)
            if upper is not None:
                before = pc.less(batch.column("unix"), upper)
                mask = before if mask is None else pc.and_(mask, before)
            if mask is not None:
                batch = batch.filter(mask)
            read[event_type] += batch.num_rows
            table = tables[event_type]
            if table is None or not batch.num_rows:
                continue
            buffers[event_type].append(batch)
            held_rows[event_type] += batch.num_rows
            row_cap = commit_row_size is not None and held_rows[event_type] >= commit_row_size
            if len(buffers[event_type]) >= commit_batch_num or row_cap:
                flush(event_type)
        for event_type in tables:
            flush(event_type)
        return read, written

    read = {"books": 0, "orders": 0, "executions": 0}
    written = dict(read)
    flatten = {"orders": 0, "executions": 0}
    checkpoint = None
    if book_table is not None:
        _book_read, written["books"], checkpoint = _write_books()
        read.update(_book_read)
        flatten.update({name: _book_read[name] for name in flatten})
    else:
        _event_read, _event_written = _write_events()
        read["orders"], read["executions"] = _event_read[Order], _event_read[Execution]
        written["orders"], written["executions"] = (
            _event_written[Order],
            _event_written[Execution],
        )
    mode = "books" if book_table is not None else "events"
    return checkpoint, flatten, mode, read, written


@app.cell
def _(books, checkpoint, flatten, mode, read, read_lower, read_upper, stage, written):
    stage.says(
        "folded in %s mode: %s",
        mode,
        ", ".join(f"{count} {product}" for product, count in read.items() if count),
    )
    # One number each, like every other stage: what this stage produced. In book
    # mode that is books, and the orders and executions nested inside them are
    # what the two flatteners read; in direct mode it is the events themselves.
    # `products` keeps the breakdown either way.
    result = stage.finished(
        read=read["books"] if books else read["orders"] + read["executions"],
        written=written["books"] if books else written["orders"] + written["executions"],
        mode=mode,
        products={"read": read, "written": written},
        flatten=flatten,
        checkpoint=checkpoint,
        scan={"start": read_lower, "end": read_upper},
    )
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
