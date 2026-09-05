import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib

    import marimo as mo
    import pyarrow
    import pyarrow.compute as pc

    from rekep.enums import EventType, Protocol
    from rekep.fix.fields import FieldRules
    from rekep.fix.registry import FixRegistry
    from rekep.fix.rules import MARKET_CATEGORY, MISC_CATEGORY, UNKNOWN_CATEGORY, Rules
    from rekep.fix.transcribe import FixCodec
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.resources import resource
    from rekep.tasks import Task
    from rekep.text import FixMsg, Message
    from rekep.times import unix_of


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse FIX

    Parse raw messages and write one category resolved against the FIX dictionary.
    """)


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_yaml(str(pathlib.Path(__file__).with_suffix(".yml"))).parameters
    category = _defaults["category"]
    project_root = _defaults["project_root"]
    source = _defaults["source"]
    start = _defaults["start"]
    end = _defaults["end"]
    fix_dictionary = _defaults["fix_dictionary"]
    null_values = _defaults["null_values"]
    plugin_keys = _defaults["plugin_keys"]
    timezone = _defaults["timezone"]
    exclude_msgtypes = _defaults["exclude_msgtypes"]
    protocols = _defaults["protocols"]
    fields = _defaults["fields"]
    catalog = _defaults["catalog"]
    table_properties = _defaults["table_properties"]
    branch = _defaults["branch"]
    merge_by = _defaults["merge_by"]
    commit_batch_num = _defaults["commit_batch_num"]
    commit_row_size = _defaults["commit_row_size"]
    limit = _defaults["limit"]
    log_level = _defaults["log_level"]
    return (
        branch,
        catalog,
        category,
        commit_batch_num,
        commit_row_size,
        end,
        exclude_msgtypes,
        fields,
        fix_dictionary,
        limit,
        log_level,
        merge_by,
        null_values,
        plugin_keys,
        project_root,
        protocols,
        source,
        start,
        table_properties,
        timezone,
    )


@app.cell
def _(log_level):
    # Records go to stderr from here on. Every cell that can emit one reads
    # `records` back, and marimo builds a cell's edges from its body -- so the
    # level is in force before the first of them runs.
    records = configure(log_level)
    return (records,)


@app.cell
def _(category):
    # The category is the only per-run input, and the table and the task name
    # are read off it, so neither can drift from the rows the run selected.
    _categories = (MARKET_CATEGORY, MISC_CATEGORY, UNKNOWN_CATEGORY)
    if category not in _categories:
        raise ValueError(f"category must be one of {_categories}, got {category!r}")
    task_name = f"parse_fix_{category}"
    target = f"fix.{category}"
    return target, task_name


@app.cell
def _(end, start):
    lower, upper = unix_of(start), unix_of(end, upper=True)
    return lower, upper


@app.cell
def _(
    commit_batch_num,
    commit_row_size,
    exclude_msgtypes,
    fields,
    fix_dictionary,
    null_values,
    plugin_keys,
    project_root,
    protocols,
    records,
    timezone,
):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
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
    if isinstance(exclude_msgtypes, (str, bytes)) or any(
        not isinstance(value, str) for value in exclude_msgtypes
    ):
        raise TypeError("exclude_msgtypes must be a sequence of strings")
    excluded = frozenset(exclude_msgtypes)
    protocol_rules = Rules() if protocols is None else Rules.from_dict(protocols)
    registry = (
        FixRegistry()
        if fix_dictionary is None
        else FixRegistry(
            cache_dir=str(resource(str(fix_dictionary), root=project_root).url),
            announce=print,
        )
    )
    field_rules = FieldRules() if fields is None else FieldRules.from_dict(fields)
    codec = FixCodec(
        rules=protocol_rules,
        registry=registry,
        null_values=frozenset(null_values),
        plugin_keys=plugin_keys,
        timezone=timezone,
        fields=field_rules,
    )
    return codec, excluded, field_rules, protocol_rules, registry


@app.cell
def _():
    field = FixMsg.into_field()
    source_field = Message.into_field()
    return field, source_field


@app.cell
def _(
    branch,
    catalog,
    category,
    lower,
    records,
    source,
    source_field,
    target,
    task_name,
    upper,
):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    store = IcebergCatalog.from_dict(catalog)
    messages = store.dataset(
        source,
        field=source_field,
        branch=branch,
    )
    stage = Stage(
        task_name,
        sources={"messages": source},
        targets={category: target},
        window=(lower, upper),
    )
    source_columns = list(messages.table_field.names if messages.exists else source_field.names)
    return messages, source_columns, stage, store


@app.cell
def _(protocol_rules):
    market_eventtypes = pyarrow.array(
        EventType.ranked_at_least(EventType.INTENT), type=pyarrow.int64()
    )
    known_protocols = pyarrow.array(
        sorted(protocol.into_stored() for protocol in protocol_rules.protocols),
        type=Protocol.into_storage_type(),
    )
    return known_protocols, market_eventtypes


@app.cell
def _(
    branch,
    category,
    codec,
    commit_batch_num,
    commit_row_size,
    excluded,
    field,
    known_protocols,
    limit,
    lower,
    market_eventtypes,
    merge_by,
    messages,
    source_columns,
    stage,
    store,
    table_properties,
    upper,
):
    # The stage named the table this category writes, and reading it back is
    # what orders this cell after the record that opened the run.
    output = store.dataset(
        stage.targets[category],
        field=field,
        table_properties=dict(table_properties),
        branch=branch,
        commit_batch_num=commit_batch_num,
        commit_row_size=commit_row_size,
    )
    # One dict, because a cell is a scope and a generator cannot rebind a name
    # in the cell that defines it.
    counts = {"read": 0, "tickered": 0, "errors": 0}
    # Every category run owns resolving the transaction clock and nesting the
    # Instrument whose class maps the canonical ticker, so it also says how
    # well it managed: which rung answered for `unix` on each row, how many
    # carry an `instrument.symbolticker`, and how many were retained with a
    # row-local transcription error. A run that hands on a weak base says so
    # here instead of two tables later.
    unixsource = {}

    def _measured(batch):
        """One parsed batch, with its clock and ticker coverage counted."""
        for rung, count in zip(
            *(
                column.to_pylist()
                for column in pc.value_counts(batch.column("unixsource")).flatten()
            ),
            strict=True,
        ):
            unixsource[rung] = unixsource.get(rung, 0) + count
        symbolticker = pc.struct_field(batch.column("instrument"), "symbolticker")
        counts["tickered"] += pc.sum(pc.not_equal(symbolticker, ""), min_count=0).as_py() or 0
        counts["errors"] += pc.sum(pc.is_valid(batch.column("error")), min_count=0).as_py() or 0
        return batch

    def _selected(batch):
        """Rows in this run's recording window and exclusive category."""
        rows = batch.num_rows
        keep = pyarrow.repeat(pyarrow.scalar(True), rows)
        recorded = batch.column("recunix")
        if lower is not None:
            keep = pc.and_kleene(keep, pc.greater_equal(recorded, lower))
        if upper is not None:
            keep = pc.and_kleene(keep, pc.less(recorded, upper))

        msgtypes = batch.column("msgtype")
        if excluded:
            excluded_msgtypes = pyarrow.array(sorted(excluded), type=msgtypes.type)
            application = pc.or_kleene(
                pc.is_null(msgtypes),
                pc.invert(pc.is_in(msgtypes, value_set=excluded_msgtypes)),
            )
            keep = pc.and_kleene(keep, application)

        eventtypes = batch.column("eventtype")
        market = pc.is_in(eventtypes, value_set=market_eventtypes)
        families = Protocol.into_family_arrow(batch.column("protocol"))
        recognised = pc.is_in(families, value_set=known_protocols)
        known_non_market = pc.or_kleene(pc.equal(eventtypes, int(EventType.MISC)), recognised)
        if category == MARKET_CATEGORY:
            category_rows = market
        elif category == MISC_CATEGORY:
            category_rows = pc.and_kleene(pc.invert(market), known_non_market)
        else:
            category_rows = pc.and_kleene(pc.invert(market), pc.invert(known_non_market))
        return batch.filter(pc.fill_null(pc.and_kleene(keep, category_rows), False))

    def _batches():
        for staged in messages.read_arrow_reader(columns=source_columns):
            if limit is not None and counts["read"] >= limit:
                break
            batch = _selected(FixMsg.from_message_batch(staged, codec))
            if limit is not None and counts["read"] + batch.num_rows > limit:
                batch = batch.slice(0, limit - counts["read"])
            if not batch.num_rows:
                continue
            counts["read"] += batch.num_rows
            yield _measured(batch)

    written = output.append_arrow_reader(
        _batches(),
        field,
        merge_by=merge_by,
        commit_row_size=commit_row_size,
        commit_batch_num=commit_batch_num,
    )
    return counts, output, unixsource, written


@app.cell
def _(category, counts, stage, unixsource, written):
    stage.says("selected %d %s rows", counts["read"], category)
    # The base the next two stages key on, said where it was built rather than
    # discovered two tables later.
    stage.says(
        "resolved unix from %s; %d of %d rows carry a symbolticker",
        ", ".join(f"{rung or 'nothing'} {count}" for rung, count in sorted(unixsource.items()))
        or "nothing",
        counts["tickered"],
        counts["read"],
    )
    stage.says("retained %d rows with FIX transcription errors", counts["errors"])
    result = stage.finished(
        read=counts["read"],
        written=written,
        skipped=counts["read"] - written,
        category=category,
        unixsource=unixsource,
        tickered=counts["tickered"],
        errors=counts["errors"],
    )
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
