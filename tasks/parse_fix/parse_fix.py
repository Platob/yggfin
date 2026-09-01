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
        In,
        IsNull,
        LessThan,
        Not,
        NotIn,
        Or,
    )

    from rekep.enums import EventType
    from rekep.fix.fields import FieldRules
    from rekep.fix.registry import FixRegistry
    from rekep.fix.rules import Rules
    from rekep.fix.transcribe import FixCodec
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import FixMsg, Message
    from rekep.times import unix_of
    from rekep.urls import Url


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse FIX

    Resolve classified market message rows against the FIX dictionary.
    """)
    return


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_yaml(str(pathlib.Path(__file__).with_suffix(".yml"))).parameters
    project_root = _defaults["project_root"]
    source = _defaults["source"]
    start = _defaults["start"]
    end = _defaults["end"]
    fix_dictionary = _defaults["fix_dictionary"]
    null_values = _defaults["null_values"]
    exclude_msgtypes = _defaults["exclude_msgtypes"]
    ul_default_version = _defaults["ul_default_version"]
    protocols = _defaults["protocols"]
    fields = _defaults["fields"]
    catalog = _defaults["catalog"]
    table_properties = _defaults["table_properties"]
    branch = _defaults["branch"]
    target_pattern = _defaults["target_pattern"]
    merge_by = _defaults["merge_by"]
    commit_batch_num = _defaults["commit_batch_num"]
    commit_row_size = _defaults["commit_row_size"]
    limit = _defaults["limit"]
    log_level = _defaults["log_level"]
    return (
        branch,
        catalog,
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
        project_root,
        protocols,
        source,
        start,
        table_properties,
        target_pattern,
        ul_default_version,
    )


@app.cell
def _(log_level):
    # Records go to stderr from here on. Every cell that can emit one reads
    # `records` back, and marimo builds a cell's edges from its body -- so the
    # level is in force before the first of them runs.
    records = configure(log_level)
    return (records,)


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
    project_root,
    protocols,
    records,
    ul_default_version,
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
            cache_dir=Url.from_string(str(fix_dictionary)).resolve(project_root),
            announce=print,
        )
    )
    field_rules = FieldRules() if fields is None else FieldRules.from_dict(fields)
    codec = FixCodec(
        rules=protocol_rules,
        registry=registry,
        null_values=frozenset(null_values),
        fields=field_rules,
        ul_default_version=ul_default_version,
    )
    if (
        codec.ul_default_version is not None
        and codec.version_named(codec.ul_default_version) is None
    ):
        raise ValueError(f"unknown UL default FIX version {codec.ul_default_version!r}")
    return codec, excluded, field_rules, protocol_rules, registry


@app.cell
def _():
    field = FixMsg.into_field()
    source_field = Message.into_field()
    return field, source_field


@app.cell
def _(branch, catalog, lower, records, source, source_field, upper):
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
        "parse_fix",
        sources={"messages": source},
        window=(lower, upper),
    )
    source_columns = list(messages.table_field.names if messages.exists else source_field.names)
    if messages.exists:
        # Cell-local: a source that does not exist yet never binds it.
        _missing = sorted({"msgtype", "entries", "protocol"} - set(messages.table_field.names))
        if _missing:
            raise ValueError(
                f"{source} is missing {_missing}; rebuild it with parse_messages before parsing FIX"
            )
    return messages, source_columns, stage, store


@app.cell
def _(excluded, lower, upper):
    def _window(lower, upper, column="unix"):
        predicates = []
        if lower is not None:
            predicates.append(GreaterThanOrEqual(column, lower))
        if upper is not None:
            predicates.append(LessThan(column, upper))
        return (
            None if not predicates else predicates[0] if len(predicates) == 1 else And(*predicates)
        )

    # The window is read off the *stored* recording clock, because that is what
    # the message stage partitioned on. `unix` moves when a transaction time
    # resolves, so filtering on it here would drop rows the interval owns.
    selection = _window(lower, upper)
    if excluded:
        # Null discriminators still need best-effort transcription. Only named
        # session liveness traffic is left in logs.messages by this stage.
        _application_messages = Or(IsNull("msgtype"), NotIn("msgtype", excluded))
        selection = (
            _application_messages if selection is None else And(selection, _application_messages)
        )
    # The two scans partition every stored `eventtype`: the market scan keeps the
    # compiled codes ranked at or above INTENT, and the terminal scan keeps the
    # complement. A code no member spells still reaches the category router instead
    # of silently matching no scan.
    market_events = In("eventtype", EventType.ranked_at_least(EventType.INTENT))
    terminal_events = Not(market_events)
    market_filter = market_events if selection is None else And(selection, market_events)
    terminal_filter = terminal_events if selection is None else And(selection, terminal_events)
    return market_events, market_filter, selection, terminal_events, terminal_filter


@app.cell
def _(
    branch,
    codec,
    commit_batch_num,
    commit_row_size,
    field,
    limit,
    market_filter,
    merge_by,
    messages,
    protocol_rules,
    source_columns,
    stage,
    store,
    table_properties,
    target_pattern,
    terminal_filter,
):
    # One dict, because a cell is a scope and a generator cannot rebind a name
    # in the cell that defines it.
    counts = {"read": 0, "market_read": 0, "tickered": 0, "errors": 0}
    # What the stages after this one key on. This one owns resolving the
    # transaction clock and nesting the Instrument whose class maps the canonical
    # ticker, so it also says how well it managed: which rung answered for `unix`
    # on each row, how many carry an `instrument.symbolticker`, and how many were
    # retained with a row-local transcription error. A run
    # that hands on a weak base says so here instead of two tables later.
    unixsource = {}

    def _measured(batch):
        """One parsed batch, with its clock and ticker coverage counted."""
        for source, count in zip(
            *(
                column.to_pylist()
                for column in pc.value_counts(batch.column("unixsource")).flatten()
            ),
            strict=True,
        ):
            unixsource[source] = unixsource.get(source, 0) + count
        symbolticker = pc.struct_field(batch.column("instrument"), "symbolticker")
        counts["tickered"] += pc.sum(pc.not_equal(symbolticker, ""), min_count=0).as_py() or 0
        counts["errors"] += pc.sum(pc.is_valid(batch.column("error")), min_count=0).as_py() or 0
        return batch

    targets = {}

    def _target(category):
        target = targets.get(category)
        if target is None:
            target = targets[category] = store.dataset(
                target_pattern.format(category=category),
                field=field,
                table_properties=dict(table_properties),
                branch=branch,
                commit_batch_num=commit_batch_num,
                commit_row_size=commit_row_size,
            )
            stage.targets[category] = target.identifier
        return target

    target = _target("market")

    def _market_batches():
        for staged in messages.read_arrow_reader(columns=source_columns, row_filter=market_filter):
            if limit is not None and counts["read"] + staged.num_rows > limit:
                staged = staged.slice(0, max(0, limit - counts["read"]))
            if not staged.num_rows:
                continue
            counts["read"] += staged.num_rows
            counts["market_read"] += staged.num_rows
            batch = _measured(FixMsg.from_message_batch(staged, codec))
            yield batch
            if limit is not None and counts["read"] >= limit:
                break

    counts["written"] = target.append_arrow_reader(
        _market_batches(),
        field,
        merge_by=merge_by,
        commit_row_size=commit_row_size,
        commit_batch_num=commit_batch_num,
    )
    counts["skipped"] = counts["market_read"] - counts["written"]
    routed = {"market": counts["market_read"]}
    buffers = {}
    held_rows = {}

    def _flush(category):
        batches = buffers.pop(category, [])
        count = held_rows.pop(category, 0)
        if not count:
            return
        landed = _target(category).append_arrow_reader(
            iter(batches),
            field,
            merge_by=merge_by,
            commit_row_size=commit_row_size,
            commit_batch_num=commit_batch_num,
        )
        counts["written"] += landed
        counts["skipped"] += count - landed

    # Loop names stay cell-local: a run with no terminal batch never binds them,
    # and a cell hands back every name it defines.
    for _staged in messages.read_arrow_reader(columns=source_columns, row_filter=terminal_filter):
        if limit is not None and counts["read"] >= limit:
            break
        if limit is not None and counts["read"] + _staged.num_rows > limit:
            _staged = _staged.slice(0, max(0, limit - counts["read"]))
        if not _staged.num_rows:
            continue
        counts["read"] += _staged.num_rows
        _batch = _measured(FixMsg.from_message_batch(_staged, codec))
        _categories = protocol_rules.into_arrow_category_array(
            _batch.column("protocol"), _batch.column("eventtype")
        )
        for _category in sorted(pc.unique(_categories).to_pylist()):
            _part = _batch.filter(pc.equal(_categories, _category))
            routed[_category] = routed.get(_category, 0) + _part.num_rows
            buffers.setdefault(_category, []).append(_part)
            held_rows[_category] = held_rows.get(_category, 0) + _part.num_rows
            _row_cap = commit_row_size is not None and held_rows[_category] >= commit_row_size
            if len(buffers[_category]) >= commit_batch_num or _row_cap:
                _flush(_category)
        if limit is not None and counts["read"] >= limit:
            break
    for _category in list(buffers):
        _flush(_category)
    return buffers, counts, held_rows, routed, target, targets, unixsource


@app.cell
def _(counts, routed, stage, unixsource):
    stage.says(
        "routed %s", ", ".join(f"{count} {category}" for category, count in sorted(routed.items()))
    )
    # The base the next two stages key on, said where it was built rather than
    # discovered two tables later.
    stage.says(
        "resolved unix from %s; %d of %d rows carry a symbolticker",
        ", ".join(f"{rung} {count}" for rung, count in sorted(unixsource.items())) or "nothing",
        counts["tickered"],
        counts["read"],
    )
    stage.says("retained %d rows with FIX transcription errors", counts["errors"])
    result = stage.finished(
        read=counts["read"],
        written=counts["written"],
        skipped=counts["skipped"],
        routed=routed,
        unixsource=unixsource,
        tickered=counts["tickered"],
        errors=counts["errors"],
    )
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
