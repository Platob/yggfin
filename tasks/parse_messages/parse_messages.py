import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib

    import marimo as mo

    from rekep import ArrowPath
    from rekep.fix import FixRegistry
    from rekep.fix.rules import Rules
    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import TextFiles
    from rekep.times import unix_of
    from rekep.urls import Url


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Parse messages

    Split text records into classified, protocol-neutral message rows.
    """)
    return


@app.cell
def parameters():
    # The adjacent document owns every default. A runner passes the whole
    # mapping to `app.run(defs=...)`, which replaces this cell.
    _defaults = Task.from_yaml(str(pathlib.Path(__file__).with_suffix(".yml"))).parameters
    project_root = _defaults["project_root"]
    source = _defaults["source"]
    fix_dictionary = _defaults["fix_dictionary"]
    protocols = _defaults["protocols"]
    pattern = _defaults["pattern"]
    header = _defaults["header"]
    recursive = _defaults["recursive"]
    spill = _defaults["spill"]
    timezone = _defaults["timezone"]
    include_regexes = _defaults["include_regexes"]
    exclude_regexes = _defaults["exclude_regexes"]
    include_msgtypes = _defaults["include_msgtypes"]
    exclude_msgtypes = _defaults["exclude_msgtypes"]
    technical_plugins = _defaults["technical_plugins"]
    plugin_keys = _defaults["plugin_keys"]
    null_values = _defaults["null_values"]
    start = _defaults["start"]
    end = _defaults["end"]
    duration_ns = _defaults["duration_ns"]
    catalog = _defaults["catalog"]
    table_properties = _defaults["table_properties"]
    branch = _defaults["branch"]
    target = _defaults["target"]
    merge_by = _defaults["merge_by"]
    batch_row_size = _defaults["batch_row_size"]
    batch_byte_size = _defaults["batch_byte_size"]
    max_row_byte_size = _defaults["max_row_byte_size"]
    commit_batch_num = _defaults["commit_batch_num"]
    commit_row_size = _defaults["commit_row_size"]
    limit = _defaults["limit"]
    log_level = _defaults["log_level"]
    return (
        batch_byte_size,
        batch_row_size,
        branch,
        catalog,
        commit_batch_num,
        commit_row_size,
        duration_ns,
        end,
        exclude_msgtypes,
        exclude_regexes,
        fix_dictionary,
        header,
        include_msgtypes,
        include_regexes,
        limit,
        log_level,
        max_row_byte_size,
        merge_by,
        null_values,
        pattern,
        plugin_keys,
        project_root,
        protocols,
        recursive,
        source,
        spill,
        start,
        table_properties,
        target,
        technical_plugins,
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
def _(end, start):
    lower, upper = unix_of(start), unix_of(end, upper=True)
    return lower, upper


@app.cell
def _(fix_dictionary, project_root, records):
    # Read, not merely named: this is the edge that puts the level in force
    # before this cell can emit a record.
    _ = records
    registry = (
        FixRegistry()
        if fix_dictionary is None
        else FixRegistry(cache_dir=Url.from_string(str(fix_dictionary)).resolve(project_root))
    )
    return (registry,)


@app.cell
def _(
    header,
    null_values,
    pattern,
    plugin_keys,
    project_root,
    protocols,
    recursive,
    registry,
    source,
    spill,
    timezone,
    end,
    start,
):
    declared = {
        "timezone": timezone,
        "protocol_rules": Rules.into_default() if protocols is None else Rules.from_dict(protocols),
        "msg_type_event_types": registry.msg_type_event_types(),
        "plugin_keys": plugin_keys,
        "null_values": null_values,
        "spill": spill,
        **({} if header is None else {"header_pattern": header}),
    }
    location = ArrowPath(str(source)).resolve(project_root)
    rows = TextFiles.from_folder(
        location,
        start=start,
        end=end,
        pattern=pattern,
        recursive=recursive,
        **declared,
    )
    if not rows.exists:
        raise FileNotFoundError(location)
    field = rows.into_struct_field()
    return field, location, rows


@app.cell
def _(location, lower, target, upper):
    stage = Stage(
        "parse_messages",
        # Masked, not spelled: a capture prefix may carry a key pair, and this
        # string reaches the task log, the result document and XCom.
        sources={"capture": location.url.masked},
        targets={"messages": target},
        window=(lower, upper),
    )
    return (stage,)


@app.cell
def _(
    branch,
    catalog,
    commit_batch_num,
    commit_row_size,
    field,
    stage,
    table_properties,
):
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
    store = IcebergCatalog.from_dict(catalog)
    # The stage named the table it writes, and reading it back is what orders
    # this cell after the record that opened the run.
    messages = store.dataset(
        stage.targets["messages"],
        field=field,
        table_properties=dict(table_properties),
        branch=branch,
        commit_batch_num=commit_batch_num,
        commit_row_size=commit_row_size,
    )
    return (messages,)


@app.cell
def _(
    batch_byte_size,
    batch_row_size,
    commit_batch_num,
    commit_row_size,
    duration_ns,
    exclude_msgtypes,
    exclude_regexes,
    field,
    include_msgtypes,
    include_regexes,
    limit,
    lower,
    max_row_byte_size,
    merge_by,
    messages,
    rows,
    technical_plugins,
    upper,
):
    counts = {"read": 0}

    def _batches():
        reader = rows.read_arrow_reader(
            batch_row_size=batch_row_size,
            batch_byte_size=batch_byte_size,
            max_row_byte_size=max_row_byte_size,
            include_regexes=include_regexes,
            exclude_regexes=exclude_regexes,
            include_msgtypes=include_msgtypes,
            exclude_msgtypes=exclude_msgtypes,
            technical_plugins=technical_plugins,
            start_unix=lower,
            end_unix=upper,
            duration_ns=duration_ns,
        )
        try:
            for batch in reader:
                if limit is not None and counts["read"] + batch.num_rows > limit:
                    batch = batch.slice(0, max(0, limit - counts["read"]))
                if batch.num_rows:
                    counts["read"] += batch.num_rows
                    yield batch
                if limit is not None and counts["read"] >= limit:
                    break
        finally:
            reader.close()

    written = messages.append_arrow_reader(
        _batches(),
        field,
        merge_by=merge_by,
        commit_row_size=commit_row_size,
        commit_batch_num=commit_batch_num,
    )
    return counts, written


@app.cell
def _(counts, stage, written):
    result = stage.finished(read=counts["read"], written=written)
    mo.tree(result)
    return (result,)


if __name__ == "__main__":
    app.run()
