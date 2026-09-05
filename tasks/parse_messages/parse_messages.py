import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import datetime
    import itertools
    import pathlib
    import re
    from collections.abc import Iterable, Iterator
    from typing import Any

    import marimo as mo
    import pyarrow
    from yggdryl import IOBase, TextOptions

    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.resources import resource
    from rekep.tasks import Task
    from rekep.text import Message
    from rekep.times import MESSAGE_HEADER, datetime_of, unix_of

    source_names = {"url": "sourceurl", "rownum": "sourcerownum"}
    time_pattern = re.compile(r"(?:\{(?:year|month|day)\}|%7[bB](?:year|month|day)%7[dD])")

    def source_roots(
        location: str, start: Any | None, end: Any | None
    ) -> tuple[str, ...]:
        """Expand calendar tokens before binding the source filesystem."""
        if time_pattern.search(location) is None:
            return (location,)
        if start is None or end is None:
            raise ValueError("a {year}, {month}, or {day} source requires start and end")
        lower = datetime_of(start)
        upper = datetime_of(end, upper=True)
        if lower is None or upper is None:
            raise ValueError("a time-pattern source requires valid start and end instants")
        if upper < lower:
            raise ValueError("end must not precede start")
        if upper == lower:
            return ()
        last = upper - datetime.timedelta(microseconds=1)
        roots: list[str] = []
        day = lower.date()
        while day <= last.date():
            rendered = location
            for name, value in {
                "year": f"{day.year:04d}",
                "month": f"{day.month:02d}",
                "day": f"{day.day:02d}",
            }.items():
                rendered = re.sub(rf"(?:\{{{name}\}}|%7[bB]{name}%7[dD])", value, rendered)
            if rendered not in roots:
                roots.append(rendered)
            day += datetime.timedelta(days=1)
        return tuple(roots)

    def message_sources(
        roots: Iterable[IOBase], pattern: str, recursive: bool
    ) -> Iterator[IOBase]:
        """Yield selected yggdryl handles, preserving root and listing order."""
        for root in roots:
            if root.is_file():
                yield root
                continue
            if not root.is_dir():
                raise FileNotFoundError(str(root.url))
            listing = root.rglob(pattern) if recursive else root.glob(pattern)
            for source in sorted(listing, key=str):
                if source.is_file():
                    yield source

    def text_options(header: str | None, batch_row_size: int) -> TextOptions:
        """Configure raw physical-line records and their optional header captures."""
        options = TextOptions()
        options.with_rownum = 1
        options.rowheader = MESSAGE_HEADER if header is None else header
        options.autotype = False
        options.batch_row_size = batch_row_size
        return options

    def message_batches(
        sources: Iterable[IOBase], options: TextOptions, field: Any
    ) -> Iterator[pyarrow.RecordBatch]:
        """Read one text object at a time into the raw Message contract."""
        for source in sources:
            reader = source.into_text(options).read_arrow_reader()
            try:
                for batch in reader:
                    names = [source_names.get(name, name) for name in batch.schema.names]
                    yield field.cast_arrow_batch(batch.rename_columns(names))
            finally:
                reader.close()


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
    _defaults = Task.from_yaml(str(pathlib.Path(__file__).with_suffix(".yml"))).parameters
    project_root = _defaults["project_root"]
    source = _defaults["source"]
    filesystem = _defaults["filesystem"]
    pattern = _defaults["pattern"]
    header = _defaults["header"]
    recursive = _defaults["recursive"]
    start = _defaults["start"]
    end = _defaults["end"]
    catalog = _defaults["catalog"]
    table_properties = _defaults["table_properties"]
    branch = _defaults["branch"]
    target = _defaults["target"]
    merge_by = _defaults["merge_by"]
    batch_row_size = _defaults["batch_row_size"]
    commit_batch_num = _defaults["commit_batch_num"]
    commit_row_size = _defaults["commit_row_size"]
    limit = _defaults["limit"]
    log_level = _defaults["log_level"]
    return (
        batch_row_size,
        branch,
        catalog,
        commit_batch_num,
        commit_row_size,
        end,
        filesystem,
        header,
        limit,
        log_level,
        merge_by,
        pattern,
        project_root,
        recursive,
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
def _(end, start):
    lower, upper = unix_of(start), unix_of(end, upper=True)
    return lower, upper


@app.cell
def _(
    batch_row_size,
    end,
    header,
    pattern,
    project_root,
    recursive,
    records,
    source,
    start,
    filesystem,
):
    _ = records
    root_names = source_roots(str(source), start, end)
    _source_root = None if filesystem is not None else project_root
    roots = tuple(resource(name, filesystem, root=_source_root) for name in root_names)
    _sources = message_sources(roots, pattern, recursive)
    _first = next(_sources, None)
    if _first is None:
        raise FileNotFoundError(source)
    sources = itertools.chain((_first,), _sources)
    field = Message.into_field()
    options = text_options(header, batch_row_size)
    source_location = (
        str(roots[0].url) if not time_pattern.search(str(source)) else str(source)
    )
    return field, options, source_location, sources


@app.cell
def _(lower, source_location, target, upper):
    stage = Stage(
        "parse_messages",
        sources={"capture": source_location},
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
    commit_batch_num,
    commit_row_size,
    field,
    limit,
    merge_by,
    messages,
    options,
    sources,
):
    counts = {"read": 0}

    def _batches():
        batches = message_batches(sources, options, field)
        try:
            for batch in batches:
                if limit is not None and counts["read"] + batch.num_rows > limit:
                    batch = batch.slice(0, max(0, limit - counts["read"]))
                if batch.num_rows:
                    counts["read"] += batch.num_rows
                    yield batch
                if limit is not None and counts["read"] >= limit:
                    break
        finally:
            batches.close()

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
