import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    from contextlib import ExitStack

    import marimo as mo
    import pyarrow
    from pyiceberg.expressions import NotEqualTo
    from yggdryl import Field, IOBase
    from yggdryl.fix import FixRegistry, global_registry, parse_arrow_reader

    from rekep.iceberg import IcebergCatalog
    from rekep.logs import Stage, configure
    from rekep.tasks import Task
    from rekep.text import UNKNOWN, Message

    SOURCE = "logs.messages"
    TARGET = "fix.messages"

    def open_registry(location):
        """One dictionary, with this crate's own derived fields registered.

        The location is bound through `IOBase`, so `registry` accepts exactly
        the URI spellings `filesystem` does - a relative `file:` path, an
        absolute one, or `s3://bucket/prefix?region=...`. `with_crate_fields`
        registers the seven facts Yggdryl derives beside the specification's
        own: the digest, the version read, the ticker, the market clock, the
        partition it falls in, and the two parent order identifiers.
        """
        dictionary = (
            global_registry()
            if location is None
            else FixRegistry.from_handle(IOBase.from_uri(location))
        )
        if not dictionary:
            raise ValueError(
                "parse_fix requires a non-empty Yggdryl FIX registry; "
                "set registry or YGGDRYL_FIX_REGISTRY"
            )
        dictionary.with_crate_fields()
        return dictionary

    #: How a raw column is spelled on the way into the reader.
    #:
    #: The reader reads five of a capture's own columns as per-row parameters,
    #: and two of the raw contract's names land on them. `branch` on a raw
    #: record is the driver that printed the line, not a FIX dialect, so it is
    #: renamed out of the way rather than pinning every row to a dialect nobody
    #: declared. `msgdirection` is renamed *into* the parameter, so the reader
    #: uses the direction the raw layer already read instead of reading it
    #: again.
    CARRIED = {"branch": "logbranch", "msgdirection": "direction"}

    def carried(schema):
        """One capture schema under the names the reader reads it by."""
        return [CARRIED.get(name, name) for name in schema.names]

    def iceberg_field(schema):
        """The parser's schema as Iceberg v2 can hold it.

        A venue stamps nanoseconds and Yggdryl reads them, so the dictionary
        declares its clocks -- and this crate its derived one -- at that
        resolution. Iceberg v2 has no nanosecond timestamp: its column is
        microseconds, and a `timestamp[ns]` handed to PyIceberg is refused
        outright.

        So every nanosecond clock is declared here at the resolution the table
        holds, and the native apply narrows it once in the cast it already
        runs. It is a stated loss rather than a hidden one: `entries` keeps
        the wire text of every pair, so what a venue actually stamped is still
        in the row.
        """
        members = [member.with_type(microseconds(member.type)) for member in schema]
        return Field.from_arrow_schema(pyarrow.schema(members), name="FixMessage")

    def microseconds(dtype):
        """One Arrow type with every nanosecond clock in it narrowed.

        A clock nested in a repeating group is a clock: the walk is the whole
        type rather than its top level, because `TrdRegTimestamp` sits inside
        group 768 and Iceberg refuses it there for the same reason.
        """
        if pyarrow.types.is_timestamp(dtype) and dtype.unit == "ns":
            return pyarrow.timestamp("us", tz=dtype.tz)
        if pyarrow.types.is_list(dtype):
            item = dtype.field(0)
            return pyarrow.list_(item.with_type(microseconds(item.type)))
        if pyarrow.types.is_large_list(dtype):
            item = dtype.field(0)
            return pyarrow.large_list(item.with_type(microseconds(item.type)))
        if pyarrow.types.is_struct(dtype):
            return pyarrow.struct([member.with_type(microseconds(member.type)) for member in dtype])
        return dtype


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
    version = _defaults["version"]
    dedup = _defaults["dedup"]
    catalog = _defaults["catalog"]
    return catalog, dedup, registry, version


@app.cell
def _():
    records = configure()
    return (records,)


@app.cell
def _(catalog, dedup, records, registry, version):
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
        # A record the raw layer could not name a MsgType for carries no FIX
        # frame, so it stays in `logs.messages` rather than becoming a row of
        # nulls here. The classification is read, never recomputed.
        source = messages.read_arrow_reader(
            Message.field(),
            row_filter=NotEqualTo("msgtype", UNKNOWN),
        )
        opened.callback(source.close)
        counts = {"read": 0}

        names = carried(source.schema)

        def _batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch.rename_columns(names)

        counted = pyarrow.RecordBatchReader.from_batches(
            pyarrow.schema(
                [field.with_name(name) for field, name in zip(source.schema, names, strict=True)]
            ),
            _batches(),
        )
        opened.callback(counted.close)
        dictionary = open_registry(registry)
        parsed = parse_arrow_reader(
            counted,
            dictionary,
            "body",
            target_version=version,
            dedup=dedup,
        )
        opened.callback(parsed.close)
        field = iceberg_field(parsed.schema)
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
