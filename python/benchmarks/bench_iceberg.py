"""Focused Iceberg commits, scans, merges and maintenance over synthetic rows."""

from __future__ import annotations

import datetime
import functools
import pathlib
import random
import shutil
import sys
import tempfile
import time
from collections.abc import Iterator
from typing import Annotated, Any

import pyarrow

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import parser, timed  # noqa: E402

from rekep import Convertible, Field, scalar  # noqa: E402
from rekep.iceberg import IcebergCatalog, IcebergDataset  # noqa: E402
from rekep.iceberg.dataset import _key_ranges, _match_filter  # noqa: E402


@scalar
class Quote(Convertible):
    """One quote, under a composite key whose halves both repeat."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, Field.primary_key(), Field.partition_key()]
    """Trading day, and the partition."""

    size: int
    """Quantity."""

    venue: str
    """Where it traded."""


@scalar
class Tick(Convertible):
    """A row under a wide composite key, clustered per commit."""

    at: Annotated[int, Field.primary_key(), Field.sort_key()]
    """A timestamp that advances with the commits."""

    h64: Annotated[int, Field.primary_key()]
    """A hash spread over the whole 62-bit range."""

    payload: str
    """Payload."""


@scalar
class LogRow(Convertible):
    """One benchmark-local stored row with an hourly partition."""

    unix: Annotated[int, Field.primary_key(), Field.sort_key()]
    """Unique nanosecond clock."""

    unixpartition: Annotated[int, Field.partition_key()]
    """Whole epoch hour used for identity partitioning."""

    plugin: str
    """Low-cardinality source spelling."""

    body: bytes
    """Representative binary payload."""


PLUGINS = ("OMSSales_Enrichment", "ULBridge", "ModuleMarketDataManager", "ObjkeyTagWrapper")
_BASE_UNIX = 1_786_665_600_000_000_000
_DAY_NS = 86_400_000_000_000
_HOUR_NS = 3_600_000_000_000

#: Table properties this package sets when commits are optimised.
OPTIMISED = {
    "commit.manifest-merge.enabled": "true",
    "write.target-file-size-bytes": str(256 * 1024 * 1024),
}


# -- the source rows ---------------------------------------------------------


def log_rows(rows: int, days: int) -> pyarrow.Table:
    """Build `rows` stored records spread over `days`.

    Spread on purpose: a table that all lands on one day cannot show whether a
    read prunes, and a partitioned table with one partition is not a
    partitioned table.
    """
    per_day = max(rows // days, 1)
    day = [min(index // per_day, days - 1) for index in range(rows)]
    unix = [_BASE_UNIX + offset * _DAY_NS + index * 1_000 for index, offset in enumerate(day)]
    return pyarrow.Table.from_pydict(
        {
            "unix": unix,
            "unixpartition": [value // _HOUR_NS for value in unix],
            "plugin": [PLUGINS[index % len(PLUGINS)] for index in range(rows)],
            "body": [
                (
                    f"payload {index}: ACCOUNT=ACCT-{index % 500:06d} "
                    f"routed XPAR qty={index % 10_000}"
                ).encode()
                for index in range(rows)
            ],
        },
        schema=LogRow.into_field().into_arrow_schema(),
    )


def batches(table: pyarrow.Table, batch_row_size: int) -> Iterator[pyarrow.RecordBatch]:
    """The table as a stream, the way a parser hands one over."""
    return iter(table.to_batches(max_chunksize=batch_row_size))


# -- the table --------------------------------------------------------------


def catalog(root: pathlib.Path) -> IcebergCatalog:
    warehouse = root / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    return IcebergCatalog(
        name="bench",
        properties={
            "type": "sql",
            "uri": f"sqlite:///{(root / 'catalog.db').as_posix()}",
            "warehouse": warehouse.as_uri(),
        },
    )


def dataset(root: pathlib.Path, *, partitioned: bool, properties: dict[str, str]) -> IcebergDataset:
    """A fresh table, partitioned by hour or not at all."""
    field = LogRow.into_field()
    if not partitioned:
        field = field.into_dataclass("Flat").into_field()
        field.field("unixpartition").is_partition_key = False
    built = catalog(root).dataset("bench.logs", field=field, table_properties=properties)
    return built.create_with()


def stats(target: IcebergDataset) -> dict[str, int]:
    """What the next reader will pay for: files, manifests, snapshots."""
    table = target.refresh().iceberg_table
    return {
        "files": table.inspect.data_files().num_rows,
        "manifests": table.inspect.manifests().num_rows,
        "snapshots": len(table.snapshots()),
    }


# -- writing ----------------------------------------------------------------


def write_case(
    table: pyarrow.Table,
    *,
    mode: str,
    commit_row_size: int | None,
    batch_row_size: int = 16_384,
    partitioned: bool = True,
    properties: dict[str, str] | None = None,
    preload: pyarrow.Table | None = None,
    plan_merges: bool = True,
) -> dict[str, Any]:
    """One write configuration, measured on a table of its own."""
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-"))
    try:
        target = dataset(root, partitioned=partitioned, properties=properties or {})
        target.plan_merges = plan_merges
        if preload is not None:  # something for a merge to match against
            target.append_arrow(preload, commit_row_size=1_000_000)
        # A merge mode is an overwrite; every other mode is putting rows in,
        # which is what `append_arrow` is -- the two are not one call with a
        # flag any more, so neither is the measurement.
        write = (
            (
                lambda: target.overwrite_arrow(
                    batches(table, batch_row_size),
                    merge_by=True,
                    commit_row_size=commit_row_size,
                )
            )
            if mode.startswith("merge")
            else (
                lambda: target.append_arrow(
                    batches(table, batch_row_size), commit_row_size=commit_row_size
                )
            )
        )
        seconds, _ = timed(write)
        report = {"seconds": seconds, "rows": table.num_rows, **stats(target)}
        report["stored"] = target.read_arrow_table().num_rows
        # A merge replaces the rows whose keys match, so a preloaded half is
        # already in the count; anything else stored what it was given.
        expected = table.num_rows if preload is None else max(table.num_rows, preload.num_rows)
        assert report["stored"] == expected, report
        return report
    finally:
        shutil.rmtree(root, ignore_errors=True)


def monotonic_insert_case(table: pyarrow.Table, commit_rows: int) -> dict:
    """Insert increasing chunks the way a chronological stream commits them."""
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-insert-"))
    try:
        target = catalog(root).dataset("bench.ticks", field=Tick.into_field()).create_with()

        def write() -> None:
            for start in range(0, table.num_rows, commit_rows):
                target.append_arrow_table(
                    table.slice(start, commit_rows), merge_by=True, commit_row_size=1_000_000
                )

        seconds, _ = timed(write)
        return {"seconds": seconds, "rows": target.records, **stats(target)}
    finally:
        shutil.rmtree(root, ignore_errors=True)


# -- reading ----------------------------------------------------------------


def read_case(target: IcebergDataset, *, row_filter: Any, columns: Any, schema: Any) -> dict:
    """One read configuration: wall time, rows, and how many files it planned."""
    scan = target.iceberg_table.scan(
        **({"row_filter": row_filter} if row_filter is not None else {}),
        selected_fields=tuple(columns) if columns else ("*",),
    )
    planned = len(list(scan.plan_files()))
    reference = target.read_arrow_table(schema, row_filter=row_filter, columns=columns)
    seconds, table = timed(
        lambda: target.read_arrow_table(schema, row_filter=row_filter, columns=columns)
    )
    # A projection or a pushed filter must not change what comes back, so the
    # answer is settled before the number is.
    assert table.equals(reference), (row_filter, columns)
    return {"seconds": seconds, "rows": table.num_rows, "planned": planned}


# -- sweeps -----------------------------------------------------------------


def header(columns: tuple[str, ...], widths: tuple[int, ...]) -> None:
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))


def sweep_write(rows: int, days: int, quick: bool) -> pathlib.Path:
    """Streaming stored rows into a table, in every shape worth trying."""
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-log-"))
    table = log_rows(rows, days)
    # One throwaway write first: the first configuration would otherwise pay for
    # importing pyiceberg, opening the catalog and warming the page cache.
    write_case(table.slice(0, 1_000), mode="append", commit_row_size=1_000_000)
    print(f"\n== write: {table.num_rows:,} rows over {days} days ==")
    header(
        ("case", "commit rows", "seconds", "rows/s", "files", "manif", "snaps", "stored"),
        (26, 12, 9, 11, 7, 6, 6, 9),
    )

    # A commit closes at the first batch boundary at or beyond its size, so a
    # commit smaller than the reader's batch is one batch: the sweep uses a
    # realistic parser batch (16k rows) and commit sizes around it.
    commits: list[int | None] = [50_000, None] if quick else [16_384, 65_536, 262_144, None]
    half = table.slice(0, table.num_rows // 2)
    # (label, mode, commit, partitioned, properties, preload, plan_merges)
    configurations: list[tuple] = []
    for commit in commits:
        configurations.append(("append", "append", commit, True, "optimised", None, True))
    for commit in commits:
        configurations.append(("merge, all new", "merge", commit, True, "optimised", None, True))
    for commit in commits:
        configurations.append(
            ("merge, half stored", "merge", commit, True, "optimised", half, True)
        )
    if not quick:
        for commit in (50_000, None):
            configurations.extend(
                [
                    ("append, no partition", "append", commit, False, "optimised", None, True),
                    ("append, iceberg defaults", "append", commit, True, "default", None, True),
                    ("merge, no partition", "merge", commit, False, "optimised", half, True),
                ]
            )

    for label, mode, commit, partitioned, props, preload, planned in configurations:
        report = write_case(
            table,
            mode=mode,
            commit_row_size=commit,
            partitioned=partitioned,
            properties=OPTIMISED if props == "optimised" else {},
            preload=preload,
            plan_merges=planned,
        )
        print(
            f"{label:>26} {('one' if commit is None else f'{commit:,}'):>12} "
            f"{report['seconds']:>9.2f} {report['rows'] / report['seconds']:>11,.0f} "
            f"{report['files']:>7,} {report['manifests']:>6,} {report['snapshots']:>6,} "
            f"{report['stored']:>9,}"
        )
    return tmp


def sweep_insert(rows: int, repeat: int) -> None:
    """What a chronological stream's insert commits cost."""
    rows = min(rows, 100_000)
    commit_rows = max(rows // 6, 1)
    table = tick_rows(rows)
    # Every row lands, once, before any of them is timed.
    warmed = monotonic_insert_case(table, commit_rows)
    assert warmed["rows"] == rows, warmed
    runs = [monotonic_insert_case(table, commit_rows) for _ in range(repeat)]
    assert all(run["rows"] == rows for run in runs), runs
    best = min(runs, key=lambda run: run["seconds"])
    print(f"\n== monotonic insert: {rows:,} rows, {commit_rows:,} per commit ==")
    header(("case", "best sec", "rows/s", "files", "manif", "snaps"), (12, 10, 11, 7, 6, 6))
    print(
        f"{'bounded':>12} {best['seconds']:>10.3f} {rows / best['seconds']:>11,.0f} "
        f"{best['files']:>7} {best['manifests']:>6} {best['snapshots']:>6}"
    )


def _store_quotes(table: pyarrow.Table) -> tuple[dict[str, int], pyarrow.Table]:
    """Write one converted result and report the storage a reader inherits."""
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-polars-"))
    try:
        target = catalog(root).dataset("bench.quotes", field=Quote.into_field()).create_with()
        target.append_arrow_table(table, commit_row_size=1_000_000)
        plan = target.scan_plan("day = '2026-08-14'")
        report = {
            "rows": target.records or 0,
            **stats(target),
            "planned": plan["files"],
            "skipped": plan["skipped"],
        }
        return report, target.read_arrow_table(Quote.into_field()).sort_by("symbol")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def sweep_polars(rows: int, repeat: int) -> None:
    """Polars export selected for the declared Arrow layout versus forced newest."""
    import polars

    from rekep.dataset import _polars_table

    rows = min(rows, 100_000)
    day = datetime.date(2026, 8, 14)
    source = polars.DataFrame(
        {
            "symbol": [f"S{i}" for i in range(rows)],
            "day": [day + datetime.timedelta(days=i % 4) for i in range(rows)],
            "size": list(range(rows)),
            "venue": ["XPAR"] * rows,
        }
    )
    target = Quote.into_field()

    def compatible() -> pyarrow.Table:
        return _polars_table(source, target, polars)

    def newest() -> pyarrow.Table:
        return target.cast_arrow_table(source.to_arrow(compat_level=polars.CompatLevel.newest()))

    compatible()
    newest()
    runs: dict[str, list[tuple[float, pyarrow.Table]]] = {
        "compatible": [],
        "forced newest": [],
    }
    calls = {"compatible": compatible, "forced newest": newest}
    for trial in range(max(repeat, 2)):
        order = ("compatible", "forced newest")
        if trial % 2:
            order = tuple(reversed(order))
        for name in order:
            runs[name].append(timed(calls[name]))

    best = {name: min(values, key=lambda value: value[0]) for name, values in runs.items()}
    assert best["compatible"][1].equals(best["forced newest"][1])
    assert best["compatible"][1].schema.equals(target.into_arrow_schema())
    stored = {name: _store_quotes(result) for name, (_, result) in best.items()}
    baseline = stored["forced newest"]
    assert stored["compatible"][0] == baseline[0]
    assert stored["compatible"][1].equals(baseline[1])

    print(f"\n== Polars -> declared Arrow: {rows:,} rows ==")
    header(
        ("case", "best sec", "rows/s", "files", "manif", "snaps", "planned", "skipped"),
        (14, 10, 12, 7, 6, 6, 8, 8),
    )
    for name in ("forced newest", "compatible"):
        seconds, _ = best[name]
        report = stored[name][0]
        print(
            f"{name:>14} {seconds:>10.4f} {rows / seconds:>12,.0f} "
            f"{report['files']:>7} {report['manifests']:>6} {report['snapshots']:>6} "
            f"{report['planned']:>8} {report['skipped']:>8}"
        )


def sweep_read(rows: int, days: int, repeat: int = 3) -> None:
    """Reading it back: what prunes, what does not, and what a projection saves."""
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-read-"))
    try:
        table = log_rows(rows, days)
        target = dataset(root, partitioned=True, properties=OPTIMISED)
        target.append_arrow(batches(table, 65_536), commit_row_size=rows // max(days, 1))
        day = datetime.date(2026, 8, 14)
        # A partition the data actually has, read from the data rather than
        # spelled out: `unixpartition` is whatever hour the first row fell
        # in, and a filter naming an empty partition measures nothing.
        hour = table.column("unixpartition")[0].as_py()
        # The unix bound of the third day: a filter on a column that is not the
        # partition, but correlates with it, so only file statistics can prune.
        third_day = (
            int(
                (
                    datetime.datetime.combine(day + datetime.timedelta(days=2), datetime.time())
                    - datetime.datetime(1970, 1, 1)
                ).total_seconds()
            )
            * 10**9
        )
        from pyiceberg.expressions import EqualTo

        plugin_filter = EqualTo("plugin", "ULBridge")
        print(f"\n== read: {table.num_rows:,} rows, {stats(target)['files']} files ==")
        header(("case", "seconds", "rows", "rows/s", "planned", "skipped"), (30, 9, 12, 12, 8, 8))
        cases = [
            ("everything", None, None, None),
            ("partition = one hour", f"unixpartition = {hour}", None, None),
            (
                "partition, 3 columns",
                f"unixpartition = {hour}",
                ["unix", "plugin", "body"],
                None,
            ),
            ("3 columns, no filter", None, ["unix", "plugin", "body"], None),
            ("correlated column", f"unix < {third_day}", None, None),
            ("no stats to prune on", plugin_filter, None, None),
            ("narrow shape (pushdown)", None, None, narrow_field()),
            ("narrow shape, store widths", None, None, "stored"),
        ]
        # Warm the process before the first case is timed. An Acero join, the
        # Arrow parquet reader and the page cache all cost their setup once,
        # and a sweep of single calls in order charges the whole of it to
        # whichever case happens to run first: measured over three
        # back-to-back `--only read` runs, "everything" came out 0.057, 0.031
        # and 0.027 -- a 2.1x spread that is nothing but warm-up.
        read_case(target, row_filter=None, columns=None, schema=None)
        for name, row_filter, columns, schema in cases:
            if schema == "stored":
                schema = stored_narrow(target)
            # And once per case, discarded: the first read of a configuration
            # touches files and builds a projection the repeats then reuse.
            read_case(target, row_filter=row_filter, columns=columns, schema=schema)
            report = min(
                (
                    read_case(target, row_filter=row_filter, columns=columns, schema=schema)
                    for _ in range(repeat)
                ),
                key=lambda found: found["seconds"],
            )
            plan = target.scan_plan(row_filter, columns=columns)
            print(
                f"{name:>30} {report['seconds']:>9.3f} {report['rows']:>12,} "
                f"{report['rows'] / report['seconds'] if report['seconds'] else 0:>12,.0f} "
                f"{report['planned']:>8,} {plan['skipped']:>8,}"
            )
    finally:
        shutil.rmtree(root, ignore_errors=True)


def sweep_maintain(rows: int, days: int) -> None:
    """What a reader holds and whether explicit compaction settles.

    Counts answer how much a reader materialises before its consumer asks and
    whether repeated compaction stops rewriting an unchanged table.
    """
    import gc

    from pyiceberg.io.pyarrow import PyArrowFile

    tmp = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-maint-"))
    try:
        table = log_rows(rows, days)

        # -- what a reader holds ------------------------------------------
        print(f"\n== reading as a stream: {table.num_rows:,} rows ==")
        header(("case", "files opened", "MiB held", "MiB total"), (30, 13, 10, 10))
        target = dataset(tmp / "read", partitioned=True, properties=OPTIMISED)
        # Small batches on purpose: a commit closes at the first batch boundary
        # at or beyond its size, so a big batch makes the commit size inert and
        # the table comes out in two files instead of the many this measures.
        target.append_arrow(batches(table, 2_048), commit_row_size=max(table.num_rows // 24, 1))
        target.read_arrow_table()  # warm the page cache, so this measures memory
        planned = target.scan_plan()["files"]
        opened: list[str] = []
        original = PyArrowFile.open

        def watched(self: Any, *args: Any, **kwargs: Any) -> Any:
            if self.location.endswith(".parquet"):
                opened.append(self.location)
            return original(self, *args, **kwargs)

        PyArrowFile.open = watched
        try:
            gc.collect()
            base = pyarrow.total_allocated_bytes()
            reader = target.read_arrow_reader()
            reader.read_next_batch()
            # A consumer that is not instantaneous, which is the only kind this
            # is about: the pool goes on decoding whether or not anyone is
            # asking, and what it has finished is what is being held.
            time.sleep(0.25)
            held = pyarrow.total_allocated_bytes() - base
            after = len(opened)
            del reader
            whole = target.read_arrow_table()
            print(
                f"{'one batch, ' + str(planned) + ' files planned':>30} {after:>13} "
                f"{held / 2**20:>10.1f} {whole.nbytes / 2**20:>10.1f}"
            )
        finally:
            PyArrowFile.open = original

        # -- does a rewrite settle, on every partition shape? -------------
        print("\n== compaction settles: files rewritten per run ==")
        header(("partitioning", "run 1", "run 2", "run 3", "rows"), (30, 8, 8, 8, 10))
        for label, built in (
            (
                "identity (unixpartition)",
                lambda root: dataset(root, partitioned=True, properties=OPTIMISED),
            ),
            ("none", lambda root: dataset(root, partitioned=False, properties=OPTIMISED)),
            ("transform (bucket)", lambda root: daily(root)),
        ):
            target = built(tmp / f"settle-{label[:8]}")
            target.append_arrow(batches(table, 2_048), commit_row_size=max(rows // 12, 1))
            runs = [target.compact(min_files=2) for _ in range(3)]
            assert runs[1:] == [0, 0], (label, runs)
            assert target.refresh().read_arrow_table().num_rows == rows, label
            print(
                f"{label:>30} {runs[0]:>8,} {runs[1]:>8,} {runs[2]:>8,} "
                f"{target.refresh().read_arrow_table().num_rows:>10,}"
            )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def sweep_update(rows: int, days: int) -> None:
    """The half of a merge that *rewrites*, on the key shape it costs most on.

    A merge that inserts is cheap and measured everywhere else here. A merge
    that updates pays for the filter naming the rows it deletes, and that
    filter is one `And(EqualTo, EqualTo)` per row for a composite key -- a tree
    pyiceberg binds once per manifest it plans. Factoring out whatever the key
    repeats is what this sweeps.

    Both key shapes, including the one that cannot be helped: a key neither
    half of which repeats groups one row per term, which is the tree the
    library already builds, and its numbers are what a merge of many updates
    costs when nothing can be factored out of it.
    """
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-update-"))
    try:
        # Sized off `rows` so the update counts below are a slice of the table,
        # not the whole of it: a merge that touches everything is a rewrite.
        wide = max(rows // (10 * max(days, 1)), 250)
        # Both shapes, because only one of them can be factored -- and a sweep
        # that left out the one that cannot would read as a claim about merges
        # rather than about keys.
        cases = (
            (
                "(symbol, day) — day repeats",
                Quote.into_field(),
                quote_rows(wide, days),
                ["symbol", "day"],
                "venue",
            ),
            (
                "(at, h64) — nothing repeats",
                Tick.into_field(),
                tick_rows(wide * days),
                ["at", "h64"],
                "payload",
            ),
        )
        for label, shape, stored, join, column in cases:
            target = catalog(root / label[:8]).dataset("bench.updated", field=shape).create_with()
            target.append_arrow(stored, commit_row_size=max(stored.num_rows // max(days, 1), 1))
            print(f"\n== updating {label}: {stored.num_rows:,} rows ==")
            header(("rows updated", "seconds", "rows/s", "terms", "files"), (14, 9, 11, 8, 7))
            index = stored.schema.get_field_index(column)
            for count in (500, 2_000, 5_000):
                if count * 2 > stored.num_rows:
                    continue
                changed = stored.slice(0, count)
                changed = changed.set_column(
                    index,
                    changed.schema.field(column),
                    pyarrow.array([f"V{i}" for i in range(count)]),
                )
                terms = _terms(_match_filter(changed, join))
                seconds, report = timed(functools.partial(target.merge_arrow_table, changed, join))
                files = target.refresh().iceberg_table.inspect.data_files().num_rows
                print(
                    f"{count:>14,} {seconds:>9.2f} {count / seconds:>11,.0f} "
                    f"{terms:>8,} {files:>7,}"
                )
                assert report == (count, 0), report
                target.merge_arrow_table(stored.slice(0, count), join)  # put them back
    finally:
        shutil.rmtree(root, ignore_errors=True)


def sweep_delete(rows: int, days: int, repeat: int) -> None:
    """Partition-pruned, partial-file, and empty streamed deletes."""
    rows = min(rows, 50_000)
    symbols = max(rows // max(days, 1), 1)
    source = quote_rows(symbols, days)
    first = datetime.date(2026, 8, 14)
    cases = (
        ("one partition", f"day = '{first.isoformat()}'"),
        ("part of one file", f"size < {max(symbols // 2, 1)}"),
        ("no match", "size < 0"),
    )
    print(f"\n== delete: {source.num_rows:,} rows over {days} days ==")
    header(
        ("case", "best sec", "removed", "rows/s", "planned", "files after", "snapshots"),
        (18, 10, 10, 12, 8, 11, 9),
    )
    for label, predicate in cases:
        runs = []
        for trial in range(max(repeat, 1)):
            root = pathlib.Path(tempfile.mkdtemp(prefix=f"rekep-bench-delete-{trial}-"))
            try:
                target = (
                    catalog(root)
                    .dataset("bench.quotes", field=Quote.into_field(), table_properties=OPTIMISED)
                    .create_with()
                )
                target.append_arrow(
                    batches(source, 4_096),
                    commit_row_size=max(source.num_rows // max(days * 2, 1), 1),
                )
                planned = target.scan_plan(predicate)["files"]
                before = len(target.iceberg_table.snapshots())
                seconds, removed = timed(functools.partial(target.delete_where, predicate))
                report = {
                    "seconds": seconds,
                    "removed": removed,
                    "planned": planned,
                    "files": target.refresh().data_files().num_rows,
                    "snapshots": len(target.iceberg_table.snapshots()) - before,
                }
                assert target.read_arrow_table().num_rows == source.num_rows - removed
                runs.append(report)
            finally:
                shutil.rmtree(root, ignore_errors=True)
        best = min(runs, key=lambda report: report["seconds"])
        rate = best["removed"] / best["seconds"] if best["removed"] else 0
        print(
            f"{label:>18} {best['seconds']:>10.3f} {best['removed']:>10,} "
            f"{rate:>12,.0f} {best['planned']:>8,} {best['files']:>11,} "
            f"{best['snapshots']:>9,}"
        )


def tick_rows(count: int) -> pyarrow.Table:
    """`count` ticks under a key neither half of which repeats."""
    source = random.Random(20_260_821)
    return pyarrow.Table.from_pydict(
        {
            "at": list(range(count)),
            "h64": [source.getrandbits(62) for _ in range(count)],
            "payload": ["XPAR"] * count,
        },
        schema=Tick.into_field().into_arrow_schema(),
    )


def sweep_backfill(rows: int, days: int) -> None:
    """Replaying keys that sit in a few bands of a wide table.

    The shape a backfill makes, and the one a single min/max range cannot prune
    at all: past `MERGE_IN_LIMIT` distinct values a key column used to become
    one range spanning everything between the bands. What a scan *plans* is the
    number here -- rows returned say nothing about files opened.
    """
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-backfill-"))
    try:
        target = catalog(root).dataset("bench.ticks", field=Tick.into_field()).create_with()
        per = max(rows // 20, 1_000)
        # The hash is drawn per *row*, not derived from the band: a real line
        # hash spreads over the whole range, so every file's bounds on it span
        # nearly everything and it prunes nothing. Deriving it from the band
        # instead gave each file a narrow hash band that pruned the table by
        # itself -- the fixture doing the work the code is supposed to.
        source = random.Random(20_260_821)
        commits = [
            pyarrow.Table.from_pydict(
                {
                    "at": [band * 10**12 + i for i in range(per)],
                    "h64": [source.getrandbits(62) for _ in range(per)],
                    "payload": ["x" * 40] * per,
                },
                schema=Tick.into_field().into_arrow_schema(),
            )
            for band in range(20)
        ]
        for commit in commits:
            target.append_arrow(commit, commit_row_size=1_000_000)
        stored = target.refresh().data_files().num_rows
        print(f"\n== backfill: {stored} files of {per:,} rows, keys clustered per file ==")
        header(("case", "planned", "skipped", "seconds", "inserted"), (30, 8, 8, 9, 9))
        for label, replay in (
            ("two distant bands", pyarrow.concat_tables([commits[1], commits[18]])),
            ("one band", commits[7]),
            ("half the table", pyarrow.concat_tables(commits[:10])),
        ):
            ranges = _key_ranges(replay, ["at", "h64"])
            plan = target.scan_plan(ranges)
            seconds, inserted = timed(functools.partial(target.insert_arrow_table, replay, True))
            assert inserted == 0, (label, inserted)
            assert plan["files"] + plan["skipped"] == stored, (label, plan)
            print(
                f"{label:>30} {plan['files']:>8} {plan['skipped']:>8} "
                f"{seconds:>9.2f} {inserted:>9,}"
            )
    finally:
        shutil.rmtree(root, ignore_errors=True)


def quote_rows(symbols: int, days: int) -> pyarrow.Table:
    """`symbols` instruments on each of `days` days: a key whose halves repeat."""
    day = datetime.date(2026, 8, 14)
    pairs = [
        (f"S{s}", day + datetime.timedelta(days=d)) for d in range(days) for s in range(symbols)
    ]
    return pyarrow.Table.from_pydict(
        {
            "symbol": [pair[0] for pair in pairs],
            "day": [pair[1] for pair in pairs],
            "size": list(range(len(pairs))),
            "venue": ["XPAR"] * len(pairs),
        },
        schema=Quote.into_field().into_arrow_schema(),
    )


def _terms(expression: Any) -> int:
    """Leaf predicates in a filter -- what pyiceberg binds, once per manifest."""
    left, right = getattr(expression, "left", None), getattr(expression, "right", None)
    if left is None and right is None:
        return 1
    return _terms(left) + _terms(right)


def daily(root: pathlib.Path) -> IcebergDataset:
    """The stored-row shape, partitioned by a *transform* of the same column.

    Every partition transform but `identity` hides which rows a partition
    holds, so the table is only addressable as a whole -- and a plan that
    cannot address parts of it has to settle as a whole too. When it did not,
    every run read the table back and wrote it out again, forever.
    """
    field = LogRow.into_field().into_dataclass("Daily").into_field()
    # `bucket[8]`, because `unixpartition` is a signed integer and Iceberg's `day`
    # transform is for dates. The point is unchanged: a transform, not the value itself.
    field.field("unixpartition").is_partition_key = "bucket[8]"
    built = catalog(root).dataset("bench.daily", field=field, table_properties=OPTIMISED)
    return built.create_with()


def stored_narrow(target: IcebergDataset) -> Any:
    """The same three columns, declared with the widths the store reads back.

    The difference between this and `narrow_field` is one conversion per string
    column per row -- which is the price of declaring `string` where Iceberg
    hands back `large_string`.
    """
    from rekep.fields import Field

    schema = target.table_field.into_arrow_schema()
    return Field.from_arrow_schema(
        pyarrow.schema([schema.field(name) for name in ("unix", "plugin", "body")]),
        "Narrow",
    )


def narrow_field() -> Any:
    """Three stored columns, as a declared shape rather than a column list."""
    from rekep.fields import Field

    schema = LogRow.into_field().into_arrow_schema()
    return Field.from_arrow_schema(
        pyarrow.schema([schema.field(name) for name in ("unix", "plugin", "body")]),
        "Narrow",
    )


def main() -> int:
    options = parser(__doc__, rows=100_000, repeat=3)
    options.add_argument("--days", type=int, default=8)
    options.add_argument(
        "--only",
        choices=[
            "write",
            "insert",
            "polars",
            "read",
            "maintain",
            "update",
            "delete",
            "backfill",
        ],
        default=None,
    )
    arguments = options.parse_args()
    rows = 5_000 if arguments.quick else arguments.rows
    days = 4 if arguments.quick else arguments.days

    if arguments.only in (None, "write"):
        shutil.rmtree(sweep_write(rows, days, arguments.quick), ignore_errors=True)
    if arguments.only in (None, "insert"):
        sweep_insert(rows, 2 if arguments.quick else arguments.repeat)
    if arguments.only in (None, "polars"):
        sweep_polars(rows, 2 if arguments.quick else arguments.repeat)
    if arguments.only in (None, "read"):
        sweep_read(rows, days, 2 if arguments.quick else arguments.repeat)
    if arguments.only in (None, "maintain"):
        sweep_maintain(min(rows, 100_000), days)
    if arguments.only in (None, "update"):
        sweep_update(min(rows, 100_000), days)
    if arguments.only in (None, "delete"):
        sweep_delete(rows, days, 1 if arguments.quick else min(arguments.repeat, 2))
    if arguments.only in (None, "backfill"):
        sweep_backfill(min(rows, 100_000), days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
