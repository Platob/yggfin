"""Benchmark the pipeline that matters: a log parsed and streamed into Iceberg."""

from __future__ import annotations

import argparse
import datetime
import functools
import pathlib
import random
import shutil
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from typing import Annotated, Any

import pyarrow

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep import Convertible, Field, Log, TextFile, scalar  # noqa: E402
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


DRIVERS = [b"OMSSales_Enrichment", b"ULBridge", b"ModuleMarketDataManager", b"ObjkeyTagWrapper"]
LEVELS = [b"(DEBUG) ", b"(INFO) ", b"(WARNING) ", b""]
TRACE = b"java.lang.IllegalStateException: synthetic\n\tat com.example.A.b(A.java:1)\n"

#: Table properties this package sets when commits are optimised.
OPTIMISED = {
    "commit.manifest-merge.enabled": "true",
    "write.target-file-size-bytes": str(256 * 1024 * 1024),
}


# -- the log ----------------------------------------------------------------


def generate(path: pathlib.Path, rows: int, days: int) -> int:
    """Write a synthetic log of `rows` records spread over `days`.

    Spread on purpose: a log that all lands on one day cannot show whether a
    read prunes, and a partitioned table with one partition is not a
    partitioned table.
    """
    per_day = max(rows // days, 1)
    with path.open("wb") as out:
        for i in range(rows):
            day = 14 + min(i // per_day, days - 1)
            second, micro = divmod(i % per_day, 1_000_000)
            out.write(
                b"2026-08-%02d %02d:%02d:%02d.%03d_%03d [250-e7256476:9effef3e6a:%05d] [%s] %s"
                % (
                    day,
                    second // 3600 % 24,
                    second // 60 % 60,
                    second % 60,
                    micro // 1000,
                    micro % 1000,
                    72500 + i % 8,
                    DRIVERS[i % len(DRIVERS)],
                    LEVELS[i % len(LEVELS)],
                )
            )
            out.write(
                b"payload %d: ACCOUNT=ACCT-%06d routed XPAR qty=%d\n" % (i, i % 500, i % 10_000)
            )
            if i % 200 == 199:
                out.write(TRACE)
    return path.stat().st_size


def parsed(path: pathlib.Path) -> pyarrow.Table:
    """The whole log as one table, so a write benchmark measures the write."""
    with TextFile.from_path(path) as log:
        return log.read_arrow_table()


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
    """A fresh table, partitioned by day or not at all."""
    field = Log.into_field()
    if not partitioned:
        field = field.into_dataclass("Flat").into_field()
        field.field("unix_hour").is_partition_key = False
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


def timed(call: Callable[[], Any]) -> tuple[float, Any]:
    started = time.perf_counter()
    result = call()
    return time.perf_counter() - started, result


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
            target.write_arrow(preload, commit_row_size=0)
        merge_by = True if mode.startswith("merge") else None
        seconds, _ = timed(
            lambda: target.write_arrow(
                batches(table, batch_row_size),
                merge_by=merge_by,
                commit_row_size=commit_row_size,
            )
        )
        report = {"seconds": seconds, "rows": table.num_rows, **stats(target)}
        report["stored"] = target.read_arrow_table().num_rows
        return report
    finally:
        shutil.rmtree(root, ignore_errors=True)


def monotonic_insert_case(table: pyarrow.Table, commit_rows: int, *, shortcut: bool) -> dict:
    """Insert increasing chunks with or without the exact-upper-bound shortcut."""
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-insert-"))
    try:
        target = catalog(root).dataset("bench.ticks", field=Tick.into_field()).create_with()
        if not shortcut:
            target.__dict__["_insert_span"] = lambda chunk, join, reference: None

        def write() -> None:
            for start in range(0, table.num_rows, commit_rows):
                target.append_arrow_table(
                    table.slice(start, commit_rows), merge_by=True, commit_row_size=0
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
    seconds, table = timed(
        lambda: target.read_arrow_table(schema, row_filter=row_filter, columns=columns)
    )
    return {"seconds": seconds, "rows": table.num_rows, "planned": planned}


# -- sweeps -----------------------------------------------------------------


def header(columns: tuple[str, ...], widths: tuple[int, ...]) -> None:
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))


def sweep_write(rows: int, days: int, quick: bool) -> pathlib.Path:
    """Streaming a parsed log into a table, in every shape worth trying."""
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-log-"))
    path = tmp / "bench.txt"
    generate(path, rows, days)
    table = parsed(path)
    # One throwaway write first: the first configuration would otherwise pay for
    # importing pyiceberg, opening the catalog and warming the page cache.
    write_case(table.slice(0, 1_000), mode="append", commit_row_size=0)
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
        # The same work through pyiceberg's own `Table.upsert`. On a slice, not
        # the whole table: its scan filter carries one term per incoming row, so
        # the full sweep would take hours (see `merge_arrow_table`).
        configurations.append(
            ("merge, official (1/100th)", "merge", None, True, "optimised", half, False)
        )
        for commit in (50_000, None):
            configurations.extend(
                [
                    ("append, no partition", "append", commit, False, "optimised", None, True),
                    ("append, iceberg defaults", "append", commit, True, "default", None, True),
                    ("merge, no partition", "merge", commit, False, "optimised", half, True),
                ]
            )

    for label, mode, commit, partitioned, props, preload, planned in configurations:
        written = table.slice(0, table.num_rows // 100) if "official" in label else table
        report = write_case(
            written,
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
    """Chronological insert commits, with the previous planned path beside them."""
    rows = min(rows, 100_000)
    commit_rows = max(rows // 6, 1)
    table = tick_rows(rows)
    monotonic_insert_case(table.slice(0, min(rows, 1_000)), commit_rows, shortcut=False)
    results: dict[str, list[dict[str, Any]]] = {"planned": [], "bounded": []}
    for trial in range(repeat):
        order = (False, True) if trial % 2 == 0 else (True, False)
        for shortcut in order:
            results["bounded" if shortcut else "planned"].append(
                monotonic_insert_case(table, commit_rows, shortcut=shortcut)
            )
    expected = None
    print(f"\n== monotonic insert: {rows:,} rows, {commit_rows:,} per commit ==")
    header(("case", "best sec", "rows/s", "files", "manif", "snaps"), (12, 10, 11, 7, 6, 6))
    for name, runs in results.items():
        best = min(runs, key=lambda run: run["seconds"])
        cost = tuple(best[key] for key in ("rows", "files", "manifests", "snapshots"))
        expected = cost if expected is None else expected
        assert cost == expected, (name, cost, expected)
        print(
            f"{name:>12} {best['seconds']:>10.3f} {rows / best['seconds']:>11,.0f} "
            f"{best['files']:>7} {best['manifests']:>6} {best['snapshots']:>6}"
        )


def _store_quotes(table: pyarrow.Table) -> tuple[dict[str, int], pyarrow.Table]:
    """Write one converted result and report the storage a reader inherits."""
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-polars-"))
    try:
        target = catalog(root).dataset("bench.quotes", field=Quote.into_field()).create_with()
        target.write_arrow_table(table, commit_row_size=0)
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
        path = root / "bench.txt"
        generate(path, rows, days)
        table = parsed(path)
        target = dataset(root, partitioned=True, properties=OPTIMISED)
        target.write_arrow(batches(table, 65_536), commit_row_size=rows // max(days, 1))
        day = datetime.date(2026, 8, 14)
        # A partition the data actually has, read from the data rather than
        # spelled out: `unix_hour` is whatever hour the generator's first line fell
        # in, and a filter naming an empty partition measures nothing.
        hour = table.column("unix_hour")[0].as_py()
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
        print(f"\n== read: {table.num_rows:,} rows, {stats(target)['files']} files ==")
        header(("case", "seconds", "rows", "rows/s", "planned", "skipped"), (30, 9, 12, 12, 8, 8))
        cases = [
            ("everything", None, None, None),
            ("partition = one hour", f"unix_hour = {hour}", None, None),
            (
                "partition, 3 columns",
                f"unix_hour = {hour}",
                ["unix", "driver_name", "message"],
                None,
            ),
            ("3 columns, no filter", None, ["unix", "driver_name", "message"], None),
            ("correlated column", f"unix < {third_day}", None, None),
            ("no stats to prune on", "driver_name = 'ULBridge'", None, None),
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


def sweep_fs(rows: int, days: int) -> None:
    """Every flow again, in store calls: what S3 would be asked, not seconds.

    Counted on `PyArrowFile` itself -- *below* the FileIO content cache, so a
    count is a call the store actually served. The same sweep runs with the
    cache off and on, because the cache is the answer to most of what the off
    leg shows: everything it removes is a manifest, manifest list or
    `metadata.json` fetched again.
    """
    import contextlib

    from pyiceberg.io.pyarrow import PyArrowFile

    from rekep.iceberg.fileio import CONTENT_CACHE

    counts: dict[str, int] = {}

    def sort(location: str) -> str:
        name = location.rsplit("/", 1)[-1]
        if name.endswith(".parquet"):
            return "data"
        if name.startswith("snap-") and name.endswith(".avro"):
            return "list"
        if name.endswith(".avro"):
            return "manifest"
        return "meta"

    @contextlib.contextmanager
    def counted():
        originals = {"open": PyArrowFile.open, "create": PyArrowFile.create}

        def watched(verb: str, original: Callable[..., Any]) -> Callable[..., Any]:
            def call(self: Any, *args: Any, **kwargs: Any) -> Any:
                key = f"{verb} {sort(self.location)}"
                counts[key] = counts.get(key, 0) + 1
                return original(self, *args, **kwargs)

            return call

        PyArrowFile.open = watched("get", originals["open"])
        PyArrowFile.create = watched("put", originals["create"])
        try:
            yield
        finally:
            PyArrowFile.open = originals["open"]
            PyArrowFile.create = originals["create"]

    def fresh(root: pathlib.Path, name: str, cached: bool) -> IcebergDataset:
        warehouse = root / f"wh-{name}"
        warehouse.mkdir(parents=True)
        properties = {
            "type": "sql",
            "uri": f"sqlite:///{(root / f'{name}.db').as_posix()}",
            "warehouse": warehouse.as_uri(),
            **({} if cached else {"rekep.io.cache-bytes": "0"}),
        }
        catalog = IcebergCatalog(name=f"fs{name}", properties=properties)
        return catalog.dataset(
            "bench.logs", field=Log.into_field(), table_properties=OPTIMISED
        ).create_with()

    def report(label: str, seconds: float) -> None:
        gets = sum(v for k, v in counts.items() if k.startswith("get"))
        puts = sum(v for k, v in counts.items() if k.startswith("put"))
        parts = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"{label:>30} {gets:>6} {puts:>6} {seconds:>9.3f}  {parts}")
        counts.clear()

    tmp = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-fs-"))
    try:
        path = tmp / "bench.txt"
        generate(path, rows, days)
        table = parsed(path)
        # One throwaway write, so the first case is not also paying for
        # importing pyiceberg and opening the first catalog.
        write_case(table.slice(0, 1_000), mode="append", commit_row_size=0)
        chunk = max(table.num_rows // 8, 1)
        half = table.slice(0, table.num_rows // 2)
        hour = table.column("unix_hour")[0].as_py()

        def leg(cached: bool) -> None:
            CONTENT_CACHE.clear()
            print(
                f"\n== store calls: {table.num_rows:,} rows, 8 commits, cache "
                f"{'on' if cached else 'off'} =="
            )
            header(("case", "GET", "PUT", "seconds", "detail"), (30, 6, 6, 9, 40))
            with counted():
                target = fresh(tmp, f"a{cached}", cached)
                counts.clear()
                seconds, _ = timed(
                    lambda: target.write_arrow(batches(table, 16_384), commit_row_size=chunk)
                )
                report("append stream", seconds)

                target = fresh(tmp, f"m{cached}", cached)
                counts.clear()
                seconds, _ = timed(
                    lambda: target.write_arrow(
                        batches(table, 16_384), merge_by=True, commit_row_size=chunk
                    )
                )
                report("merge, all new", seconds)

                target = fresh(tmp, f"h{cached}", cached)
                target.write_arrow(half, commit_row_size=0)
                counts.clear()
                seconds, _ = timed(
                    lambda: target.write_arrow(
                        batches(table, 16_384), merge_by=True, commit_row_size=chunk
                    )
                )
                report("merge, half stored", seconds)

                target = fresh(tmp, f"i{cached}", cached)
                target.write_arrow(table, commit_row_size=0)
                counts.clear()
                seconds, _ = timed(
                    lambda: target.append_arrow(
                        batches(table, 16_384), merge_by=True, commit_row_size=chunk
                    )
                )
                report("insert-only, full replay", seconds)

                target = fresh(tmp, f"r{cached}", cached)
                target.write_arrow(batches(table, 16_384), commit_row_size=chunk)
                counts.clear()
                seconds, _ = timed(target.read_arrow_table)
                report("read everything", seconds)
                seconds, _ = timed(
                    lambda: target.read_arrow_table(row_filter=f"unix_hour = {hour}")
                )
                report("read one partition", seconds)
                seconds, _ = timed(lambda: target.read_arrow_reader(limit=100).read_all())
                report("read limit=100", seconds)
                seconds, _ = timed(lambda: target.scan_plan(f"unix_hour = {hour}"))
                report("scan_plan one partition", seconds)
                seconds, _ = timed(target.read_arrow_table)
                report("read everything, again", seconds)
                seconds, _ = timed(target.optimize)
                report("optimize", seconds)

        for cached in (False, True):
            leg(cached)
        stats = CONTENT_CACHE.stats()
        print(
            f"\ncache: {stats['hits']} hits, {stats['misses']} misses, "
            f"{stats['entries']} entries, {stats['bytes'] / 2**10:.0f} KiB held"
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def sweep_maintain(rows: int, days: int) -> None:
    """The maintenance a streaming table needs, and what a reader holds.

    Three questions seconds on a local disk answer badly and counts answer
    exactly: how much of a table a *reader* materialises before its consumer
    asks, how much of the metadata `maybe_optimize` walks to decide there is
    nothing to do, and whether compaction settles -- which is the difference
    between a routine that costs nothing on a quiet table and one that reads
    and rewrites the whole table every time it runs.
    """
    import gc

    from pyiceberg.io.pyarrow import PyArrowFile
    from pyiceberg.manifest import ManifestFile
    from pyiceberg.table.inspect import InspectTable

    walks: list[str] = []
    partitions, entries = InspectTable.partitions, ManifestFile.fetch_manifest_entry

    def counted(self: Any, snapshot_id: int | None = None) -> Any:
        walks.append("partitions")
        return partitions(self, snapshot_id)

    def fetched(self: Any, io: Any, discard_deleted: bool = True) -> Any:
        walks.append("manifest")
        return entries(self, io, discard_deleted)

    InspectTable.partitions, ManifestFile.fetch_manifest_entry = counted, fetched
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-maint-"))
    try:
        path = tmp / "bench.txt"
        generate(path, rows, days)
        table = parsed(path)

        # -- what a reader holds ------------------------------------------
        print(f"\n== reading as a stream: {table.num_rows:,} rows ==")
        header(("case", "files opened", "MiB held", "MiB total"), (30, 13, 10, 10))
        target = dataset(tmp / "read", partitioned=True, properties=OPTIMISED)
        # Small batches on purpose: a commit closes at the first batch boundary
        # at or beyond its size, so a big batch makes the commit size inert and
        # the table comes out in two files instead of the many this measures.
        target.write_arrow(batches(table, 2_048), commit_row_size=max(table.num_rows // 24, 1))
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

        # -- deciding, and settling ---------------------------------------
        print("\n== maintenance ==")
        header(("case", "partitions", "manifests", "seconds", "result"), (30, 11, 10, 9, 26))

        def maintenance(label: str, call: Callable[[], Any]) -> None:
            walks.clear()
            seconds, out = timed(call)
            print(
                f"{label:>30} {walks.count('partitions'):>11} {walks.count('manifest'):>10} "
                f"{seconds:>9.3f}  {out!s:.26}"
            )

        quiet = dataset(tmp / "quiet", partitioned=True, properties=OPTIMISED)
        quiet.write_arrow(table.slice(0, 2_000), commit_row_size=0)
        maintenance("maybe_optimize, quiet table", quiet.maybe_optimize)

        frayed = dataset(tmp / "frayed", partitioned=True, properties=OPTIMISED)
        frayed.write_arrow(batches(table, 2_048), commit_row_size=max(table.num_rows // 24, 1))
        maintenance("maybe_optimize, frayed", frayed.maybe_optimize)
        maintenance("maybe_optimize, settled", frayed.maybe_optimize)

        # -- does a rewrite settle, on every partition shape? -------------
        print("\n== compaction settles: files rewritten per run ==")
        header(("partitioning", "run 1", "run 2", "run 3", "rows"), (30, 8, 8, 8, 10))
        for label, built in (
            (
                "identity (unix_hour)",
                lambda root: dataset(root, partitioned=True, properties=OPTIMISED),
            ),
            ("none", lambda root: dataset(root, partitioned=False, properties=OPTIMISED)),
            ("transform (bucket)", lambda root: daily(root)),
        ):
            target = built(tmp / f"settle-{label[:8]}")
            target.write_arrow(batches(table, 2_048), commit_row_size=max(rows // 12, 1))
            runs = [target.compact(min_files=2) for _ in range(3)]
            print(
                f"{label:>30} {runs[0]:>8,} {runs[1]:>8,} {runs[2]:>8,} "
                f"{target.refresh().read_arrow_table().num_rows:>10,}"
            )
    finally:
        InspectTable.partitions, ManifestFile.fetch_manifest_entry = partitions, entries
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
            target.write_arrow(stored, commit_row_size=max(stored.num_rows // max(days, 1), 1))
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
            target.write_arrow(commit, commit_row_size=0)
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
    """The log shape again, partitioned by a *transform* of the same column.

    Every partition transform but `identity` hides which rows a partition
    holds, so the table is only addressable as a whole -- and a plan that
    cannot address parts of it has to settle as a whole too. When it did not,
    every run read the table back and wrote it out again, forever.
    """
    field = Log.into_field().into_dataclass("Daily").into_field()
    # `bucket[8]`, because `unix_hour` is an int64 and Iceberg's `day` transform is
    # for dates. The point is unchanged: a transform, not the value itself.
    field.field("unix_hour").is_partition_key = "bucket[8]"
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
        pyarrow.schema([schema.field(name) for name in ("unix", "driver_name", "message")]),
        "Narrow",
    )


def narrow_field() -> Any:
    """Three of the eight columns, as a declared shape rather than a column list."""
    from rekep.fields import Field

    schema = Log.into_field().into_arrow_schema()
    return Field.from_arrow_schema(
        pyarrow.schema([schema.field(name) for name in ("unix", "driver_name", "message")]),
        "Narrow",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--days", type=int, default=8)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument(
        "--only",
        choices=["write", "insert", "polars", "read", "fs", "maintain", "update", "backfill"],
        default=None,
    )
    arguments = parser.parse_args()
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
    if arguments.only in (None, "fs"):
        sweep_fs(min(rows, 100_000), days)
    if arguments.only in (None, "maintain"):
        sweep_maintain(min(rows, 100_000), days)
    if arguments.only in (None, "update"):
        sweep_update(min(rows, 100_000), days)
    if arguments.only in (None, "backfill"):
        sweep_backfill(min(rows, 100_000), days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
