"""Focused Iceberg commits, scans, replacements and maintenance over synthetic rows."""

from __future__ import annotations

import datetime
import functools
import pathlib
import random
import shutil
import sys
import tempfile
import time
from typing import Annotated, Any

import pyarrow

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import parser, peak_memory, timed  # noqa: E402

from rekep import Convertible, scalar  # noqa: E402
from rekep.fields import (  # noqa: E402
    partition_key,
    primary_key,
    replace_field,
    sort_key,
)
from rekep.iceberg import IcebergCatalog, IcebergDataset  # noqa: E402
from rekep.iceberg.dataset import _key_bounds  # noqa: E402


@scalar
class Quote(Convertible):
    """One quote, under a composite key whose halves both repeat."""

    symbol: Annotated[str, primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, primary_key(), partition_key()]
    """Trading day, and the partition."""

    size: int
    """Quantity."""

    venue: str
    """Where it traded."""


@scalar
class Tick(Convertible):
    """A row under a wide composite key, clustered per commit."""

    at: Annotated[int, primary_key(), sort_key()]
    """A timestamp that advances with the commits."""

    h64: Annotated[int, primary_key()]
    """A hash spread over the whole 62-bit range."""

    payload: str
    """Payload."""


@scalar
class LogRow(Convertible):
    """One benchmark-local stored row with an hourly partition."""

    unix: Annotated[int, primary_key(), sort_key()]
    """Unique nanosecond clock."""

    timepartition: Annotated[int, partition_key()]
    """Whole epoch hour used for identity partitioning."""

    branch: str
    """Low-cardinality source spelling."""

    body: bytes
    """Representative binary payload."""


BRANCHES = ("OMSSales_Enrichment", "ULBridge", "ModuleMarketDataManager", "ObjkeyTagWrapper")
_BASE_UNIX = 1_786_665_600_000_000_000


def log_field(name: str, partition: str | None) -> Any:
    """Clone the log shape with one selected Iceberg partition transform."""
    field = replace_field(LogRow.into_field(), name=name)
    member = field.field("timepartition")
    member.set_partition(False)
    if partition is not None:
        member.iceberg["partition_key"] = partition
    field.set_field(member.name, member)
    return field


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
            "timepartition": [value // _HOUR_NS for value in unix],
            "branch": [BRANCHES[index % len(BRANCHES)] for index in range(rows)],
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


def batches(table: pyarrow.Table, batch_row_size: int) -> pyarrow.RecordBatchReader:
    """The table as a stream, the way a parser hands one over."""
    return table.to_reader(max_chunksize=batch_row_size)


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
    field = LogRow.into_field() if partitioned else log_field("Flat", None)
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
) -> dict[str, Any]:
    """One write configuration, measured on a table of its own.

    `mode` is `append` for the blind write, `replace` for the keyed one and
    `partitions` for the keyless one that empties every partition it touches.
    """
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-"))
    try:
        target = dataset(root, partitioned=partitioned, properties=properties or {})
        if preload is not None:  # something for a replace to take out
            target.append_arrow(preload, commit_row_size=1_000_000)
        if mode == "append":

            def write() -> int:
                return target.append_arrow(
                    batches(table, batch_row_size), commit_row_size=commit_row_size
                )

        else:

            def write() -> int:
                return target.overwrite_arrow(
                    batches(table, batch_row_size),
                    merge_by=mode == "replace",
                    commit_row_size=commit_row_size,
                )

        # What the write held: Arrow's high-water mark over it, which is where
        # a writer that collects its chunk instead of staging it shows up --
        # the wall clock is much the same either way.
        with peak_memory() as peak:
            seconds, _ = timed(write)
            held = peak()
        report = {
            "seconds": seconds,
            "rows": table.num_rows,
            "peak": held / 2**20,
            **stats(target),
        }
        report["stored"] = target.read_arrow_table().num_rows
        # A replace takes out the rows whose keys match, so a preloaded half
        # is already in the count; a keyless one empties the partitions it
        # touches, which here is every one the preload filled.
        if mode == "partitions" or preload is None:
            expected = table.num_rows
        else:
            expected = max(table.num_rows, preload.num_rows)
        assert report["stored"] == expected, report
        return report
    finally:
        shutil.rmtree(root, ignore_errors=True)


def monotonic_replace_case(table: pyarrow.Table, commit_rows: int) -> dict:
    """Replace increasing chunks the way a chronological stream commits them.

    Every chunk's keys sit above every stored file's, so the bounds a replace
    plans by admit no file: this is the cost of the verb when it has nothing
    to take out.
    """
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-stream-"))
    try:
        target = catalog(root).dataset("bench.ticks", field=Tick.into_field()).create_with()

        def write() -> None:
            for start in range(0, table.num_rows, commit_rows):
                target.overwrite_arrow_table(
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
        ("case", "commit rows", "seconds", "rows/s", "peak MiB", "files", "manif", "snaps"),
        (26, 12, 9, 11, 9, 7, 6, 6),
    )

    # A commit closes at the first batch boundary at or beyond its size, so a
    # commit smaller than the reader's batch is one batch: the sweep uses a
    # realistic parser batch (16k rows) and commit sizes around it.
    commits: list[int | None] = [None] if quick else [16_384, None]
    half = table.slice(0, table.num_rows // 2)
    # (label, mode, commit, partitioned, properties, preload)
    configurations: list[tuple] = []
    for commit in commits:
        configurations.append(("append", "append", commit, True, "optimised", None))
    for commit in commits:
        configurations.append(("replace, all new", "replace", commit, True, "optimised", None))
    for commit in commits:
        configurations.append(("replace, half stored", "replace", commit, True, "optimised", half))
    for commit in commits:
        configurations.append(("replace, replay", "replace", commit, True, "optimised", table))
    for commit in commits:
        configurations.append(
            ("partitions, replay", "partitions", commit, True, "optimised", table)
        )
    if not quick:
        for commit in (None,):
            configurations.extend(
                [
                    ("append, no partition", "append", commit, False, "optimised", None),
                    ("append, iceberg defaults", "append", commit, True, "default", None),
                    ("replace, no partition", "replace", commit, False, "optimised", half),
                ]
            )

    for label, mode, commit, partitioned, props, preload in configurations:
        report = write_case(
            table,
            mode=mode,
            commit_row_size=commit,
            partitioned=partitioned,
            properties=OPTIMISED if props == "optimised" else {},
            preload=preload,
        )
        print(
            f"{label:>26} {('one' if commit is None else f'{commit:,}'):>12} "
            f"{report['seconds']:>9.2f} {report['rows'] / report['seconds']:>11,.0f} "
            f"{report['peak']:>9.1f} {report['files']:>7,} {report['manifests']:>6,} "
            f"{report['snapshots']:>6,}"
        )
    return tmp


def sweep_stream(rows: int, repeat: int) -> None:
    """What a chronological stream's replace commits cost."""
    rows = min(rows, 100_000)
    commit_rows = max(rows // 6, 1)
    table = tick_rows(rows)
    # Every row lands, once, before any of them is timed.
    warmed = monotonic_replace_case(table, commit_rows)
    assert warmed["rows"] == rows, warmed
    runs = [monotonic_replace_case(table, commit_rows) for _ in range(repeat)]
    assert all(run["rows"] == rows for run in runs), runs
    best = min(runs, key=lambda run: run["seconds"])
    print(f"\n== chronological replace: {rows:,} rows, {commit_rows:,} per commit ==")
    header(("case", "best sec", "rows/s", "files", "manif", "snaps"), (12, 10, 11, 7, 6, 6))
    print(
        f"{'bounded':>12} {best['seconds']:>10.3f} {rows / best['seconds']:>11,.0f} "
        f"{best['files']:>7} {best['manifests']:>6} {best['snapshots']:>6}"
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
        # spelled out: `timepartition` is whatever hour the first row fell
        # in, and a filter naming an empty partition measures nothing.
        hour = table.column("timepartition")[0].as_py()
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

        branch_filter = EqualTo("branch", "ULBridge")
        print(f"\n== read: {table.num_rows:,} rows, {stats(target)['files']} files ==")
        header(("case", "seconds", "rows", "rows/s", "planned", "skipped"), (30, 9, 12, 12, 8, 8))
        cases = [
            ("everything", None, None, None),
            ("partition = one hour", f"timepartition = {hour}", None, None),
            (
                "partition, 3 columns",
                f"timepartition = {hour}",
                ["unix", "branch", "body"],
                None,
            ),
            ("3 columns, no filter", None, ["unix", "branch", "body"], None),
            ("correlated column", f"unix < {third_day}", None, None),
            ("no stats to prune on", branch_filter, None, None),
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
        header(("partitioning", "run 1", "run 2", "rows"), (30, 8, 8, 10))
        for label, built in (
            (
                "identity (timepartition)",
                lambda root: dataset(root, partitioned=True, properties=OPTIMISED),
            ),
            ("none", lambda root: dataset(root, partitioned=False, properties=OPTIMISED)),
            ("transform (bucket)", lambda root: daily(root)),
        ):
            target = built(tmp / f"settle-{label[:8]}")
            target.append_arrow(batches(table, 2_048), commit_row_size=max(rows // 12, 1))
            runs = [target.compact(min_files=2) for _ in range(2)]
            assert runs[1] == 0, (label, runs)
            assert target.refresh().read_arrow_table().num_rows == rows, label
            print(
                f"{label:>30} {runs[0]:>8,} {runs[1]:>8,} "
                f"{target.refresh().read_arrow_table().num_rows:>10,}"
            )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def sweep_update(rows: int, days: int) -> None:
    """The half of a replace that *rewrites*, on both composite-key shapes.

    A replace of new keys is cheap and measured everywhere else here. One that
    takes stored rows out pays for the files its key bounds admit: each is
    read, joined against the chunk's keys, and written back without them. A
    key whose halves repeat and one whose halves never do bound files
    differently, so both are swept.
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
            print(f"\n== replacing {label}: {stored.num_rows:,} rows ==")
            header(("rows replaced", "seconds", "rows/s", "planned", "files"), (14, 9, 11, 8, 7))
            index = stored.schema.get_field_index(column)
            for count in (100, 500, 2_000):
                if count * 2 > stored.num_rows:
                    continue
                changed = stored.slice(0, count)
                changed = changed.set_column(
                    index,
                    changed.schema.field(column),
                    pyarrow.array([f"V{i}" for i in range(count)]),
                )
                planned = target.scan_plan(_key_bounds(changed, join))["files"]
                seconds, written = timed(
                    functools.partial(target.overwrite_arrow_table, changed, merge_by=join)
                )
                files = target.refresh().iceberg_table.inspect.data_files().num_rows
                print(
                    f"{count:>14,} {seconds:>9.2f} {count / seconds:>11,.0f} "
                    f"{planned:>8,} {files:>7,}"
                )
                assert written == count, written
                target.overwrite_arrow_table(stored.slice(0, count), merge_by=join)  # back
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

    The shape a backfill makes, and the one a min/max range is worst at: two
    distant bands bound everything between them, so the files in between are
    opened to prove they hold none of the keys. What a scan *plans* is the
    number here -- rows returned say nothing about files opened.
    """
    root = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-backfill-"))
    try:
        target = catalog(root).dataset("bench.ticks", field=Tick.into_field()).create_with()
        bands = 10
        per = max(rows // bands, 100)
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
            for band in range(bands)
        ]
        for commit in commits:
            target.append_arrow(commit, commit_row_size=1_000_000)
        stored = target.refresh().data_files().num_rows
        print(f"\n== backfill: {stored} files of {per:,} rows, keys clustered per file ==")
        header(("case", "planned", "skipped", "seconds", "replaced"), (30, 8, 8, 9, 9))
        for label, replay in (
            ("two distant bands", pyarrow.concat_tables([commits[1], commits[-2]])),
            ("one band", commits[3]),
            ("half the table", pyarrow.concat_tables(commits[: bands // 2])),
        ):
            # A replace deletes the files it empties and lands its chunk as
            # new ones, so what the table holds is counted before each case.
            stored = target.refresh().data_files().num_rows
            bounds = _key_bounds(replay, ["at", "h64"])
            plan = target.scan_plan(bounds)
            seconds, replaced = timed(
                functools.partial(target.overwrite_arrow_table, replay, merge_by=True)
            )
            assert replaced == replay.num_rows, (label, replaced)
            assert target.refresh().read_arrow_table().num_rows == per * bands, label
            assert plan["files"] + plan["skipped"] == stored, (label, plan)
            print(
                f"{label:>30} {plan['files']:>8} {plan['skipped']:>8} "
                f"{seconds:>9.2f} {replaced:>9,}"
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


def daily(root: pathlib.Path) -> IcebergDataset:
    """The stored-row shape, partitioned by a *transform* of the same column.

    Every partition transform but `identity` hides which rows a partition
    holds, so the table is only addressable as a whole -- and a plan that
    cannot address parts of it has to settle as a whole too. When it did not,
    every run read the table back and wrote it out again, forever.
    """
    # `bucket[8]`, because `timepartition` is a signed integer and Iceberg's `day`
    # transform is for dates. The point is unchanged: a transform, not the value itself.
    field = log_field("Daily", "bucket[8]")
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
        pyarrow.schema([schema.field(name) for name in ("unix", "branch", "body")]),
        "Narrow",
    )


def narrow_field() -> Any:
    """Three stored columns, as a declared shape rather than a column list."""
    from rekep.fields import Field

    schema = LogRow.into_field().into_arrow_schema()
    return Field.from_arrow_schema(
        pyarrow.schema([schema.field(name) for name in ("unix", "branch", "body")]),
        "Narrow",
    )


def main() -> int:
    options = parser(__doc__, rows=20_000, repeat=2)
    options.add_argument("--days", type=int, default=4)
    options.add_argument(
        "--only",
        choices=[
            "write",
            "stream",
            "read",
            "maintain",
            "update",
            "delete",
            "backfill",
        ],
        default=None,
    )
    arguments = options.parse_args()
    rows = 2_500 if arguments.quick else arguments.rows
    days = 2 if arguments.quick else arguments.days

    if arguments.only in (None, "write"):
        shutil.rmtree(sweep_write(rows, days, arguments.quick), ignore_errors=True)
    if arguments.only in (None, "stream"):
        sweep_stream(rows, 1 if arguments.quick else arguments.repeat)
    if arguments.only in (None, "read"):
        sweep_read(rows, days, 1 if arguments.quick else arguments.repeat)
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
