"""Benchmark TextFile streaming: rows/s, MB/s, and peak Arrow memory."""

from __future__ import annotations

import collections
import contextlib
import gzip
import os
import pathlib
import random
import sys
import tempfile
import threading
import time
import tracemalloc
import warnings

import pyarrow

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import best_of, parser  # noqa: E402

from rekep.fix import (  # noqa: E402
    SOH,
    FixCodec,
    FixRegistry,
    Rules,
    parse_arrow_array,
)

# The message sweep races variants of `parse_arrow_array` against the thing
# itself, so it is built out of the same pieces: the token classifier, the
# offsets healing, the checksum cut and the column sampling, imported rather
# than copied. A benchmark that reimplemented them would be racing two
# parsers rather than two cuts.
from rekep.fix.message import (  # noqa: E402
    _PAIR_TOKEN,
    CHECKSUM,
    _boundaries,
    _column_style,
    _tag_numbers,
    _until_checksum,
    parse_pairs,
)
from rekep.text import FixMsg, TextFile, TextFiles  # noqa: E402
from rekep.text.text_file import (  # noqa: E402
    DEFAULT_BATCH_ROW_SIZE,
    HEADER_PATTERN,
)

PLUGINS = [b"OMSSales_Enrichment", b"ULBridge", b"ModuleMarketDataManager", b"ObjkeyTagWrapper"]
LEVELS = [b"(DEBUG) ", b"(INFO) ", b"(WARNING) ", b""]
TRACE = b"java.lang.IllegalStateException: synthetic\n\tat com.example.A.b(A.java:1)\n"


def generate(path: pathlib.Path, rows: int, trace_every: int = 200, start: int = 0) -> int:
    """Write a synthetic log of `rows` records; returns its size in bytes.

    `trace_every` is how often a multi-line stack trace appears. Folding those
    into the row above is the one piece of per-line work that is not a regex
    match, so a log full of them is the parser's bad case.

    `start` offsets the row numbers, which is what makes a *folder* of these
    files a capture rather than 500 copies of one file: without it every file
    holds the same bytes, and a compressor with a long-range matcher reports a
    ratio that says more about the fixture than about the data.
    """
    with path.open("wb") as out:
        for index in range(rows):
            i = start + index
            second, micro = divmod(i, 1_000_000)
            out.write(
                b"2026-08-14 %02d:%02d:%02d.%03d_%03d [250-e7256476:9effef3e6a:%05d] [%s] %s"
                % (
                    second // 3600 % 24,
                    second // 60 % 60,
                    second % 60,
                    micro // 1000,
                    micro % 1000,
                    72500 + i % 8,
                    PLUGINS[i % len(PLUGINS)],
                    LEVELS[i % len(LEVELS)],
                )
            )
            out.write(
                b"payload %d: ACCOUNT=ACCT-%06d routed XPAR qty=%d\n" % (i, i % 500, i % 10_000)
            )
            if trace_every and index % trace_every == trace_every - 1:
                out.write(TRACE)
    return path.stat().st_size


def run(path: pathlib.Path, batch_row_size: int, read_byte_size: int) -> dict[str, float]:
    """One parse of the whole file; wall time and peak Arrow allocation."""
    base = pyarrow.total_allocated_bytes()
    peak = 0
    rows = 0
    started = time.perf_counter()
    with TextFile.from_path(path) as log:
        reader = log.into_arrow_reader(batch_row_size=batch_row_size, read_byte_size=read_byte_size)
        for batch in reader:
            rows += batch.num_rows
            peak = max(peak, pyarrow.total_allocated_bytes() - base)
    elapsed = time.perf_counter() - started
    return {"rows": rows, "seconds": elapsed, "peak_arrow_mb": peak / 2**20}


def sweep(rows: int, quick: bool) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        plain = pathlib.Path(tmp) / "bench.txt"
        nbytes = generate(plain, rows)
        gz = pathlib.Path(tmp) / "bench.txt.gz"
        gz.write_bytes(gzip.compress(plain.read_bytes(), compresslevel=1))
        gz_mib = gz.stat().st_size / 2**20
        print(f"{rows:,} rows, {nbytes / 2**20:.1f} MiB plain, {gz_mib:.1f} MiB gz")
        columns = ("file", "batch_rows", "read_KiB", "seconds", "rows/s", "MB/s", "peakMiB")
        widths = (6, 10, 9, 8, 10, 7, 8)
        print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

        configs = (
            [(65_536, 1 << 22)]
            if quick
            else [
                (16_384, 1 << 16),
                (65_536, 1 << 16),
                (65_536, 1 << 20),
                (65_536, 1 << 22),
                (65_536, 1 << 23),
                (262_144, 1 << 22),
            ]
        )
        for name, path in (("plain", plain), ("gz", gz)):
            for batch_row_size, read_byte_size in configs:
                result = run(path, batch_row_size, read_byte_size)
                print(
                    f"{name:6} {batch_row_size:>10,} {read_byte_size >> 10:>9,} "
                    f"{result['seconds']:>8.2f} {result['rows'] / result['seconds']:>10,.0f} "
                    f"{nbytes / 2**20 / result['seconds']:>7.1f} {result['peak_arrow_mb']:>8.1f}"
                )


def variants(rows: int, repeat: int) -> None:
    """The axes that are not batch sizes: continuations, hashing, decoding.

    Each is measured on its own file so the comparison is like for like, and
    the throughputs are per *parsed row*, not per byte -- a log stuffed with
    stack traces carries more bytes for the same number of rows.
    """
    print(f"\n{rows:,} rows, best of {repeat}")
    columns = ("case", "MiB", "seconds", "rows/s", "MB/s")
    widths = (28, 7, 9, 12, 8)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        cases: list[tuple[str, pathlib.Path, dict]] = []
        densities = (("no stack traces", 0), ("a trace every 200", 200), ("half traces", 2))
        for label, every in densities:
            path = root / f"traces-{every}.txt"
            generate(path, rows, every)
            cases.append((label, path, {}))
        plain = root / "traces-200.txt"
        cases.append(("no folding", plain, {"fold_continuations": False}))
        cases.append(("64 KiB reads", plain, {"read_byte_size": 1 << 16}))
        cases.append(("64 MiB reads", plain, {"read_byte_size": 1 << 26}))

        gz = root / "traces-200.txt.gz"
        gz.write_bytes(gzip.compress(plain.read_bytes(), compresslevel=1))
        cases.append(("gzip", gz, {}))

        for label, path, options in cases:
            nbytes = path.stat().st_size
            fastest = float("inf")
            for _ in range(repeat):
                started = time.perf_counter()
                TextFile.from_path(path).read_arrow_reader(**options).read_all()
                fastest = min(fastest, time.perf_counter() - started)
            print(
                f"{label:>28} {nbytes / 2**20:>7.1f} {fastest:>9.3f} "
                f"{rows / fastest:>12,.0f} {nbytes / 2**20 / fastest:>8.1f}"
            )


def folders(rows: int, repeat: int) -> None:
    """A capture is a folder, not a file: rotations, an archive, some gzipped.

    The same rows every time, cut into a different number of files, so the
    column that moves is fragmentation and nothing else. `chained by hand` is
    the configuration expected to be bad: the per-file readers concatenated
    with no batch combining, which is what a set of small files costs if the
    short batches are handed downstream as they come.
    """
    print(f"\n{rows:,} rows through a folder, best of {repeat}")
    columns = ("case", "files", "seconds", "rows/s", "batches", "peakMiB")
    widths = (26, 6, 9, 12, 9, 8)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        shapes = (("one", 1), ("few", 20), ("many", 500))
        for name, count in shapes:
            folder = root / name
            folder.mkdir()
            per = rows // count
            for index in range(count):
                # Each file holds its own rows, not a copy of the first file's.
                generate(folder / f"app.{index}.txt", per, start=index * per)
        gzipped = root / "many-gz"
        gzipped.mkdir()
        for source in sorted((root / "many").iterdir()):
            (gzipped / f"{source.name}.gz").write_bytes(
                gzip.compress(source.read_bytes(), compresslevel=1)
            )

        def parse(folder: pathlib.Path, chained: bool = False) -> tuple[int, int, int]:
            """Rows, batches and peak Arrow bytes out of one pass.

            Peak is sampled *inside* the loop: by the time a pass is over the
            batches are released, and a reading taken then says nothing about
            what a consumer had to hold.
            """
            files = TextFiles.from_folder(folder)
            if chained:
                batches = (batch for log in files.into_files() for batch in _drained(log))
            else:
                batches = files.into_arrow_batches()
            base = pyarrow.total_allocated_bytes()
            counted = 0
            seen = 0
            peak = 0
            for batch in batches:
                counted += batch.num_rows
                seen += 1
                peak = max(peak, pyarrow.total_allocated_bytes() - base)
            return counted, seen, peak

        cases = (
            ("one file", root / "one", 1, False),
            ("20 files", root / "few", 20, False),
            ("500 files", root / "many", 500, False),
            ("500 files, chained by hand", root / "many", 500, True),
            ("500 files, gzipped", gzipped, 500, False),
        )
        for label, folder, count, chained in cases:
            parse(folder, chained)  # warm: the first pass in a process pays for the rest
            fastest, batches, peak = float("inf"), 0, 0
            for _ in range(repeat):
                started = time.perf_counter()
                counted, batches, held = parse(folder, chained)
                elapsed = time.perf_counter() - started
                peak = max(peak, held)
                assert counted == rows, f"{label} parsed {counted:,} of {rows:,}"
                fastest = min(fastest, elapsed)
            print(
                f"{label:>26} {count:>6,} {fastest:>9.3f} {rows / fastest:>12,.0f} "
                f"{batches:>9,} {peak / 2**20:>8.1f}"
            )

        _byte_flows(root / "many", repeat)
        _listing(root / "many", repeat)


def _drained(log: TextFile) -> list[pyarrow.RecordBatch]:
    """One file's batches, read and closed, with nothing combined across files."""
    with log:
        return list(log.into_arrow_batches())


def _byte_flows(folder: pathlib.Path, repeat: int) -> None:
    """The other half of a set: shipping the bytes rather than parsing them.

    Peak is Python's own allocation (`tracemalloc`), because these chunks are
    `bytes` and never reach Arrow's allocator. `into_bytes()` is here to be
    bad: it is the same stream with nothing bounding it.
    """
    nbytes = sum(path.stat().st_size for path in folder.iterdir())
    print(f"\n{nbytes / 2**20:.1f} MiB of log bytes, best of {repeat}")
    columns = ("flow", "seconds", "MB/s", "outMiB", "peakMiB")
    widths = (26, 9, 8, 8, 9)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    flows = (
        ("streamed raw", None, lambda files: _consume(files.into_byte_chunks())),
        ("streamed gzip", "gzip", lambda f: _consume(f.into_byte_chunks(compression="gzip"))),
        ("streamed zstd", "zstd", lambda f: _consume(f.into_byte_chunks(compression="zstd"))),
        ("into_bytes (materialised)", None, lambda files: (len(files.into_bytes()), 0)),
    )
    raw = TextFiles.from_folder(folder).into_bytes()
    for label, codec, flow in flows:
        _verify(folder, codec, raw)  # a truncated stream is a better ratio, not a failure
        flow(TextFiles.from_folder(folder))  # warm
        fastest, produced, peak = float("inf"), 0, 0
        for _ in range(repeat):
            tracemalloc.start()
            started = time.perf_counter()
            produced, _ = flow(TextFiles.from_folder(folder))
            elapsed = time.perf_counter() - started
            peak = max(peak, tracemalloc.get_traced_memory()[1])
            tracemalloc.stop()
            fastest = min(fastest, elapsed)
        print(
            f"{label:>26} {fastest:>9.3f} {nbytes / 2**20 / fastest:>8.1f} "
            f"{produced / 2**20:>8.2f} {peak / 2**20:>9.1f}"
        )


def _verify(folder: pathlib.Path, codec: str | None, raw: bytes) -> None:
    """Check a flow's answer before it is timed, outside the timed region.

    A codec stream that lost its trailer decodes short -- and would print as a
    *better* ratio rather than as a failure, which is the one way a benchmark
    can flatter the code it measures.
    """
    files = TextFiles.from_folder(folder)
    if codec is None:
        assert files.into_bytes() == raw, "the raw flow changed the bytes"
        return
    blob = files.into_bytes(compression=codec)
    with pyarrow.CompressedInputStream(pyarrow.BufferReader(blob), codec) as stream:
        assert stream.read() == raw, f"the {codec} flow does not decode back"


def _consume(chunks) -> tuple[int, int]:
    """Bytes and chunks out of a flow, holding neither."""
    produced = 0
    seen = 0
    for chunk in chunks:
        produced += len(chunk)
        seen += 1
    return produced, seen


def _listing(folder: pathlib.Path, repeat: int) -> None:
    """What the walk itself costs, since a scan of a store is not free."""
    files = TextFiles.from_folder(folder)
    fastest = float("inf")
    counted = 0
    for _ in range(repeat):
        started = time.perf_counter()
        counted = sum(1 for _ in files.into_urls())
        fastest = min(fastest, time.perf_counter() - started)
    print(f"\nwalking {counted:,} paths: {fastest * 1000:.1f} ms")


# -- the message layer -------------------------------------------------------


#: What a FIX-carrying capture is made of, by share of lines. Three protocols
#: and not two: the cost of a line the pipeline will *not* parse is the number
#: that says whether classifying first is worth anything, so the majority case
#: has to be in the mix rather than assumed away.
CATEGORY_SHARES: tuple[tuple[str, int], ...] = (("OTHER", 60), ("FIX", 25), ("UL", 15))

#: The separator the generated messages use, stated rather than detected: a
#: benchmark that let each implementation sample the column would be timing
#: `detect_separator` in some of them and not in others.
CAPTURE_SEPARATOR = "|"


def capture(path: pathlib.Path, rows: int, seed: int = 5) -> tuple[int, list[str]]:
    """Write a mixed capture, and say which protocol each row carries.

    `CATEGORY_SHARES` is dealt out deterministically -- a seeded shuffle of one
    hundred slots, repeated -- so the mix is exact rather than approximately
    right, and two runs of the benchmark read the same file.

    **No continuations.** Every line is a record here, so row `i` of the parsed
    stream is line `i` and the per-protocol split needs no classifier at all.
    That is deliberate: the classifier is what the next phase adds, and a
    benchmark that had to guess a protocol would be timing that guess too.
    """
    generate = random.Random(seed)
    slots = [name for name, share in CATEGORY_SHARES for _ in range(share)]
    generate.shuffle(slots)
    protocols: list[str] = []
    with path.open("wb") as out:
        for i in range(rows):
            protocol = slots[i % len(slots)]
            protocols.append(protocol)
            second, micro = divmod(i, 1_000_000)
            out.write(
                b"2026-08-14 %02d:%02d:%02d.%03d_%03d [250-e7256476:9effef3e6a:%05d] [%s] %s"
                % (
                    second // 3600 % 24,
                    second // 60 % 60,
                    second % 60,
                    micro // 1000,
                    micro % 1000,
                    72500 + i % 8,
                    PLUGINS[i % len(PLUGINS)],
                    LEVELS[i % len(LEVELS)],
                )
            )
            out.write(_capture_line(protocol, i, generate).encode() + b"\n")
    return path.stat().st_size, protocols


def _capture_line(protocol: str, i: int, generate: random.Random) -> str:
    """One message payload of each protocol, in the spellings the logs use."""
    if protocol == "FIX":
        fields = [
            "8=FIX.4.2",
            "9=176",
            "35=D",
            f"34={1000 + i}",
            "49=BUYSIDE",
            "56=XPAR",
            "52=20260814-00:05:01.147",
            f"11=ORD-{i:010d}",
            f"55=S{i % 512}",
            "54=1",
            f"38={generate.randint(1, 10_000)}",
            "40=2",
            f"44={generate.random() * 100:.4f}",
            "59=0",
            "10=203",
        ]
        body = CAPTURE_SEPARATOR.join(fields)
        return f"sending >> {body}{CAPTURE_SEPARATOR} << queued seq={1000 + i}"
    if protocol == "UL":
        entries = SOH.join(["PARTYID=BUYSIDE", "PARTYIDSOURCE=D", "PARTYROLE=1"])
        other = SOH.join(["PARTYID=XPAR", "PARTYIDSOURCE=G", "PARTYROLE=17"])
        fields = [
            "#ISINCODE=XX0000084733",
            "#CFICODE=FXXXSX",
            f"#SYMBOL=S{i % 512}",
            "#SIDE=1",
            f"#ORDERQTY={generate.randint(1, 10_000)}",
            f"#PRICE={generate.random() * 100:.4f}",
            "#NOPARTYIDS=2",
            f"#NOPARTYIDS[0]={entries}",
            f"#NOPARTYIDS[1]={other}",
            "#TRANSACTTIME=20260814-00:05:01.148",
            "#UNKNOWNVENUEFIELD=Z9",
        ]
        return "toBridge " + CAPTURE_SEPARATOR.join(fields)
    return f"After Enrichment -> ACCOUNT=ACCT-{i % 500:06d} CLIENTID=MCFP2 VENUE=XPAR qty={i % 997}"


def messages(rows: int, repeat: int, quick: bool) -> None:
    """The hot path of the message layer, per protocol, implementation by implementation.

    Three stages, each raced over the same capture: the line/header split that
    already ships, the cut from a message to its `(key, value)` pairs, and the
    resolution of a rendered key to a FIX tag. The point is not that one of
    them is fast -- it is which implementation each stage should be made of,
    answered against numbers rather than by default.

    Everything is streamed at `DEFAULT_BATCH_ROW_SIZE`: stage one reads the
    whole capture without holding it, and stages two and three race over one
    batch of that size *per protocol*, filled from the same stream, because a
    batch is the unit the parser is handed.
    """
    # A deprecation notice printed between two rows of a table is a table
    # nobody can paste anywhere; the behaviour it warns about is not one this
    # sweep depends on.
    warnings.filterwarnings("ignore", message=".*empty_as_null.*")
    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "capture.txt"
        nbytes, protocols = capture(path, rows)
        counted = collections.Counter(protocols)
        shares = " ".join(f"{name} {counted[name] / rows:.0%}" for name, _ in CATEGORY_SHARES)
        print(f"\n{rows:,} rows, {nbytes / 2**20:.1f} MiB, {shares}, best of {repeat}")

        _split_stage(path, rows, nbytes, repeat)
        _header_stage(path, repeat)
        columns = _protocol_columns(path, protocols, DEFAULT_BATCH_ROW_SIZE if not quick else 8_192)
        _pairs_stage(columns, repeat)
        _tags_stage(columns, repeat)


def _header_stage(path: pathlib.Path, repeat: int) -> None:
    """Stage zero: lines -> the four columns a header carries, two ways.

    The one row-at-a-time loop left in the parser, raced against doing it in
    kernels: one RE2 pass over the whole column, the continuations numbered by
    a cumulative sum and joined back by a group-by. Both answer the same rows,
    which is asserted before either is timed.

    It is here so the answer stays measured. The kernel reading is the shape
    this package prefers everywhere else, and on this fixture it loses: RE2
    walks an alternation of three timestamp shapes over every byte of the
    capture, where the loop stops at the first character of a line that is not
    a header.
    """
    raw = path.read_bytes()
    lines = [line for line in raw.split(b"\n") if line]
    print(f"\n  lines -> header columns, {len(lines):,} lines")
    print(f"    {'implementation':>34} {'lines/s':>12}")
    indices = tuple(
        HEADER_PATTERN.groupindex[name]
        for name in ("timestamp", "thread_name", "plugin_code", "message")
    )

    def by_loop() -> list[tuple[bytes, ...]]:
        rows: list[tuple[bytes, ...]] = []
        match = HEADER_PATTERN.match
        for line in lines:
            found = match(line)
            if found is None:
                if rows:
                    stamp, thread, plugin, message = rows[-1]
                    rows[-1] = (stamp, thread, plugin, (message or b"") + b"\n" + line)
                continue
            rows.append(found.group(*indices))
        return rows

    def by_kernel() -> tuple[pyarrow.Array, ...]:
        compute = pyarrow.compute
        column = pyarrow.array(lines, pyarrow.binary()).cast(pyarrow.string(), safe=False)
        # `(?s)` because RE2 takes no flag argument; the Python twin is
        # compiled `re.DOTALL`.
        found = compute.extract_regex(column, "(?s)" + HEADER_PATTERN.pattern.decode())
        stamps = compute.struct_field(found, "timestamp")
        header = compute.is_valid(stamps)
        # A continuation belongs to the row above it: number the rows by how
        # many headers have been seen, then join each row's messages back.
        row = compute.subtract(compute.cumulative_sum(header.cast(pyarrow.int32())), 1)
        message = compute.if_else(header, compute.struct_field(found, "message"), column)
        grouped = (
            pyarrow.table({"row": row, "message": message})
            .group_by("row", use_threads=False)
            .aggregate([("message", "list")])
        )
        keep = compute.greater_equal(grouped.column("row"), 0)
        return (
            compute.filter(stamps, header),
            compute.filter(compute.struct_field(found, "thread_name"), header),
            compute.filter(compute.struct_field(found, "plugin_code"), header),
            compute.filter(compute.binary_join(grouped.column("message_list"), "\n"), keep),
        )

    looped, kerneled = by_loop(), by_kernel()
    assert len(looped) == len(kerneled[0]), "the two readings found different rows"
    assert [one[3].decode("utf-8", "replace") for one in looped[:64]] == (
        kerneled[3].to_pylist()[:64]
    ), "the two readings carry different messages"
    for label, reading in (("per line, in Python (ships)", by_loop), ("one RE2 pass", by_kernel)):
        seconds = best_of(reading, repeat)
        print(f"    {label:>34} {len(lines) / seconds:>12,.0f}")


def _split_stage(path: pathlib.Path, rows: int, nbytes: int, repeat: int) -> None:
    """Stage one: a whole capture through FIX parsing, with rules on and off.

    Timed here rather than carried over from the tables above because the
    fixture is a different one -- 40% of these lines are messages, and they are
    much longer than the payloads that sweep generates -- and a baseline read
    off another fixture is not a baseline.

    Two rows, because the interesting number is the **difference**: what
    reading every FIX message costs against not reading any. Both read the same
    raw `Message` batches before crossing the `FixMsg` boundary. A rule set
    with no rules in it reads every line as OTHER, which parses nothing, so the
    second row is FIX parsing switched off -- and it is also the configuration
    a caller gets by asking for it.
    """
    print("\n  a whole capture, streamed")
    print(f"    {'configuration':>34} {'rows/s':>12} {'MB/s':>8} {'peak RSS':>10}")
    cases = (
        ("the message layer, on", FixCodec()),
        ("no rules at all", FixCodec(rules=Rules(rules=[]))),
    )
    for label, codec in cases:
        fastest, peak = float("inf"), 0
        for _ in range(repeat):
            before = _rss_bytes()
            with _peak_rss() as sampled:
                started = time.perf_counter()
                seen = 0
                with TextFile.from_path(
                    path,
                    msg_type_event_types=codec.registry.msg_type_event_types(),
                ) as log:
                    for batch in log.into_arrow_batches(batch_row_size=DEFAULT_BATCH_ROW_SIZE):
                        parsed = FixMsg.from_message_arrow_batch(batch, codec)
                        seen += parsed.num_rows
                elapsed = time.perf_counter() - started
            assert seen == rows, f"{seen} rows parsed out of {rows}"
            fastest, peak = min(fastest, elapsed), max(peak, sampled() - before)
        print(
            f"    {label:>34} {rows / fastest:>12,.0f} "
            f"{nbytes / 2**20 / fastest:>8.1f} {_mib(peak):>10}"
        )


def _protocol_columns(
    path: pathlib.Path, protocols: list[str], batch_row_size: int
) -> dict[str, pyarrow.Array]:
    """One batch of messages per protocol, taken off the same streamed capture.

    Bounded by construction: the stream is dropped as soon as every protocol
    has `batch_row_size` rows, so what is held is three batches and not a
    capture. Nothing is done to the lines: the parser reads a bridge's `#`
    marker, its message start and its nested group entries itself now, so the
    column that reaches the race is the column the pipeline gets.
    """
    collected: dict[str, list[str]] = {name: [] for name, _ in CATEGORY_SHARES}
    row = 0
    with TextFile.from_path(path) as log:
        for batch in log.into_arrow_batches(batch_row_size=DEFAULT_BATCH_ROW_SIZE):
            for message in batch.column("message").to_pylist():
                bucket = collected[protocols[row]]
                if len(bucket) < batch_row_size:
                    bucket.append(message)
                row += 1
            if all(len(one) >= batch_row_size for one in collected.values()):
                break
    return {name: pyarrow.array(lines, pyarrow.string()) for name, lines in collected.items()}


def _pairs_stage(columns: dict[str, pyarrow.Array], repeat: int) -> None:
    """Stage two: message -> pairs, four implementations over the same column.

    The scalar tokenizer is the reference and every other implementation is
    asserted equal to it, pair for pair, before anything is timed -- so a row
    in this table is a row that gave the right answer.

    `numpy` is a variant of the Arrow path with one thing changed: the
    `split_pattern` + two `list_element` cut replaced by offsets arithmetic
    over the flattened token buffer. It exists for the tag-mode cut and only
    there, because the named path builds its keys with `extract_regex` and has
    no `list_element` to replace.
    """
    print("\n  message -> pairs")
    header = ("protocol", "implementation", "rows/s", "pairs/s", "peak RSS")
    print(f"    {header[0]:>9} {header[1]:>26} {header[2]:>12} {header[3]:>12} {header[4]:>10}")
    for name, column in columns.items():
        reference = [parse_pairs(line, CAPTURE_SEPARATOR) for line in column.to_pylist()]
        pairs = sum(len(one) for one in reference)
        candidates: list[tuple[str, object]] = [
            ("parse_pairs", lambda c=column: _scalar_pairs(c)),
            ("parse_arrow_array", lambda c=column: parse_arrow_array(c, CAPTURE_SEPARATOR)),
            ("numpy over the buffers", lambda c=column: _numpy_pairs(c)),
            ("polars", lambda c=column: _polars_pairs(c)),
        ]
        for label, run in candidates:
            try:
                answer = run()
            except _NotApplicable as why:
                print(f"    {name:>9} {label:>26} {'n/a':>12} {'':>12} {str(why):>10}")
                continue
            assert _as_pairs(answer) == reference, (
                f"{label} disagrees with the scalar parser on {name}"
            )
            fastest, peak = _best_of(run, repeat)
            print(
                f"    {name:>9} {label:>26} {len(column) / fastest:>12,.0f} "
                f"{pairs / fastest:>12,.0f} {_mib(peak):>10}"
            )


class _NotApplicable(Exception):
    """An implementation that cannot run here, or has nothing to replace."""


def _optional(name: str) -> object:
    """The module, or `_NotApplicable` -- a race one runner cannot enter is not a failure."""
    import importlib

    try:
        return importlib.import_module(name)
    except ImportError as error:
        raise _NotApplicable(f"no {name}") from error


def _scalar_pairs(column: pyarrow.Array) -> list[list[tuple[str, str]]]:
    return [parse_pairs(line, CAPTURE_SEPARATOR) for line in column.to_pylist()]


def _numpy_pairs(column: pyarrow.Array) -> pyarrow.MapArray:
    """`parse_arrow_array`'s tag-mode body with the cut done in numpy.

    Every kernel here is the shipped one; the two `list_element` calls are what
    is replaced. The cut does **not** trim whitespace, which the shipped one
    does -- so this is the faster half of a choice already raced in
    `bench_fix.py`, and it only agrees where the tokens carry no padding. The
    assertion above is what keeps that honest.
    """
    numpy = _optional("numpy")

    compute = pyarrow.compute
    values = column.cast(pyarrow.string(), safe=False)
    begun = compute.struct_field(
        compute.extract_regex(values, r"(?s)(?:^|[^0-9])(?P<msg>8=FIXT?.*)"), "msg"
    )
    values = compute.if_else(compute.is_null(begun), values, begun)
    tokens = compute.split_pattern(values, CAPTURE_SEPARATOR)
    flat = tokens.values
    if _column_style(column)[1]:
        raise _NotApplicable("no cut")
    matched = compute.fill_null(compute.match_substring_regex(flat, _PAIR_TOKEN), False)
    kept = compute.filter(flat, matched)
    tags, entries = _numpy_cut(kept, numpy)
    counted = compute.cumulative_sum(matched.cast(pyarrow.int32()))
    bounds = pyarrow.concat_arrays([pyarrow.array([0], pyarrow.int32()), counted])
    offsets = bounds.take(_boundaries(tokens))
    parents = compute.filter(compute.list_parent_indices(tokens), matched)
    tags, entries, offsets = _until_checksum(tags, entries, offsets, parents, named=False)
    return pyarrow.MapArray.from_arrays(offsets, tags, entries)


def _numpy_cut(kept: pyarrow.Array, numpy) -> tuple[pyarrow.Array, pyarrow.Array]:
    """`token -> (key, value)` as index arithmetic over the flattened buffer.

    One pass to find every `=` byte, a `searchsorted` to give each token its
    own first one, and one ragged gather per half -- the `repeat` + `cumsum`
    index the rest of this package builds an interleave from, in numpy rather
    than in Arrow.
    """
    if not len(kept):
        empty = pyarrow.array([], pyarrow.string())
        return empty, empty
    _, offsets_buffer, data_buffer = kept.buffers()[:3]
    offsets = numpy.frombuffer(offsets_buffer, dtype=numpy.int32, count=len(kept) + 1)
    data = numpy.frombuffer(data_buffer, dtype=numpy.uint8)
    starts = offsets[:-1].astype(numpy.int64)
    ends = offsets[1:].astype(numpy.int64)
    marks = numpy.flatnonzero(data[: int(ends[-1])] == 0x3D)
    first = marks[numpy.searchsorted(marks, starts, side="left")]
    return (
        _gathered(data, starts, first - starts, numpy),
        _gathered(data, first + 1, ends - first - 1, numpy),
    )


def _gathered(data, starts, lengths, numpy) -> pyarrow.Array:
    """A string array of `data[start : start + length]` per row, in one take."""
    bounds = numpy.concatenate(([0], numpy.cumsum(lengths)))
    index = (
        numpy.arange(int(bounds[-1]), dtype=numpy.int64)
        - numpy.repeat(bounds[:-1], lengths)
        + numpy.repeat(starts, lengths)
    )
    return pyarrow.StringArray.from_buffers(
        len(starts),
        pyarrow.py_buffer(bounds.astype(numpy.int32).tobytes()),
        pyarrow.py_buffer(data[index].tobytes()),
    )


#: `_TOKEN_VECTOR` and `_MEMBER_VECTOR` in the Rust regex crate's spelling of
#: the same grammar -- named groups as `(?<name>)`, the dot-matches-newline
#: flag scoped to the group it is needed in. Written out rather than reused so
#: the polars row is polars' own work and not a translation cost.
_POLARS_TOKEN = r"^[ \t\r\n\x0b\x0c]*(?<key>\d+)[ \t\r\n\x0b\x0c]*=(?<rest>(?s:.*))$"
_POLARS_TOKEN_NAMED = (
    r"^[ \t\r\n\x0b\x0c]*(?<key>\d+|[A-Za-z][A-Za-z0-9_.\-]*)"
    r"(?:\[(?<index>\d+)\])?"
    r"(?:\.(?<member>[A-Za-z0-9_.\-]+))?"
    r"[ \t\r\n\x0b\x0c]*=(?<rest>(?s:.*))$"
)
_POLARS_MEMBER = (
    r"^[ \t\r\n\x0b\x0c]*(?<member>\d+|[A-Za-z][A-Za-z0-9_.\-]*)"
    r"[ \t\r\n\x0b\x0c]*=(?<value>(?s:.*))$"
)


def _polars_pairs(column: pyarrow.Array) -> object:
    """The same cut as one polars expression chain, ending in one list per row.

    `str.split` then `str.extract_groups`, which is the shape the question was
    asked in -- and then the two things the Arrow path also does and a race
    that skipped them would be flattering: the checksum cut, and the regroup
    back to one row per line. A token that matches nothing is dropped, and a
    line left with no pairs comes back as an empty list, which is what the map
    column holds for it.
    """
    polars = _optional("polars")

    named, entry_separator = _column_style(column)[1:]
    if entry_separator is not None:
        raise _NotApplicable("no entries")
    frame = polars.DataFrame({"line": polars.from_arrow(column)}).with_row_index("row")
    if not named:
        frame = frame.with_columns(
            polars.coalesce(
                polars.col("line").str.extract(r"(?s)(?:^|[^0-9])(8=FIXT?.*)", 1),
                polars.col("line"),
            ).alias("line")
        )
    cut = (
        frame.with_columns(polars.col("line").str.split(CAPTURE_SEPARATOR).alias("token"))
        .explode("token")
        .with_columns(
            polars.col("token")
            .str.extract_groups(_POLARS_TOKEN_NAMED if named else _POLARS_TOKEN)
            .alias("cut")
        )
        .unnest("cut")
        .filter(polars.col("key").is_not_null())
    )
    if named:
        inner = polars.col("rest").str.extract_groups(_POLARS_MEMBER)
        grouped = polars.col("index").is_not_null() & polars.col("member").is_null()
        cut = cut.with_columns(inner.alias("inner")).with_columns(
            polars.when(grouped & polars.col("inner").struct.field("member").is_not_null())
            .then(polars.col("inner").struct.field("member"))
            .otherwise(polars.col("member"))
            .alias("member"),
            polars.when(grouped & polars.col("inner").struct.field("member").is_not_null())
            .then(polars.col("inner").struct.field("value"))
            .otherwise(polars.col("rest"))
            .alias("rest"),
        )
        cut = cut.with_columns(
            polars.concat_str(
                polars.col("key"),
                polars.when(polars.col("index").is_not_null())
                .then(polars.concat_str(polars.lit("["), polars.col("index"), polars.lit("]")))
                .otherwise(polars.lit("")),
                polars.when(polars.col("member").is_not_null())
                .then(polars.concat_str(polars.lit("."), polars.col("member")))
                .otherwise(polars.lit("")),
            ).alias("key")
        )
    checksum = (polars.col("key") == CHECKSUM).cast(polars.Int32)
    cut = cut.with_columns(checksum.cum_sum().over("row").alias("seen")).filter(
        polars.col("seen") - checksum == 0
    )
    pairs = cut.group_by("row").agg(
        polars.struct(
            polars.col("key"),
            polars.col("rest").str.strip_chars(" \t\r\n\x0b\x0c").alias("value"),
        ).alias("pairs")
    )
    return (
        frame.select("row")
        .join(pairs, on="row", how="left")
        .sort("row")
        .with_columns(
            polars.col("pairs").fill_null(polars.lit([], dtype=polars.List(polars.Struct)))
        )
    )


def _as_pairs(answer: object) -> list[list[tuple[str, str]]]:
    """Whatever an implementation returned, as the scalar parser's own pairs."""
    if isinstance(answer, list):
        return answer
    if isinstance(answer, pyarrow.Array):
        return [
            [] if row is None else [(key, value) for key, value in row]
            for row in answer.to_pylist()
        ]
    rows = answer.get_column("pairs").to_list()
    return [
        [] if row is None else [(entry["key"], entry["value"]) for entry in row] for row in rows
    ]


def _tags_stage(columns: dict[str, pyarrow.Array], repeat: int) -> None:
    """Stage three: a rendered name -> the tag FIX gave it, over a whole column.

    The keys come off the bridge column's own pairs, reduced to their trailing
    name segment **outside the clock** -- `NOPARTYIDS[0].PARTYID` says where a
    field sits, not what it is, and stripping the decoration is the same one
    kernel for every implementation. What is raced is the resolution itself.

    The dictionary is the real one (`data/fix.zip`), because how many names are
    in it is exactly what separates a hash probe from a join.
    """
    keys = _bridge_keys(columns["UL"])
    names = _dictionary()
    if not names:
        print("\n  pairs -> tags: skipped, no FIX dictionary at data/fix.zip")
        return
    print(f"\n  pairs -> tags ({len(keys):,} keys, {len(names):,} names)")
    print(f"    {'implementation':>34} {'keys/s':>12} {'resolved':>10} {'peak RSS':>10}")
    lowered = pyarrow.compute.utf8_lower(keys)
    candidates = (
        ("_tag_numbers (what ships)", lambda: _tag_numbers(lowered, names, pyarrow.int32())),
        ("python dict over to_pylist", lambda: _python_tags(lowered, names)),
        ("pyarrow.compute.index_in", lambda: _index_in_tags(lowered, names)),
        ("polars join", lambda: _polars_tags(lowered, names)),
    )
    reference = None
    for label, run in candidates:
        answer = run()
        if reference is None:
            reference = answer.to_pylist()
        assert answer.to_pylist() == reference, f"{label} resolves differently"
        fastest, peak = _best_of(run, repeat)
        resolved = len(reference) - reference.count(None)
        print(
            f"    {label:>34} {len(keys) / fastest:>12,.0f} "
            f"{resolved / len(reference):>10.0%} {_mib(peak):>10}"
        )


def _bridge_keys(column: pyarrow.Array) -> pyarrow.Array:
    """Every key the bridge column parses to, decoration stripped."""
    maps = parse_arrow_array(column, CAPTURE_SEPARATOR)
    listed = maps.cast(
        pyarrow.list_(pyarrow.struct([("key", pyarrow.string()), ("value", pyarrow.string())]))
    )
    keys = pyarrow.compute.struct_field(pyarrow.compute.list_flatten(listed), 0)
    return pyarrow.compute.extract_regex(keys, r"(?P<name>[A-Za-z0-9_\-]+)(?:\[\d+\])?$").field(
        "name"
    )


def _dictionary() -> dict[str, int]:
    """`{name: tag}` out of the dictionary this repository publishes."""
    archive = pathlib.Path(__file__).resolve().parent.parent.parent / "data" / "fix.zip"
    if not archive.exists():
        return {}
    return FixRegistry(cache_dir=archive).tags()


def _python_tags(lowered: pyarrow.Array, names: dict[str, int]) -> pyarrow.Array:
    return pyarrow.array([names.get(key) for key in lowered.to_pylist()], pyarrow.int32())


def _index_in_tags(lowered: pyarrow.Array, names: dict[str, int]) -> pyarrow.Array:
    compute = pyarrow.compute
    known = pyarrow.array(list(names), pyarrow.string())
    tags = pyarrow.array(list(names.values()), pyarrow.int32())
    return compute.take(tags, compute.index_in(lowered, value_set=known))


def _polars_tags(lowered: pyarrow.Array, names: dict[str, int]) -> pyarrow.Array:
    polars = _optional("polars")

    frame = polars.DataFrame({"key": polars.from_arrow(lowered)})
    table = polars.DataFrame(
        {"name": list(names), "tag": polars.Series(list(names.values()), dtype=polars.Int32)}
    )
    joined = frame.join(table, left_on="key", right_on="name", how="left")
    return joined.get_column("tag").to_arrow().cast(pyarrow.int32())


def _best_of(run, repeat: int) -> tuple[float, int]:
    """Fastest of `repeat` timed calls, and the most RSS any of them added.

    The untimed warm-up counts toward the peak and not toward the clock. That
    is the one call that pays for a first touch -- an allocator that has not
    grown yet, a lazily imported module, a kernel's own one-off state -- and
    leaving it out reported 0.0 MiB for a path that had plainly allocated,
    because every later run was answered out of an arena the first one grew.
    """
    fastest, peak = float("inf"), 0
    for attempt in range(repeat + 1):
        before = _rss_bytes()
        with _peak_rss() as sampled:
            started = time.perf_counter()
            run()
            elapsed = time.perf_counter() - started
        if attempt:
            fastest = min(fastest, elapsed)
        peak = max(peak, sampled() - before)
    return fastest, peak


@contextlib.contextmanager
def _peak_rss():
    """The highest resident set size seen while the body runs, sampled.

    Arrow's own allocator is what every table above reports and it is exact --
    but it cannot see a numpy buffer or a polars frame, and this sweep races
    three allocators against each other. RSS is the one figure all three land
    in. It is *sampled*, so a spike shorter than the interval is missed, and it
    is read from `/proc`, so it is a Linux number: elsewhere the column reads
    `n/a` rather than a zero that would look like an answer.
    """
    peak = [_rss_bytes()]
    stop = threading.Event()

    def watch() -> None:
        while not stop.wait(0.001):
            peak[0] = max(peak[0], _rss_bytes())

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        yield lambda: peak[0]
    finally:
        stop.set()
        watcher.join()
        peak[0] = max(peak[0], _rss_bytes())


def _rss_bytes() -> int:
    """Resident bytes right now, or 0 where the OS does not say."""
    try:
        with open("/proc/self/statm") as handle:
            return int(handle.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError, AttributeError):
        return 0


def _mib(peak: int) -> str:
    """A byte count as MiB, or `n/a` where nothing measured it."""
    return f"{peak / 2**20:.1f}" if peak > 0 else "n/a"


def stamps(rows: int, repeat: int) -> None:
    """Reading a column of header timestamps, per shape a capture may write.

    The one stage the accepted-spelling set decides. Every shape is sliced
    from the offsets its declaration gives, so what this measures is whether
    admitting three shapes rather than one costs the common case anything --
    and what a batch mixing them costs, which is the case that used to be read
    a row at a time.
    """
    from rekep.text.text_file import _local_micros
    from rekep.times import unix_of

    shaped = {
        "iso millis": "2026-08-14 00:05:01.147",
        "iso micros": "2026-08-14 00:05:01.147250",
        "iso split": "2026-08-14 00:05:01.147_250",
        "iso nanos": "2026-08-14 00:05:01.147250123",
        "iso seconds": "2026-08-14 00:05:01",
        "fix millis": "20260814-00:05:01.147",
        "compact millis": "20260814000501147",
        "compact seconds": "20260814000501",
    }
    print(f"\n{rows:,} header stamps, best of {repeat}")
    columns = ("shape", "seconds", "stamps/s")
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, (16, 9, 12), strict=True)))
    for name, spelled in shaped.items():
        column = [spelled.encode()] * rows
        # Verified before it is timed: a fast path that answers wrongly is not
        # a fast path. The scalar reader is the reference.
        expected = unix_of(spelled)
        found = _local_micros(column[:1])[0].as_py()
        assert unix_of(found) == expected, f"{name} reads {found}, not {spelled}"
        seconds, _ = _best_of(lambda column=column: (_local_micros(column), rows)[1], repeat)
        print(f"{name:>16} {seconds:>9.3f} {rows / seconds:>12,.0f}")
    mixed = [spelled.encode() for spelled in shaped.values()] * (rows // len(shaped))
    found = _local_micros(mixed)
    assert [unix_of(one.as_py()) for one in found[: len(shaped)]] == [
        unix_of(spelled) for spelled in shaped.values()
    ], "a mixed column reads every shape as itself"
    seconds, _ = _best_of(lambda: (_local_micros(mixed), len(mixed))[1], repeat)
    print(f"{'all eight mixed':>16} {seconds:>9.3f} {len(mixed) / seconds:>12,.0f}")


def main() -> int:
    options = parser(__doc__, rows=200_000, repeat=3)
    options.add_argument(
        "--only",
        choices=("sweep", "variants", "folders", "messages", "stamps"),
        default=None,
    )
    arguments = options.parse_args()
    rows = 50_000 if arguments.quick else arguments.rows
    repeat = 1 if arguments.quick else arguments.repeat
    if arguments.only in (None, "sweep"):
        sweep(rows, arguments.quick)
    if arguments.only in (None, "variants"):
        variants(rows, repeat)
    if arguments.only in (None, "folders"):
        folders(rows, repeat)
    if arguments.only in (None, "stamps"):
        stamps(rows, repeat)
    if arguments.only in (None, "messages"):
        messages(rows, repeat, arguments.quick)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
