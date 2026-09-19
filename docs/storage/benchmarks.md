# Benchmarks

Reference host, fastest of two warmed runs. Not portable service-level
guarantees: they include local PyIceberg transaction work and fresh
Python/Marimo startup.

## Message parsing

70,000 header-plus-body records, schema and first/last row verified before
timing -- past the 65,536-row native batch boundary.

```bash
cd python
uv run python benchmarks/bench_message.py
```

| source | native rows/s | `Message` rows/s | decoded MiB/s | first batch ms |
| --- | ---: | ---: | ---: | ---: |
| URI local plain | 107,322 | 96,420 | 13.5 | 714 |
| URI local gzip | 98,351 | 89,443 | 12.5 | 689 |

*native* drains header-framed text batches without an output field. *`Message`*
installs `Message.into_field()` on that same native reader, adding conversion to
`timestamp[us, UTC]`, derivation of `timepartition`, the `bodyhash` digest, and
strict final verification. The contract path measured 9–10% below framing
alone on this run; there is no Python row pass between them.

The remaining product work is [on the roadmap](../roadmap/index.md). Staging a
remote object locally does not help ingestion: an earlier diagnostic that
copied gzip to a local resource reached 83,868 rows/s against 97,813 direct in
the same run. `IOBase.buffered()` is a positional-read cache the sequential
record reader bypasses, so a remote source stays streamed directly.

## Message to Iceberg

Three 100,000-row gzip objects in consecutive UTC hours, each parsed by a fresh
task process into one local table: 300,000 rows, three hourly data files, three
snapshots.

| interval | stage rows/s | fresh-process wall rows/s |
| --- | ---: | ---: |
| 1, including table creation | 25,497 | 8,974 |
| 2 | 27,586 | 7,039 |
| 3 | 30,048 | 14,229 |

```text
partition   timepartition_hour: hour → epoch hours 496296-496298
pruning     a one-hour filter read 100,000 rows from one file, pruned two
compression 1.82 MiB gzip → 12.58 MiB decoded, three batches
```

Fresh-process rates are dominated by Python/Marimo startup rather than by the
pipeline. A replay of an interval lands its 100,000 rows again over the ones
it landed -- one more data file and one more snapshot -- and reports them as
written; what that replace costs over an append is measured below.

## Iceberg internals

```bash
cd python
uv run python benchmarks/bench_iceberg.py           # 20,000 rows over four days
uv run python benchmarks/bench_iceberg.py --quick   # 2,500 rows over two days
```

Bounded writes, scans, keyed replacement, maintenance and deletion over
synthetic rows, on pyiceberg 0.12. The test suite smokes the quick write path
only; exhaustive timing is opt-in.

The write sweep reports `peak MiB`: the Arrow high-water mark over one write,
measured through a proxy memory pool installed for that write alone, because
a pool never lowers its high-water mark and a process that has already
allocated would otherwise answer about the process. It is where a writer that
collects a chunk instead of staging it shows up; the wall clock is much the
same either way.

### Writes

20,000 rows of the log shape over four days, read in 16,384-row batches: `one`
commits the stream once, `16,384` once per batch. A replace lands on a table
already holding none, half or all of its rows.

| case | commit rows | seconds | rows/s | peak MiB | files | snapshots |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| append | 16,384 | 0.05 | 401,630 | 1.0 | 5 | 2 |
| append | one | 0.03 | 643,413 | 1.5 | 4 | 1 |
| replace, all new | 16,384 | 0.05 | 391,609 | 1.0 | 5 | 2 |
| replace, all new | one | 0.04 | 539,452 | 1.5 | 4 | 1 |
| replace, half stored | 16,384 | 0.09 | 230,340 | 2.0 | 5 | 3 |
| replace, half stored | one | 0.06 | 354,833 | 2.0 | 4 | 2 |
| replace, replay | 16,384 | 0.12 | 172,504 | 2.0 | 5 | 3 |
| replace, replay | one | 0.07 | 284,577 | 2.0 | 4 | 2 |
| partitions, replay | 16,384 | 0.07 | 293,522 | 2.0 | 5 | 3 |
| partitions, replay | one | 0.05 | 434,380 | 2.0 | 4 | 2 |
| append, no partition | one | 0.03 | 722,526 | 1.0 | 1 | 1 |
| append, iceberg defaults | one | 0.03 | 627,100 | 1.5 | 4 | 1 |
| replace, no partition | one | 0.05 | 439,132 | 2.0 | 1 | 2 |

A replace of rows the table does not hold costs what an append costs: the
key bounds it plans by admit no stored file. A replay reads the keys of every
file it replaces and commits an overwrite snapshot that deletes them, and
lands at about half an append's rate; a keyless replay of the same partitions
deletes them by their manifests without reading a file, and sits between the
two. `snapshots` counts one commit per bounded chunk on top of the preload,
and the peak is the chunk and the one batch of a stored file read beside it.

### A chronological stream

20,000 rows replaced in 3,333-row commits, every commit's keys above every
stored file's: 0.180 s, 110,970 rows/s, seven files and seven snapshots. That
is the cost of the verb when it has nothing to take out -- about 26 ms per
commit, which is the commit itself (a manifest, a metadata file and the
catalog) and not the rows.

### Replacing stored rows

2,000 stored rows in four files, on both composite-key shapes; `planned` is
the files the chunk's key bounds admit, each read by its keys and written
back without the rows it loses.

| key | rows replaced | seconds | rows/s | planned |
| --- | ---: | ---: | ---: | ---: |
| `(symbol, day)`, day repeats | 100 | 0.04 | 2,409 | 1 |
| `(symbol, day)`, day repeats | 500 | 0.06 | 8,513 | 2 |
| `(at, h64)`, nothing repeats | 100 | 0.05 | 1,853 | 1 |
| `(at, h64)`, nothing repeats | 500 | 0.07 | 7,349 | 2 |

The time is the commit and the one or two files rewritten, not the rows: five
times the rows cost a third more.

### Backfill

Ten files of 2,000 rows, each holding one band of keys, with the hash drawn
per row so it prunes nothing on its own. A min/max range is worst on two
distant bands: it plans everything between them.

| replayed | planned | skipped | seconds |
| --- | ---: | ---: | ---: |
| two distant bands | 8 | 2 | 0.09 |
| one band | 2 | 7 | 0.07 |
| half the table | 5 | 4 | 0.10 |

The six files between the two bands are planned but not rewritten: each is
read by its keys alone, found to keep every row, and stands. Measured on
200,000 rows in ten files, best of three, against reading every planned file
whole:

| replayed | whole files | keys first |
| --- | ---: | ---: |
| two distant bands, six files stand | 0.136 s | 0.102 s |
| every file, all go | 0.240 s | 0.241 s |
| half of one file, rewritten | 0.051 s | 0.056 s |

Reading keys first is what a file that stands or goes whole costs, and a few
milliseconds more for one that is rewritten -- the shape a window replay
makes is the first two.

### Deletes

20,000 rows over four days; `planned` is the files the predicate admits.

| case | seconds | removed | rows/s | planned |
| --- | ---: | ---: | ---: | ---: |
| one partition | 0.036 | 5,000 | 137,963 | 2 |
| part of one file | 0.046 | 2,500 | 54,618 | 1 |
| no match | 0.016 | 0 | 0 | 0 |

A file the predicate provably covers whole is deleted unread; one it covers in
part is read and written back without the rows.

## Staged writes

Append over one 70 MiB chunk of 524,288 rows, against the same chunk handed
whole to PyIceberg's writer. Peak is the Arrow high-water mark over the
commit, in chunks.

| partitions | peak, whole | peak, staged |
| ---: | ---: | ---: |
| 1 | 2.01 | 1.07 |
| 4 | 1.78 | 1.52 |
| 24 | 1.21 | 1.15 |

What a replace holds over an append is on the [Iceberg page](iceberg.md):
one batch of the stored file it rewrites, beside the chunk.

Wall time is not in the table because run-to-run spread swamped the
difference on this host. What is systematic is where the time goes: handing a
chunk to PyIceberg's writer submits every partition to a thread pool, and
staging encodes them one after another, each streamed into the store through
that same writer's output stream. Measured on 200,000 rows of 400-byte
payload, best of three:

| partitions | whole: wall, cpu | staged: wall, cpu |
| ---: | --- | --- |
| 4 | 0.157s, 0.273s | 0.173s, 0.172s |
| 8 | 0.132s, 0.205s | 0.177s, 0.175s |
| 32 | 0.156s, 0.264s | 0.215s, 0.213s |

Staging spends less processor time -- it never copies a partition out of the
chunk -- and more wall clock, because it spends it on one thread. A chunk
spanning one or two partitions, which is what this pipeline writes, has
nothing to parallelize either way.

Splitting a chunk by partition inside PyIceberg copies each partition twice and
holds every copy at once, because it submits all of them to its pool before
the first file is written. Staging takes one partition out of the chunk at a
time.
