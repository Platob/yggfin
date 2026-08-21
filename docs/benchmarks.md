# Benchmarks

Two benchmarks ship with the package, and every number below came out of them on
one machine, **measured twice**: what reproduced is stated as a number, what did
not is called noise.

```bash
cd python
uv run python benchmarks/bench_text_file.py     # parsing a log
uv run python benchmarks/bench_iceberg.py       # parse, stream into Iceberg, read back
uv run python benchmarks/bench_iceberg.py --only maintain   # the maintenance
uv run python benchmarks/bench_iceberg.py --only update     # the half that rewrites
uv run python benchmarks/bench_iceberg.py --only backfill   # replaying clustered keys
uv run python benchmarks/bench_cast.py          # casting data onto a shape
```

The Iceberg numbers use a local SQLite catalog and a file warehouse, so they are
storage-latency-free: they measure planning, commit and Arrow work, which is
what this package is responsible for. On an object store every commit also pays
a round trip — which makes the number of commits matter *more*, not less. What
those round trips add up to is measured separately:
`bench_iceberg.py --only fs` counts every store call each flow makes
([the results](#what-the-store-is-asked)).

## Parsing

400,000 rows, 54.4 MiB of synthetic log, best of three.

| case | seconds | rows/s |
| --- | --- | --- |
| `read_arrow_table` | 0.99–1.12 | 358k–402k |
| reader, 16k batches | 0.94–1.10 | 364k–425k |
| reader, 256k batches | 1.00–1.31 | 306k–399k |
| with `timezone="Europe/Paris"` | 1.00–1.05 | 380k–399k |
| no continuation folding | 0.97–1.01 | 396k–412k |

Every configuration lands between 306k and 425k rows/s: naming a timezone
(one `assume_timezone` kernel per batch) and folding wrapped lines both cost
less than the spread between two runs of the same configuration, and batch size
does not separate them either. The parser is bound by the per-line regex, not
by any of these.

What *does* move it, on a million rows, best of three:

| case | rows/s | vs the baseline |
| --- | --- | --- |
| a stack trace every 200 lines (the baseline) | 393k | — |
| no stack traces at all | 393k | none |
| a trace every other line | 322k | −18% |
| no continuation folding | 410k | +4% |
| 64 KiB reads | 375k | −5% |
| 64 MiB reads | 291k | −26% |
| **blake2b line hash** (no `xxhash` installed) | 264k | **−33%** |
| gzip (12.5 MiB instead of 136 MiB) | 379k | −4% |

Two things to take from that. Folding is cheap until continuations dominate:
a trace every 200 lines costs nothing measurable, one every other line costs
18%, because folding is the only per-line work that is not a regex match.
And the `fast` extra is worth a third of the parser -- `pip install
"rekep[fast]"` swaps blake2b for xxhash. (The two hashes are not
interchangeable: `hash64` is stable within an environment, not across
environments that differ in whether xxhash is installed.)

Read size has a floor and a ceiling: 64 KiB is syscall-bound, 64 MiB spends
more time waiting for a whole read than parsing it. The 4 MiB default sits
where both are flat. Compression is close to free in rows/s -- Arrow decodes in
its C++ layer while the row loop is the bottleneck -- so a gzipped log parses
at nearly the same rate from a tenth of the bytes.

## Streaming into Iceberg

400,000 parsed rows over 8 days, partitioned by day, written from a reader whose
batches are 16,384 rows.

### How much a commit costs

| commit rows | seconds | rows/s | files | manifests | snapshots |
| --- | --- | --- | --- | --- | --- |
| 16,384 | 1.9–2.7 | 148k–213k | 32 | 7 | 25 |
| 65,536 | 0.50–0.64 | 623k–797k | 14 | 7 | 7 |
| 262,144 | 0.23–0.28 | 1.4M–1.7M | 9 | 2 | 2 |
| 1,000,000 | 0.26–0.32 | 1.3M–1.6M | 8 | 1 | 1 |
| the whole stream | 0.27–0.51 | 790k–1.5M | 8 | 1 | 1 |

Twenty-five commits leave **7** manifests rather than 25, because
`commit.manifest.min-count-to-merge` is set: Iceberg's own default waits for a
hundred manifests before merging any, which no stream of this size ever
reaches.

A commit is a file, a manifest and a snapshot, and every later scan pays for all
three — planning is linear in the number of files. That is why
`commit_row_size` defaults to a million rows rather than to the batch: below
about 250k rows the commits, not the data, are the work.

!!! note "A commit cannot be smaller than a batch"

    Chunks close at the first batch boundary at or beyond `commit_row_size`, so
    16,384 above is "one batch per commit". Asking for less changes nothing.

### Appending, merging, and what pyiceberg's own upsert costs

| case | commit rows | seconds | rows/s |
| --- | --- | --- | --- |
| append | 1,000,000 | 0.26–0.32 | 1.3M–1.6M |
| merge, every key new | 1,000,000 | 0.28–0.71 | 564k–1.4M |
| merge, half already stored | 1,000,000 | 0.76–0.89 | 449k–527k |
| **merge through `Table.upsert`** | one commit | 11.3–11.6 (for **4,000** rows) | **344–354** |

The last row is not a typo and not the same amount of work: pyiceberg's own
upsert was given a hundredth of the data and still took twenty times longer than
the full merge above it. Its scan filter carries one equality term per incoming
row, so the cost grows faster than the chunk:

| chunk rows | 1 join column | 2 join columns |
| --- | --- | --- |
| 500 | 6,200 rows/s | 700 rows/s |
| 1,000 | 9,100 rows/s | 730 rows/s |
| 2,000 | 7,100 rows/s | 590 rows/s |
| 4,000 | 10,700 rows/s | 440 rows/s |

Head to head on the same table (4,000-row chunk, 20,000-row table), with
identical results:

| scenario | planned merge | `Table.upsert` |
| --- | --- | --- |
| every key new | 0.09 s | 2.47 s (28×) |
| every key stored, values unchanged | 0.20 s | 9.83 s (48×) |
| half new, half unchanged | 0.20 s | 8.12 s (42×) |

Those are the streaming shapes: new data, and replays of data that has not
changed. A merge where most rows genuinely *change* is a different story —
about 2× — because the delete half still carries pyiceberg's exact per-row
filter, which it must: a range there would delete rows the chunk never touched.
Finding the rows is what got fast; rewriting them costs what it costs.

[How the merge is planned](iceberg.md#how-a-merge-is-planned) explains why. The
two paths are compared row by row in `tests/iceberg/test_coherence.py`; set
`plan_merges=False` to use the library's own.

Building that scan filter is itself measured, because it runs once per commit
and it used to hash the whole key column to decide it could not name the values
in it. Probing a 201-row slice first answers the same question, on a 400k-row
chunk:

| merge key | before | after |
| --- | --- | --- |
| one high-cardinality integer | 27.0 ms | 0.4 ms |
| an integer and a string | 69.9 ms | 6.3 ms |
| one eight-value partition column | 1.3 ms | 1.5 ms |

The last row is the tax: where there really are few distinct values, the probe
is paid on top of the full pass — and that is the case where naming them one by
one prunes to exactly the right partitions, so it is worth paying.

### Partitioning and properties

| case | commit rows | rows/s | files |
| --- | --- | --- | --- |
| append, partitioned by day | 65,536 | 623k–797k | 14 |
| append, no partition | 50,000 | 1.01M–1.03M | 7 |
| append, Iceberg's default properties | 50,000 | 802k–852k | 14 |
| merge, no partition | 50,000 | 506k | 5 |

Partitioning costs about half the write throughput here, because eight days
means up to eight files per commit instead of one. It buys the read below.

### What the store is asked

Seconds on a local disk cannot show what a scan-per-chunk flow does to S3, so
this sweep counts **store calls** instead, on the file handles themselves —
below the FileIO cache, so a count is a call the store actually served: one
`open` is a GET, one `create` a PUT. 100,000 rows streamed in 8 commits,
measured twice with identical counts:

| flow | GETs, cache off | GETs, cache on |
| --- | --- | --- |
| append stream | 13 | **0** |
| merge, every key new | 40 | **0** |
| merge, half stored | 33 | 5 |
| insert-only, full replay | 24 | 10 |
| read everything | 18 | 10 |
| read one partition | 5 | 2 |
| read `limit=100` | 9 | **1** |
| `scan_plan` one partition | 11 | **0** |
| optimize (compact + sweep) | 71 | 10 |

Three separate changes produce that column, and they compose:

- **The content cache** ([what the store is asked](iceberg.md#what-the-store-is-asked)):
  manifests, manifest lists and `metadata.json` are immutable, so `ArrowFileIO`
  serves them from memory after the first fetch — and caches them *as they are
  written*, which is why a pure append stream makes zero GETs: every file the
  next chunk's plan wants is one this process just wrote. Over the sweep: 214
  hits, 0 misses, ~500 KiB held.
- **Plan-once merges**: a merge plans its scan once, and a plan of zero files
  commits the chunk as an append with no reader built — so `merge, all new`
  matches `append stream` exactly. (`Table.upsert` reads the table either way.)
- **Limit-aware planning**: with no filter, `limit` cuts the *plan*, not just
  the rows — one file opened where pyiceberg submits all eight.

The wall-clock is not the point on a local disk — the counts are exact and
reproduce to the call, the seconds are noisy — but it moves the same way: the
cached append stream landed in 0.29–0.32 s against 0.95–1.6 s uncached across
three runs. On an object store, where every one of those calls is a round
trip, the counts *are* the seconds.

## Reading it back

500,000 rows in 15 files, best of three, **warmed twice** before anything is
timed — once for the process and once per case. Without that the sweep was a
story about warm-up: three back-to-back runs put "everything" at 0.057, 0.031
and 0.027 s, a 2.1× spread that is nothing but an Acero join and a parquet
reader paying their setup in whichever case ran first.

`planned` is how many files the scan opened; `skipped` is what the filter saved.
The counts reproduce exactly; the seconds are a shared machine and move ±30%
between runs, so both runs are quoted.

| case | seconds | rows | planned | skipped |
| --- | --- | --- | --- | --- |
| everything | 0.080–0.111 | 500,000 | 15 | 0 |
| `date = '2026-08-14'` (partition) | 0.024–0.025 | 62,500 | 1 | 14 |
| partition + 3 of 8 columns | 0.016–0.019 | 62,500 | 1 | 14 |
| 3 of 8 columns, no filter | 0.055–0.061 | 500,000 | 15 | 0 |
| `recorded_at_unix < …` (correlates with the partition) | 0.042–0.062 | 125,000 | 3 | 12 |
| `driver_name = 'ULBridge'` (no useful statistics) | 0.093–0.105 | 125,000 | 15 | **0** |
| narrow shape, projection from the shape | 0.058–0.064 | 500,000 | 15 | 0 |
| narrow shape declared with the store's widths | 0.050–0.059 | 500,000 | 15 | 0 |

One more, measured separately because it is a write-side choice: sorting each
commit on the column a read filters. On a single 600k-row commit, a filter
matching the top 5% of `recorded_at_unix` values took **214 ms** when the rows arrived
shuffled and **22 ms** when the commit was sorted (`sort_by=["recorded_at_unix"]`), with
one file planned in both cases. That is row-group skipping inside the file, and
it only exists because `write.parquet.row-group-limit` is set: Iceberg's
default of a million rows per group would make the whole file one group with
nothing to skip.

Three things worth taking away:

- **A partition filter is worth 14 of 15 files.** A filter on a column that
  merely *correlates* with the partition still skips 12 — Iceberg prunes on
  per-file column bounds, not only on partitions.
- **A filter that cannot prune says nothing about it.** The `driver_name` filter
  returns exactly the right rows and reads every file. `scan_plan` is how you
  see that:

    ```python
    quotes.scan_plan("driver_name = 'ULBridge'")["skipped"]   # 0
    ```

- **Declaring narrower widths than the store costs a conversion per row —
  less than this page used to claim.** Isolated, warm and best-of-nine, twice:
  three columns into `string` took 60.2–64.4 ms against 55.9–61.6 ms in the
  store's own `large_string`, with medians 68.6–72.8 against 62.0–67.8. That is
  7–9%, consistently in the same direction, and not the 25% an unwarmed sweep
  reported — the difference was warm-up charged to whichever case ran first.
  Declaring the store's widths (`dataset.table_field`) is still the right
  answer where a read is hot and the shape is only there to select columns; it
  is not the difference it looked like.

## Maintenance, and what a reader holds

`bench_iceberg.py --only maintain`. Three things seconds on a local disk answer
badly and counts answer exactly. Same fixture, both legs run twice, every number
below reproduced to the digit.

**What a reader holds.** One batch of a 20-file, 21.1 MiB table, with a consumer
that is not instantaneous:

| | files opened | MiB held |
| --- | --- | --- |
| before | 20 | 18.9 |
| after | **8** | **7.3** |

`ArrowScan` submits every planned file to its thread pool at once and each
finished one holds a whole file's decoded batches until the consumer reaches it,
which makes `read_arrow_reader` a `read_arrow_table` that takes longer. The plan
now goes over a group at a time, the group being the pool's own width — so what
is held scales with the pool, not with the table. Draining the whole reader took
54.6–56.5 ms against 59.6–72.4 ms before: the bound costs nothing.

**What `maybe_optimize` walks to say no**, which is every call of a converged
`auto_optimize` stream:

| table | partition reads | manifest reads | seconds |
| --- | --- | --- | --- |
| quiet, before | 1 | 1 | 0.005–0.006 |
| quiet, after | **0** | **0** | 0.001–0.002 |
| settled, before | 1 | 4 | 0.009 |
| settled, after | **0** | **0** | 0.001 |

**Whether compaction converges**, in files rewritten per run:

| partitioning | run 1 | run 2 | run 3 |
| --- | --- | --- | --- |
| identity (`recorded_at_date`) | 13 | 0 | 0 |
| none | 10 | 0 | 0 |
| transform (`day`), before | 13 | **4** | **4** |
| transform (`day`), after | 13 | **0** | **0** |

The four-files-forever is what made every `optimize` on a `day`- or
`bucket[16]`-partitioned table a full read and a full rewrite, while the rows
never changed.

## Backfilling

`bench_iceberg.py --only backfill`. A replay of keys that sit in a few bands of
a wide table — 20 files of 5,000 rows, the key clustered per file, the hash half
of it drawn per row so file bounds on it span everything and prune nothing.
`planned` is what the scan opens; the rows it returns say nothing about that.

| case | planned | seconds |
| --- | --- | --- |
| two distant bands | 18 → **2** | 0.14–0.19 → 0.09 |
| one contiguous band | 1 → 1 | 0.02 → 0.04 |
| half the table | 10 → 10 | 0.14–0.16 → 0.18–0.21 |

Past 200 distinct values a key column cannot be named one value at a time, and
the single min/max range it became spans everything between the bands. It is
described by up to eight ranges now, found by placing every value in one of 64
equal slices of `[min, max]` and merging the occupied ones back — no sort, and
a slice reports the exact min and max of what landed in it, so the union covers
every value however the index arithmetic rounded.

The last two rows are the cost, quoted because they are real: a chunk with no
gap to find pays the banding pass and prunes exactly what it did before. On a
400k-row integer column that is 7.8 ms, against 31–420 ms for the `unique` that
sorting would need.

## Updating what is stored

`bench_iceberg.py --only update`. A merge that *inserts* is measured everywhere
else here; this is the half that rewrites. The filter naming the rows to delete
is one term per row for a composite key, and pyiceberg binds that tree once per
manifest it plans. 10,000 stored rows over four days, a `(symbol, day)` key,
both legs run twice:

| rows updated | terms | seconds | rows/s |
| --- | --- | --- | --- |
| 500 | 1,000 → **2** | 0.95 → **0.22** | 527 → 2,245 |
| 2,000 | 4,000 → **2** | 4.55 → **0.23** | 440 → 8,935 |
| 5,000 | 10,000 → **4** | 21.16 → **0.43** | 236 → 11,589 |

`terms` is the leaf count of the delete filter, and it is the whole story:
profiled at 5,000 rows, 15.8 of the 18.1 seconds went on nineteen
`_InclusiveMetricsEvaluator` constructions at 770 ms each, against 0.4 ms for
the key ranges beside it. Grouping the rows on the key column with the fewest
distinct values says the repeated half once. The filter stays exact, and
[the tests](https://github.com/Platob/yggfin/blob/main/python/tests/iceberg/test_coherence.py)
compare it against pyiceberg's own row for row rather than against itself.

A key that repeats nothing is left alone — one group per row is the tree
`create_match_filter` already builds — so this is never the slower of the two.

## Casting

`benchmarks/bench_cast.py`, 200,000 rows per batch, best of seven, against
pyarrow's own cast on the same data (it asserts the two agree before timing):

| case | rows/s | vs `Array.cast` |
| --- | --- | --- |
| batch, already the right shape | — | returned as-is |
| batch, full reshape | 431k–542k per column-pass | 1.39–1.58× |
| struct, member added | 8.5B (zero-copy) | 1.07–1.13× |
| list of structs | 5.9B–6.9B | 0.98–1.16× |
| map, narrowed value | 1.6B–2.0B | 1.78–2.13× |
| stream of 16 batches | 287M–310M | — |
| map → struct | 3.3M | Arrow refuses |
| struct → map | 17M | Arrow refuses |
| struct → list | 21M | Arrow refuses |
| list → large list | 1.2B | 0.83× |

The last four are conversions `Array.cast` will not do at all. `map → struct` is
the slowest because it is one `map_lookup` pass per member; the rest are
`take` with computed indices.
