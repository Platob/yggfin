# Benchmarks

Two benchmarks ship with the package, and every number below came out of them on
one machine, **measured twice**: what reproduced is stated as a number, what did
not is called noise.

```bash
cd python
uv run python benchmarks/bench_text_file.py     # parsing a log
uv run python benchmarks/bench_iceberg.py       # parse, stream into Iceberg, read back
uv run python benchmarks/bench_cast.py          # casting data onto a shape
```

The Iceberg numbers use a local SQLite catalog and a file warehouse, so they are
storage-latency-free: they measure planning, commit and Arrow work, which is
what this package is responsible for. On an object store every commit also pays
a round trip — which makes the number of commits matter *more*, not less.

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
| every key already stored | 0.20 s | 9.83 s (48×) |
| half and half | 0.20 s | 8.12 s (42×) |

[How the merge is planned](iceberg.md#how-a-merge-is-planned) explains why. The
two paths are compared row by row in `tests/iceberg/test_coherence.py`; set
`plan_merges=False` to use the library's own.

### Partitioning and properties

| case | commit rows | rows/s | files |
| --- | --- | --- | --- |
| append, partitioned by day | 65,536 | 623k–797k | 14 |
| append, no partition | 50,000 | 1.01M–1.03M | 7 |
| append, Iceberg's default properties | 50,000 | 802k–852k | 14 |
| merge, no partition | 50,000 | 506k | 5 |

Partitioning costs about half the write throughput here, because eight days
means up to eight files per commit instead of one. It buys the read below.

## Reading it back

400,000 rows in 14 files, best of three. `planned` is how many files the scan
opened; `skipped` is what the filter saved.

| case | seconds | rows | planned | skipped |
| --- | --- | --- | --- | --- |
| everything | 0.082–0.097 | 400,000 | 14 | 0 |
| `date = '2026-08-14'` (partition) | 0.019–0.022 | 50,000 | 1 | 13 |
| partition + 3 of 8 columns | 0.015–0.019 | 50,000 | 1 | 13 |
| 3 of 8 columns, no filter | 0.087–0.094 | 400,000 | 14 | 0 |
| `unix < …` (correlates with the partition) | 0.039–0.045 | 100,000 | 3 | 11 |
| `driver = 'ULBridge'` (no useful statistics) | 0.086–0.096 | 100,000 | 14 | **0** |
| narrow shape, projection from the shape | 0.075–0.080 | 400,000 | 14 | 0 |
| narrow shape declared with the store's widths | 0.055–0.063 | 400,000 | 14 | 0 |

Three things worth taking away:

- **A partition filter is worth 13 of 14 files.** A filter on a column that
  merely *correlates* with the partition still skips 11 — Iceberg prunes on
  per-file column bounds, not only on partitions.
- **A filter that cannot prune says nothing about it.** The `driver` filter
  returns exactly the right rows and reads every file. `scan_plan` is how you
  see that:

    ```python
    quotes.scan_plan("driver = 'ULBridge'")["skipped"]   # 0
    ```

- **Declaring narrower widths than the store costs a conversion per row.**
  Reading three columns into `string` took 0.075–0.080 s where the same three
  columns in the store's own `large_string` took 0.055–0.063 s — about 25%, and
  it reproduced across both runs. Declare the store's widths
  (`dataset.table_field`) when a read is hot and the shape is only there to
  select columns.

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
