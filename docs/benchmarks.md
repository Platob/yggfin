# Benchmarks

Three harnesses under `python/benchmarks/`, each a plain script with
`--rows` and `--quick`. Run them before and after touching a hot path —
that is the rule in `AGENTS.md`, and these tables are what it produced.

```bash
cd python
uv run python benchmarks/bench_log_file.py
uv run python benchmarks/bench_message_parser.py
uv run python benchmarks/bench_iceberg_upsert.py
```

!!! note "What these numbers are, and are not"
    One timed pass per configuration, no warmup, no repetition, on a 4 vCPU
    Xeon @ 2.80 GHz / 15 GiB microVM, Python 3.12, pyarrow 25.0.1,
    pyiceberg 0.11.1, with the `fast` extra (xxhash) installed. Every number
    below was measured twice; where the two runs disagreed enough to matter,
    it says so. **Read them as ratios, not as a spec sheet** — the shapes
    reproduce, the absolute figures move with the machine.

## Reading logs — `bench_log_file.py`

1,000,000 rows, 136.3 MiB plain, 12.5 MiB gzipped.

| file | batch_rows | read_KiB | seconds | rows/s | MiB/s | peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| plain | 16,384 | 64 | 3.02 | 330,783 | 45.1 | 2.7 |
| plain | 65,536 | 64 | 3.22 | 310,898 | 42.4 | 10.7 |
| plain | 65,536 | 1,024 | 3.16 | 316,242 | 43.1 | 10.7 |
| plain | 65,536 | 4,096 | 3.14 | 318,215 | 43.4 | 10.7 |
| plain | 65,536 | 8,192 | 3.02 | 330,880 | 45.1 | 10.7 |
| plain | 262,144 | 4,096 | 4.66 | 214,385 | 29.2 | 42.9 |
| gz | 16,384 | 64 | 3.24 | 308,981 | 42.1 | 3.8 |
| gz | 65,536 | 64 | 3.33 | 300,682 | 41.0 | 12.0 |
| gz | 65,536 | 1,024 | 3.50 | 286,054 | 39.0 | 12.0 |
| gz | 65,536 | 4,096 | 3.28 | 304,791 | 41.5 | 12.0 |
| gz | 65,536 | 8,192 | 3.26 | 306,669 | 41.8 | 12.0 |
| gz | 262,144 | 4,096 | 4.57 | 218,897 | 29.8 | 44.8 |

**`batch_rows=262,144` is a real regression, not noise**: 30–45% slower *and*
four times the memory, in both runs. The 16K and 65K rows are all within a
few percent of each other and swap places between runs — do not tune on
that spread. `peak MiB` is deterministic: byte-identical across both runs,
and the reason the default batch is 65,536 rather than as large as possible.

Gzip costs a few percent in the five pairings at 16K and 65K batches —
decompression is cheaper than the parse it feeds. Not in the sixth: at
262,144 the gz row is *faster* than plain here (218,897 vs 214,385 rows/s),
and re-running flips that around, so that pairing says nothing. It is the
same row that is 30% slower than the rest either way. Two caveats on the
columns: throughput is
computed against the *plain* size, so the gz rows report logical rows and
bytes, not the 12.5 MiB actually read off disk; and the file is written and
re-read on the same box, so these are parse rates with a warm page cache, not
storage rates.

## Parsing messages — `bench_message_parser.py`

1,000,000 FIX-shaped messages through `rekep.jobs.parse_fields`.

| batch_rows | fields/row | seconds | rows/s |
| ---: | ---: | ---: | ---: |
| 16,384 | 5 | 4.58 | 218,351 |
| 65,536 | 5 | 4.71 | 212,414 |
| 65,536 | 10 | 9.06 | 110,347 |
| 262,144 | 10 | 10.34 | 96,678 |

The tightest of the three — every figure reproduced within 4%. The
relationship worth knowing: **cost is linear in fields per row, not in
rows.** Doubling fields from 5 to 10 halves throughput (212k → 110k, and
204k → 109k on the second run). `batch_rows` is not a lever here: 16K and
65K differ by ~3%, inside the noise.

That linearity is why the parser stays a per-row regex rather than moving to
`pyarrow.compute`: the work is proportional to the segments in the message,
which no columnar kernel changes.

## Writing Iceberg — `bench_iceberg_upsert.py`

20,000 rows per round arriving in 500-row batches — the shape a streaming
transform actually produces — then a second reader that is half overlapping
keys (updates) and half new ones (inserts).

| commit_row_size | append rows/s | merge rows/s | files left |
| ---: | ---: | ---: | ---: |
| 500 | 10,259 | 879 | 40 |
| 5,000 | 95,565 | 7,418 | 4 |
| 20,000 | 90,114 | 13,286 | 1 |

This is the table behind `commit_row_size`' existence, and it says three things,
in descending order of confidence:

1. **`files left` is deterministic** — 40 / 4 / 1 in both runs. It is the
   cost that keeps being paid after the write returns: every scan of that
   table opens forty files instead of one. Compaction can repair it
   afterwards; not creating it is cheaper.
2. **Merging genuinely rewards larger chunks**, monotonically and within 5%
   across runs: 879 → 7,418 → 13,286 rows/s. A merge compares against
   existing data, so a small chunk pays that planning cost again and again.
3. **`commit_row_size=500` is catastrophic for both**, by roughly 9× on append
   and 15× on merge, stably.

What the table does *not* support: any ranking between 5,000 and 20,000 on
the **append** column. Those two flipped between runs (95,565 / 90,114 then
86,441 / 116,303) because the timed window is only ~0.2 s there — treat the
append column as approximate above 5,000.

These are the least portable numbers of the three: a local SQLite catalog and
a file warehouse on real ext4, so every commit is a real `fsync`. A REST
catalog and object storage add a network round trip per commit and will not
resemble this at all — the *ratios* are the transferable part.
