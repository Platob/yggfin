# Logs

`LogFile` exposes a trading application log as a readable binary stream and as
Arrow record batches.

## The format

One record per line, wrapped lines folding into the previous record's message:

```
2026-08-14 00:05:01.167_520 [77-e7256476:9effef3e6a:72503] [ModuleMarketDataManager] (DEBUG) Found code ...
^timestamp                  ^thread_name                   ^driver                   ^level  ^message
```

The `(LEVEL)` is optional; the fractional second carries millis and micros
separated by an underscore. Lines that do not match the header pattern — stack
traces, wrapped payloads — fold into the preceding message, so a multi-line
exception stays one record.

## Reading

```python
from rekep.logs import LogFile

log = LogFile.from_url("s3://bucket/app.txt.gz")   # or from_path, or from_
table = log.into_arrow_table()                     # whole file
reader = log.into_arrow_reader()                   # streaming batches
```

Construction is lazy — nothing is opened until the first read. Plain files are
seekable; compressed ones decode transparently but read forward only.

## Performance

The hot path is Arrow-centric: timestamps are converted per *batch* with
`pyarrow.compute` (sliced at fixed offsets, joined, one ISO cast), the per-row
Python work is a regex match, a tuple append and a line hash. With the `fast`
extra installed, `xxhash` replaces `blake2b` for the hash.

Two knobs, both named by unit and dimension:

- `batch_row_size` — rows per emitted batch; bounds memory.
- `read_byte_size` — bytes per stream read. On an object store each read is
  one ranged HTTP request, so this is also the request granularity; the 4 MiB
  default keeps S3 GET counts low without holding much memory.

`benchmarks/bench_log_file.py` measures both time and peak Arrow allocation
across these knobs; run it before and after touching the parser.

## Filesystems

URLs resolve through `pyarrow.fs.FileSystem.from_uri`, cached per URL so an
object store's credential chain is not re-walked per file. Opening many files
on one bucket? Build the filesystem once and pass `filesystem=` explicitly.
