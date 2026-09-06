# Yggdryl prompt: finish compressed text streaming

Start at merged main `083da992`. Keep the public Rust, Python, and JavaScript
shape and optimize the Rust core first.

Decode every RFC 1952 gzip member in order. Replace single-member
`flate2::read::GzDecoder` use with `MultiGzDecoder` in whole-byte, `reader`, and
`reader_send` paths. Keep zstd's existing concatenated-frame behavior as the
control and correct `docs/coding/zstd.md`, which currently calls `load`
single-frame. Compression selection remains suffix/media-type based; do not
sniff bytes.

Make terminal ownership eager. On decoder/transport error and when a row or
byte limit becomes satisfied, drop the active leaf stream before yielding the
terminal error or EOF. Subsequent pulls fuse; explicit close and drop remain
idempotent. A first pull may read ahead at most one encoded 1 MiB fetch window.

Add an error-on-record-overflow policy beside the existing truncation behavior
for `max_record_byte_size`. It must drain no further than needed to identify
the overflow, release the leaf immediately, and report its URL and physical
row number. This lets exact-body consumers impose a fixed bound without
silently changing bytes.

Preserve lazy schema discovery, one sequential open per leaf, configurable
row batches with a 65,536 default, 64 KiB line windows, exact source URL and
row number, and bounded transport read-ahead. Do not materialize a whole
encoded/decoded object, route sequential scans through the positional
`Buffered` cache, stage remote files locally, or delay the first batch until
EOF.

Profile `python/src/holder/fs.rs:449-463`. If allocations are material, replace
the foreign Arrow-filesystem `read -> Vec -> copy` seam with a one-copy
`readinto`/buffer-protocol path plus a compatible `read` fallback. Preserve
custom `PyFileSystem` support, sticky errors, 1 MiB requests, and close-once.

After transport correctness is fixed, replace the text reader's remaining
generic `Scalar` row wrappers with dedicated Arrow builders. Keep the
zero-copy canonical Utf8/Binary values added in `083da992`; reuse row/body
buffers and compiled capture locations, and do not allocate URL or capture
wrappers per row or revalidate values the parser just produced. Preserve exact
metadata, nulls, physical row numbers, multiline framing, truncation/overflow
accounting, and batch/error timing.

Test native Rust byte/read/send paths and Python `IOBase.read_arrow_reader`
over local, counting Arrow, folder, malformed, gzip, and zstd sources. Cover
two independently compressed members/frames, early explicit close, exhaustion,
limit satisfaction, malformed member 2, error fusion, and drop. Assert rows
and members occur once, overflow never emits a partial record, each leaf opens
and closes once, and backend reads do not exceed the fetch window.

Benchmark zero/one/four captures, single- versus multi-member gzip, local Arrow
FS, `PyFileSystem`, and an S3-compatible transport over at least five warmed
samples. Verify output before timing and reject a local decoded-rows/s
regression above 5%. Record baseline and head commits, release/debug mode,
PyArrow version, backend, corpus bytes/rows, and median/range for every case.
