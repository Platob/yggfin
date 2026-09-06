# Yggdryl prompt: remove IOBase transfer bottlenecks

Start at merged main `083da992`. Optimize Rust first, then keep Python and
JavaScript thin. Preserve injected Arrow filesystem identity, opaque paths,
lazy construction, missing-as-empty reads, sticky first errors, and close-once
behavior. Add no filesystem or compatibility layer.

## Retained transfers

Add one internal output transaction over the existing `ByteWriter`: open one
sibling temporary stream, coalesce writes to at most the existing 1 MiB fetch
size, finish the codec/container, close once, then publish by same-filesystem
move using the backend's documented publication guarantees, and remove the
temporary object on failure. Preserve the old target only where the backend
guarantees replace-on-success. Otherwise reject this transactional path or
document and test the weaker post-failure state; never call an object-store
move atomic without that guarantee. Use the current working-set-bounded
fallback only when no bound location exists and state that rollback is
unbounded unless it retains the old target. Never emulate a stream with
repeated `IOBase::pwrite`.

Route `compress_into_with_level` and `decompress_into_with` through it first.
They currently make every encoder write or 64 KiB decoded block call
filesystem `pwrite`, which performs metadata lookup plus open/write/close.
Then stream structured JSON/YAML/TOML and text, IPC, Parquet, and Avro record
overwrites into the same sink; each currently accumulates the complete encoded
object in `Vec<u8>`. Preserve empty overwrite, schema metadata, outer codecs,
input-error timing, codec finish errors, and first-failure precedence.

Coalesce the Python Arrow sink before `PyOutputStream.write`; otherwise
streaming replaces one full-object `PyBytes` copy with excessive GIL calls and
small allocations. Keep `write_bytes` as the control: at 64 MiB it already
measures within about 20% of direct PyArrow on the baseline named below.

Compressed text append must stop decoding and re-encoding the complete old
object into memory. First implement a bounded transactional rewrite. Then,
after the concatenated-stream work in `yggdryl-text-streaming.md`, benchmark
appending a gzip member or zstd frame while preserving the exact separator.
Keep rewrite for codecs or backends without a valid append contract. Avoid
`commit_row_size` turning monolithic IPC/Parquet/Avro leaves into repeated
complete rewrites; cadence is native only for folders and Iceberg.

## Whole-byte reads

Keep `083da992`'s direct whole-read delegation through Text, Coded, and media
wrappers; a composed gzip value must not run a size pass and then decode it a
second time.

Optimize `holder::fs::File::read_all_bytes`. It currently allocates one vector
per 64 KiB stream item, copies each into a growing vector, then Python copies
that vector into `PyBytes`. Retain exactly one sequential open and one reusable
window. A metadata size may reserve but never define correctness: cover absent,
unknown, growing, shrinking, short-read, and close-failure sources. Compare
64 KiB with the existing 1 MiB transport size and construct the Python result
with the fewest safe copies.

Add a buffer-protocol/`readinto` fast path to the three Python Arrow reader
methods in `python/src/holder/fs.rs`, retaining `read` for arbitrary custom
`PyFileSystem` implementations. Never retain an exported mutable buffer after
the synchronous call. The control is `IOBase.open_input_file().read()`, which
already matches direct PyArrow throughput.

Do not put one-pass text reads behind `Buffered`: they already retain one leaf
stream under a 1 MiB `BufReader`. Override `Buffered::read_all_bytes` so an
object larger than its budget cannot cause one inner open per 64 KiB page;
bypass the cache or populate only a fitting working set. `pstream_bytes`
already delegates directly and bypasses pages. Make explicit
filesystem `IOBase.open()` retain one random-access reader until `close()` so
repeated `pread` does not reopen every time.

The generic unbound `copy_into` currently stages the complete source and keeps
the complete prior target despite documenting bounded transfer. Make it truly
bounded when rollback can be transactional; otherwise state the unavoidable
unbound rollback memory explicitly.

## Proof

Use counting Rust filesystems plus Python `LocalFileSystem`, native-delegate
`PyFileSystem`, and fsspec `PyFileSystem`. Compare bytes/records with the old
implementation before timing. Cover 0/1/short/1 MiB/64 MiB values, partial
reads and writes, source growth/shrink, error after prefix, missing `readinto`,
early drop, explicit open/close, and codec-finish failure. Assert one open and
close per sequential operation, calls no larger than 1 MiB, temporary cleanup,
the temporary `ByteWriter` receiving bytes before the final input batch is
pulled, and target publication only after the encoder/container finishes.
Measure seven interleaved warmed samples: median/range throughput, calls,
first-batch latency, and peak retained bytes.

Historical Windows medians from release-built Yggdryl 0.1.1 at `ac093526` with
PyArrow 25.0.1, for 64 MiB cached reads, were 1.6-1.8 GiB/s through
pathlib/PyArrow, 1.5-1.7 GiB/s through
`IOBase.open_input_file().read()`, and 0.37 GiB/s through
`IOBase.read_bytes`. Default `Buffered` fell to about 0.04 GiB/s once the
object exceeded its 8 MiB budget. Reject a whole-read regression, a text-read
regression above 5%, any sequential reopen amplification, or record-write
memory proportional to total input size.
