# Upstream prompt: logical text records

Implement logical-record framing in yggdryl text media without adding a
yggfin-specific parser.

Start from current yggdryl main, where `IOBase.from_fs` and `TextOptions`
already exist. Bump the package version and publish these APIs with this work;
the public `0.1.1` release does not contain the complete Python boundary that
yggfin consumes.

Use `TextOptions.rowheader` as the record-start expression when framing is
enabled. A matching physical line starts a logical record; subsequent
nonmatching lines append to that record's binary `body`, separated by `\n`,
until the next match or EOF. Remove the matched header only from the first
line. Preserve the starting physical `rownum`, never join across source
objects, and make a leading unmatched fragment explicitly configurable as
keep, drop, or error.

The implementation must stream across input and Arrow batch boundaries. It
must not buffer a whole file or reopen a handle. The schema must remain known
before reading, including for empty, missing, compressed, and foreign
`pyarrow.fs.FileSystem` resources.

Add a per-logical-record decoded-byte limit independent of `max_row_size` and
`max_byte_size`. Retain the bounded prefix, drain excess bytes without holding
them, and expose a nullable dropped-byte count or equivalent typed diagnostic
column. If useful, add a separate decoded-byte batch bound; do not change the
existing total-result meaning of `max_byte_size`.

Expose the same options and behavior in Rust, Python, and JavaScript. Add
focused tests for:

- LF, CRLF, and CR physical terminators;
- continuations spanning read buffers and output batches;
- leading unmatched input and EOF without a final terminator;
- exact and over-limit records, including very large continuation lines;
- gzip and zstd sources;
- local, S3-compatible, subtree, and custom Arrow filesystems;
- empty and absent sources answering the full schema before iteration;
- row numbers remaining the first physical line of each logical record;
- records never joining across two handles or files.

Add a bounded benchmark comparing physical-line mode with framed records over
short single-line records, multi-line records, and one oversized record.
Document that `rowheader` captures remain nullable and that `body` always
contains exact source bytes after header removal and terminator normalization.
