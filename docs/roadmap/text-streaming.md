# Compressed text streaming

`parse_messages` reads a capture one physical line at a time. Three gaps in
that path are visible from here.

## Concatenated members

| input | today | wanted |
| --- | --- | --- |
| single-member gzip | streams | unchanged |
| concatenated zstd frames | read in order | unchanged, and the control |
| concatenated gzip members | first member only | every RFC 1952 member, in order |

The decoder moves from single-member to multi-member reading in the whole-byte,
`reader` and `reader_send` paths. Compression selection stays suffix and
media-type based -- no byte sniffing.

## A byte bound that does not change bytes

`max_record_byte_size` truncates today, which silently rewrites `body`. So
`parse_messages` [leaves it unset](../pipeline/tasks/parse-messages.md).

An error-on-overflow policy beside the truncating one lets an exact-body
consumer impose a fixed bound: it drains no further than needed to identify the
overflow, releases the leaf immediately, and reports the source URL and
physical row number. A partial record is never emitted.

## Eager terminal ownership

On a decoder or transport error, and when a row or byte limit is satisfied, the
active leaf stream is dropped *before* the terminal error or EOF is yielded.
Later pulls fuse; close and drop stay idempotent. A first pull may read ahead at
most one encoded 1 MiB fetch window.

## What must not change

```text
lazy schema discovery          one sequential open per leaf
65,536-row default batches     64 KiB line windows
exact source URL and rownum    bounded transport read-ahead
```

Never: materialize a whole encoded or decoded object, route a sequential scan
through the positional cache, stage a remote file locally, or delay the first
batch until EOF.

## Proof

Local, counting-Arrow, folder, malformed, gzip and zstd sources, over the
native byte paths and `IOBase.read_arrow_reader`. Two independently compressed
members, early close, exhaustion, limit satisfaction, a malformed second
member, error fusion, and drop. Assert every row and member occurs exactly
once, each leaf opens and closes once, and no backend read exceeds the fetch
window.

Benchmark zero, one and four captures; single- versus multi-member gzip; local
Arrow FS, `PyFileSystem`, and an S3-compatible transport; at least five warmed
samples. **Reject a decoded-rows/s regression above 5%.**
