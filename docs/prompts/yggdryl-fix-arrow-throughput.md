# Yggdryl prompt: make FIX Arrow parsing column-native

!!! note "Still open"

    The reader itself landed: `FixBatchReader::from_column` is the runtime
    path, reachable from Python as
    [`parse_arrow_reader`](../fix/decode.md). What is still open is this page's
    subject -- it goes through `RecordBatch -> Scalar rows -> RecordBatch` per
    batch, which is what makes the cost scale with carried bytes rather than
    with parsed columns.


Start at `c9c84b24` on `codex/fix-arrow-python`, rebased after PR 53 merges.
Implement in Rust, then keep Python and JavaScript bindings thin. Preserve the
source-first schema, child metadata, collision refusal, `fixbranch` parameter,
row-in/row-out default, malformed-row behavior, bounds, laziness, and error
fusion. Add no second FIX parser or batch type.

Replace `FixBatchReader::from_column`'s whole-batch
`RecordBatch -> Scalar rows -> RecordBatch` round trip. Resolve the payload and
optional parameter arrays once from the source schema, read binary/UTF-8 values
directly, and build only the native FIX columns. Reuse each untouched source
array in the output; when output bounds split a source batch, reuse zero-copy
slices. Keep at most one source batch and one output batch resident.

Make the no-dedup path allocation-independent of the number and width of
carried source columns. For `dedup=true`, retain selected row indexes only and
apply Arrow `take` once per emitted slice; do not rebuild source values through
`Scalar`. A source error must yield any completed prefix once, then the same
error, then fuse. Closing the output must release the input immediately.

Prove exact parity with the current reader for binary and UTF-8 payloads,
mixed valid/prose rows, every parameter column, nulls, empty input, 1/65,536
row bounds, byte bounds, max rows, dedup across source-batch boundaries,
collisions, and pull-time errors. Assert buffer identity for unsplit carried
arrays and shared buffers for slices.

Benchmark release builds against `a8c254a5` after checking identical batches:
1, 1,024, and 65,536-row source batches; narrow and 32-column captures; binary
and UTF-8 bodies; dedup off/on. Report rows/s, first-batch latency, peak retained
bytes, and allocations per row. Reject a narrow no-dedup regression and require
wide-source cost to scale with parsed columns rather than carried bytes.
