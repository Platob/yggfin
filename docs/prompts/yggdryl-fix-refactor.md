# Next session: rebuild FIX on Yggdryl

Start from current yggfin `main`. Do not restore deleted Rekep FIX, CFB,
Orchestra, registry, metadata, Entry/FixMsg, enum, or market compatibility
classes from Git history.

Use `yggdryl.fix.FixRegistry`, `FixMsg`, `global_registry`, native `Field.fix`,
and `IOBase` as the only FIX/resource primitives. Yggfin keeps only its Arrow
message-to-domain transforms and PyIceberg tables.

Before coding, compare required behavior with current Yggdryl and list only
real gaps: streaming wire bytes to Arrow, selected dictionary ingestion,
version/branch rules, repeating groups, diagnostics, and market projection. Put
general missing behavior in Yggdryl, Rust first with Python/JavaScript bindings;
do not implement a second registry or Field view in yggfin.

Add one vertical slice at a time:

1. raw `Message` batch -> native Yggdryl `FixMsg` batch;
2. one persisted FIX table through existing yggfin Iceberg APIs;
3. only then minimal domain projection required by a concrete consumer.

Use Git history only as a behavior oracle. Port tests for observable inputs,
Arrow outputs, repeated-tag order, unknown values, batch bounds, and throughput;
never port implementation-shaped tests. Compare optimized batch code with a
small reference before timing it. Delete every temporary adapter when the
native capability lands.
