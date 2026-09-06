# Parse FIX

`parse_fix` streams `logs.messages` through Yggdryl's native FIX reader and
merges the resulting rows into `fix.messages`.

```bash
rekep task run tasks/parse_fix/parse_fix.json
```

```json
{
  "parameters": {
    "registry": null
  }
}
```

`null` selects Yggdryl's process registry. It must already be populated through
`YGGDRYL_FIX_REGISTRY` or explicit installation; an empty process registry is
refused before `fix.messages` is created. Set `registry` to a local or remote
dictionary directory accepted by `FixRegistry.from_handle`; for example,
`s3://example-bucket/config/fix?region=eu-west-1`.

The registry fixes the table schema. It must be Iceberg-compatible, and it must
stay the same for the life of `fix.messages`; changing it requires an explicit
table-schema migration rather than implicit evolution during ingestion.

The source is one schema-bearing `RecordBatchReader`. Native
`parse_arrow_reader` reads the binary `body` column, preserves the raw source
columns and their metadata, and appends the fixed FIX columns selected by the
registry. FIX columns are named by stable numeric tag; `entries` keeps every
wire pair in arrival order and `unmapped` exposes pairs the registry did not
resolve. One input row remains one output row, including prose or an unreadable
message, so `url` and `rownum` still identify the result.

Yggdryl's generated registry declares every native FIX timestamp, including
timestamps nested in repeating groups and the derived capture timestamp, as
`timestamp[us, UTC]`, so it writes to Iceberg v2 without a precision shim. A
custom registry remains authoritative: unsupported types such as
`timestamp[ns]` are refused by the PyIceberg boundary before `fix.messages` is
created.

The output schema is available before the first batch. The application builds
its native `Field` with `Field.from_arrow_schema`, applies that field to the
reader, and hands the stream directly to Iceberg. No Rekep FIX parser, registry,
or row model exists. The selected Yggdryl registry owns the FIX types and
metadata.

The [checked `FixMsg` snapshot](../../contracts/index.md) is the 95-column
schema generated from the full registry at pinned Yggdryl `c9c84b24`. It
round-trips through `Field.from_json` and supports schema-only Iceberg tests
without parsing a row. The runtime registry remains authoritative; the snapshot
is regenerated when that pin or registry changes.

`fix.messages` keeps the source `(url, rownum)` primary key and the source
`timepartition` hourly Iceberg declaration. Replaying the same raw rows creates
no data file or snapshot.
