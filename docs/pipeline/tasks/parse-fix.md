# Parse FIX

`parse_fix` streams the classified rows of `logs.messages` through Yggdryl's
native FIX reader and merges the result into `fix.messages`.

```bash
rekep task run tasks/parse_fix/parse_fix.json
```

```json
{
  "parameters": {
    "registry": "file:config/fix",
    "version": "FIX.4.4",
    "dedup": true
  }
}
```

## The dictionary

`registry` is bound through `IOBase.from_uri`, so it accepts a relative `file:`
path, an absolute one, or `s3://example-bucket/config/fix?region=eu-west-1`.
`null` selects Yggdryl's process registry, which must already be populated
through `YGGDRYL_FIX_REGISTRY` or explicit installation; an empty registry is
refused before `fix.messages` is created.

The task then calls `with_crate_fields`, registering the seven facts Yggdryl
derives beside the specification's own -- the digest, the version read, the
ticker, the market clock, the partition, and the two parent order identifiers.
They occupy Yggdryl's own branch, so no standard tag is claimed.

The registry fixes the table schema. It must be Iceberg-compatible, and it must
stay the same for the life of `fix.messages`; changing it requires an explicit
table-schema migration rather than implicit evolution during ingestion. A
dictionary that declares a nanosecond field is refused at the PyIceberg
boundary -- that is the dictionary's decision to change rather than this task's
to paper over. The one exception is Yggdryl's own derived market clock, which
is read at nanoseconds because that is what a venue stamps: this task declares
that single column at microseconds so the native apply narrows it once, in the
cast it already runs.

## What it reads

The source is `logs.messages` under `msgtype != 'unknown'`, pushed into scan
planning. A record the raw layer could not name a `MsgType` for carries no FIX
frame, so it stays where it is rather than becoming a row of nulls here. The
classification is read, never recomputed --
[`parse_messages`](parse-messages.md) decided it once.

## What it writes

Native `parse_arrow_reader` reads the binary `body` column, carries the raw
source columns and their metadata through unchanged, and appends the fixed
eighty-seven FIX columns Yggdryl's schema projects. Columns are named by stable numeric tag, because
a tag is the one name a field has in every version and every dialect; `entries`
keeps every wire pair in arrival order and `unmapped` exposes the pairs the
dictionary did not resolve. One input row remains one output row, including a
prose or unreadable body, so `url` and `rownum` still identify the result --
`dedup` is the one exception and says so.

The output schema is available before the first batch. The application builds
its native `Field` with `Field.from_arrow_schema`, applies that field to the
reader, and hands the stream directly to Iceberg. No Rekep FIX parser,
registry, or row model exists. The selected Yggdryl dictionary owns the FIX
types and metadata, so a dictionary that types a tag differently is a
differently typed table without a code change. It does not own the width: the
projection is a fixed set of tags, and every pair outside it is in `entries`.

[Decode](../../fix/decode.md) states the frame location, the separator
spellings and the typing rules in full.

## The two renamed columns

The reader reads five of a capture's own columns as per-row parameters, and two
of the raw contract's names land on them:

| raw column | carried as | why |
| --- | --- | --- |
| `branch` | `logbranch` | on a raw record this is the driver that printed the line, not a FIX dialect; left alone it would pin every row to a dialect nobody declared |
| `msgdirection` | `direction` | this *is* the parameter, so the reader uses the direction the raw layer already read instead of reading it again |

The rename is what makes column `385` equal the raw layer's own reading rather
than a second, independent one. [Decode](../../fix/decode.md) lists all five
parameter columns.

## The columns Yggdryl derives

Beside the specification's own fields, the row carries what the crate computes
from the message:

| column | contract |
| --- | --- |
| `30001` | `fixed_size_binary[16]`, the XXH3-128 digest of the message |
| `30002` | the FIX version the row was read as |
| `30003` | the cross-venue ticker |
| `30004` | the market clock: the first clock the message answers, narrowed to microseconds here |
| `30005` | `int64`, the partition that clock falls in |
| `30006`, `30007` | the two parent order identifiers |

`msghash` is separate and comes from the raw layer: it digests the exact body
bytes rather than the parsed message, so it does not depend on how a line was
numbered. [Quality](../../fix/quality.md) says when each one is the right key.

## Deduplication and standardization

`dedup` drops a record whose digest repeats the previous emitted record's. It
is sequential rather than global, which is what removes a retransmitted order
or a repeated heartbeat without collapsing two genuine events. Switching it on
surrenders the row-in/row-out correspondence: the output no longer aligns with
the input by position, and what went is counted rather than silent.

`version` is the version built messages are expressed in. A capture spans
dialects and versions -- one session writes `FIX.4.2`, another `FIX.4.4` -- and
declaring one means every row is resolved against the same field lineage
whatever it arrived as. Column `30002` records the version each row was
actually read as, so the standardization never hides what the wire said.

## The checked snapshot

The [`FixMsg` snapshot](../../contracts/index.md) is the 101-column schema this
checkout's dictionary generates. It round-trips through `Field.from_json` and
supports schema-only Iceberg tests without parsing a row. The runtime registry
remains authoritative; the snapshot is regenerated when the dictionary changes.

`fix.messages` keeps the source `(url, rownum)` primary key and the source
`timepartition` hourly Iceberg declaration. Replaying the same raw rows creates
no data file or snapshot.
