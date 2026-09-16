# parse_messages

`parse_messages` recursively reads a local or object-store text source and
publishes one raw row per physical line to `logs.messages`.

## Task document

```json
{
  "name": "parse_messages",
  "application": "parse_messages.py",
  "parameters": {
    "filesystem": "file:data/capture",
    "catalog": {
      "name": "rekep",
      "properties": {
        "type": "sql",
        "uri": "sqlite:///data/catalog.db",
        "warehouse": "data/warehouse"
      }
    }
  }
}
```

| parameter | required | meaning |
| --- | :---: | --- |
| `filesystem` | yes | one file, directory, or object-store prefix |
| `catalog.name` | yes | catalog instance name |
| `catalog.properties` | yes | PyIceberg catalog and FileIO settings |

## Source URI forms

| source | example |
| --- | --- |
| relative local | `file:data/capture` |
| absolute POSIX | `file:///srv/capture/2026-08-14` |
| absolute Windows | `file:///C:/capture/2026-08-14` |
| AWS S3 | `s3://market-capture/ulbridge/2026/08/14?region=eu-west-1` |
| S3-compatible | `s3://capture/day?endpoint_override=minio:9000&scheme=http&force_path_style=true` |

The source URI configures capture reading. Iceberg S3 settings are separate
catalog properties because the source reader and table FileIO are independent
owners.

## Parse step

```python
from rekep import IOBase, Message

source = IOBase.from_uri("file:data/capture")
reader = source.read_arrow_reader(options=Message.text_options())

assert reader.schema.equals(Message.into_field().into_arrow_schema(), check_metadata=True)
```

The row header is the bridge's own `ULBRIDGE_ROWHEADER`, stated by the native
core and spelled once in `rekep.times` — pinned against the core's own text
rather than respelled per reader — and every capture it declares is named for
the column it fills: `timestamp`, `threadId`,
`bridgesessionid`, `msgctxid`, `msgseqnum`, `pluginid` and `level`. `body`
starts immediately after the matched header. `sourceurl` and `rownum` come from
traversal, and `bodyhash` is computed from the exact body bytes during field
application.

`bridgesessionid` is the session *instance* the bridge handled the line on
(65032) — never `sendersessionid` (65007), which is what a bridge row spells
for the counterparty session a message names; two connections to one
counterparty are two instances, so they are two facts. `msgctxid` fills 65008,
`msgseqnum` fills `MsgSeqNum` (34) on a frame that stated none, and `sourceurl`
fills 65026. A stored row therefore goes on through the FIX codec without one
spelling being translated into another.

See the [complete 12-column schema](../../products/message.md#complete-schema).

## Streaming behavior

- One leaf is opened at a time.
- Transport read-ahead is byte-bounded; emitted batches are row-bounded.
- Directory traversal is recursive and naturally sorted.
- gzip and zstd are decompressed while reading.
- An injected Arrow filesystem keeps its identity and opaque path spelling.
- The writer receives one `RecordBatchReader`; it does not collect the capture
  into a table first.

One physical record is not byte-bounded until the decoder can reject overflow
without changing exact bodies. A huge line can therefore exceed the target
batch size. The decoder's support for concatenated gzip members should be
validated before production use because staging locally is not a substitute
for a streaming decoder fix.

## Write step

The task opens `logs.messages` with `Message.into_field()` and appends the reader
with `merge_by=True`. A missing table is created. Existing `(sourceurl, rownum)`
keys are skipped; new keys are inserted.

## Run

```bash
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameter 'filesystem="file:/srv/captures/2026-08-14"'
```

For S3:

```bash
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameter 'filesystem="s3://market-capture/ulbridge/2026/08/14?region=eu-west-1"' \
  --parameters-file /run/rekep/catalog.json
```

## Failures

A missing source, unreadable object, invalid URI, malformed catalog, or failed
commit fails the task. A line whose header does not match is data: its source
identity and body are retained with null header fields.
