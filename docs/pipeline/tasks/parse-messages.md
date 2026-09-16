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
    "rowheader": null,
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
| `rowheader` | no | the row header to frame each line with; `null` is the bridge's own |
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

## A bridge that writes the header its own way

`rowheader` reads one. A capture is written by several loggers and they do not
always agree on the clock: the shipped 14-line sample spells its fraction
`.147`, `,148` and `.147_250` in one file, and the default header reads only
the first of those, so twelve of its fourteen lines carry no clock at all.
Widening the fraction is a parameter rather than an edit:

```python
from rekep import IOBase, Message
from rekep.times import ULBRIDGE_ROWHEADER

widened = ULBRIDGE_ROWHEADER.replace(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})",
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.,]\d{3}(?:_\d{3})?)",
)
source = IOBase.from_uri("file:data/capture")
plain = source.read_arrow_reader(options=Message.text_options()).read_all()
read = source.read_arrow_reader(options=Message.text_options(widened)).read_all()

# Every physical line is a row either way; what changes is how many it dated.
assert plain.num_rows == read.num_rows == 14
assert plain.num_rows - plain.column("timestamp").null_count == 2
assert read.num_rows - read.column("timestamp").null_count == 10
```

What a header may change is the layout. What it may not change is the names:
the columns above are the contract, and a read drops a capture no column holds
without a word — a table that lands complete, keyed, and empty down one
column. So the names are checked where the mismatch is still legible, and a
header that renames or omits one is refused by name:

```python
from rekep import Message
from rekep.times import ULBRIDGE_ROWHEADER

renamed = ULBRIDGE_ROWHEADER.replace(r"(?P<level>[A-Z]+)", r"(?P<severity>[A-Z]+)")
try:
    Message.text_options(renamed)
except ValueError as refusal:
    assert "captures nothing for level" in str(refusal)
    assert "captures severity, which no column holds" in str(refusal)
```

`Message.captures()` is the set it is checked against, stated by the contract
rather than beside it.

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
