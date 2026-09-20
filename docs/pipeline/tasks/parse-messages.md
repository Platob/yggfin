# parse_messages

`parse_messages` recursively reads a local or object-store text source and
publishes one raw row per physical line the window covers to `logs.messages`.

## Task document

```json
{
  "name": "parse_messages",
  "application": "parse_messages.py",
  "parameters": {
    "filesystem": "file:data/capture",
    "rowheader": null,
    "start": null,
    "end": null,
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
| `start` | no | the window's inclusive start; `null` is one day before `end` |
| `end` | no | the window's exclusive end; `null` is the instant the run starts, and a whole day such as `2026-08-14` is the end of that day |
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
core and spelled once in `rekep.times` -- pinned against the core's own text
rather than respelled per reader -- and every capture it declares is named for
the column it fills: `timestamp`, `threadId`,
`msgsessionid`, `msgctxid`, `msgseqnum`, `pluginid` and `level`. `body` is the
whole line, row header included: the core retains the whole record and reads
the captures off it, and `bodyhash` is the digest of those bytes, computed
during field application. `sourceurl` and `rownum` come from traversal, and
`curruuid`, the last column, is the line's own identity the read states -- a
UUIDv7 over the XXH3-64 of its bytes, at no instant, because the read dates no
line -- which a message parsed out of the stored line names as its one
`srcuuids` entry.

`msgsessionid` is the session *instance* the bridge handled the line on
(65032) -- never what the message itself says about the counterparty session it
names; two connections to one counterparty are two instances, so they are two
facts. `msgctxid` fills 65008,
`msgseqnum` fills `MsgSeqNum` (34) on a frame that stated none, and `sourceurl`
fills 65026. `pluginid` fills nothing: it rides in front of the FIX row under
this name, and the row's own column for the plugin is `msgpluginid`. A stored
row therefore goes on through [`parse_fix_bronze`](parse-fix-bronze.md)
without one spelling being translated into another.

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
without a word -- a table that lands complete, keyed, and empty down one
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

See the [complete 13-column schema](../../products/message.md#complete-schema).

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

## The window

Every line is read and counted, and the ones the window covers go on: those
whose `timepartition` -- the capture clock, and the column the table is laid
out by -- falls in `[start, end)`, and those with no clock at all, which a
header that did not match leaves and no window could place. The window is
the last day, ending at the instant the run starts, when the document names
neither bound; `start` and `end` read the way every instant here does, and
`end` naming a whole day means the end of that day.

```python
from rekep.times import window_of

lower, upper = window_of("2026-08-14", "2026-08-14")
assert (lower.isoformat(), upper.isoformat()) == (
    "2026-08-14T00:00:00+00:00",
    "2026-08-15T00:00:00+00:00",
)
```

Under Airflow the operator hands each run its data interval as `start` and
`end`; a bound the run's own conf names wins over the interval.

## Write step

The task opens `logs.messages` with `Message.into_field()` and replaces the
reader's rows on `bodyhash`, the digest of the exact line bytes: a stored row
carrying one of the window's keys is taken out and the window's row lands, in
one commit per bounded chunk. A missing table is created. A replay of the
window lands the same rows again and the table holds each line once -- and a
line the bridge printed twice, byte for byte, is one row, because the key is
of the bytes and of nothing else; the bundled capture's 144 physical lines are
141 stored ones for that reason, 3 of them repeated exactly.

## Sample rows

The sample is chain `e7254b12:9f03166699` of `python/tests/data/ulbridge.log`,
a partial fill and the fill that closed the order: the ten lines a FIX message
was parsed out of, as `parse_messages` lands them in `logs.messages`. An
identity is shown by its last eight hex digits behind a leading `…`, and the
stored value is sixteen bytes; `bodyhash` is shown by its first eight.

--8<-- "docs/pipeline/tasks/samples/parse-messages.md"

The first table is `rownum` and the seven captures read off each line's
header. All ten are thread `15255` on session `e7254b12`, and the bridge
handled them in two contexts: rows 6, 7, 8, 9, 10, 11, 15 and 22 are
`9f03166699` at sequence `40218`, the partial fill, and rows 35 and 36 are
`9f0316669a` at `40219`, the fill. What makes lines of two contexts one chain
is the walk, on [`parse_fix_silver`](parse-fix-silver.md#sample-rows); nothing
read here knows it.

`timestamp` reads `2026-08-14 14:46:39.769` on those eight rows and `.770` on
rows 35 and 36. It is the clock the bridge printed, and it is what
[the window](#the-window) is taken on, but it dates no message: the frame on
row 6 states `52=20260814-12:46:39.761` past where the sample cuts the line
off, two hours earlier, and that is the clock the parse reads instead, on
[`parse_fix_bronze`](parse-fix-bronze.md#sample-rows).

The second table is what each line printed after its header. Rows 6 and 35
are `OMS_X1_TradeCapture` at `INFO`, `Receiving :` and the FIX frame the
bridge received. Rows 7 and 36 are the same plugin at `DEBUG`,
`RouteMessage :` and the bridge's own key=value restatement of the frame it
just received. Rows 8, 9, 10 and 11 are `After Enrichment ->`, the same
message logged again after an enrichment step, twice by
`TECH_AddFields_OMS_X1`, and row 15 is `After -->` from `Force_IRIS_ByPass`.
Row 22 is `PushMessage :`, the message handed on to `ULFilter`, the
destination the bridge resolved for it.

Ten lines are ten rows: no two of these are the same bytes. An enrichment
step need not have added a field for that. Rows 8 and 9 are both
`TECH_AddFields_OMS_X1` logging `After Enrichment ->`, and the second is 118
bytes shorter, because the step dropped `FIRM.SOURCE`, dropped an empty party
sub-group, and rewrote every repeating group's sub-field separator into a
shorter one. Every `curruuid` here begins `0000000000007000`: the
[UUIDv7](#parse-step) the read states opens with the instant it is dated by,
and a line is dated by none.

`tools/pipeline_samples.py` regenerates the file from a run over the fixture,
and the integration suite checks it with `--check`.

## Run

```bash
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameter 'filesystem="file:/srv/captures/2026-08-14"' \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
```

Without `start` and `end` the run covers the last day, which is what a
scheduled run over a live capture wants and what a dated capture is outside.

For S3:

```bash
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameter 'filesystem="s3://market-capture/ulbridge/2026/08/14?region=eu-west-1"' \
  --parameters-file /run/rekep/catalog.json
```

## Failures

A missing source, unreadable object, invalid URI, malformed catalog, a bound
that names no instant, an empty window, or a failed commit fails the task. A
line whose header does not match is data: its source identity and body are
retained with null header fields, and it is in every window.
