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

assert reader.schema.equals(Message.read_field().into_arrow_schema(), check_metadata=True)
```

The row header is the bridge's own `ULBRIDGE_ROWHEADER`, stated by the native
core and spelled once in `rekep.times` -- pinned against the core's own text
rather than respelled per reader. It captures `mtime`, `msgthreadid`,
`msgsessionid`, `msgctxid`, `msgseqnum`, `msgpluginid` and `loglevel`, each
named for what the native read fills from it. `mtime` fills no column of its
own: it is the record clock, and naming it that is what makes the read settle
`currunix` from it, so the line's instant is stated once rather than read
twice. `body` is the whole line, row header included, as text the read decoded
it to: the core retains the whole record and reads the captures off it.
`currhashcode` is the content code the read states over those bytes, and it is
the key: nothing here computes a digest beside it. The read states that code
unsigned, which is why the reader above is checked against `read_field()` and
not the narrower field a table holds. `sourceurl` and `rownum` come from
traversal, and `curruuid` is the line's own identity the read states -- a
UUIDv7 over the settled instant and that content code, on every row the read
produces -- which a message parsed out of the stored line names as its one
`srcuuids` entry.

`sourceurl`, `rownum`, `msgthreadid`, `loglevel` and `body` are raw to
`logs.messages` and stay there. The other four are FixMsg columns a raw line
fills: `msgsessionid` (65032), `msgctxid` (65008) and `msgpluginid` (65009)
are the crate's own fields, and `msgseqnum` fills `MsgSeqNum` (34) where a
frame stated none. Each fills its field because the capture is named what the
field is named -- which is why the plugin is captured as `msgpluginid` and no
longer as `pluginid`, a spelling that left the column empty on every FIX row.
`parse_fix_bronze` reads the `body`, emits only native FixMsg columns, and
records the raw row's `curruuid` in `srcuuids`; it carries no raw column
beside the FIX row.

## A bridge that writes the header its own way

`rowheader` reads one. A capture is written by several loggers and they do not
always agree on the clock: the shipped 14-line sample spells its fraction
`.147`, `,148` and `.147_250` in one file, and the default header reads only
the first of those, so twelve of its fourteen lines settle at the epoch pin
instead of on their own clock. Widening the fraction is a parameter rather
than an edit:

```python
from rekep import IOBase, Message
from rekep.times import EPOCH, ULBRIDGE_ROWHEADER

widened = ULBRIDGE_ROWHEADER.replace(
    r"(?P<mtime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})",
    r"(?P<mtime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.,]\d{3}(?:_\d{3})?)",
)
source = IOBase.from_uri("file:data/capture")
plain = source.read_arrow_reader(options=Message.text_options()).read_all()
read = source.read_arrow_reader(options=Message.text_options(widened)).read_all()


def dated(table):
    # Every row states an instant, so a line the header could not date is
    # counted by the pin it settled at rather than by a null.
    return sum(instant != EPOCH for instant in table.column("currunix").to_pylist())


# Every physical line is a row either way; what changes is how many it dated.
assert plain.num_rows == read.num_rows == 14
assert dated(plain) == 2
assert dated(read) == 10
```

What a header may change is the layout. What it may not change is the names:
the names the read fills from are the contract, and a capture named anything
else is dropped in silence -- a whole column of nulls and no error, or a clock
that settles nothing. So the names are checked where the mismatch is still
legible, and a header that renames or omits one is refused by name:

```python
from rekep import Message
from rekep.times import ULBRIDGE_ROWHEADER

renamed = ULBRIDGE_ROWHEADER.replace(r"(?P<loglevel>[A-Z]+)", r"(?P<severity>[A-Z]+)")
try:
    Message.text_options(renamed)
except ValueError as refusal:
    assert "captures nothing for loglevel" in str(refusal)
    assert "captures severity, which this read fills nothing from" in str(refusal)
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

## The window

Every line is read and counted, and the ones the window covers go on: those
whose `currunix` -- the event the read settled over the line, and the column
the table is laid out by -- falls in `[start, end)`, and those at the epoch
pin, where a header that could not date a line leaves it. The pin is in every
window, so a header that did not match loses no line. The window is the last
day, ending at the instant the run starts, when the document names neither
bound; `start` and `end` read the way every instant here does, and `end`
naming a whole day means the end of that day.

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
reader's rows on `currhashcode`, the code the read states over the exact line
bytes: a stored row
carrying one of the window's keys is taken out and the window's row lands, in
one commit per bounded chunk. A missing table is created. A replay of the
window lands the same rows again and the table holds each line once -- and a
line the bridge printed twice, byte for byte, is one row, because the key is
of the bytes and of nothing else; the bundled capture's 144 physical lines are
141 stored ones for that reason, 3 of them repeated exactly.

## Sample rows

The sample is 29 capture lines from `python/tests/data/ulbridge.log` that the
FIX walk later joins under business chain `00026877711XOEA0`, as
`parse_messages` lands them in `logs.messages`. An
identity is shown by its last eight hex digits behind a leading `…`, and the
stored value is sixteen bytes. `currhashcode` is shown whole, and a table
stores it as the signed integer Iceberg has, so half the codes read back
negative.

--8<-- "docs/pipeline/tasks/samples/parse-messages.md"

The generated include is authoritative for row membership and values. This
stage knows only capture headers and bytes; the business chain is established
later by [`parse_fix_silver`](parse-fix-silver.md#sample-rows).

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
retained, its header columns are null, and its `currunix` is the epoch pin,
which every window covers.
