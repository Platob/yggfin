# parse_messages

`parse_messages` recursively reads a local or object-store text source and
publishes one row per line the window covers to `logs.messages`.

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

The row header is the bridge's own `ULBRIDGE_ROWHEADER`, spelled once in
`rekep.times` and pinned against the native `yggdryl.fix.ULBRIDGE_ROWHEADER`
by `python/tests/test_times.py`: the same bracket part for part, under the
names the read fills from. The native constant names its clock `timestamp`
and its level `level` and therefore dates nothing; yggfin's spells `mtime`
and `loglevel`, with a wider, optional fraction. It captures `mtime`,
`msgthreadid`, `msgsessionid`, `msgctxid`, `msgseqnum`, `msgpluginid` and
`loglevel`, each named for what the native read fills from it. `mtime` fills
no column of its own: it is the record clock, consumed into `currunix` at
nanoseconds UTC whatever the width of its fraction, so the line's instant is
stated once rather than read twice. `parse_mtime` is the native default, on,
and is never turned off. The fraction reads every width this bridge writes:
three digits under a point (`.147`) or a comma (`,148`), none at all, and
grouped micros (`.524_315`). Its width types no column; what it decides is
which lines the header matches.

`body` is the line past its header -- what the bridge printed after the
bracket, as text the read decoded it to -- and is empty where the header
consumed the line. `crosscode` is the object the line was read from, as the
identifier the read was addressed under: the URL where that is a location,
the name itself where a handle was addressed by a `urn:` or an `arn:`, null
for a buffer nothing addressed. `seqnum` is the line's row number there,
counted from 1 and null where zero. `currhashcode` digests that object, the
header's captures except the clock, the row number and then the body, so two
lines of identical bytes answer two codes; nothing here computes a digest
beside it. `curruuid` is the line's own identity: a UUIDv7 packing the
microsecond of `currunix` and the whole 64-bit digest, which a message parsed
out of the stored line names as its one `srcuuids` entry. The read states the
code and the row number unsigned and the identity as a `uuid`, which is why
the reader above is checked against `read_field()` and not the field a table
holds: `read_field()` projects the native row onto the contract's twelve
columns at the read's own types, and `rekep.fields.stored_arrow_reader` views
the two unsigned columns into `int64` and casts the rest -- nanoseconds to
microseconds, `uuid` to sixteen bytes -- at the storage boundary.

`msgthreadid`, `loglevel` and `body` exist only in `logs.messages`. The other
four captures are FixMsg columns a line fills: `msgsessionid` (65032),
`msgctxid` (65008) and `msgpluginid` (65009) are the crate's own fields, and
`msgseqnum` fills `MsgSeqNum` (34) where a frame stated none. Each fills its
field because the capture is named what the field is named. `crosscode` and
`seqnum` stand on both shapes and mean the row they sit on: the object a line
was read from and its row number here, a message's chain identifier and its
step in the chain on a FIX row. `parse_fix_raw` reads the `body`, emits only
native FixMsg columns, and records the line's `curruuid` in `srcuuids`; it
carries no column of the line beside the FIX row.

## A bridge that writes the header its own way

`rowheader` reads one. A capture is written by several loggers and they do not
always agree on the clock: the shipped 14-line sample spells its fraction
`.147`, `,148` and `.147_250` in one file, and the shipped header reads all
three. Its 10 lines under the bridge's bracket are dated by the header; its 4
lines under no bracket -- three Java stack lines and one line with no level
-- are dated by the file's own modification time, with every capture null. A
bridge writing six digits straight on, `.147250`, is read by a header of its
own through `Message.text_options(rowheader)`; the width is a parameter, not
an edit of the constant, and it types no column either way.

```python
from rekep import IOBase, Message
from rekep.times import ULBRIDGE_ROWHEADER

widened = ULBRIDGE_ROWHEADER.replace(
    r"(?:[.,]\d{3}(?:_\d{3})?)?", r"(?:[.,]\d{3}(?:_?\d{3})?)?"
)
source = IOBase.from_uri("file:data/capture")
plain = source.read_arrow_reader(options=Message.text_options()).read_all()
read = source.read_arrow_reader(options=Message.text_options(widened)).read_all()


def dated(table):
    # A line the header matched states its captures; one it did not match
    # states none, and its instant is its object's modification time.
    return table.num_rows - table.column("msgpluginid").null_count


# Every line is a row either way. The sample spells no six-digit fraction, so
# the wider header dates the same lines the shipped one does.
assert plain.num_rows == read.num_rows == 14
assert dated(plain) == dated(read) == 10
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

The window is pushed into the read: the task sets `options.filter` to
`where_within("currunix", window)`, a native `Filter` spelling
`currunix >= '<start>' and currunix < '<end>'` and nothing else. The decode
still cuts every line and the record surface answers the clause over the rows
the lines become, so the read answers only the lines whose `currunix` -- the
event the read settled over the line, and the column the table is laid out by
-- falls in `[start, end)`. Nothing is filtered after the read, and the
result's `read` is the lines the window covers; a window the capture falls
outside reads 0. A line the header did not match is dated by the
modification time of the object it was read from, so the window of that
instant covers it and no other does; a line at the epoch -- one read from a
handle with no clock at all -- is in the window that covers 1970 alone.
The window is the last day, ending at the instant the run starts, when the
document names neither bound; `start` and `end` read the way every instant
here does, and `end` naming a whole day means the end of that day.

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
reader's rows on `curruuid`, the source-line identity: a stored row
carrying one of the window's keys is taken out and the window's row lands, in
one commit per bounded chunk. A missing table is created. If no existing file
can contain a key, this keyed write commits as an append; a matching replay is
an overwrite of only the affected files. The identity packs the line's
instant and its content code, and the code digests the object the line was
read from and its row number beside its body, so the fixture's 144 lines
answer 144 distinct codes and 144 identities and land as 144 rows -- distinct
without a composite key. The same bytes read from two URIs answer two codes
and two identities.

## Sample rows

The sample is 27 capture lines from `python/tests/data/ulbridge.log` that the
FIX walk later joins under business chain `00026877711XOEA0`, as
`parse_messages` lands them in `logs.messages`. An
identity is shown by its last eight hex digits behind a leading `…`, and the
stored value is sixteen bytes. `currhashcode` is shown whole, and a table
stores it as the signed integer Iceberg has, so half the codes read back
negative.

--8<-- "docs/pipeline/tasks/samples/parse-messages.md"

The generated include is authoritative for row membership and values. This
stage knows only capture headers and bytes; the business chain is established
later by [`parse_fix_refined`](parse-fix-refined.md#sample-rows).

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
line the header does not match is data: every capture is null, its whole
text is its body, and its `currunix` is the modification time of the object
it was read from -- a file's mtime, an S3 object's `LastModified`. Its
identity derives from that instant, so replay such a line from where the
capture was read, never from a copy written at another time.
