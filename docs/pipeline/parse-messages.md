# parse_messages

`parse_messages(source, catalog, window, *, rowheader=None, target=MESSAGES)`
recursively reads a local or object-store text source and lands one row per
line the window covers in `logs.messages`.

```python
import tempfile
from pathlib import Path

from rekep.iceberg import IcebergCatalog
from rekep.pipeline import Landed, parse_messages
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
catalog = IcebergCatalog.from_dict(
    {
        "name": "rekep",
        "properties": {
            "type": "sql",
            "uri": f"sqlite:///{root}/catalog.db",
            "warehouse": str(root / "warehouse"),
        },
    }
)
day = window_of("2026-08-14", "2026-08-14")
try:
    assert parse_messages("file:data/capture", catalog, day) == Landed(read=144, written=144)
    # A replay lands the same rows over the ones it landed: one per line.
    assert parse_messages("file:data/capture", catalog, day) == Landed(read=144, written=144)
    # A window the capture falls outside reads none of it.
    outside = window_of("2026-08-15", "2026-08-15")
    assert parse_messages("file:data/capture", catalog, outside).read == 0
    assert catalog.dataset("logs.messages").read_arrow_table().num_rows == 144

    try:
        parse_messages("file:data/elsewhere", catalog, day)
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("a capture that does not exist is refused")
finally:
    catalog.close()
```

`source` is an `IOBase`, or a URI bound here and closed after. One that does
not exist is refused: the native read of an absent path answers no rows, which
would read as a window without lines. `rowheader` names the header of a bridge
that writes the same facts in [a layout of its own](#a-bridge-that-writes-the-header-its-own-way).

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
and its level `level` and therefore dates nothing; rekep's spells `mtime`
and `loglevel`, with a wider, optional fraction. It captures `mtime`,
`msgthreadid`, `msgsessionid`, `msgctxid`, `msgseqnum`, `msgpluginid` and
`loglevel`, each named for what the native read fills from it. `mtime` fills
no column of its own: it is the record clock, consumed into `currunix` at
nanoseconds UTC whatever the width of its fraction, so the line's instant is
stated once rather than read twice. `parse_mtime` is the native default, on,
and is never turned off. The fraction reads every width this bridge writes:
three digits under a point (`.769`) or a comma (`,769`), none at all, and
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
step in the chain on a FIX row. [`parse_fix_raw`](parse-fix-raw.md) reads the
`body`, emits only native FixMsg columns, and records the line's `curruuid` in
`srcuuids`; it carries no column of the line beside the FIX row.

## A bridge that writes the header its own way

`rowheader` reads one. A capture is written by several loggers and they do not
always agree on the clock: the shipped capture spells its fraction `.769` on
129 lines and `.524_315` on 15, and the shipped header dates all 144. A line a
header misses is still a row -- dated by the modification time of the object
it was read from, with every capture null -- so the fraction a header admits
decides which lines it dates. A bridge writing six digits straight on,
`.524315`, is read by a header of its own through
`Message.text_options(rowheader)`; the width is a parameter, not an edit of
the constant, and it types no column either way.

```python
from rekep import IOBase, Message
from rekep.times import ULBRIDGE_ROWHEADER

fraction = r"(?:[.,]\d{3}(?:_\d{3})?)?"
widened = ULBRIDGE_ROWHEADER.replace(fraction, r"(?:[.,]\d{3}(?:_?\d{3})?)?")
narrowed = ULBRIDGE_ROWHEADER.replace(fraction, r"(?:[.,]\d{3})?")
source = IOBase.from_uri("file:data/capture")


def read(rowheader=None):
    return source.read_arrow_reader(options=Message.text_options(rowheader)).read_all()


def dated(table):
    # A line the header matched states its captures; one it did not match
    # states none, and its instant is its object's modification time.
    return table.num_rows - table.column("msgpluginid").null_count


plain, wide, narrow = read(), read(widened), read(narrowed)

# Every line is a row whatever the header. The capture spells no six-digit
# fraction, so the wider header dates the same lines the shipped one does; a
# header without the grouped micros misses the 15 lines that spell them.
assert plain.num_rows == wide.num_rows == narrow.num_rows == 144
assert dated(plain) == dated(wide) == 144
assert dated(narrow) == 129
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
rather than beside it. `parse_messages(..., rowheader=widened)` lands the
lines under such a header, and the same check refuses it there before
anything is read.

See the [complete 12-column schema](../products/message.md#complete-schema).

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

The window is pushed into the read: the stage sets `options.filter` to
`where_within("currunix", window)`, a native `Filter` spelling
`currunix >= '<start>' and currunix < '<end>'` and nothing else. The decode
still cuts every line and the record surface answers the clause over the rows
the lines become, so the read answers only the lines whose `currunix` -- the
event the read settled over the line, and the column the table is laid out by
-- falls in `[start, end)`. Nothing is filtered after the read, and `read` is
the lines the window covers; a window the capture falls outside reads 0. A
line the header did not match is dated by the modification time of the object
it was read from, so the window of that instant covers it and no other does;
a line at the epoch -- one read from a handle with no clock at all -- is in
the window that covers 1970 alone. `window_of` given neither bound is the last
day, ending at the instant it is read; `start` and `end` read the way every
instant here does, and `end` naming a whole day means the end of that day.

```python
from rekep.times import window_of

lower, upper = window_of("2026-08-14", "2026-08-14")
assert (lower.isoformat(), upper.isoformat()) == (
    "2026-08-14T00:00:00+00:00",
    "2026-08-15T00:00:00+00:00",
)
```

## Write step

The stage opens `logs.messages` with `Message.into_field()` and replaces the
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

## Failures

A missing source, unreadable object, invalid URI, a header that renames or
omits a capture, or a failed commit raises. A line the header does not match
is data: every capture is null, its whole text is its body, and its
`currunix` is the modification time of the object it was read from -- a
file's mtime, an S3 object's `LastModified`. Its identity derives from that
instant, so replay such a line from where the capture was read, never from a
copy written at another time.
