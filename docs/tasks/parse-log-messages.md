# parse_log_messages

`parse_log_messages(source, storages, window=None, *, rowheader=None, timezone=TIMEZONE, target=LOG_MESSAGES)`
reads a local or object-store text capture and lands one row per line whose
`currunix` falls in the window -- every line, given none -- in
`bronze.record_keeping.log_messages`.

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import LOG_MESSAGES, Landed, parse_log_messages
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
capture = "file:data/capture/ulbridge.log"
day = window_of("2026-08-14", "2026-08-14")
with storages:
    assert parse_log_messages(capture, storages, day) == Landed(read=144, written=144)
    # A rerun lands the same rows over the ones it landed: one per line.
    assert parse_log_messages(capture, storages, day) == Landed(read=144, written=144)
    # Given no window, every line lands, and the answer names the whole hours
    # its dated lines span, for the tasks after it to run over.
    landed = parse_log_messages(capture, storages)
    assert (landed.read, landed.written) == (144, 144)
    assert [bound.isoformat() for bound in landed.window] == [
        "2026-08-14T01:00:00+00:00",
        "2026-08-14T22:00:00+00:00",
    ]
    # A window the capture falls outside reads none of it.
    outside = window_of("2026-08-15", "2026-08-15")
    assert parse_log_messages(capture, storages, outside).read == 0

    lines = storages.dataset(LOG_MESSAGES)
    try:
        assert lines.read_arrow_table().num_rows == 144
    finally:
        lines.close()

    try:
        parse_log_messages("file:data/elsewhere.log", storages, day)
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("a capture that does not exist is refused")
```

`source` is an `IOBase`, or a URI bound here and closed after; a folder or a
prefix is read recursively, in natural order. One that does not exist is
refused, because the read of an absent path answers no rows, which would read
as a window without lines. Given no `window` the task lands
[every line](#the-window), `rowheader` reads a bridge that writes the same
facts in [a layout of its own](#a-bridge-that-writes-its-header-its-own-way),
and `timezone` names [the zone the bridge prints its clock in](#the-clocks-zone).

## Source URI forms

| source | example |
| --- | --- |
| relative local | `file:data/capture` |
| absolute POSIX | `file:///srv/capture/2026-08-14` |
| absolute Windows | `file:///C:/capture/2026-08-14` |
| AWS S3 | `s3://market-capture/ulbridge/2026/08/14?region=eu-west-1` |
| S3-compatible | `s3://capture/day?endpoint_override=minio:9000&scheme=http&force_path_style=true` |

The source URI configures the capture read; the catalogs' S3 settings are
their own properties, because the capture reader and the tables' FileIO are
independent owners. gzip and zstd objects are decompressed as they are read.

## The row

The read is `rekep.text.text_options()`, and the row it answers is the whole
of the table: `rekep.text.log_message_field()` is the read's own field,
narrowed to what Iceberg stores and nothing hand-declared beside it. It holds
23 columns: the 16 event columns every table opens with, then `body`, then
one column per row-header capture.

```python
from rekep import IOBase
from rekep.text import CAPTURES, RECORD_CLOCK, log_message_field, text_options

source = IOBase.from_uri("file:data/capture/ulbridge.log")
try:
    reader = source.read_arrow_reader(options=text_options())
    first = next(iter(reader))
    reader.close()
finally:
    source.close()

names = [member.name for member in log_message_field()]
assert len(names) == 23
assert names[:3] == ["currunix", "creaunix", "execunix"]
assert names[16:] == [
    "body",
    "msgthreadid",
    "msgsessionid",
    "msgctxid",
    "msgseqnum",
    "msgpluginid",
    "loglevel",
]
assert CAPTURES - {RECORD_CLOCK} == set(names[17:])
assert first.column("seqnum")[0].as_py() == 1
assert first.column("crosscode")[0].as_py() == "local://bound/data/capture/ulbridge.log"
assert first.column("msgpluginid")[0].as_py() == "ULBridge"
assert first.column("msgseqnum")[0].as_py() == 3088
```

| column | on a line |
| --- | --- |
| `currunix` | the instant the read settled over the line: the header's `mtime` capture, read in `timezone`, at nanoseconds UTC; a line the header did not date takes the modification time of the object it was read from |
| `curruuid` | the line's identity, a UUIDv7 over that instant and `currhashcode`; the table's key |
| `currhashcode` | XXH3-64 over the object, the header's captures except the clock, the row number and the body: two lines of identical bytes answer two codes |
| `crosscode` | the object the line was read from, as the identifier the read was addressed under: `local://bound/data/capture/ulbridge.log` for `file:data/capture/ulbridge.log` |
| `seqnum` | the line's row number in that object, counted from 1 |
| `state` | `UNKNOWN` (0): a line is not a lifecycle |
| `body` | the line past its row header, as text; empty where the header consumed the line |
| `msgthreadid`, `loglevel` | the bridge's thread and level, which stay on the line |
| `msgsessionid`, `msgctxid`, `msgseqnum`, `msgpluginid` | the bracket's session instance, context, sequence and plugin; the parse fills the FIX fields of the same names from them |

The other event columns -- `creaunix`, `execunix`, `recdunix`, `exprunix`,
`prevunix`, `snapunix`, `prevuuid`, `srcuuids` -- are empty on every line.
[The table page](../tables/bronze/log_messages.md) lists every column and
[its samples](../samples/bronze/log_messages.md) the capture's first lines.

## The row header

The header is `rekep.times.ULBRIDGE_ROWHEADER`, the bridge's layout as the
native read ships it, with its clock and its level captured under the names
the read fills: `rekep.text.CAPTURES` names `mtime`, the record clock the
read settles `currunix` from and stores nowhere else, and the six columns
above, each named for what the read fills from it. Nothing maps a spelling
onto a tag in between: the bracket's `msgseqnum` is `msgseqnum`, and it fills
`MsgSeqNum(34)` where a frame stated none.

The clock's fraction is three digits after a point, optionally followed by
the micros some of the bridge's loggers group after them, such as
`.524_315`. Its width types no column; what it decides is which lines the
header matches. A clock spelling
its millis after a comma, or no fraction at all, is not this layout: its
line is left unmatched, keeps its whole text as its body, states every
capture null, and is dated by its object's modification time. A bridge that
writes one is read under a header of its own.

## The clock's zone

The clock states no offset, so it is read in `timezone`, an IANA zone name:
the zone the bridge prints in, `rekep.times.TIMEZONE` unless stated --
`Europe/Zurich`, where the shipped capture's bridge prints. Read in its own
zone, a line is dated in the hour of the message it carries. Read in another, every line is
dated hours away from its message: the shipped capture read as UTC dates its
lines two hours after the frames they carry, which splits one delivery's
observations and moves messages across hourly windows
([Late events](../dags/index.md#late-events)).

```python
import datetime

from rekep import IOBase
from rekep.text import text_options

UTC = datetime.timezone.utc
source = IOBase.from_uri("file:data/capture/ulbridge.log")


def first_clock(**zone):
    reader = source.read_arrow_reader(options=text_options(**zone))
    try:
        return next(iter(reader)).column("currunix")[0].as_py()
    finally:
        reader.close()


try:
    # The first line is printed `2026-08-14 14:46:39.769`, in the bridge's summer time.
    local, utc = first_clock(), first_clock(timezone="UTC")
finally:
    source.close()

assert local == datetime.datetime(2026, 8, 14, 12, 46, 39, 769000, tzinfo=UTC)
assert utc - local == datetime.timedelta(hours=2)
```

## A bridge that writes its header its own way

`rowheader` reads one. A capture is written by several loggers, and they do
not always agree on the clock: the shipped capture spells its fraction `.769`
on 129 lines and `.524_315` on 15, and the shipped header dates all 144. A
line a header misses is still a row -- dated by its object's modification
time, with every capture null -- so the fraction a header admits decides
which lines it dates. A header widened to a comma and to no fraction at all
dates the lines the shipped one leaves to their object's clock:

```python
import datetime
import os
import tempfile
from pathlib import Path

from rekep import IOBase
from rekep.text import text_options
from rekep.times import ULBRIDGE_ROWHEADER

fraction = r"\.\d{3}(?:_\d{3})?"
assert fraction in ULBRIDGE_ROWHEADER
widened = ULBRIDGE_ROWHEADER.replace(fraction, r"(?:[.,]\d{3}(?:_\d{3})?)?")
narrowed = ULBRIDGE_ROWHEADER.replace(fraction, r"\.\d{3}")

capture = Path(tempfile.mkdtemp()) / "spellings.log"
capture.write_text(
    "2026-08-14 14:46:39.769 [23] [Jolokia] (DEBUG) a point\n"
    "2026-08-14 14:46:39.769_315 [23] [Jolokia] (DEBUG) grouped micros\n"
    "2026-08-14 14:46:39,769 [23] [Jolokia] (DEBUG) a comma\n"
    "2026-08-14 14:46:39 [23] [Jolokia] (DEBUG) no fraction\n",
    encoding="utf-8",
)
modified = datetime.datetime(2026, 8, 15, tzinfo=datetime.timezone.utc)
os.utime(capture, (modified.timestamp(), modified.timestamp()))


def read(uri, rowheader=None):
    source = IOBase.from_uri(uri)
    try:
        return source.read_arrow_reader(options=text_options(rowheader)).read_all()
    finally:
        source.close()


def dated(table):
    # A line the header matched states its captures; one it missed states none.
    return table.num_rows - table.column("msgpluginid").null_count


plain = read(capture.as_uri())
bodies = plain.column("body").to_pylist()
assert bodies[:2] == ["a point", "grouped micros"]
# The comma and the bare second are left whole, at the object's modification time.
assert bodies[2:] == capture.read_text(encoding="utf-8").splitlines()[2:]
assert plain.column("currunix").to_pylist()[2:] == [modified, modified]
assert dated(read(capture.as_uri(), widened)) == 4

shipped = "file:data/capture/ulbridge.log"
assert dated(read(shipped)) == dated(read(shipped, widened)) == 144
assert dated(read(shipped, narrowed)) == 129
```

What a header may change is the layout; what it may not change is the names,
because a capture named anything else is dropped in silence -- a column of
nulls and no error, or a clock that settles nothing. So a header that renames
or omits a capture is refused, by name, before anything is read:

```python
from rekep.text import text_options
from rekep.times import ULBRIDGE_ROWHEADER

renamed = ULBRIDGE_ROWHEADER.replace(r"(?P<loglevel>[A-Z]+)", r"(?P<severity>[A-Z]+)")
try:
    text_options(renamed)
except ValueError as refusal:
    assert "captures nothing for loglevel" in str(refusal)
    assert "captures severity, which the read fills nothing from" in str(refusal)
else:
    raise AssertionError("a renamed capture is refused")
```

## The window

The window is the read's own `where`: the task sets the options' filter to
`currunix >= start and currunix < end`, and the record surface answers it over
the rows the lines become, so nothing is filtered after the read and `read`
is the lines the window covers. A line is in the window its header's clock
falls in; a line the header did not match is in the window of its object's
modification time; a line read from a handle with no clock at all sits at the
epoch, in the window that covers 1970.

Given no window, every line is read and staged in a local Arrow stream file
while the span of the instants it was dated at is measured, then landed, and
`Landed.window` answers the whole hours that span covers --
`rekep.times.hour_window` over the earliest and latest `currunix`, lines at
the epoch dating nothing -- for the tasks after it to run over; it is None
where a window was given or no line was dated. The shipped capture spans
`[2026-08-14 01:00, 2026-08-14 22:00)` UTC.

## The write

The write replaces the window's rows on `curruuid` within the hour of
`currunix`: a stored row carrying one of the window's keys is taken out and
the window's row lands, in one commit per bounded chunk. If no stored file can
contain a key, the commit is an append. A rerun lands the same rows under the
same keys, so the table holds each line once.

The identity digests the object a line was read from, so the same bytes read
through two URIs, or from a copy written at another time, answer other
identities. Replay a capture from where it was first read.

## Streaming

One object is opened at a time, transport read-ahead is byte-bounded, emitted
batches are row-bounded, and the writer receives one `RecordBatchReader`;
nothing collects a capture into a table first, and a remote object is never
staged locally -- a windowless read stages the rows it decoded, not the
object. One physical line is not byte-bounded: a huge line can exceed
the target batch size. Concatenated gzip members should be validated against
the decoder before a production run, because staging locally is not a
substitute for a streaming decoder fix.
