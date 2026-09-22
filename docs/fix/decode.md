# Decode rules

The same `FixCodec` powers one-line inspection and Arrow batch parsing. Syntax
adapters only produce ordered key/value pairs; one builder performs registry
resolution, code translation, typing, group construction, arrival recording,
derived stamps, and schema projection.

Parsing is the first of two stages over that one codec, two tasks over two
tables -- parse into `fix.bronze`, lifecycle into `fix.silver` -- and each
stage has two doors that answer the same messages from the same bytes:
`fix_parse_lines` and `fix_lifecycle_messages` read messages one at a time,
`fix_parse_arrow_reader` and `fix_lifecycle_arrow_reader` read a stored table
in batches.

## From log line to raw Message

The text read frames each line under the ULBridge header and keeps the whole
line. For example:

```text
2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218]
[OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4|35=8|55=ABBN.S|...
```

becomes one `Message` whose `msgthreadid`, `msgsessionid`, `msgctxid`,
`msgseqnum`, `msgpluginid`, and `loglevel` are read off the header and whose
`body` is the line itself, header included, as text. `currunix` is the instant
the read settles over the line, `currhashcode` codes its content and
`curruuid` is the identity the read states over it; the read states all three,
before `logs.messages` is written.

Every capture is named for the column the native read fills from it, and the
row header is the one `ULBRIDGE_ROWHEADER` every reader takes, pinned against
the native core's own text: `mtime` is the record clock `currunix` is settled
from, and it fills no column of its own, because the row keeps the settled
instant rather than a second reading of it; `msgsessionid` is the session
*instance* the bridge handled the line on (65032) and not what the message
itself says about the counterparty session it names; `msgpluginid` is the
plugin that wrote the line (65009); `msgseqnum` fills `MsgSeqNum(34)` where the
frame stated none. Nothing maps a spelling onto a tag in between, and a capture
under any other name fills nothing -- a column of nulls and no error, or a
clock that settles nothing -- so `Message.text_options` refuses a supplied
header that renames one.

```python
from rekep import IOBase, Message

source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
raw = source.read_arrow_reader(options=Message.text_options())
first_batch = next(iter(raw))

assert first_batch.column("rownum")[0].as_py() == 1
assert first_batch.column("msgpluginid")[0].as_py() == "ULBridge"
assert first_batch.column("msgseqnum")[0].as_py() == 3088

raw.close()
source.close()
```

## Payload location and syntax choice

`parse_line` first locates a `key=value` frame behind log prose. It then
makes one syntax decision for the whole payload; characters inside values are
never reinterpreted as a new syntax. It answers a message per frame the line
carried: a line carrying two answers two, and a line carrying none answers
none.

| decision | parser |
| --- | --- |
| first key before `=` is all ASCII digits | numeric FIX |
| an XML document is found behind a prefix | FIXML |
| payload starts with `{` | ULBridge/Jolokia configuration JSON |
| no key/value frame exists but the line contains `<` | FIXML |
| otherwise | ULLINK/bridge name-value row |

Use a specific method when the caller already knows the syntax. A syntax
adapter answers the resolved pairs it read; `parse_line` and `parse_text_line`
are what answer messages:

| method | accepted input | answers |
| --- | --- | --- |
| `parse_fix_line` | numeric tags | resolved pairs |
| `parse_ullink_line` | names, alternate spellings, `#` keys, and packed groups | resolved pairs |
| `parse_plugin_line` | bridge configuration JSON | one message per `ObjectName` |
| `parse_fixml_line` | XML attributes | resolved pairs |
| `parse_pairs` | ordered pairs already split by the caller | resolved pairs |

## Numeric FIX rules

1. A pinned separator wins.
2. Printed SOH spellings `^A`, `\\x01`, `<SOH>`, and `{SOH}` are converted to
   byte `0x01` once.
3. Otherwise SOH, `|`, or `;` is inferred from the frame.
4. Each segment splits at its first `=`. Empty keys and segments without `=`
   are ignored; duplicate tags retain arrival order.
5. Parsing stops after `CheckSum(10)`; text after it belongs to the log line,
   not the message.
6. Binary/data fields honor the byte length in the preceding length field, so
   a payload may contain the outer separator. This applies to tags 89, 91, 96,
   213, 349, 351, 353, 355, 357, 359, 361, 363, 365, 446, 619, 622, 1185,
   1398, 1402, 1404, and 1469.
7. `XmlData(213)` containing a bridge row is parsed after the outer FIX pairs.
   Outer values win where both layers state the same field.

```python
from rekep.fix import fix_codec, fix_registry

codec = fix_codec(fix_registry())
message = next(
    iter(codec.parse_line(b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"))
)

assert message.field.name == "D"
assert message.by_tag(11).as_py() == "ORD-1"
assert message.by_name("orderqty").as_py() == 12.0
# Tag 38 is read through the crate's own `qty`, exactly, and the message
# re-emits the header and the event's own tags in front of what arrived.
assert float(message.qty.as_py()) == 12.0
assert message.into_bytes(ord("|")).startswith(b"8=FIX.4.4|35=D|")
assert b"11=ORD-1" in message.into_bytes(ord("|"))
```

## ULLINK and bridge-row rules

A row uses `|` when one is present, otherwise spaces. Each segment splits at
its first `=`. Names resolve through case/separator folding, the dictionary's
alternate spellings, and code
sets.

### Hash-prefixed keys

`#NOPARTYIDS=1` and `#NOPARTYIDS[0]=...` are bridge group spellings. The
leading `#` is removed only when the row has no non-empty bare key with the
same folded identity. Thus `ORDERID=123|#ORDERID=345` preserves two distinct
keys rather than collapsing two facts into one.

### Packed groups

The counter and occurrences are separate pairs:

```text
#NOPARTYIDS=1|
#NOPARTYIDS[0]=PARTYID=BROKER••PARTYIDSOURCE=C••PARTYROLE=1••|
```

The member separator may be EOT+ETX, SOH, `••`, or `▯▯`. Declared member names
also let a separator-less occurrence be split. The result is a list of structs
under `parties`, counted by `nopartyids`; nested group paths are rendered
recursively. Missing or
out-of-order indices create null gaps. Residue that cannot be split remains an
unknown value rather than failing the row.

```python
from rekep.fix import fix_codec, fix_registry

codec = fix_codec(fix_registry())
member = "\x04\x03"
row = (
    "recv |MSGTYPE=D|SYMBOL=TTF|SIDE=1|ORDERQTY=1200|#NOPARTYIDS=1|"
    f"#NOPARTYIDS[0]=PARTYID=BUYSIDE{member}PARTYIDSOURCE=D{member}PARTYROLE=1|"
)
message = next(iter(codec.parse_line(row.encode())))

assert message.by_name("nopartyids").as_py() == 1
assert message.by_path("Parties[0].PartyID").as_py() == "BUYSIDE"
assert message.by_path("Parties[0].PartyRole").as_py() == 1
```

A row that states a group and nothing else states no message: the counter and
its occurrences are pairs like any others, so the line still has to be one the
codec reads as a message.

## Registry transcription

This bridge row demonstrates the full name/alias/code/type path:

```text
MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|
```

| arrival | registry resolution | stored value |
| --- | --- | --- |
| `MSGTYPE=executionreport` | canonical `msgtype`, tag 35; code name to `8` | `msgtype = "8"` |
| `SYMBOL=HOLN` | canonical `symbol`, tag 55 | `symbol = "HOLN"` |
| `SIDE=buy` | canonical `side`, tag 54; the event's own spelling | `side = "BUY"` |
| `LASTSHARES=235` | another spelling of `lastqty`, tag 32 | `lastqty = 235.0`, and the trait `qty` answers 235 exactly |
| `LASTPX=72.28` | canonical `lastpx`, tag 31 | `lastpx = 72.28`, and the trait `px` answers 72.28 exactly |

```python
from rekep.fix import fix_codec, fix_registry

body = b"MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|"
message = next(iter(fix_codec(fix_registry()).parse_line(body)))

assert message.by_tag(35).as_py() == "8"
assert message.by_name("lastqty").as_py() == 235.0
assert message.by_name("side").as_py() == "BUY"
# The price the message is about is what it last traded, exactly.
assert float(message.px.as_py()) == 72.28
# An entry is `(tag, name, value, entries)`, each pair under the tag and name
# the dictionary made of its key: `SIDE=buy` is the entry `(54, "side", "1")`.
assert [(tag, name) for tag, name, _, _ in message.entries()] == [
    (55, "symbol"),
    (54, "side"),
    (59, "timeinforce"),
    (381, "grosstradeamt"),
]
```

The typed message retains its protocol entries in memory. In a fixed Arrow
row, `fixentries` is residual: fields and complete groups represented by
lifted columns are omitted from that second representation.

## JSON configuration and FIXML

A payload beginning with `{` is read as a bridge/Jolokia configuration
document. `Plugin.from_json_bytes` exposes each plugin, while
`parse_plugin_line` reads one message per `ObjectName` the document names, and
none where it names no configuration. ObjectName properties such as plugin type
remain owned by the ObjectName; declared arrays become groups; unknown
attributes remain nullable text.

FIXML contributes attributes in document order. Namespace prefixes are removed
from attribute names, nested elements flatten into paths, and element names do
not invent protocol fields.

A FIXML row that names no message type is one the codec refuses before it
builds a frame, because the untyped line is one of the three the message-type
filter excludes by default, so a document worth a row names its own:

```python
from rekep.fix import fix_codec

xml = b'<FIXML><Order MsgType="D" ClOrdID="A-1" Side="1" OrdQty="5"/></FIXML>'
message = next(iter(fix_codec().parse_line(xml)))

assert message.by_tag(35).as_py() == "D"
assert message.by_name("clordid").as_py() == "A-1"
```

## Resolution, translation, and typing

For each pair the builder:

1. parses a numeric tag or folds a name/path;
2. resolves it against the one namespace the registry is;
3. creates an unknown nullable text field if no definition exists;
4. records the pair in the message's entry tree;
5. treats trimmed empty text, `null`, `<null>`, `none`, `n/a`, and `[n/a]`
   case-insensitively as absent by default;
6. translates a versioned code name or wire code;
7. converts binary data from original bytes and scalar data from cleaned text;
8. leaves the typed value null on conversion failure while retaining the
   pair as residual data.

Version selection is what the row itself said: `ApplVerID(1128)`,
`BeginString(8)`, then the registry's newest applicable version. No version is
pinned on the codec. Native `FixCodec` validates keyword names; useful pins
include `default_sending_time`, `official_time_delay_ms`, `separator`,
`payload_column`, `capture_names`, `null_values`, `direction`,
`batch_byte_size`, `batch_row_size`, `include_msgtypes`, `exclude_msgtypes`,
`threads`, and `snapshot_ns`. Version affects code spelling, not column
identity. The batch defaults are 32,768 rows and 128 MiB;
`official_time_delay_ms` defaults to 1,000; `threads` defaults to the
available CPU count and zero means one.
Prefix stripping belongs to `TextOptions.lstrip`, which accepts a list of
anchored regular expressions such as `[r"^\s*-->\s*"]`; it changes the
retained raw `body` and therefore its identity. It is not a codec option and
the FIX tasks do not enable it. Native FIX already locates frames after
whitespace or `-->`; preserving header captures behind any earlier prefix
requires a supplied `rowheader` pattern that includes that prefix.

## Message ordering and derived values

Resolved children are ordered as FIX header, body, trailer, then the crate's
own fields, and the row ends `metadata`, `nofixentries`, `fixentries`. The last
two describe only the residual protocol tree in an Arrow row.
`beginstring` is supplied when the input did not state one. The parse dates a
message by the official transaction clock standing within
`official_time_delay_ms` of the `SendingTime(52)` it stated --
`TransactTime(60)`, else the `TrdRegTimestamp(769)` whose
`TrdRegTimestampType(770)` says it is about the event or a hop -- and by that
`SendingTime` otherwise: one stating none takes the codec's `UNDATED` floor --
never the capture's own clock, which stamps nothing, and never the instant the
parse ran -- until the walk dates it by the `TransactTime(60)` it states.
`currhashcode` is the content code over the event's facts, its text, its
metadata, the stated header cells and the entry tree; `curruuid` is an RFC 9562
UUIDv7: the settled millisecond in its leading 48 bits, the sequence's low 12
in `rand_a`, and in `rand_b` the low 62 of XXH3-64 over the big-endian
`(seqnum, digest)` tuple seeded by `crosshashcode`.
No partition column is materialized beside them, because a FIX row has none of
its own.

A parse fills what a message implied about itself -- its deprecated fields
restated to their latest spellings, the dictionary's own derivations run, the
`identifiers` its message component declares filled -- so there is no
enriching stage after it, and `fix.bronze` is that and nothing more.
**lifecycle** is the one stage that follows, and it reads the messages as the
chains they belong to, filling what a message implied about the message before
it: `prevuuid` and `prevunix` naming the step before, `seqnum` where this one
stands, `parentuuids` what it descends from, and the `creaunix`, `exprtime`
and `state` its chain folds forward. Those four are empty on every bronze row,
because nothing has walked yet, and `fix.silver` is the same rows walked.
`snapunix` is empty on every row that is not a reading a walk took, which is
why it is nullable.

## Arrow parse step

```python
import uuid

import pyarrow

from rekep import Message
from rekep.fix import fix_codec, fix_parse_arrow_reader, fix_registry
from rekep.times import EPOCH

schema = Message.into_field().into_arrow_schema()
batch = pyarrow.RecordBatch.from_pylist(
    [
        {
            "currunix": EPOCH,
            "curruuid": b"\x01" * 16,
            "currhashcode": 0,
            "sourceurl": "file:///capture.log",
            "rownum": 1,
            "body": "Receiving : 8=FIX.4.4|35=D|55=AAPL|10=000|",
            "msgthreadid": None,
            "msgsessionid": None,
            "msgctxid": None,
            "msgseqnum": 7,
            "msgpluginid": "OMS",
            "loglevel": "INFO",
        }
    ],
    schema=schema,
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
codec = fix_codec(fix_registry())
parsed = fix_parse_arrow_reader(codec, source)
table = parsed.read_all()

assert table.column("symbol").to_pylist() == ["AAPL"]
assert table.column("srcuuids").to_pylist() == [[uuid.UUID(bytes=b"\x01" * 16)]]
assert table.num_columns == 128
assert table.schema.names[-3:] == ["metadata", "nofixentries", "fixentries"]
```

The input batch is the raw 12-column `Message` contract, in the native event
layout's own order. The output is exactly the native 128-column FixMsg
contract: it holds neither `body` nor a column raw to a line. The input line's
`curruuid` becomes a `srcuuids` provenance entry, so `sourceurl`, `rownum`,
`msgthreadid`, `loglevel` and the line's own text remain available by joining
back to `logs.messages`. The parse reads those stored sixteen bytes back as the
identity the read stated over the line rather than recomputing one, so the join
is exact.

## Two doors onto the same messages

`fix_parse_lines` is the line door of the parse and `fix_lifecycle_messages`
the line door of the walk, over messages read one at a time. A line door pins
the header's capture order on the codec, because it resolves a bracket part by
position. It dates no message from `currunix`, the instant the read settled
over the line -- a bridge stamps a line the way a log is stamped, not the way
`SendingTime` is spelled -- so an undated message takes the codec's `UNDATED`
floor, and so does the same message read through the batch door. That floor is
the epoch, the same instant a line the header could not date settles at, so
both sit in every window rather than outside all of them.

```python
from rekep import IOBase
from rekep.fix import fix_codec, fix_parse_lines, fix_registry, fix_text_options

options = fix_text_options()
codec = fix_codec(fix_registry(), options=options)
source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
lines = source.read_text_lines(options=options)
messages = list(fix_parse_lines(codec, lines))

assert len(messages) == 79
assert messages[0].field.name == "8"

source.close()
```

That capture holds 144 lines and answers 79 messages, because a row is a
message and not a line. `fix_parse_arrow_reader` is the batch door of the
parse and `fix_lifecycle_arrow_reader` the batch door of the walk: a stored
table in, rows out under the parse's own shape. They are what
`parse_fix_bronze` and `parse_fix_silver` take, because each holds a table.
One line carrying two frames answers two rows with the same source UUID; one
carrying none answers no row at all. The parse folds every hop that logged
one message onto one identity, so those 79 messages are 49 bronze
events; the walk merges the observations of one event and therefore lands 22
rows for this fixture.

```python
import uuid

import pyarrow

from rekep import Message
from rekep.fix import fix_codec, fix_parse_arrow_reader, fix_registry
from rekep.times import EPOCH

schema = Message.into_field().into_arrow_schema()
lines = [
    "Receiving : 8=FIX.4.4|35=D|55=AAPL|10=000|",
    "Enrichment execution[&SetEnv]",
    "Relaying : 8=FIX.4.4|35=D|55=AAPL|10=000| and 8=FIX.4.4|35=8|55=HOLN|10=000|",
]
batch = pyarrow.RecordBatch.from_pylist(
    [
        {
            "currunix": EPOCH,
            "curruuid": rownum.to_bytes(16, "big"),
            "currhashcode": 0,
            "sourceurl": "file:///capture.log",
            "rownum": rownum,
            "body": body,
            "msgthreadid": None,
            "msgsessionid": None,
            "msgctxid": None,
            "msgseqnum": None,
            "msgpluginid": "OMS",
            "loglevel": "INFO",
        }
        for rownum, body in enumerate(lines, start=1)
    ],
    schema=schema,
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
codec = fix_codec(fix_registry())
parsed = fix_parse_arrow_reader(codec, source)
table = parsed.read_all()

assert table.column("symbol").to_pylist() == ["AAPL", "AAPL", "HOLN"]
assert table.column("srcuuids").to_pylist() == [
    [uuid.UUID(bytes=bytes.fromhex("00" * 15 + "01"))],
    [uuid.UUID(bytes=bytes.fromhex("00" * 15 + "03"))],
    [uuid.UUID(bytes=bytes.fromhex("00" * 15 + "03"))],
]
assert table.num_columns == 128

parsed.close()
```

## Failure behavior

`FixCodec.parse_line(b"")` raises because no row exists. A payload nobody could
read becomes an `unknown` message so one damaged cell cannot terminate a
capture. I/O errors, an invalid payload column, malformed root options, and an
invalid registry remain errors. One source row yields one row per message it
carried -- two frames answer two rows, log prose answers none -- and a replay of
the same bytes answers the same messages under the same identities.

## Try one line

Paste a captured line -- numeric FIX, a ULLINK bridge row, configuration JSON,
or FIXML -- and read the pairs the codec resolves out of it, each against the
bundled registry:

<div data-fix="decode"></div>
