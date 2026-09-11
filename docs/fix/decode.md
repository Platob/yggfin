# Decode rules

One `FixCodec` powers one-line inspection and Arrow batch parsing. Syntax
adapters only produce ordered key/value pairs; one builder then performs
registry resolution, code translation, typing, group construction, arrival
recording, derived stamps, and schema projection. Everything on this page is
that one builder, seen from a different door.

## A pin is on the codec, a stage is a call

Every parameter that describes the whole run is stated once, when the codec is
built. Every parameter that describes one piece of work is an argument to the
call that does it.

| pin | meaning |
| --- | --- |
| `registry` | the dictionary every key resolves against |
| `branch` | the dialect a row is read under, unless its `pluginid` names another |
| `version` | which code spellings a value translates through |
| `separator` | the frame separator, when a capture is known to escape it |
| `payload_column` | the column a batch reads its line bytes from |
| `capture_names` | what the row-header captures of a decoded line are called |
| `null_values` | the spellings that mean nothing was sent |
| `direction` | what an unmarked line is read as: `sent`, `recv`, or `unknown` |
| `batch_byte_size` | the raw bytes one output batch targets |

```python
from rekep.fix import FixCodec, fix_codec, fix_registry

codec = fix_codec()

assert codec.registry == fix_registry()
assert codec.branch == "ulbridge"
assert codec.payload_column == "body"
assert codec.direction == "sent"
assert FixCodec(fix_registry()).branch is None
```

`rekep.fix.fix_codec()` is the one the bundled tasks use: the bundled registry,
the bridge dialect, and `Message.body` as the payload column.

## From log line to raw Message

The text stage removes only the matched ULBridge header. For example:

```text
2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218]
[OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4|35=8|55=ABBN.S|...
```

becomes one `Message` whose `timestamp`, `threadId`, `sessionUid`, `msgCtxId`,
`seqNum`, `pluginid`, and `level` come from the header and whose `body` starts
at `Receiving :`. The exact body digest is computed before `logs.messages` is
written.

```python
from rekep import IOBase, Message

source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
raw = source.read_arrow_reader(options=Message.text_options())
first_batch = next(iter(raw))

assert first_batch.column("rownum")[0].as_py() == 1
assert first_batch.column("pluginid")[0].as_py() == "ULBridge"

raw.close()
source.close()
```

The capture's own columns are named after what they state, and that naming is
load-bearing rather than cosmetic: [Capture columns](capture.md) is the rule
that decides which of them fill a FIX field and which are carried in front of
one.

## Every reader is the same parse

| reader | takes | answers |
| --- | --- | --- |
| `parse_line` | one captured line, verb and prose included | `FixMessages`, lazily: one message, or one per MBean of a bulk document |
| `parse_lines` | any iterable of lines | the same, one line at a time |
| `parse_text_line` | one decoded line: its body, clock and row-header captures | `FixMessages` |
| `parse_text_lines` | any iterable of those | the same, lazily |
| `parse_text_arrow_reader` | a `RecordBatchReader` of capture rows | a `RecordBatchReader` of fixed rows |
| `parse_ulconfig_line` | one bridge configuration document | `FixMessages`, one per selected MBean |
| `parse_fix_line` | one numeric frame | one `FixMsg` |
| `parse_ullink_line` | one bridge frame, keys spelled as names | one `FixMsg` |
| `parse_fixml_line` | one FIXML row | one `FixMsg` |
| `parse_pairs` | pairs the caller already split | one `FixMsg` |

The four stream doors answer a lazy iterator, so one message out of a capture of
ten million costs one. A line the reader cannot read is an error item the
stream continues past, never the end of the run.

```python
from rekep.fix import fix_codec

codec = fix_codec()
lines = [
    b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|10=000|",
    b"MSGTYPE=executionreport|SYMBOL=HOLN|LASTSHARES=235|",
]
symbols = [message.by_name("symbol").as_py() for message in codec.parse_lines(lines)]

assert symbols == ["AAPL", "HOLN"]
assert len(list(codec.parse_line(lines[0]))) == 1
```

## Payload location and syntax choice

`parse_line` first locates a `key=value` frame behind log prose. It then makes
one syntax decision for the whole payload; characters inside values are never
reinterpreted as a new syntax.

| decision | parser |
| --- | --- |
| first key before `=` is all ASCII digits | numeric FIX |
| an XML document is found behind a prefix | FIXML |
| payload starts with `{` | ULBridge/Jolokia configuration JSON |
| no key/value frame exists but the line contains `<` | FIXML |
| otherwise | ULLINK/bridge name-value row |

Message-code inference is shallower still and needs no registry, which is what
lets a capture be triaged before it is parsed:

```python
from rekep.fix import FixCodec

assert FixCodec.infer_msgtype_bytes(b"8=FIX.4.4|35=AE|") == b"AE"
assert FixCodec.infer_msgtype_text("MSGTYPE=executionreport|") == "executionreport"
```

## Numeric FIX rules

1. A pinned separator wins.
2. Printed SOH spellings `^A`, `\\x01`, `<SOH>`, and `{SOH}` are converted to
   byte `0x01` once, before the frame is split. A frame carrying a real `0x01`
   is never scanned for them.
3. Otherwise SOH, `|`, or `;` is inferred from the frame. A frame decides
   where its own pairs end, so `parse_fix_line` takes no separator argument.
4. Each segment splits at its first `=`. Empty keys and segments without `=`
   are ignored; duplicate tags retain arrival order.
5. Parsing stops after `CheckSum(10)`; text after it belongs to the log line,
   not the message.
6. A binary or data field honours the byte length its preceding length field
   states, but only where the frame stated a boundary there, so a payload may
   contain the outer separator without a stated length being trusted blindly.
   This applies to tags 89, 91, 96, 213, 349, 351, 353, 355, 357, 359, 361,
   363, 365, 446, 619, 622, 1185, 1398, 1402, 1404, and 1469.
7. `XmlData(213)` containing a bridge row is parsed after the outer FIX pairs.
   Outer values win where both layers state the same field.

```python
from rekep.fix import fix_codec

message = fix_codec().parse_fix_line(
    b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
)

assert message.field.name == "D"
assert message.by_tag(11).as_py() == "ORD-1"
assert message.by_name("orderqty").as_py() == 12.0
assert message.into_bytes(ord("|")) == (
    b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
)
```

## ULLINK and bridge-row rules

A row uses `|` when one is present, otherwise spaces. Each segment splits at
its first `=`. Names resolve through case/separator folding, aliases, and code
sets.

### Hash-prefixed keys

`#NOPARTYIDS=1` and `#NOPARTYIDS[0]=...` are bridge group spellings. The
leading `#` is dropped only when the row has no non-empty bare key of the same
folded identity. A marked pair restating a bare pair's bytes is one pair, row
and entries alike; a marked pair stating other bytes keeps its own column and
its own entry. So `ORDERID=123|#ORDERID=123` is `OrderID` once, while
`ORDERID=123|#ORDERID=345` is `OrderID` 123 beside `#ORDERID` 345.

### Packed groups

The counter and the occurrences are separate pairs:

```text
#NOPARTYIDS=1|
#NOPARTYIDS[0]=PARTYID=BROKER••PARTYIDSOURCE=C••PARTYROLE=1••|
```

The member separator may be EOT+ETX, SOH, `••`, or `▯▯`. Only the first
spelling an occurrence actually carries splits it, so a value legitimately
holding the other byte is not broken into fields nobody wrote. When neither
control spelling is present, the reader scans for the next direct member name
the addressed group declares, longest name first, so `PartyIDSource` is not
shortened to `PartyID`. A run with no declared boundary stays whole.

A group packed inside an occurrence nests at the same level as its siblings and
renders as `Parties[0].PartySubIDs[0].PartySubID`, at any depth up to the
sixty-four levels a message schema may nest. That rendering builds the row and
nothing else: the arrival record keeps the one pair the bridge wrote.

```python
from rekep.fix import fix_codec

message = fix_codec().parse_ullink_line(
    "#NOPARTYIDS=1|"
    "#NOPARTYIDS[0]=PARTYID=BROKER••PARTYIDSOURCE=C••PARTYROLE=1••|".encode()
)

assert message.by_path("Parties[0].PartyID").as_py() == "BROKER"
assert message.by_path("Parties[0].PartyRole").as_py() == 1
assert message.by_tag(453).as_py() == 1
```

A value path enters a list by its occurrence, so the index is written as
`Parties[0]`. A schema path has no positions to skip, which is why
`registry.field_by_path("Parties.PartyID")` needs none.

## Registry transcription

This bridge row demonstrates the full name/alias/code/type path:

```text
MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|
```

| arrival | registry resolution | stored value |
| --- | --- | --- |
| `MSGTYPE=executionreport` | canonical `msgtype`, tag 35; code name → `8` | `msgtype = "8"` |
| `SYMBOL=HOLN` | canonical `symbol`, tag 55 | `symbol = "HOLN"` |
| `SIDE=buy` | canonical `side`, tag 54; code name → `1` | `side = b"1"` |
| `LASTSHARES=235` | alias of `lastqty`, tag 32 | `lastqty = 235.0` |
| `LASTPX=72.28` | canonical `lastpx`, tag 31 | `lastpx = 72.28` |

```python
from rekep.fix import fix_codec

body = b"MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|"
message, = fix_codec().parse_line(body)

assert message.by_tag(35).as_py() == "8"
assert message.by_name("lastqty").as_py() == 235.0
assert message.entries() == [
    (35, "MSGTYPE", "executionreport"),
    (55, "SYMBOL", "HOLN"),
    (54, "SIDE", "buy"),
    (32, "LASTSHARES", "235"),
    (31, "LASTPX", "72.28"),
]
```

The typed projection is normalized; `entries()` is the original transcription.

## Nothing is lost at the end

An entry is three facts and its children: the tag its key resolved to, `0`
where the key named no field, and the key and value exactly as the line spelled
them. It states no branch — the branch a pair resolved in is one decision about
the whole message, so `FixMsg.branch` is where it is asked for, and a hundred
pairs do not repeat it a hundred times.

```python
from rekep.fix import fix_codec

message, = fix_codec().parse_line(b"MSGTYPE=executionreport|SYMBOL=AAPL|VENUEPRIVATEKEY=x|")
unknown = [entry for entry in message.entries() if entry[0] == 0]

assert message.branch == "ulbridge"
assert unknown == [(0, "VENUEPRIVATEKEY", "x")]
```

Neither the row nor the record derives from the other. A translated `1` cannot
say whether the wire carried `1` or `buy`, so lossless re-emission is only
possible from the record — which is exactly what makes the round trip work.

## JSON configuration and FIXML

A payload beginning with `{` is read as a bridge/Jolokia configuration
document. `parse_ulconfig_line` yields one flat typed message per selected
MBean, including every response; an empty bulk or wildcard answer yields none.
ObjectName properties such as plugin type remain owned by the ObjectName;
declared arrays become groups; unknown attributes remain nullable text.

FIXML contributes attributes in document order. Namespace prefixes are removed
from attribute names, nested elements flatten into paths, and element names do
not invent protocol fields.

```python
from rekep.fix import fix_codec

xml = b'<FIXML><Order ClOrdID="A-1" Side="1" OrderQty="5"/></FIXML>'
message = fix_codec().parse_fixml_line(xml)

assert message.by_name("clordid").as_py() == "A-1"
assert message.by_name("orderqty").as_py() == 5.0
```

## Resolution, translation, and typing

For each pair the builder:

1. parses a numeric tag or folds a name/path;
2. resolves the row's branch, then the standard branch;
3. creates an unknown nullable text field if no definition exists;
4. records the original pair in arrival order;
5. treats trimmed `""`, `null`, and `<null>` as absent by default, and a field's
   own `fix:nulls` spellings as absent after it resolves;
6. translates a versioned code name or wire code;
7. converts binary data from original bytes and scalar data from cleaned text;
8. leaves the typed value null on conversion failure while retaining the
   arrival pair.

A stated absence produces no field and no entry, because a key that said
nothing was sent is not a key that was sent.

Version selection is: explicit codec/row version, `ApplVerID(1128)`,
`BeginString(8)`, branch default, then the registry's newest applicable
version. Version affects code spelling, not column identity.

## Message ordering and derived values

Resolved children are ordered as FIX header, body, trailer, then runtime
fields. `beginstring` is supplied when the input did not state one. The market
timestamp uses the source row clock first, then message clocks in precision
order, then the Unix epoch. `unixpartition` follows it. `msghash` is computed
from the arrival record with session-envelope tags excluded.

Filling in what a message implied is its own stage, never a flag on the parse:

| stage | what it does |
| --- | --- |
| `enrich_message(message)` | fills one message's implied values — ISIN, MIC, state, quantities |
| `enrich_messages(messages)` | the same over a lazy stream |
| `enrich_messages_arrow_reader(reader)` | the same over batches of rows, without re-parsing them |
| `into_latest()` | restates a message at the dictionary's newest version |
| `lifecycle(messages)` | stamps `persistentid` from the rows before each one |

Only the row is filled. The arrival record is what the wire carried and is left
alone, so `into_bytes` re-emits the received line either way, and a stated
value is never replaced.

```python
from rekep.fix import fix_codec

codec = fix_codec()
wire = b"8=FIX.4.4|35=8|55=HOLN|38=300|14=300|31=72.28|32=300|39=2|150=F|10=0|"
message, = codec.parse_line(wire)
filled = codec.enrich_message(message)

assert message.get_by_name("leavesqty") is None
assert filled.by_name("leavesqty").as_py() == 0.0
assert filled.into_bytes(ord("|")) == message.into_bytes(ord("|"))
```

## Arrow parse step

```python
import pyarrow

from rekep import Message
from rekep.fix import fix_codec

schema = Message.into_field().into_arrow_schema()
batch = pyarrow.RecordBatch.from_pylist(
    [
        {
            "url": "file:///capture.log",
            "rownum": 1,
            "timestamp": None,
            "timepartition": None,
            "threadId": None,
            "sessionUid": None,
            "msgCtxId": None,
            "seqNum": 7,
            "pluginid": "OMS",
            "level": "INFO",
            "bodyhash": None,
            "body": b"Receiving : 8=FIX.4.4|35=D|55=AAPL|10=000|",
        }
    ],
    schema=schema,
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
parsed = fix_codec().parse_text_arrow_reader(source)
table = parsed.read_all()

assert table.column("symbol").to_pylist() == ["AAPL"]
assert table.column("msgseqnum").to_pylist() == [7]
assert table.column("pluginid").to_pylist() == ["OMS"]
assert table.column("msgdirection").to_pylist() == [b"RECV"]
assert table.schema.names[-2:] == ["nofixentries", "nounmappedfixentries"]
```

The output schema is decided before the first row is read, from the source
schema and the dictionary alone — never from the data. Batches close on the raw
bytes of the payload column against `batch_byte_size`, so several small input
batches accumulate into one output batch and one oversized input batch is split
by rows in proportion. A batch always holds at least one row.

## Failure behavior

`parse_line(b"")` raises because no row exists. The Arrow reader converts
content-level failures into an `unknown` message, so one damaged cell cannot
terminate a capture. I/O errors, an invalid payload column, malformed root
options, and an invalid registry remain errors. One source row always yields
one result row, except a bulk configuration document, which yields one per
MBean.

```python
import pytest

from rekep.fix import fix_codec

with pytest.raises(ValueError, match="expected a captured row"):
    list(fix_codec().parse_line(b""))
```

## Try one line

Paste a captured line — numeric FIX, a ULLINK bridge row, configuration JSON,
or FIXML — and read the pairs the codec resolves out of it, each against the
bundled registry:

<div data-fix="decode"></div>
