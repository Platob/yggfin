# Decode rules

The same `FixCodec` powers one-line inspection and Arrow batch parsing. Syntax
adapters only produce ordered key/value pairs; one builder performs registry
resolution, code translation, typing, group construction, arrival recording,
derived stamps, and schema projection.

Parsing is the first of two stages over that one codec — parse, then
lifecycle — and the two doors onto them answer the same messages from the same
bytes: `fix_line_messages` reads lines one at a time, `fix_arrow_messages` and
`fix_arrow_reader` read a stored capture in batches.

## From log line to raw Message

The text stage removes only the matched ULBridge header. For example:

```text
2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218]
[OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4|35=8|55=ABBN.S|...
```

becomes one `Message` whose `timestamp`, `threadId`, `msgsessionid`,
`msgctxid`, `msgseqnum`, `pluginid`, and `level` come from the header and whose
`body` starts at `Receiving :`. The exact body digest is computed before
`logs.messages` is written.

Every capture is named for the column it fills, and the row header is the one
`ULBRIDGE_ROWHEADER` every reader takes, pinned against the native core's own
text: `msgsessionid`
is the session *instance* the bridge handled the line on (65032) and not
what the message itself says about the counterparty session
it names; `msgseqnum` fills `MsgSeqNum(34)`
where the frame stated none. Nothing maps a spelling onto a tag in between.

```python
from rekep import IOBase, Message

source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
raw = source.read_arrow_reader(options=Message.text_options())
first_batch = next(iter(raw))

assert first_batch.column("rownum")[0].as_py() == 1
assert first_batch.column("pluginid")[0].as_py() == "ULBridge"
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
| `MSGTYPE=executionreport` | canonical `msgtype`, tag 35; code name → `8` | `msgtype = "8"` |
| `SYMBOL=HOLN` | canonical `symbol`, tag 55 | `symbol = "HOLN"` |
| `SIDE=buy` | canonical `side`, tag 54; the event's own spelling | `side = "BUY"` |
| `LASTSHARES=235` | another spelling of `lastqty`, tag 32 | `lastqty = 235.0`, and `qty = 235` exactly |
| `LASTPX=72.28` | canonical `lastpx`, tag 31 | `lastpx = 72.28`, and `px = 72.28` exactly |

```python
from rekep.fix import fix_codec, fix_registry

body = b"MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|"
message = next(iter(fix_codec(fix_registry()).parse_line(body)))

assert message.by_tag(35).as_py() == "8"
assert message.by_name("lastqty").as_py() == 235.0
assert message.by_name("side").as_py() == "BUY"
# The price the message is about is what it last traded, exactly.
assert float(message.px.as_py()) == 72.28
# An entry is `(tag, name, value, entries)`, and what a typed column holds is
# not repeated in it: the arrival record is what the columns did not take.
assert [(tag, name) for tag, name, _, _ in message.entries()] == [
    (55, "symbol"),
    (381, "grosstradeamt"),
]
```

The typed projection is normalized; `entries()` is what arrived and no column
claimed, each pair carrying the tag and name the dictionary made of its key.

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
4. records the original pair in arrival order;
5. treats trimmed `""`, `null`, and `<null>` as absent by default;
6. translates a versioned code name or wire code;
7. converts binary data from original bytes and scalar data from cleaned text;
8. leaves the typed value null on conversion failure while retaining the
   arrival pair.

Version selection is what the row itself said: `ApplVerID(1128)`,
`BeginString(8)`, then the registry's newest applicable version. No version is
pinned on the codec — `fix_codec` refuses `version=` by name, along with any
keyword that is not one of its ten pins (`default_sending_time`, `separator`,
`payload_column`, `capture_names`, `null_values`, `direction`,
`batch_byte_size`, `batch_row_size`, `include_msgtypes`, `exclude_msgtypes`).
Version affects code spelling, not column identity.

## Message ordering and derived values

The row is band-ordered, not source-ordered: when it happened, which event it
is, which message and session carried it, which instrument and what the market
says about trading it, which order, what it states, how it went, the groups
kept whole, then every standard tag no band claimed. It ends `metadata`,
`nofixentries`, `fixentries`: the arrival record is a group named after itself,
under the counter that counts it.
`beginstring` is supplied when the input did not state one. A message that
stated no `TransactTime(60)` and no `SendingTime(52)` of its own takes the
codec's `UNDATED` floor — never the capture's own clock, which stamps nothing,
and never the instant the parse ran. `hashcode` is the content code over the
event's facts, its text, its metadata, the stated header cells and the entry
tree; `curruuid` is the identity over that code and the settled instant. No
partition column is materialized beside them, because a FIX row has none of its
own.

A parse fills what a message implied but did not carry — normalized symbol,
ISIN, MIC, `identifiers`, the side's own quote lane, the quantity another
quantity states — so there is no enriching stage after it. A derived value is
never silently presented as a stated wire pair: the arrival record is what the
wire carried and is left alone. **lifecycle** is the one stage that follows,
and it reads the messages as the chains they belong to: `prevuuid` and
`prevunix` naming the step before, `seqnum` where this one stands, `prevpx` and
`prevqty` what that step settled on, and the chain's first `creatunix`.
`snapunix` is empty on every row that is not a reading a walk took, which is
why it is nullable. A door turns that stage off with `lifecycle=False`.

## Arrow parse step

```python
import pyarrow

from rekep import Message
from rekep.fix import fix_codec, fix_registry, fix_text_options

schema = Message.into_field().into_arrow_schema()
batch = pyarrow.RecordBatch.from_pylist(
    [
        {
            "sourceurl": "file:///capture.log",
            "rownum": 1,
            "timestamp": None,
            "timepartition": None,
            "threadId": None,
            "msgsessionid": None,
            "msgctxid": None,
            "msgseqnum": 7,
            "pluginid": "OMS",
            "level": "INFO",
            "bodyhash": b"\x00" * 16,
            "body": b"Receiving : 8=FIX.4.4|35=D|55=AAPL|10=000|",
        }
    ],
    schema=schema,
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
codec = fix_codec(fix_registry(), options=fix_text_options())
parsed = codec.parse_text_arrow_reader(source)
table = parsed.read_all()

assert table.column("symbol").to_pylist() == ["AAPL"]
assert table.column("msgseqnum").to_pylist() == [7]
assert table.num_columns == 130
assert table.schema.names[-3:] == ["metadata", "nofixentries", "fixentries"]
```

Source columns lead the output unless a fixed column owns the same folded
name. A capture named for the field it fills therefore folds onto it:
`sourceurl`, `msgctxid` and `msgseqnum` land inside the fixed projection rather
than beside it. Message values always win over capture fills, and the capture's
own `timestamp` fills nothing: it is context and dates no message.

That is 130 because the core's door carries the capture whole: it reads each
message out of the payload column and answers every column it was given.
`fix_arrow_reader` below answers 128 — the same row less `body` and
`bodyhash`, which are what the parse *reads* rather than columns of its answer.
Both are a line's fact, a row after the parse is an event's, and `logs.messages`
is where they live.

## Two doors onto the same messages

`fix_line_messages` is the line door: `parse_text_lines`, then `lifecycle`,
over lines read one at a time. It dates no message from the line's own capture
clock — a bridge spells that clock the way a log spells one, not the way
`SendingTime` is spelled — so an undated message takes the codec's `UNDATED`
floor, and so does the same message read through the batch door.

```python
from rekep import IOBase, Message
from rekep.fix import fix_codec, fix_line_messages, fix_registry, fix_text_options

options = fix_text_options(Message.into_field())
codec = fix_codec(fix_registry(), options=options)
source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
lines = source.read_text_lines(options=options)
messages = list(fix_line_messages(codec, lines))

assert len(messages) == 79
assert messages[0].field.name == "8"
assert messages[0].by_name("pluginid").as_py() == "Virtu_TritonBlack_TradeCapture"

source.close()
```

That capture holds 144 lines and answers 79 messages, because a row is a
message and not a line. `fix_arrow_messages` is the batch door over the same
stages, and `fix_arrow_reader` runs it end to end: a stored capture in, settled
rows out under the shape `fix_parse_field` publishes — the parse's own, less
the line's text. One line carrying two frames answers two
rows under one `rownum`; one carrying none answers no row at all. The walk then
folds every hop that logged one message onto one identity, so those 79 messages
are 53 events.

```python
import pyarrow

from rekep import Message
from rekep.fix import fix_arrow_reader, fix_codec, fix_registry, fix_text_options

schema = Message.into_field().into_arrow_schema()
lines = [
    b"Receiving : 8=FIX.4.4|35=D|55=AAPL|10=000|",
    b"Enrichment execution[&SetEnv]",
    b"Relaying : 8=FIX.4.4|35=D|55=AAPL|10=000| and 8=FIX.4.4|35=8|55=HOLN|10=000|",
]
batch = pyarrow.RecordBatch.from_pylist(
    [
        {
            "sourceurl": "file:///capture.log",
            "rownum": rownum,
            "timestamp": None,
            "timepartition": None,
            "threadId": None,
            "msgsessionid": None,
            "msgctxid": None,
            "msgseqnum": None,
            "pluginid": "OMS",
            "level": "INFO",
            "bodyhash": b"\x00" * 16,
            "body": body,
        }
        for rownum, body in enumerate(lines, start=1)
    ],
    schema=schema,
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
codec = fix_codec(fix_registry(), options=fix_text_options())
settled = fix_arrow_reader(codec, source)
table = settled.read_all()

assert table.column("rownum").to_pylist() == [1, 3, 3]
assert table.column("symbol").to_pylist() == ["AAPL", "AAPL", "HOLN"]
assert table.num_columns == 128

settled.close()
```

## Failure behavior

`FixCodec.parse_line(b"")` raises because no row exists. A payload nobody could
read becomes an `unknown` message so one damaged cell cannot terminate a
capture. I/O errors, an invalid payload column, malformed root options, and an
invalid registry remain errors. One source row yields one row per message it
carried — two frames answer two rows, log prose answers none — and a replay of
the same bytes answers the same messages under the same identities.

## Try one line

Paste a captured line — numeric FIX, a ULLINK bridge row, configuration JSON,
or FIXML — and read the pairs the codec resolves out of it, each against the
bundled registry:

<div data-fix="decode"></div>
