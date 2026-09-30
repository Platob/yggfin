# Decode rules

One `FixCodec` powers one-line inspection and Arrow batch parsing. Syntax
adapters only produce ordered key/value pairs; one builder performs registry
resolution, code translation, typing, group construction, residual recording,
derived stamps and the fixed row's projection.

The codec has two stages, each landing a table: the parse, which
[`parse_fix_messages_raw`](../tasks/parse-fix-messages-raw.md) lands in bronze
`fix_messages`, and the lifecycle walk, which
[`parse_fix_messages_refined`](../tasks/parse-fix-messages-refined.md) lands
in silver. Each has two doors that answer the same messages from the same
bytes: `rekep.fix.fix_parse_lines` and `fix_lifecycle_messages` read
messages one at a time, `fix_parse_arrow_reader` and
`fix_lifecycle_arrow_reader` read a stored table in batches.

## From a log line to a stored line

The text read frames each line under the bridge's row header: the captures
are what the header stated, and `body` is what the bridge printed after it.

```text
2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218]
[OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4|35=8|55=ABBN.S|...
```

becomes one `log_messages` row whose `msgthreadid`, `msgsessionid`,
`msgctxid`, `msgseqnum`, `msgpluginid` and `loglevel` are read off the header
and whose `body` is `Receiving : 8=FIX.4.4|35=8|55=ABBN.S|...`.
[`parse_log_messages`](../tasks/parse-log-messages.md#the-row) documents the
row; `msgsessionid` is the session *instance* the bridge handled the line on
(65020) and not the counterparty session a message names, `msgpluginid` the
plugin that wrote the line (65017), and `msgseqnum` fills `MsgSeqNum(34)`
where a frame stated none.

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
adapter answers the resolved pairs it read; `parse_line` and
`parse_text_line` are what answer messages:

| method | accepted input | answers |
| --- | --- | --- |
| `parse_fix_line` | numeric tags | resolved pairs |
| `parse_ullink_line` | names, alternate spellings, `#` keys, and packed groups | resolved pairs |
| `parse_plugin_line` | bridge configuration JSON | one message per `ObjectName` |
| `parse_fixml_line` | XML attributes | resolved pairs |
| `parse_pairs` | ordered pairs already split by the caller | resolved pairs |

## Numeric FIX rules

1. A pinned separator wins.
2. Printed SOH spellings `^A`, `\\x01`, `<SOH>` and `{SOH}` are converted to
   byte `0x01` once.
3. Otherwise SOH, `|` or `;` is inferred from the frame.
4. Each segment splits at its first `=`. Empty keys and segments without `=`
   are ignored; duplicate tags retain arrival order.
5. Parsing stops after `CheckSum(10)`; text after it belongs to the log line,
   not the message.
6. Binary/data fields honor the byte length in the preceding length field, so
   a payload may contain the outer separator. This applies to tags 89, 91, 96,
   213, 349, 351, 353, 355, 357, 359, 361, 363, 365, 446, 619, 622, 1185,
   1398, 1402, 1404 and 1469.
7. `XmlData(213)` containing a bridge row is parsed after the outer FIX pairs.
   Outer values win where both layers state the same field.

```python
from rekep import FixCodec

codec = FixCodec.from_env()
message = next(iter(codec.parse_line(b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|")))

assert message.field.name == "D"
assert message.by_tag(11).as_py() == "ORD-1"
assert message.by_name("orderqty").as_py() == 12
# Tag 38 is read through the crate's own `quantity`, exactly, and the message
# re-emits the header and the event's own tags in front of what arrived.
assert message.quantity.as_py() == 12
assert message.into_bytes(ord("|")).startswith(b"8=FIX.4.4|35=D|")
assert b"11=ORD-1" in message.into_bytes(ord("|"))
```

## ULLINK and bridge-row rules

A row uses `|` when one is present, otherwise spaces. Each segment splits at
its first `=`. Names resolve through case and separator folding, the
dictionary's alternate spellings, and code sets.

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

The member separator may be EOT+ETX, SOH, `••` or `▯▯`. Declared member
names also let a separator-less occurrence be split. The result is a list of
structs under `parties`, counted by `nopartyids`; nested group paths are
rendered recursively. Missing or out-of-order indices create null gaps.
Residue that cannot be split remains an unknown value rather than failing the
row.

```python
from rekep import FixCodec

codec = FixCodec.from_env()
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

This bridge row demonstrates the full name, alias, code and type path:

```text
MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|
```

| arrival | registry resolution | stored value |
| --- | --- | --- |
| `MSGTYPE=executionreport` | canonical `msgtype`, tag 35; code name to `8` | `msgtype = "8"` |
| `SYMBOL=HOLN` | canonical `symbol`, tag 55 | `symbol = "HOLN"` |
| `SIDE=buy` | canonical `side`, tag 54; stored as its code | `side = 1`, `Side.BUY` |
| `LASTSHARES=235` | another spelling of `lastqty`, tag 32 | `lastqty = 235`, an exact decimal |
| `LASTPX=72.28` | canonical `lastpx`, tag 31 | `lastpx = 72.28`, an exact decimal |

```python
from decimal import Decimal

from rekep import FixCodec, Side

body = b"MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|"
message = next(iter(FixCodec.from_env().parse_line(body)))

assert message.by_tag(35).as_py() == "8"
# `Side(54)` is stored as its code, which reads back as its member.
assert message.by_name("side").as_py() is Side.BUY
assert message.by_name("lastqty").as_py() == 235
assert message.by_name("lastpx").as_py() == Decimal("72.28")
# `price` is Price(44) alone, which this row does not state.
assert message.price is None
# An entry is `(tag, name, value, entries)`, each pair under the tag and name
# the dictionary made of its key.
assert [(tag, name) for tag, name, _, _ in message.entries()] == [
    (55, "symbol"),
    (54, "side"),
    (59, "timeinforce"),
    (381, "grosstradeamt"),
]
```

The typed message retains its protocol entries in memory. In a fixed Arrow
row, `fixentries` is residual: fields and complete groups represented by
lifted columns are omitted from that second representation. It is a sorted
map keyed `tag:name` as the dictionary spells the field, `55:symbol`, whose
value is a field's wire text or a group's entries as JSON keyed the same way;
a pair no dictionary definition names is no entry of it but a key of
`metadata`, spelled as the message spelled it.

## JSON configuration and FIXML

A payload beginning with `{` is read as a bridge/Jolokia configuration
document. `parse_plugin_line` reads one message per `ObjectName` the document
names, and none where it names no configuration. ObjectName properties such as
plugin type remain owned by the ObjectName; declared arrays become groups;
unknown attributes remain nullable text.

FIXML contributes attributes in document order. Namespace prefixes are
removed from attribute names, nested elements flatten into paths, and element
names do not invent protocol fields. A FIXML document worth a row names its
own message type:

```python
from rekep import FixCodec

xml = b'<FIXML><Order MsgType="D" ClOrdID="A-1" Side="1" OrdQty="5"/></FIXML>'
message = next(iter(FixCodec.from_env().parse_line(xml)))

assert message.by_tag(35).as_py() == "D"
assert message.by_name("clordid").as_py() == "A-1"
```

## Resolution, translation and typing

For each pair the builder:

1. parses a numeric tag or folds a name or path;
2. resolves it against the one namespace the registry is;
3. creates an unknown nullable text field if no definition exists;
4. records the pair in the message's entry tree;
5. treats trimmed empty text, `null`, `<null>`, `none`, `n/a` and `[n/a]`
   case-insensitively as absent by default -- `null_values` replaces the set;
6. translates a versioned code name or wire code;
7. converts binary data from original bytes and scalar data from cleaned text;
8. leaves the typed value null on conversion failure while retaining the pair
   as residual data.

Version selection is what the row itself said: `ApplVerID(1128)`,
`BeginString(8)`, then the registry's newest applicable version. No version
is pinned on the codec, and a version affects code spelling, not column
identity. The native codec validates every pin: `default_sending_time`,
`official_time_delay_ms` (1,000 by default), `separator`, `payload_column`,
`capture_names`, `null_values`, `direction`, `batch_byte_size` and
`batch_row_size` (128 MiB and 32,768 rows by default), `include_msgtypes`,
`exclude_msgtypes`, `threads` (the available CPU count by default, and zero
means one) and `snapshot_ns`. Prefix stripping belongs to the text read's
`TextOptions.lstrip`, not to the codec, and no task enables it.

## Dates, identities and derived values

Resolved children are ordered as FIX header, body, trailer, then the crate's
own fields, and the row ends `metadata`, `fixentries`.
`beginstring` is supplied when the input did not state one.

The parse dates a message by the official transaction clock standing within
`official_time_delay_ms` of the `SendingTime(52)` it stated --
`TransactTime(60)`, else a `TrdRegTimestamp(769)` about the event or a hop --
and by that `SendingTime` otherwise. One stating no `SendingTime` measures
its transaction clock against the line it was read off the same way, and is
dated by the line where none stands that near; one read off no line at all
takes the codec's `default_sending_time`, which the tasks pin at
`rekep.fix.UNDATED`, the epoch, so a replay of the same bytes answers the
same identity. The walk then dates a message by the `TransactTime(60)` it
states where the parse could not.

`currhashcode` is the content code over the event's facts, its text, its
metadata, the stated header cells and the entry tree; `curruuid` the identity
the codec derives from the instant and that content, which rekep stores
unchanged and never reconstructs. `crosscode` is the first of `OrderID`,
`ClOrdID`, `OrigClOrdID`, `QuoteID`, `QuoteReqID` and `MDReqID` stated, and
`crossuuid` and `crosshashcode` derive from it. `msgsesseventid` joins the
message type, the session instance, the context and `MsgSeqNum` by `:`
where all four are stated. `state` is the [lifecycle code](../tables/states.md)
the message's own status tags or message type ask for.

A parse fills what a message implied about itself -- its deprecated fields
restated to their latest spellings, the dictionary's own derivations run --
and nothing more: `seqnum`, `prevuuid` and `prevunix` are empty on every
bronze row. The walk fills what a message implied about the one before it,
and the `creaunix`, `exprunix` and `state` its chain folds forward.

## Arrow parse step

```python
import uuid

import pyarrow

from rekep import FixCodec
from rekep.fix import PARSE_COLUMNS, UNDATED, fix_parse_arrow_reader
from rekep.text import log_message_field
from rekep.times import EPOCH

stored = log_message_field().into_arrow_schema()
schema = pyarrow.schema([stored.field(name) for name in PARSE_COLUMNS])
bodies = [
    "Receiving : 8=FIX.4.4|35=D|55=AAPL|10=000|",
    "Enrichment execution[&SetEnv]",
    "Relaying : 8=FIX.4.4|35=D|55=AAPL|10=000| and 8=FIX.4.4|35=8|55=HOLN|10=000|",
]
batch = pyarrow.RecordBatch.from_pylist(
    [
        {
            "currunix": EPOCH,
            "curruuid": uuid.UUID(int=seqnum),
            "body": body,
            "msgpluginid": "OMS",
        }
        for seqnum, body in enumerate(bodies, start=1)
    ],
    schema=schema,
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
parsed = fix_parse_arrow_reader(FixCodec.from_env(default_sending_time=UNDATED), source)
table = parsed.read_all()

# Prose answers no row; a line carrying two frames answers two.
assert table.column("symbol").to_pylist() == ["AAPL", "AAPL", "HOLN"]
assert table.column("srcuuids").to_pylist() == [
    [uuid.UUID(int=1)],
    [uuid.UUID(int=3)],
    [uuid.UUID(int=3)],
]
assert table.num_columns == 133
assert table.schema.names[-2:] == ["metadata", "fixentries"]
```

The input is the stored `log_messages` row projected to
`rekep.fix.PARSE_COLUMNS`, the seven columns a parse consumes, which is what
`parse_fix_messages_raw` pushes into its scan. The output is the dictionary's
fixed row, which holds neither `body`, `msgthreadid` nor `loglevel`; its
`crosscode` and `seqnum` are the message's chain and its step in it, not the
line's object and row number. The line's `curruuid` becomes the row's one
`srcuuids` entry: the table stores it as the `uuid` the read stated over
the line, and the parse reads it as it is, so the join back is exact.

## Two doors onto the same messages

`fix_parse_lines` is the line door of the parse, over lines read straight
off a capture. It pins the header's capture order on the codec, because it
resolves a bracket part by position:

```python
from rekep import FixCodec, IOBase
from rekep.fix import UNDATED, fix_parse_lines
from rekep.text import text_options

options = text_options()
codec = FixCodec.from_env(default_sending_time=UNDATED, capture_names=options.capture_names)
source = IOBase.from_uri("file:data/capture/ulbridge.log")
try:
    messages = list(fix_parse_lines(codec, source.read_text_lines(options=options)))
finally:
    source.close()

assert len(messages) == 135
assert messages[0].field.name == "8"
assert messages[0].by_name("msgpluginid").as_py() == "ULBridge"
```

The capture's 144 lines carry 79 frames and answer 135 messages, because a
row is a message and not a line, and each of the 56 reports of a fill answers
the execution it reports beside itself. `fix_parse_arrow_reader` is the batch
door the task uses, over a stored table; over the capture's day it answers
the same 135 messages, which the key folds to 77 bronze rows.

## Failure behavior

`FixCodec.parse_line(b"")` raises because no row exists. A payload nobody
could read becomes an `unknown` message, so one damaged cell cannot terminate
a capture. I/O errors, an invalid payload column, malformed options and an
invalid registry remain errors. A replay of the same bytes answers the same
messages under the same identities.

## Try one line

Paste a captured line -- numeric FIX, a ULLINK bridge row, configuration
JSON, or FIXML -- and read the pairs the codec resolves out of it, each
against the bundled registry:

<div data-fix="decode"></div>
