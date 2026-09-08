# Decode rules

The same `FixCodec` powers one-line inspection and Arrow batch parsing. Syntax
adapters only produce ordered key/value pairs; one builder performs registry
resolution, code translation, typing, group construction, arrival recording,
derived stamps, and schema projection.

## From log line to raw Message

The text stage removes only the matched ULBridge header. For example:

```text
2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218]
[OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4|35=8|55=ABBN.S|...
```

becomes one `Message` whose `timestamp`, `threadId`, `sessionUid`, `msgCtxId`,
`seqNum`, `plugin`, and `level` come from the header and whose `body` starts at
`Receiving :`. The exact body digest is computed before `logs.messages` is
written.

```python
from rekep import IOBase, Message

source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
raw = source.read_arrow_reader(options=Message.text_options())
first_batch = next(iter(raw))

assert first_batch.column("rownum")[0].as_py() == 1
assert first_batch.column("plugin")[0].as_py() == "ULBridge"

raw.close()
source.close()
```

## Payload location and syntax choice

`transform_line` first locates a `key=value` frame behind log prose. It then
makes one syntax decision for the whole payload; characters inside values are
never reinterpreted as a new syntax.

| decision | parser |
| --- | --- |
| first key before `=` is all ASCII digits | numeric FIX |
| an XML document is found behind a prefix | FIXML |
| payload starts with `{` | ULBridge/Jolokia configuration JSON |
| no key/value frame exists but the line contains `<` | FIXML |
| otherwise | ULLINK/bridge name-value row |

Use a specific method when the caller already knows the syntax:

| method | accepted input |
| --- | --- |
| `transform_fix_line` | numeric tags |
| `transform_ullink_line` | names, aliases, `#` keys, and packed groups |
| `transform_ulconfig_line` | bridge configuration JSON |
| `transform_fixml_line` | XML attributes |
| `transform_pairs` | ordered pairs already split by the caller |

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
from rekep.fix import FixCodec, fix_registry

codec = FixCodec(fix_registry(), branch="ulbridge")
message = codec.transform_fix_line(
    b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
)

assert message.field.name == "D"
assert message.by_tag(11).as_py() == "ORD-1"
assert message.by_name("orderqty").as_py() == 12.0
assert message.to_bytes(ord("|")) == (
    b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
)
```

## ULLINK and bridge-row rules

A row uses `|` when one is present, otherwise spaces. Each segment splits at
its first `=`. Names resolve through case/separator folding, aliases, and code
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
under `nopartyids`; nested group paths are rendered recursively. Missing or
out-of-order indices create null gaps. Residue that cannot be split remains an
unknown value rather than failing the row.

```python
from rekep.fix import FixCodec, fix_registry

codec = FixCodec(fix_registry(), branch="ulbridge")
message = codec.transform_ullink_line(
    "#NOPARTYIDS=1|"
    "#NOPARTYIDS[0]=PARTYID=BROKER••PARTYIDSOURCE=C••PARTYROLE=1••|".encode()
)

assert message.by_path("NoPartyIDs.0.PartyID").as_py() == "BROKER"
assert message.by_path("NoPartyIDs.0.PartyRole").as_py() == 1
```

## Registry transcription

This bridge row demonstrates the full name/alias/code/type path:

```text
MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|
```

| arrival | registry resolution | stored value |
| --- | --- | --- |
| `MSGTYPE=executionreport` | canonical `msgtype`, tag 35; code name → `8` | `msgtype = b"8"` |
| `SYMBOL=HOLN` | canonical `symbol`, tag 55 | `symbol = "HOLN"` |
| `SIDE=buy` | canonical `side`, tag 54; code name → `1` | `side = b"1"` |
| `LASTSHARES=235` | alias of `lastqty`, tag 32 | `lastqty = 235.0` |
| `LASTPX=72.28` | canonical `lastpx`, tag 31 | `lastpx = 72.28` |

```python
from rekep.fix import FixCodec, fix_registry

body = b"MSGTYPE=executionreport|SYMBOL=HOLN|SIDE=buy|LASTSHARES=235|LASTPX=72.28|"
message = FixCodec(fix_registry(), branch="ulbridge").transform_line(body)

assert message.by_tag(35).as_py() == "8"
assert message.by_name("lastqty").as_py() == 235.0
assert [(key, value) for _, _, key, value in message.entries()] == [
    ("MSGTYPE", "executionreport"),
    ("SYMBOL", "HOLN"),
    ("SIDE", "buy"),
    ("LASTSHARES", "235"),
    ("LASTPX", "72.28"),
]
```

The typed projection is normalized; `entries()` is the original
transcription.

## JSON configuration and FIXML

A payload beginning with `{` is read as a bridge/Jolokia configuration
document. `UlPlugin.from_json_bytes` exposes each plugin, while
`transform_ulconfig_line` creates a typed message. ObjectName properties such
as plugin type remain owned by the ObjectName; declared arrays become groups;
unknown attributes remain nullable text.

FIXML contributes attributes in document order. Namespace prefixes are removed
from attribute names, nested elements flatten into paths, and element names do
not invent protocol fields.

```python
from rekep.fix import FixCodec, fix_registry

xml = b'<FIXML><Order ClOrdID="A-1" Side="1" OrderQty="5"/></FIXML>'
message = FixCodec(fix_registry()).transform_fixml_line(xml)

assert message.by_name("clordid").as_py() == "A-1"
assert message.by_name("orderqty").as_py() == 5.0
```

## Resolution, translation, and typing

For each pair the builder:

1. parses a numeric tag or folds a name/path;
2. resolves the selected branch, then standard fields;
3. creates an unknown nullable text field if no definition exists;
4. records the original pair in arrival order;
5. treats trimmed `""`, `null`, and `<null>` as absent by default;
6. translates a versioned code name or wire code;
7. converts binary data from original bytes and scalar data from cleaned text;
8. leaves the typed value null on conversion failure while retaining the
   arrival pair.

Version selection is: explicit codec/row version, `ApplVerID(1128)`,
`BeginString(8)`, branch default, then the registry's newest applicable
version. Version affects code spelling, not column identity.

## Message ordering and derived values

Resolved children are ordered as FIX header, body, trailer, then runtime
fields. `beginstring` is supplied when the input did not state one. The market
timestamp uses the source row clock first, then message clocks in precision
order, then the Unix epoch. `unixpartition` follows it. `msghash` is computed
from the arrival record with session-envelope tags excluded.

Enrichment is opt-in. It can derive normalized symbol, ISIN, MIC, parent ids,
and lifecycle state; a derived value is never silently presented as a stated
wire pair.

## Arrow parse step

```python
import pyarrow

from rekep import Message
from rekep.fix import fix_registry, parse_arrow_reader

schema = Message.field().into_arrow_schema()
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
            "plugin": "OMS",
            "level": "INFO",
            "bodyhash": None,
            "body": b"Receiving : 8=FIX.4.4|35=D|55=AAPL|10=000|",
        }
    ],
    schema=schema,
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
parsed = parse_arrow_reader(source, fix_registry(), "body", branch="ulbridge")
table = parsed.read_all()

assert table.column("symbol").to_pylist() == ["AAPL"]
assert table.column("msgseqnum").to_pylist() == [7]
assert table.schema.names[-2:] == ["nofixentries", "nounmappedfixentries"]
```

Source columns lead the output unless a fixed column owns the same folded
name. A source timestamp and message context therefore fill fixed columns;
`seqNum` remains carried and can also fill `msgseqnum`. Message values always
win over source-column fills.

## Failure behavior

`FixCodec.transform_line(b"")` raises because no row exists. The Arrow reader
converts content-level failures into an `unknown` message so one damaged cell
cannot terminate a capture. I/O errors, an invalid payload column, malformed
root options, and an invalid registry remain errors. With deduplication off,
one source row always yields one result row.
