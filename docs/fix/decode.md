# Decode

Two entry points, one builder underneath. `parse_arrow_reader` is the Arrow
boundary: a source `RecordBatchReader` in, a reader whose schema the
[dictionary](registry.md) alone fixes out. `FixReader` is the per-line one: one
captured line in, one `FixMsg` out. Both split the same dialects and resolve
against the same registry, so a line read either way resolves the same.

One input row is one output row -- prose, an unreadable frame and an empty body
included -- so the source keys still identify the result. `dedup=True` is the
one exception; see
[Quality](quality.md#sequential-deduplication).

```python
import pyarrow

from yggdryl import IOBase
from yggdryl.fix import FixRegistry, parse_arrow_reader

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.with_crate_fields()

schema = pyarrow.schema(
    [pyarrow.field("rownum", pyarrow.int64()), pyarrow.field("body", pyarrow.binary())]
)
lines = [
    b"sending >> 8=FIX.4.4|35=D|55=TTF|54=1|38=1200|44=41.2500|10=000| << queued",
    b"2026-08-14 00:05:02,001 [FixSession_XPAR] (INFO) heartbeat scheduled",
]
batch = pyarrow.RecordBatch.from_arrays(
    [pyarrow.array([1, 2]), pyarrow.array(lines)], schema=schema
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])

parsed = parse_arrow_reader(source, registry, "body").read_all()
assert parsed.num_rows == 2
# A column is typed as the dictionary declares it: MsgType is a fixed-width
# eight-byte code, padded on the wire's short value, and Price a float.
assert parsed.column("35").to_pylist() == [b"D\x00\x00\x00\x00\x00\x00\x00", None]
assert parsed.column("44").to_pylist() == [41.25, None]
assert parsed.column("rownum").to_pylist() == [1, 2]
```

The reader stays lazy across the boundary: PyArrow pulls one batch at a time,
and nothing in a row's content can fail a batch.

Most keywords are the per-stream form of an argument `FixReader` takes per call
-- `branch`, `source_version`, `target_version`, `null_values` -- beside
`name`, `direction`, `dedup` and the two batch bounds, which belong to the
stream alone. `separator` is the exception: on this path a row's own `sep`
column decides, and the keyword is not read. A capture can also state some of them per row, in its own columns, and a
column outranks the keyword because it is the more specific statement:

| column | what it states |
| --- | --- |
| `branch` | the dialect the row is written in |
| `beginstring` | the version the row is written in |
| `targetversion` | the version the built message is expressed in |
| `sep` | the byte this row's numeric frame is split on, instead of sniffing it |
| `direction` | `SENT` or `RECV`, outranking the verbs beside the frame |

Column names fold, and a column that is absent, null or empty is silence rather
than an instruction. That makes two of the raw contract's own names land on
parameters by accident, so
[`parse_fix` renames them](../pipeline/tasks/parse-fix.md#the-two-renamed-columns)
before the reader sees the stream: `branch` on a raw record is the driver that
printed the line rather than a FIX dialect, and `msgdirection` is renamed *into*
`direction` so the reader uses the reading the raw layer already made.

## The output schema

Three parts, in this order.

1. The capture's own columns, unchanged, with their metadata. A capture column
   whose name a FIX column already takes is dropped rather than renamed or
   duplicated.
2. One column per schema tag, **named by the tag**, typed as the dictionary
   declares it.
3. `entries`, then `unmapped`.

Named by tag because a tag is the one name a field has in every version and
every dialect: tag 32 is `LastShares` in 4.2 and `LastQty` in a newer one, and
a column named either changes meaning when a venue upgrades. The spelling stays
on the field's `display` metadata, so a renderer shows `MsgType` over column
`35`.

`fix_schema_tags()` is that list, in column order: the standard header, the
fields a financial consumer reads, the three repeating groups worth persisting
whole, the trailer, this crate's
[seven derived fields](registry.md#the-fields-this-crate-adds), and
`MsgDirection` (385), which no message carries on the wire. A schema tag the
dictionary does not declare falls back to this crate's own field where there is
one and is skipped otherwise, so the width is the dictionary's plus that
fallback: `config/fix` answers 87 tag columns plus the two lists.

A dictionary field that is not a schema tag -- `PartyID` (448), `TimeInForce`
(59) -- still resolves. It gets no column of its own, and it is in `entries`
rather than in `unmapped`.

Both lists are
`list<struct<tag: int32, branch: string, key: string, value: string>>`.
`entries` is the whole arrival record, in order and untranslated, which is what
makes a row lossless: the wire is rebuilt from it and never from the columns.
`unmapped` is a view over that record -- the pairs no dictionary explained --
and it holds nothing `entries` does not. `tag` is the tag the frame spelled -- `0` only
where the key was symbolic and named no field -- and `key` is that spelling, so
a numeric tag no dictionary explains keeps its number and a symbolic bridge key
survives verbatim. See
[Quality](quality.md#coverage-what-unmapped-is-for) for what to do with a
non-empty `unmapped`.

### Without reading a row

`FixProjection` resolves those columns once, from the dictionary and the source
schema alone. Its root is exactly the schema the reader will answer, so it is
the way to declare a table before a batch exists.

```python
import pyarrow

from yggdryl import Field, IOBase
from yggdryl.fix import FixProjection, FixRegistry, parse_arrow_reader

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.with_crate_fields()

capture = pyarrow.schema(
    [
        pyarrow.field("url", pyarrow.string()),
        pyarrow.field("rownum", pyarrow.int64()),
        pyarrow.field("body", pyarrow.binary()),
    ]
)
projection = FixProjection(registry, "fix", Field.from_arrow_schema(capture, "line"))
declared = projection.field.into_arrow_schema()

assert declared.names[:3] == ["url", "rownum", "body"]
assert declared.names[3:6] == ["8", "9", "35"]
assert declared.names[-2:] == ["entries", "unmapped"]
assert projection.carried == 3
assert projection.position_of(35) == 5

empty = pyarrow.RecordBatchReader.from_batches(capture, [])
assert parse_arrow_reader(empty, registry, "body").schema == declared
```

`fix_schema(registry, "fix")` is the FIX half alone, without a capture in front
of it. `projection.position_of(tag)` is how a row is read by index instead of
by a dictionary lookup per column.

## Locating the frame

A capture line is a message wrapped in whatever the process printed around it,
so the frame is located first and everything before it is prefix. Reading from
byte zero would take `sending >> 8` as the first key and pick the wrong dialect
on nearly every real line.

- The frame opens at the first `8=` pair; failing that the first `35=`; failing
  that the first pair of any kind.
- The dialect is decided once, from the frame, and never re-sniffed: all ASCII
  digits before the frame's first `=` means numeric FIX, anything else means a
  bridge row. A `#` or a `<` inside a *value* is then part of that value.
- A numeric frame is split on `SOH` where the body holds one, and on `|`
  otherwise. A bridge row is split on `|` where it has one, and on the space
  byte otherwise -- a tab is part of a value, not a separator.
- A segment splits at its **first** `=` only, so `58=a;b` is one Text field
  rather than two.
- Tag 10 closes the message: whatever the log wrote after the checksum is
  prose, not a further pair.

A log that cannot print `0x01` writes `^A`, `\x01`, `<SOH>` or `{SOH}` instead.
It is the same frame with its separator escaped on the way into the log, so a
body holding no real `SOH` is unescaped once -- the earliest of the four
spellings wins, every occurrence of that one becomes the byte it stands for,
and the frame is then split on `SOH` alone. A body that holds a real `SOH`
keeps a printed spelling as text, because there the escape is somebody's value.

```python
from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
reader = FixReader(registry)

prefixed = reader.text("sending >> 8=FIX.4.4|35=D|55=TTF|10=000| << queued seq=1092")
assert [entry[2] for entry in prefixed.entries()] == ["8", "35", "55", "10"]

printed = reader.bytes(b"8=FIX.4.4^A35=D^A55=TTF^A10=000^A")
assert printed.entries() == prefixed.entries()

held = reader.bytes(b"8=FIX.4.4\x0135=D\x0155=TTF\x0158=A^AB\x0110=000\x01")
assert held.by_tag(58).as_py() == "A^AB"

bridge = reader.text("MSGTYPE=D|SYMBOL=TTF|SIDE=1|")
assert [entry[0] for entry in bridge.entries()] == [35, 55, 54]
assert [entry[2] for entry in bridge.entries()] == ["MSGTYPE", "SYMBOL", "SIDE"]
```

The one shape the scan cannot separate is a `|`-separated frame that also
prints `^A` inside a value: the unescape runs over the whole body, so the
pipes stop separating anything.

## Nothing is skipped

A row with no message type is built anyway and named `unknown` -- every pair
that parsed becomes a field, and the entries record the whole row. `unknown` is
safe because every `MsgType` the code set declares is one or two characters, so
none can collide with it.

A line that is not a row at all -- no bytes -- is still one output row, with an
empty `entries`. `parse_fix` never sees these: it reads `logs.messages` under
`msgtype != 'unknown'`, on the classification `parse_messages` already stored.

Every pair that arrived is in `entries`. The one exception is a stated absence:
by default the values `""`, `null` and `<null>` are read as never having been
sent, so they produce neither a field nor an entry. Filtering happens before
typing, so nothing tries to read `<null>` as a price and then file the failure.
A venue for whom the text `null` is a value passes `null_values=[]` and keeps
every literal.

```python
from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
reader = FixReader(registry)

nameless = reader.text("thread=main level=INFO note=hello")
assert nameless.field.name == "unknown"
assert [entry[2] for entry in nameless.entries()] == ["thread", "level", "note"]
assert [entry[0] for entry in nameless.entries()] == [0, 0, 0]

absent = reader.text("8=FIX.4.4|35=D|44=|58=<null>|10=000|")
assert [entry[0] for entry in absent.entries()] == [8, 35, 10]
assert FixReader(registry, null_values=[]).text("58=<null>|").by_tag(58).as_py() == "<null>"
```

## Typing, and what a bad value does

Each column is typed once, as the dictionary declares it. Three FIX spellings
are rewritten before that cast, because no Arrow cast reads them:

| declared type | wire spelling | cast reads |
| --- | --- | --- |
| `timestamp` | `20260814-00:05:01.147` | `2026-08-14T00:05:01.147Z` |
| `date32` | `20260814` | `2026-08-14` |
| `bool` | `Y` / `N` | `true` / `false` |

A value the declared type cannot read **stays null**. It never fails the row it
arrived on, the raw text is still in `entries`, and the refusal is readable
through the message's `anomalies()` -- a null nobody can explain is worse than
the value that actually arrived.

```python
from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
message = FixReader(registry).text("8=FIX.4.4|35=D|38=notanumber|10=000|")

assert message.by_tag(38).is_null()
assert message.entries()[2] == (38, None, "38", "notanumber")
assert message.anomalies() == ['38 (38) would not type from "notanumber"']
```

[Encode](encode.md#typing-on-the-way-out) is the same table read the other way.

## Repeating groups

A group's column is its **counter** tag -- `453` for `NoPartyIDs` -- typed
`list<struct<...>>` over the members the dictionary declares for it. The column
holds the occurrences, not the count.

A bridge states the group by name and index: `#NOPARTYIDS=2` is the counter and
`#NOPARTYIDS[0]=...` is one occurrence whose *value* is a run of member pairs
packed behind a separator. ULLINK writes `EOT` then `ETX`; a bridge relaying
into a FIX session writes `SOH` instead. Both split, and the first spelling an
occurrence actually carries is the one that splits it, so a value legitimately
holding the other byte is not broken into fields nobody wrote.

Members are placed by name against the dictionary's declared members, so an
occurrence stating fewer of them is nulls in the rest rather than a refusal.

```python
import pyarrow

from yggdryl import IOBase
from yggdryl.fix import FixRegistry, parse_arrow_reader

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
schema = pyarrow.schema([pyarrow.field("body", pyarrow.binary())])
rows = [
    b"MSGTYPE=D|#NOPARTYIDS=2|"
    b"#NOPARTYIDS[0]=PARTYID=BROKERX\x04\x03PARTYIDSOURCE=D\x04\x03PARTYROLE=1|"
    b"#NOPARTYIDS[1]=PARTYID=CLIENTY\x01PARTYIDSOURCE=D\x01PARTYROLE=3|",
    b"MSGTYPE=D|#NOPARTYIDS=1|#NOPARTYIDS[0]=PARTYID=BROKERX|",
]
batch = pyarrow.RecordBatch.from_arrays([pyarrow.array(rows)], schema=schema)
parties = (
    parse_arrow_reader(pyarrow.RecordBatchReader.from_batches(schema, [batch]), registry)
    .read_all()
    .column("453")
    .to_pylist()
)

# Every member the dictionary declares for the group is a struct field, named
# by its own folded name, whether or not the occurrence stated it.
assert parties[0] == [
    {
        "partyid": "BROKERX",
        "partyidsource": "D",
        "partyrole": 1,
        "partyrolequalifier": None,
    },
    {
        "partyid": "CLIENTY",
        "partyidsource": "D",
        "partyrole": 3,
        "partyrolequalifier": None,
    },
]
assert parties[1] == [
    {
        "partyid": "BROKERX",
        "partyidsource": None,
        "partyrole": None,
        "partyrolequalifier": None,
    }
]
```

A numeric frame states no occurrence boundary -- `453=2` is followed by flat
`448`, `447`, `452` pairs -- and nesting is not inferred from repetition, since
a wrong split invents a field. The `453` column is an empty list there, and the
members are in `entries` under their own tags; a tag repeating outside a group
answers as the sequence of the values it carried.

## Try it

<div data-fix="decode">Loading the dictionary…</div>

The browser widget mirrors this scan -- the same frame location, the same six
separator spellings, the same printed-`SOH` unescape -- and shows what each
step decided: the classification, the frame offset and separator, every
resolved column with its typed value, the unmapped pairs, and the raw
`entries`. It runs against a [generated dump](registry.md#the-dump-these-pages-read)
of `config/fix`; nothing you paste leaves the tab.

## Reading one message at a time

`FixReader` is for a single message -- a support question, a reproduction, a
mapping you are about to change. `text` and `bytes` pick the dialect from the
frame; `fixtext(body, separator)` and `ultext(body)` pin it; `pairs` takes
key/value tuples already split.

```python
from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.with_crate_fields()
reader = FixReader(registry, target_version="FIX.4.4")

message = reader.text(
    "8=FIX.4.4|35=D|49=BUYSIDE|56=XPAR|11=ORD-0000038106|55=TTF|54=1|"
    "38=1200|44=41.2500|60=20260814-00:05:01.147|10=203|"
)

assert message.field.name == "D"
assert message.by_tag(55).as_py() == "TTF"
assert message.by_name("Side").as_py() == "1"
assert message["ClOrdID"].as_py() == "ORD-0000038106"
assert message.market_timestamp().as_py().isoformat() == "2026-08-14T00:05:01.147000+00:00"
assert len(message.digest()) == 16
assert message.anomalies() == []
assert message.entries()[0] == (8, None, "8", "FIX.4.4")
```

`FixMsg` is one row typed against the registry it was resolved against. It
answers by identifier, by tag, by name and by dotted path, `entries()` gives
the arrival record back as tuples, and it hashes and pickles by its schema and
value while equality also reads the dictionary it was resolved against.

Building one per row is what the Arrow reader exists to avoid: each message
carries its own field, and a per-row model re-does the dictionary work a fixed
schema resolves once. Over a capture, use `parse_arrow_reader` and let
[`parse_fix`](../pipeline/tasks/parse-fix.md) run it.
