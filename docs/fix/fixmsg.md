# FixMsg

The FIX reading of a `Message`: dictionary resolution, structured components,
event time and market identities. What a row *is* and how to build one are on
the [FixMsg product](../products/fixmsg.md) page; this is how the FIX stage
reads it.

```python
from rekep import FixCodec, FixMsg, FixRegistry, TextFiles

registry = FixRegistry(cache_dir="data/fix")
source = TextFiles.from_folder(
    "s3://bucket/capture",
    pattern="*.log*",
    timezone="Europe/Paris",
    msg_type_event_types=registry.msg_type_event_types(),
)
codec = FixCodec(registry=registry)

for batch in source.read_arrow_reader(batch_row_size=65_536):
    parsed = FixMsg.from_message_batch(batch, codec)
```

A `FixRegistry` alone is enough — the codec derives from it, the packaged one
by default. A full `FixCodec` serves only feeds whose rules or field
declarations differ.

## Best-effort rows

```python
from rekep import FixMsg, Message

line = "8=FIX.4.4|9=12x|35=D|11=C1|55=AAPL|44=abc|54=1|10=000|"
row = FixMsg.from_message_batch([Message(message=line)]).to_pylist()[0]

print(row["clordid"], row["instrument"]["symbol"], row["price"])
print(row["error"])
```

```text
C1 AAPL None
BodyLength <9>: invalid 12x; Price <44>: invalid abc
```

Typed values that cannot be read become null and `error` says which readings
degraded. Body and component spellings remain in `entries`; a lifted session
spelling remains in its diagnostic because that field left `entries` at the raw
stage. An unexpected data error first retries vector slices, then retains only
the irreducible row with its raw arguments and exception text; valid neighbours
keep their order and parsed columns. Schema/projection mistakes still raise
before row isolation.

`error` is processing metadata and is not digested. A degraded row keeps the
raw stage's `vhash`, so changing parser wording cannot change its identity. It
is separate from `reason`, which keeps FIX `Text <58>` and upstream business
diagnostics.
`parse_instruments` and `parse_market` push down `error IS NULL`, and the class
conversion APIs enforce the same quarantine for callers outside the notebooks.

The stage consumes the arguments `TextFile`/`TextFiles` already stored rather
than tokenizing the payload again. A batch arriving from Iceberg carries
`large_string` where the contract says `string`, so it is brought onto the
`Message` declaration first — narrowed to the columns the batch has, because
`parse_fix` projects `message` away and filling it back in would invent text
the reader deliberately left behind.

Classification uses the codec's rules; the stored
[`protocol`](../enums/protocol.md) fills only the rows those rules call
`OTHER`.

```yaml
# Discard operational traffic before argument tokenization; empty retains it.
include_msgtypes: []
exclude_msgtypes: ["0", "1"]
```

## Names

A column is named by folding its FIX name: lowercase, with every separator
dropped. `MsgSeqNum` is `msgseqnum`, `OrigClOrdID` is `origclordid`,
`CFICode` is `cficode`. One name everywhere — the Arrow column, the Python
attribute and the stored document all spell it the same way — and no
snake-case alias beside it.

The dictionary's own spelling is not lost: every column carries it as
`fix:display`, which is what a reader is shown.

```python
from rekep import FixMsg

column = FixMsg.into_field().field("origclordid")
print(column.name, column.fix.display, column.fix.tag)
```

```text
origclordid OrigClOrdID 41
```

Field metadata retains the tag, datatype, description, versions and
enumerated values. FIX UTC timestamps become Arrow timestamps at microsecond
precision; one whose timezone is undocumented stays naive.

Structured components fold their members the same way:

```python
from rekep import FixMsg, Message

line = "8=FIX.4.4|35=D|11=C1|453=1|448=BUY-A|447=D|452=3|10=000"
staged = Message.from_text(line, message=line)
print(FixMsg.from_message_batch([staged]).to_pylist()[0]["parties"])
```

```text
[{'partyid': 'BUY-A', 'partyidsource': 'D', 'partyrole': 3}]
```

`Instrument`, `Parties`, `TrdRegTimestamps`, `SideTrdRegTS` and
`SecurityAltID` each carry the members they declare and nothing else. The
instrument's `legs` stay inside that component. What a component does not
project stays in `entries`, under the key the wire carried — one residual for
the row, not a second one on every entry.

## Ordered residue

`entries` is what no promoted column or component took, plus a promoted
field's raw text when its typed value cannot reproduce the wire spelling
(`0010.5000` stored as `10.5`). A list, not a map, because repeated fields and
wire order are data.

| member | what it holds |
| --- | --- |
| `tag` | a numeric key from the payload, a resolved tag, or `0` while unresolved |
| `key` | the full spelling without `#`; an indexed component keeps only its terminal |
| `value` | the value carried; always present, `""` when explicitly empty |
| `comp` | an indexed container prefix, such as `NoPartyIDs[0]` |

Raw `Message.entries` is always a list. A `FixMsg` carrying no recognized
message has null `entries`; a parsed message with no residue has an empty one.
The `MsgType` discriminator is promoted to `msgtype` and never duplicated here.

Every field a row keeps lands in exactly one of three places, and never two:

| where | what goes there |
| --- | --- |
| a typed column | a field the registry names and the row states once |
| `entries` | a field the registry knows that a column cannot hold alone |
| `unmap` | a key the registry has no record of |

A component's members leave `entries` with the component. A field written
twice under both spellings is one field when the two agree and stays two
entries when they do not, because choosing between them would be a guess.
`unmap` is null where every key resolved, rather than an empty list of the
fields that did not.

## Reading one row

`get` reads promoted columns and `entries` through the same accessor, by
numeric tag, canonical name, component path or whole vendor key:

```python
from rekep import FixMsg

row = FixMsg.from_text("8=FIX.4.4|35=D|11=C1|10=000")
print(row.get(11), row.get("ClOrdID"), sep="\n")
```

```text
Reading(found=True, key='11', raw='C1')
Reading(found=True, key='11', raw='C1')
```

A key no registry record explains still answers typed where its value spells
one of five unambiguous shapes — integer, float, dashed date, clock time,
boolean word — and stays text otherwise, keeping the raw spelling either way.
That is a floor under registry promotion, not a replacement: a field worth a
real typed column earns one through `rekep fix registry promote`.

`from_text` and `from_pairs` accept `registry=` and link it privately onto the
row, so `get`, `pairs`, the repeating-group readers and market translation all
resolve through one dictionary; an unlinked row reads the packaged one. The
link is reader state, never a stored column. `into_fix_events` carries it into
the translator, and a translator built with its own `registry` links that back
— one translation, exactly one dictionary.

## Direction

Read from the header verb, and only where it opens the line before the
payload's first token, so the same words inside a payload never answer:

```python
from rekep import Message

for text in ("Receiving : 8=FIX.4.4|35=D|10=0", "Sending : 8=FIX.4.4|35=D|10=0"):
    print(Message.from_text(text, message=text).direction)
```

```text
RECV
SENT
```

`UNKNOWN` is most rows — bridge re-log lines repeat a payload without repeating
the verb, and no answer beats a guessed one. The verb has to open before the
first token the row's protocol could start with, which
`rekep.fix.rules.CODEC_ANCHORS` spells per codec. It resolves at the message
stage, where the raw line and its protocol reading last coexist; the FIX stage
re-resolves any row still carrying its text and keeps the stored answer where
`parse_fix` projected the text away.

A `35=U...` wrapper may carry a rendered bridge payload with its own
`MSGTYPE`. The wrapper names the envelope and the payload names the message,
so the rendered discriminator is the row's MsgType. Its flat fields are read
beside the numeric ones under [Ordered residue](#ordered-residue)'s one rule;
indexed group members are never treated as duplicates.

## Stored categories

`parse_fix` partitions the table with two pushed scans — the market code set
(kinds ranked at least `INTENT`) and its complement:

| table | what lands there |
| --- | --- |
| `fix.market` | the market code set |
| `fix.misc` | non-technical `MISC`, and an unknown discriminator on a recognized transport |
| `fix.unknown` | an unknown event on an unrecognized transport |

Registry-declared technical MsgTypes and plugins enter no FIX table. Both
scans project `message` out, so every resulting `FixMsg.message` is null.

Market readers consume only `fix.market`, ordered by
`(unix, msgseqnum, hash)`. Each row carries its reference facts in the final
`instrument` struct. `parse_instruments` turns those components into
`InstrumentUpdate` events in `market.instruments`. Failed rows remain in their
original `fix.*` category for audit and are not translated downstream.
