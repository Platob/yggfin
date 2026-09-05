# FixMsg

The FIX reading of a `Message`: dictionary resolution, structured components,
event time and market identities. What a row *is* and how to build one are on
the [FixMsg product](../products/fixmsg.md) page; this is how the FIX stage
reads it.

```python
from rekep import FixCodec, FixMsg, FixRegistry, Message
from yggdryl import IOBase, TextOptions

registry = FixRegistry(cache_dir="data/fix")
codec = FixCodec(registry=registry, timezone="Europe/Paris")
options = TextOptions()
options.with_rownum = 1
options.batch_row_size = 65_536

source = IOBase("s3://bucket/capture/app.log.gz").into_text(options)
for batch in source.read_arrow_reader():
    names = [
        {"url": "sourceurl", "rownum": "sourcerownum"}.get(name, name)
        for name in batch.schema.names
    ]
    raw = Message.into_field().cast_arrow_batch(batch.rename_columns(names))
    parsed = FixMsg.from_message_batch(raw, codec)
```

A `FixRegistry` alone is enough — the codec derives from it, the packaged one
by default. A full `FixCodec` also carries feed-specific rules, field
declarations, plugin key aliases, null spellings and the recording timezone.
The task adds its configured header captures before casting yggdryl's batches
onto `Message`.

## Best-effort rows

```python
from rekep import FixMsg, Message

line = "8=FIX.4.4|9=12x|35=D|11=C1|55=AAPL|44=abc|54=1|10=000|"
row = FixMsg.from_message_batch([Message(body=line)]).to_pylist()[0]

print(row["clordid"], row["instrument"]["symbol"], row["lastpx"])
print(row["error"])
```

```text
C1 AAPL None
BodyLength <9>: invalid 12x; Price <44>: invalid abc
```

Typed values that cannot be read become null and `error` says which readings
degraded. Body and component spellings that remain useful for audit land in
the parsed `entries`; a lifted session spelling stays in its diagnostic. An
unexpected data error first retries vector slices, then retains only the
irreducible row with its raw arguments and exception text; valid neighbours
keep their order and parsed columns. Schema mistakes still raise before row
isolation.

`error` is processing metadata and is not digested. A degraded row keeps the
identity derived from its raw body, so changing parser wording cannot change
its identity. It is separate from `reason`, which keeps FIX `Text <58>` and
upstream business diagnostics.
`parse_instruments` and `parse_market` push down `error IS NULL`, and the class
conversion APIs enforce the same quarantine for callers outside the tasks.

The stage consumes raw `Message` rows previously read by yggdryl and stored in
Iceberg. A batch arriving from Iceberg carries `large_binary` where the
contract says `binary`, so it is brought onto the `Message` declaration first.
`FixMsg` classifies and tokenizes `body`, resolves the registry, then consumes
the raw bytes; its stored schema contains only typed columns and ordered
residual entries.

```yaml
# Discard operational traffic after parsing; empty retains it.
exclude_msgtypes: ["0", "1"]
```

## Names

A column is named by folding its FIX name: lowercase, with every separator
dropped. `MsgSeqNum` is `msgseqnum`, `OrigClOrdID` is `origclordid`,
`CFICode` is `cficode`. One name everywhere — the Arrow column, the Python
attribute and the stored document all spell it the same way — and no
snake-case alias beside it.

The dictionary's own spelling is not lost: a lifted column carries it as
`fix:name`.

```python
from rekep import FixMsg

column = FixMsg.into_field().field("origclordid")
print(column.name, column.fix.canonical, column.fix.tag)
```

```text
origclordid OrigClOrdID 41
```

Field metadata retains the tag, canonical name and datatype. Versions,
message usage, sources and enumerated values remain in the registry. FIX UTC
timestamps become Arrow timestamps at microsecond precision; one whose
timezone is undocumented stays naive.

Structured components fold their members the same way:

```python
from rekep import FixMsg, Message

line = "8=FIX.4.4|35=D|11=C1|453=1|448=BUY-A|447=D|452=3|10=000"
staged = Message.from_text(line)
print(FixMsg.from_message_batch([staged]).to_pylist()[0]["parties"])
```

```text
[{'partyid': 'BUY-A', 'partyidsource': 'D', 'partyrole': 3, 'partyrolequalifier': None}]
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

A raw `Message` has no `entries`, protocol or discriminator columns. A
`FixMsg` carrying no recognized message has null `entries`; a parsed message
with no residue has an empty one. The `MsgType` discriminator is promoted to
`msgtype` and never duplicated here.

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
from rekep import FixMsg

for text in ("Receiving : 8=FIX.4.4|35=D|10=0", "Sending : 8=FIX.4.4|35=D|10=0"):
    print(FixMsg.from_text(text).direction)
```

```text
RECV
SENT
```

`UNKNOWN` is most rows — bridge re-log lines repeat a payload without repeating
the verb, and no answer beats a guessed one. The verb has to open before the
first token the row's protocol could start with, which
`rekep.fix.rules.CODEC_ANCHORS` spells per codec. It resolves at the FIX
boundary, where the raw body and its protocol reading coexist.

A `35=U...` wrapper may carry a rendered bridge payload with its own
`MSGTYPE`. The wrapper names the envelope and the payload names the message,
so the rendered discriminator is the row's MsgType. Its flat fields are read
beside the numeric ones under [Ordered residue](#ordered-residue)'s one rule;
indexed group members are never treated as duplicates.

## Stored categories

The one `parse_fix` definition receives a category for three independent runs.
Each scans raw `logs.messages`, parses the batch, drops configured MsgTypes and
applies one mutually exclusive Arrow mask:

| task | table | selection after parsing |
| --- | --- | --- |
| `parse_fix_market` | `fix.market` | kinds ranked at least `INTENT` |
| `parse_fix_misc` | `fix.misc` | not market, and either `MISC` or a recognized protocol |
| `parse_fix_unknown` | `fix.unknown` | not market, not `MISC`, and an unrecognized protocol |

MsgTypes listed by `exclude_msgtypes` enter no FIX table. Every resulting
`FixMsg` schema excludes `body`. The three runs deliberately repeat parsing so
`logs.messages` stays independent of every FIX dictionary and protocol rule.

Market readers consume only `fix.market`, ordered by
`(unix, msgseqnum, hash)`. Each row carries its reference facts in the final
`instrument` struct. `parse_instruments` turns those components into
`InstUpdate` events in `market.instruments`. Failed rows remain in their
original `fix.*` category for audit and are not translated downstream.
