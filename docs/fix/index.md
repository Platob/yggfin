# FIX

`FixMsg` transcribes wire FIX, rendered FIX/UL, XML, and reference envelopes
into one Arrow contract. Repeated fields and unresolved source values remain
ordered in `entries` and `unmap`.

```python
from rekep import FixMsg

message = FixMsg.from_text(
    "8=FIX.4.4|35=D|11=C1|55=IBM|54=1|38=10|44=125.5|10=001"
)

print(message.protocol.code)
print(message.get("Side").raw)
print(message.instrument.symbolticker)
```

```text
FIX4.4
1
IBM
```

The separator is detected from SOH, pipe, EOT/ETX, `^A`, `^`, semicolon, or
marked `#Name=Value` text. The Arrow parser resolves each distinct spelling
once per batch.

## Packaged registry

An unconfigured process reads the deterministic `rekep/fix/registry.zip`
inside the installed package. Construction and lookup never download data.

```python
from rekep.fix import FixRegistry

registry = FixRegistry()
side = registry.field("Side", "4.4")

print(side.fix.tag, side.dtype)
print(side.fix.decode("1"), side.fix.meaning("1"))
```

```text
54 string
Buy Buy
```

Pass `cache_dir` only when a task deliberately uses another complete registry
store:

```python
registry = FixRegistry(cache_dir="s3://example/registries/venue.zip")
```

Use the [registry browser](registry.md) for fields, namespaces, components,
groups, values, provenance, refresh commands, and coverage. The dedicated
[encode](encode.md) and [decode](decode.md) pages show scalar and Arrow value
conversion.

## Version selection

Wire FIX reads `BeginString <8>`. FIXT then reads `ApplVerID <1128>` or
`DefaultApplVerID <1137>`. The resolved version is stored in `protocol`, so a
persisted row is reproducible.

UL bridge rows commonly omit all three fields. `FixCodec.ul_default_version`
declares the application version used for those rows and defaults to `4.4`:

```python
from rekep.fix import FixCodec

codec = FixCodec(ul_default_version="5.0.SP2")
```

This default applies only to unversioned UL. Unknown ordinary FIX evidence is
not silently replaced.

## Reference envelopes

```python
from rekep import Message

reference = Message.from_text(
    "Referential(XLON|equity|dbi;GB00BN7SWP63_XLON_GBX|["
    "quantity-type=, tick-size-scale-id=PRIMARY|[[0|0.01], [100|0.05]]])"
)

print(reference.protocol.code)
print([(entry.comp, entry.key, entry.value) for entry in reference.entries])
```

The bracket-depth scanner keeps the tick ladder as one list. Empty values are
null; unknown bag members remain ordered entries for later declarations.

## One ordered field model

```python
message = FixMsg.from_text(
    "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=D|"
    "#NOPARTYIDS=1|#NOPARTYIDS[3]=PARTYID=P1 PARTYROLE=3|"
)

print(message.parties)
print(message.error)
```

Component and repeating-group declarations come from the registry. Indexed
entries may be sparse, out of order, or truncated; readable members are kept
and count disagreements become row diagnostics. Unknown component members
remain residual entries.

The [component guide](components.md), [repeating-group guide](repeating-groups.md),
and interactive [transcription workspace](transcribe.md) display the same tree
recursively.

## Alternate tags and names

`fix:aliases` resolves alternate names. `fix:tags` declares ordered numeric
tags that carry the same value; the canonical tag leads and the registry
coalesces the rest without a message-specific converter.

```yaml
name: lastpx
type: double
fix:
  name: LastPx
  tag: '31'
  tags: ['90031', '91031']
```

```python
import pyarrow

from rekep.fix import FieldAccess, FixCodec

codec = FixCodec()
entries = codec.into_entries(
    codec.into_pairs(pyarrow.array(["31=125.5|90031=125.4|"]), "FIX"),
    "4.4",
)
tags = registry.field_tags("LastPx", "4.4")
values = FieldAccess.first_arrow_tags(entries, tags, len(entries))
lastpx = registry.arrow_coalesce_tags(
    "LastPx", values, len(entries), version="4.4"
)
```

Unknown enumeration values remain raw strings. A disputed or unknown source
datatype maps to Arrow `string` while its original FIX datatype stays in
metadata.

## Best-effort transcription

One malformed row never aborts its batch. `FixMsg.error` records invalid
typed values, malformed XML, ambiguous groups, and count mismatches. The raw
row identity is retained for a degraded transcription so two different bad
values cannot merge as one event.

```python
import pyarrow.compute as compute

from rekep import Message

raw_messages = next(
    iter(
        Message.into_arrow_reader(
            [Message(body=b"XmlApi: <Order ClOrdID='broken'>")]
        )
    )
)
batch = FixMsg.from_message_batch(raw_messages)
failed = batch.filter(compute.is_valid(batch.column("error")))
```

The parser drops configured null spellings before lookup. Package defaults
include empty text, `null`, `<null>`, `n/a`, `none`, and `NONE`; a feed can
replace that set on `FixCodec` or `TextFile`.

## Arrow shapes

```python
from rekep import FixMsg, Instrument

print(FixMsg.into_field().field("instrument").dtype)
print(Instrument.into_field().field("maturitydate").dtype)
```

FIX dates and times are stored as `timestamp[us]`. Fields whose specification
states UTC use `timestamp[us, tz=UTC]`; local market dates stay timezone-naive.
Nested members are declared last so Iceberg column bounds continue to cover
the flat columns readers filter on.

## Refresh and edit

The [registry CLI](shell.md) is the only mutation surface:

```bash
rekep fix registry find --store data/fix 9001
rekep fix registry definitions --store data/fix 9001
rekep fix registry scrape --output data/fix --source fix-latest
rekep fix registry scrape --output data/fix --source fix-latest --offline
rekep fix registry check --store data/fix
```

Refresh uses cached complete source files through registered adapters. It
stages, validates, and atomically publishes deterministic artifacts; ordinary
registry construction does none of that work.
