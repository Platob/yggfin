# Encode and round-trip

`FixMsg.into_bytes()` emits the arrival record, not the fixed table projection.
Derived stamps and capture-column fills are therefore not invented on the wire.

```python
from rekep.fix import fix_codec

wire = b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
message, = fix_codec().parse_line(wire)

assert message.into_bytes(ord("|")) == wire
```

## What round-trips

| input fact | emitted |
| --- | --- |
| original key and value text | yes |
| duplicate tags and arrival order | yes |
| nested group occurrences | yes, in normalized path spelling when decoded from packed bridge groups |
| unknown fields | yes |
| capture `url`, `rownum`, `pluginid`, or timestamp | no |
| derived `msghash`, market timestamp, MIC, state | no |
| enriched or restated values | no |
| typed-column canonical value | only through its original arrival pair |

A message constructed only from pairs has no obligation to synthesize
`BeginString`; the codec may add it to the typed message so the row has a
version, but it remains absent from `entries()` and from re-encoding.

Enrichment and `into_latest()` fill the row and leave the record alone, so a
filled or restated message re-emits the line it was read from, byte for byte:

```python
from rekep.fix import fix_codec

codec = fix_codec()
wire = b"8=FIX.4.2|35=D|11=A|47=A|55=AAPL|10=0|"
message, = codec.parse_line(wire)

assert codec.enrich_message(message).into_bytes(ord("|")) == wire
assert message.into_latest().into_bytes(ord("|")) == wire
```

## Choose a separator

```python
from rekep.fix import fix_codec

message = fix_codec().parse_pairs(
    [("35", "D"), ("11", "ORD-1"), ("55", "AAPL")]
)

assert message.into_bytes(ord("|")) == b"35=D|11=ORD-1|55=AAPL|"
assert message.into_bytes(1) == b"35=D\x0111=ORD-1\x0155=AAPL\x01"
```

Use numeric FIX input when byte-for-byte wire framing matters. Bridge rows can
normalize group paths during decoding because their packed member delimiters
are a logging representation, not a FIX wire format.

## Build a message

Enter tags or names and their values, choose a separator, and read the bytes
`FixMsg.into_bytes()` emits for them:

<div data-fix="encode"></div>
