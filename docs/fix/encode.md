# Encode and round-trip

`FixMsg.into_bytes()` emits the arrival record, not the fixed table projection.
Derived stamps and capture-column fills are therefore not invented on the wire.

```python
from rekep.fix import fix_codec, fix_registry

wire = b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
message = next(iter(fix_codec(fix_registry()).parse_line(wire)))

assert message.into_bytes(ord("|")) == wire
```

## What round-trips

| input fact | emitted |
| --- | --- |
| original key and value text | yes |
| duplicate tags and arrival order | yes |
| nested group occurrences | yes, in normalized path spelling when decoded from packed bridge groups |
| unknown fields | yes |
| capture `sourceurl`, `rownum`, `pluginid`, or timestamp | no |
| derived `msghash`, `msgphash`, settled instants, MIC, state | no |
| typed-column canonical value | only through its original arrival pair |

A message constructed only from pairs has no obligation to synthesize
`BeginString`; the codec may add it to the typed message so the row has a
version, but it remains absent from `entries()` and from re-encoding.

## Choose a separator

```python
from rekep.fix import fix_codec, fix_registry

message = next(iter(fix_codec(fix_registry()).parse_line(b"35=D|11=ORD-1|55=AAPL|")))

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
