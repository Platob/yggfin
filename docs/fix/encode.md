# Encode and round-trip

`FixMsg.into_bytes()` emits a canonical FIX frame from the message as it now
stands. It uses lifted fields plus residual `fixentries`; it does not replay a
saved arrival byte sequence.

```python
from rekep.fix import fix_codec, fix_registry

wire = b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
message = next(iter(fix_codec(fix_registry()).parse_line(wire)))
again = message.into_bytes(ord("|"))

assert again.startswith(b"8=FIX.4.4|")
assert all(pair in again for pair in (b"35=D", b"11=ORD-1", b"55=AAPL", b"54=1", b"38=12"))
```

## What round-trips

| input fact | result |
| --- | --- |
| successfully typed scalar | emitted from its canonical lifted field |
| complete represented group | emitted from its lifted nested column |
| unknown or unrepresentable pair | retained in residual `fixentries` |
| duplicate or ambiguous occurrence | retained conservatively in `fixentries` |
| capture location and row header | not protocol content |
| derived event identity, clocks and state | not protocol tags unless the FIX message stated them |

Body length, checksum, pair spelling, and sibling order may be canonicalized.
The fixed Arrow row is a semantic fixed point: reading and writing it preserves
the message facts, including residual unknown fields and groups.

## Choose a separator

```python
from rekep.fix import fix_codec, fix_registry

message = next(iter(fix_codec(fix_registry()).parse_line(b"35=D|11=ORD-1|55=AAPL|")))

assert b"35=D|" in message.into_bytes(ord("|"))
assert b"35=D\x01" in message.into_bytes(1)
```

## Build a message

Enter tags or names and their values, choose a separator, and read the
canonical bytes `FixMsg.into_bytes()` emits:

<div data-fix="encode"></div>
