# Encode

`FixMsg.to_bytes()` emits the parsed arrival record in order. It reads
`FixMsg.entries()`, the same data published as `nofixentries` in the fixed
Arrow row; typed projection columns are views over that record and are not a
second encoding source.

## Round trip

```python
from yggdryl import IOBase
from yggdryl.fix import FixCodec, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
codec = FixCodec(registry)
message = codec.transform_line(b"8=FIX.4.4|35=D|55=AAPL|10=000|")

wire = message.to_bytes()
assert b"\x01" in wire
assert codec.transform_line(wire).entries() == message.entries()

printed = message.to_bytes(ord("|"))
assert printed == b"8=FIX.4.4|35=D|55=AAPL|10=000|"
```

The default separator is SOH (`0x01`). Supplying another byte changes only the
emitted separator. Pair order, repeated keys, empty values, and the exact key
spelling remain the arrival record's.

## Build from pairs

```python
from yggdryl import IOBase
from yggdryl.fix import FixCodec, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
codec = FixCodec(registry)
message = codec.transform_pairs(
    [("8", "FIX.4.4"), ("35", "D"), ("55", "AAPL"), ("10", "000")]
)

assert message.by_tag(55).as_py() == "AAPL"
assert message.to_bytes(ord("|")) == b"8=FIX.4.4|35=D|55=AAPL|10=000|"
```

`transform_pairs` parses values through the same registry as byte input. A
value that does not type stays in `entries()` while the typed field remains
null. `null_values` on `FixCodec` controls which input spellings mean absence.

## Envelope fields

Encoding is lossless reproduction, not session repair. `to_bytes()` does not
invent, reorder, or recalculate `BodyLength(9)` or `CheckSum(10)`; it emits
exactly what the arrival record held. A session component that wants a valid
new wire frame owns those transport calculations.

The parsed-message `msghash` excludes the session envelope, so changing a
sequence number or sending time can change the exact bytes without changing
message identity. The raw `bodyhash` still distinguishes those captures.

## Arrow rows

`fix.messages` keeps the complete arrival record in `nofixentries`. Python's
current binding exposes object-level `to_bytes()` rather than an Arrow writer;
read one row into the codec object only when re-emission is actually required.
Analytics should stay on typed Arrow columns.
