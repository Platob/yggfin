# Enums

```python
from rekep.enums import Side, State

side = Side.from_fix("1")
state = State.from_str("OPEN")

assert side is Side.BUY
assert state.is_live
assert int(side) == int.from_bytes(b"BUY\0", "big")
assert int(state) == int.from_bytes(b"20OPEN\0\0", "big")
```

A code enum persists as the integer its own code packs into -- `int32` for
a four-byte code, `int64` for an eight-byte one. Its Arrow field carries the
enum name and the complete stored-value lookup under `enum:*` metadata.

## Message codes

| Enum | Contract |
| --- | --- |
| [Direction](direction.md) | Transport movement stated before a payload. |
| [Protocol](protocol.md) | Grammar the payload's keys are written in. |

## Event codes

| Enum | Contract |
| --- | --- |
| [EventType](event-type.md) | Kind of assertion carried by an event. |
| [State](state.md) | Ordered event lifecycle. |

## Instrument codes

| Enum | Contract |
| --- | --- |
| [AssetKind](asset-kind.md) | Tradable asset class. |
| [MIC](mic.md) | ISO 10383 market identifier. |
| [Currency](currency.md) | ISO 4217 code packed into four bytes. |
| [SecurityIDSource](security-id-source.md) | Scheme an identifier is issued under. |
| [OptionKind](option-kind.md) | Put or call direction. |

## Market codes

Generic across orders, executions and quotes: an execution carries a side and
a market kind exactly as the order that caused it does.

| Enum | Contract |
| --- | --- |
| [Side](side.md) | Direction. |
| [MarketKind](market-kind.md) | Pricing and execution semantics. |
| [TimeInForce](time-in-force.md) | Order lifetime. |

Every code is built on one base -- the public `Ascii32` and its eight-byte
`Ascii64` -- which packs the readable spelling left-justified into the stored
integer. [ASCII codes](ascii-codes.md) is that rule, with an encoder on it.

A vocabulary that is one FIX field read as a code -- `Side`, `TimeInForce`,
`OptionKind` -- declares which field, and takes the wire codes from the
dictionary rather than compiling a copy of them. A scheme the dictionary
enumerates in full, such as `SecurityIDSource <22>`, has no enum here at all.

Order is a separate fact from identity. A member may declare a *rank*, and a
vocabulary ranked in hundred-wide bands answers "what does this broadly mean"
through `band` and "which codes rank at least this far" through
`ranked_at_least` and `ranked_below` -- finite code sets a storage scan
pushes down.

An enum's Arrow shape, `EnumName.into_arrow_type()`, is one cached dictionary
type -- its codes as values, indexed as wide as the packed integer a column
stores, though the indices themselves are positions -- while columns store the
bare integer every engine reads.

An open vocabulary remembers a code it learnt at runtime, so the next read of
the same value is the same member. That memory is bounded at 4,096 codes per
enum and evicts the least recently registered, so `from_int(int(member)) is
member` holds for what a batch is working with and not for every code a long
process has ever seen.
