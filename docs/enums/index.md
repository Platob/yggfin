# Enums

```python
from rekep.enums import Side, State

side = Side.from_fix("1")
state = State.from_str("OPEN")

assert side is Side.BUY
assert state.is_live
assert int(side) == int.from_bytes(b"BUY", "big")
assert int(state) == int.from_bytes(b"OPEN", "big")
```

A market enum persists as the integer its own code packs into -- `int32` for
a four-byte code, `int64` for an eight-byte one. Its Arrow field carries the
enum name and the complete stored-value lookup under `enum:*` metadata.

## Event codes

| Enum | Contract |
| --- | --- |
| [EventType](event-type.md) | Kind of assertion carried by an event. |
| [State](state.md) | Ordered event lifecycle. |

## Instrument codes

| Enum | Contract |
| --- | --- |
| [AssetKind](asset-kind.md) | Tradable asset class. |
| [IdSource](id-source.md) | Instrument identifier scheme. |
| [MIC](mic.md) | ISO 10383 market identifier. |
| [Currency](currency.md) | ISO 4217 code packed into four bytes. |
| [OptionKind](option-kind.md) | Put or call direction. |

## Order codes

| Enum | Contract |
| --- | --- |
| [Side](side.md) | Order direction. |
| [MarketKind](market-kind.md) | Pricing and execution semantics. |
| [TimeInForce](time-in-force.md) | Order lifetime. |

Every code is built on one base -- the public `AsciiInt32` and its eight-byte
`AsciiInt64` -- and packs its readable spelling into the stored integer,
right-justified with leading NULs, so a short code stores as the plain
integer of its own bytes.

Order is a separate fact from identity. A member may declare a *rank*, and a
vocabulary that ranks in hundred-wide bands answers "what does this broadly
mean" through `band` and "which codes rank at least this far" through
`ranked_at_least`, `ranked_below` and `ranked_between` -- finite code sets a
storage scan pushes down, where an ordinal vocabulary would have compared a
range. An enum's Arrow shape, `EnumName.into_arrow_type()`, is one cached
dictionary type -- the packed integer indexing the readable codes -- while
columns store the plain integer every engine reads.
