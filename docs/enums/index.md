# Enums

```python
from rekep.enums import Side, State

side = Side.from_fix("1")
state = State.from_code(219)

assert side is Side.BUY
assert state is State.OPEN
assert int(side) == 1_112_889_600
assert int(state) == 200
```

Market enums persist as `int32`. Their Arrow fields carry the enum name and
the complete stored-value lookup under `enum:*` metadata.

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

`Ranged` enums use hundred-wide bands. An unknown stored value resolves to its
band floor, so new detailed codes retain their broad meaning. ASCII enums --
built on the public `AsciiInt32` and `AsciiInt64` bases -- pack their readable
code into the stored integer, NUL-padded to the storage width; their pages
show both forms. An ASCII enum's Arrow shape is one extension singleton,
`EnumName.into_arrow_type()`, whose storage stays the plain integer column
every engine reads.
