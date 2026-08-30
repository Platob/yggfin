# ASCII codes

Every stable code in this package is its own readable spelling, packed into the
integer a column stores. `Ascii32` takes four bytes into an `int32`, `Ascii64`
takes eight into an `int64`, and nothing else differs between them.

```python
from rekep.enums import Side, State

assert Side.BYTE_WIDTH == 4 and State.BYTE_WIDTH == 8
print(int(Side.BUY), int.from_bytes(b"BUY\0", "big"))
print(int(State.PENDING_NEW), int.from_bytes(b"11PNDNEW", "big"))
```

```text
1112889600 1112889600
3544702678800942423 3544702678800942423
```

The code sits **left-justified** and is padded right with NULs, so the integer
orders exactly as the text does and a raw column dump reads back as its
spelling. `from_int` is the only reader of a stored value.

```python
assert int(Side.BUY) < int(Side.SELL) and "BUY" < "SELL"
print(Side.from_int(1112889600), State.from_int(3544702678800942423))
print(Side.into_arrow_type(), State.into_arrow_type(), sep="\n")
```

```text
BUY 11PNDNEW
dictionary<values=string, indices=int32, ordered=0>
dictionary<values=string, indices=int64, ordered=0>
```

Four bytes forces abbreviation — `SELL_SHORT` is stored `SHRT` — so a
vocabulary that wants its spellings takes eight. A column stores the bare
integer either way; the dictionary type above is what a reader is handed when
it asks for the code spelled out.

## Try it

Type a code to see the value it stores, or a value to see the code it came
from. The arithmetic here is the packing rule, not a lookup table, so a
spelling this package has never compiled still answers.

<div class="ascii-codes" data-ascii-codes data-source="../../assets/enum-codes.json"></div>

## What a code may be

A code is at most `BYTE_WIDTH` characters of printable ASCII — bytes 32 to
126. A spelling too long, holding a NUL, or outside ASCII is not a code, and
`from_int` answers `UNKNOWN` for a stored value that decodes to one:

```python
assert Side.from_int(1) is Side.UNKNOWN, "no code packs to 1"
assert State.from_str("nothing here") is State.UNKNOWN
print(Side.UNKNOWN.code == "", int(Side.UNKNOWN))
```

```text
True 0
```

Because the top bit is never set by printable ASCII, a valid code is always a
non-negative integer even though the column is signed.

## Rank, and what a code broadly means

A member may declare a **rank** separately from its value. A vocabulary ranked
in hundred-wide bands answers "what does this broadly mean" through `band`, and
a scan pushes the finite code sets `ranked_at_least` and `ranked_below`
spell:

```python
from rekep.enums import EventType

print(State.FILLED.band, EventType.EXECUTION.band, sep=" ")
print(len(EventType.ranked_at_least(EventType.STATE)))
```

```text
40DONE FACT
2
```

A vocabulary that declares no ranks ranks each member by its own packed code,
so every code is its own band.

## Which base each enum takes

| Enum | Base | Stored | Set |
| --- | --- | --- | --- |
| [EventType](event-type.md) | `Ascii64` | `int64` | closed |
| [State](state.md) | `Ascii64` | `int64` | closed |
| [AssetKind](asset-kind.md) | `Ascii64` | `int64` | closed |
| [MarketKind](market-kind.md) | `Ascii64` | `int64` | closed |
| [OptionKind](option-kind.md) | `Ascii64` | `int64` | closed |
| [Protocol](protocol.md) | `Ascii64` | `int64` | open |
| [Direction](direction.md) | `Ascii32` | `int32` | closed |
| [MIC](mic.md) | `Ascii32` | `int32` | open |
| [Currency](currency.md) | `Ascii32` | `int32` | open |
| [Side](side.md) | `Ascii32` | `int32` | closed |
| [TimeInForce](time-in-force.md) | `Ascii32` | `int32` | closed |

A **closed** set answers only on the codes it compiles, which keeps a Python
answer and a pushed code-set filter on the same rows. An **open** one —
`Protocol`, `MIC`, `Currency` — registers a code it meets, and even there only
an exact round trip of the stored bytes registers.
