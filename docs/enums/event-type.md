# EventType

```python
from rekep.enums import EventType

kind = EventType.from_int(1178682196)
assert kind is EventType.FACT
assert str(kind) == "FACT"
assert EventType.BOOK.is_snapshot
```

The stored `int64` is the event kind's ASCII mnemonic -- explicit spellings
like `ORDER` and `EXECUTED`, right-justified and packed big-endian with
leading NULs -- readable in raw column dumps, exact in scans. `from_int`
answers only on the compiled codes: any other integer is `UNKNOWN`, never a
near-miss respelling. The band order predicates reason over rides in each
member's *rank*; a kind question compares ranks
(`kind.rank >= EventType.INTENT.rank`), and a storage scan filters on the
finite code sets `ranked_at_least`/`ranked_below` spell rather than on a
range of the stored value.

Ranks double as the ids the original ordinal release stored, so
`from_stored` reads either generation of id back to its member.

| Key | Mnemonic | Stored value | Rank | Meaning |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` | `` | 0 | 0 | No event kind was resolved. |
| `MISC` | `MISC` | 1296651075 | 10 | A recognized message outside the market event families. |
| `INTENT` | `INTENT` | 80600770104916 | 100 | Band floor for instructions. |
| `ORDER` | `ORDER` | 340682622290 | 110 | An order instruction or lifecycle event. |
| `QUOTE` | `QUOTE` | 349323613253 | 120 | A quote instruction or response. |
| `FACT` | `FACT` | 1178682196 | 200 | Band floor for occurrences. |
| `EXECUTION` | `EXECUTED` | 4996819942064276804 | 210 | A trade or execution fact. |
| `STATE` | `STATE` | 357895853125 | 300 | Band floor for state snapshots. |
| `BOOK` | `BOOK` | 1112493899 | 320 | An order-book delta or snapshot. |
| `INSTRUMENT_STATE` | `ISTATE` | 80622244680773 | 400 | Band floor for instrument state. |
| `INSTRUMENT` | `INSTRMT` | 20633793103809876 | 410 | Instrument reference state. |
