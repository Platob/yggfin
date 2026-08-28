# EventType

[`Ascii64`](ascii-codes.md){ .enum-base } — eight bytes of printable ASCII packed left-justified into one `int64`, a closed set, so a stored value is a compiled code or it is `UNKNOWN`.

```python
from rekep.enums import EventType

kind = EventType.from_int(5062401484197462016)
assert kind is EventType.FACT
assert str(kind) == "FACT"
assert EventType.BOOK.is_snapshot
```

Eight bytes buy explicit spellings -- `ORDER`, `EXECUTED` -- where four would
have forced abbreviations, so a raw column dump reads as itself. `from_int`
answers only on the compiled codes: any other integer is `UNKNOWN`, never a
near-miss respelling.

The order the bands read in rides in each member's *rank*. A kind question
compares ranks (`kind.rank >= EventType.INTENT.rank`), and a storage scan
filters on the finite code sets `ranked_at_least`/`ranked_below` spell rather
than on a range of the stored value.

| Key | Mnemonic | Stored value | Rank | Meaning |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` | `` | 0 | 0 | No event kind was resolved. |
| `MISC` | `MISC` | 5569073961448243200 | 10 | A recognized message outside the market event families. |
| `INTENT` | `INTENT` | 5282252069595774976 | 100 | Band floor for instructions. |
| `ORDER` | `ORDER` | 5715705941605744640 | 110 | An order instruction or lifecycle event. |
| `QUOTE` | `QUOTE` | 5860677713446043648 | 120 | A quote instruction or response. |
| `FACT` | `FACT` | 5062401484197462016 | 200 | Band floor for occurrences. |
| `EXECUTION` | `EXECUTED` | 4996819942064276804 | 210 | A trade or execution fact. |
| `STATE` | `STATE` | 6004496033382400000 | 300 | Band floor for state snapshots. |
| `BOOK` | `BOOK` | 4778124913204527104 | 320 | An order-book delta or snapshot. |
| `INSTRUMENT_STATE` | `ISTATE` | 5283659427399139328 | 400 | Band floor for instrument state. |
| `INSTRUMENT` | `INSTRMT` | 5282251034575328256 | 410 | Instrument reference state. |
