# EventType

[`Ascii64`](ascii-codes.md){ .enum-base } — eight bytes of printable ASCII packed left-justified into one `int64`, an open vocabulary, so a valid code registers when first read.

```python
from rekep.enums import EventType

kind = EventType.from_int(5062401484197462016)
assert kind is EventType.FACT
assert str(kind) == "FACT"
assert EventType.BOOK.is_snapshot
```

Eight bytes buy explicit spellings -- `ORDER`, `EXECUTED` -- where four would
have forced abbreviations, so a raw column dump reads as itself. `from_int`
registers a well-formed future code and rejects malformed packed bytes.

The order the bands read in rides in each member's *rank*. A kind question
compares ranks (`kind.rank >= EventType.INTENT.rank`), and a storage scan
filters on the finite code sets `ranked_at_least`/`ranked_below` spell rather
than on a range of the stored value.

| Key | Mnemonic | Stored value | Rank | Meaning |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` | `` | 0 | 0 | No event kind was resolved. |
| `MISC` | `MISC` | 5569073961448243200 | 10 | A recognized message outside the market event families. |
| `SESSION` | `SESSION` | 6000293695718379008 | 20 | FIX session or administrative traffic. |
| `INTENT` | `INTENT` | 5282252069595774976 | 100 | Band floor for instructions. |
| `ORDER` | `ORDER` | 5715705941605744640 | 110 | An order instruction or lifecycle event. |
| `QUOTE` | `QUOTE` | 5860677713446043648 | 120 | A quote instruction or response. |
| `INDICATION` | `INDICATE` | 5282234494403826757 | 130 | An indication of interest. |
| `ALLOCATION` | `ALLOCATE` | 4705219614009807941 | 140 | An allocation instruction or response. |
| `SETTLEMENT` | `SETTLE` | 6000294799574630400 | 150 | A settlement instruction or response. |
| `FACT` | `FACT` | 5062401484197462016 | 200 | Band floor for occurrences. |
| `EXECUTION` | `EXECUTED` | 4996819942064276804 | 210 | A trade or execution fact. |
| `INSTRUMENT` | `INSTRMT` | 5282251034575328256 | 220 | An observed InstrumentUpdate reference event. |
| `CONFIRMATION` | `CONFIRM` | 4850181387486121216 | 230 | A confirmation or affirmation fact. |
| `NEWS` | `NEWS` | 5640010122345316352 | 240 | News or event communication. |
| `STATE` | `STATE` | 6004496033382400000 | 300 | Band floor for state snapshots. |
| `BOOK` | `BOOK` | 4778124913204527104 | 320 | An order-book delta or snapshot. |
| `POSITION` | `POSITION` | 5786935620606185294 | 330 | A position report or adjustment. |
| `COLLATERAL` | `COLLATRL` | 4850179214098584140 | 340 | A collateral state or response. |
| `PARTY` | `PARTY` | 5782993918744330240 | 350 | Party reference state. |
