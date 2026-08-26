# EventType

```python
from rekep.enums import EventType

kind = EventType.from_code(219)
assert kind is EventType.FACT
assert EventType.BOOK.is_snapshot
```

Event types are banded by what a row asserts. Band markers are valid query
boundaries as well as fallback values for unknown detailed codes.

| Key | Stored value | Meaning |
| --- | ---: | --- |
| `UNKNOWN` | 0 | No event kind was resolved. |
| `MISC` | 10 | A recognized message outside the market event families. |
| `INTENT` | 100 | Band floor for instructions. |
| `ORDER` | 110 | An order instruction or lifecycle event. |
| `QUOTE` | 120 | A quote instruction or response. |
| `FACT` | 200 | Band floor for occurrences. |
| `EXECUTION` | 210 | A trade or execution fact. |
| `STATE` | 300 | Band floor for state snapshots. |
| `BOOK` | 320 | An order-book delta or snapshot. |
| `INSTRUMENT_STATE` | 400 | Band floor for instrument state. |
| `INSTRUMENT` | 410 | Instrument reference state. |
