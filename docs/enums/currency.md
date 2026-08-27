# Currency

```python
from rekep.enums import Currency

usd = Currency.from_str("USD")
assert usd.code == "USD"
assert Currency.from_str("$") is usd
```

Currency packs its three uppercase letters as big-endian ASCII in an
`int32`, right-justified with a leading NUL like every other ASCII code --
so the stored value is simply the integer of the letters themselves, and no
decimal count rides in the value. `Currency.register` accepts another
ISO 4217 alphabetic code; the table lists the built-in members.

An earlier release stored `CCCn` -- the letters plus an ASCII decimal-count
digit in the fourth byte. `Currency.from_stored` (and the replay readers
built on `from_str`) still read that generation: the letters name the
currency and the digit drops. `from_int` stays exact on today's codes.

| Key | Stored value |
| --- | ---: |
| `UNKNOWN` | 0 |
| `USD` | 5,591,876 |
| `EUR` | 4,543,826 |
| `GBP` | 4,670,032 |
| `JPY` | 4,870,233 |
| `CHF` | 4,409,414 |
| `CAD` | 4,407,620 |
| `AUD` | 4,281,668 |
| `NZD` | 5,134,916 |
| `CNY` | 4,410,969 |
| `HKD` | 4,737,860 |
| `SGD` | 5,457,732 |
| `SEK` | 5,457,227 |
| `NOK` | 5,132,107 |
| `DKK` | 4,475,723 |
| `PLN` | 5,262,414 |
| `CZK` | 4,414,027 |
| `HUF` | 4,740,422 |
| `MXN` | 5,068,878 |
| `BRL` | 4,346,444 |
| `ZAR` | 5,914,962 |
| `INR` | 4,804,178 |
| `KRW` | 4,936,279 |
| `TWD` | 5,527,364 |
| `XAU` | 5,783,893 |
| `XAG` | 5,783,879 |
| `XPT` | 5,787,732 |
| `XPD` | 5,787,716 |
| `XTS` | 5,788,755 |
| `XXX` | 5,789,784 |
