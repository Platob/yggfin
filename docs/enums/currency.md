# Currency

```python
from rekep.enums import Currency

usd = Currency.from_str("USD")
assert usd.code == "USD"
assert Currency.from_str("$") is usd
```

Currency packs its three uppercase letters as big-endian ASCII in an
`int32`, left-justified with a trailing NUL like every other ASCII code, so
the stored value orders exactly as the code does and carries nothing but the
letters. `Currency.register` accepts another ISO 4217 alphabetic code; the
table lists the built-in members.

| Key | Stored value |
| --- | ---: |
| `UNKNOWN` | 0 |
| `USD` | 1,431,520,256 |
| `EUR` | 1,163,219,456 |
| `GBP` | 1,195,528,192 |
| `JPY` | 1,246,779,648 |
| `CHF` | 1,128,809,984 |
| `CAD` | 1,128,350,720 |
| `AUD` | 1,096,107,008 |
| `NZD` | 1,314,538,496 |
| `CNY` | 1,129,208,064 |
| `HKD` | 1,212,892,160 |
| `SGD` | 1,397,179,392 |
| `SEK` | 1,397,050,112 |
| `NOK` | 1,313,819,392 |
| `DKK` | 1,145,785,088 |
| `PLN` | 1,347,177,984 |
| `CZK` | 1,129,990,912 |
| `HUF` | 1,213,548,032 |
| `MXN` | 1,297,632,768 |
| `BRL` | 1,112,689,664 |
| `ZAR` | 1,514,230,272 |
| `INR` | 1,229,869,568 |
| `KRW` | 1,263,687,424 |
| `TWD` | 1,415,005,184 |
| `XAU` | 1,480,676,608 |
| `XAG` | 1,480,673,024 |
| `XPT` | 1,481,659,392 |
| `XPD` | 1,481,655,296 |
| `XTS` | 1,481,921,280 |
| `XXX` | 1,482,184,704 |
