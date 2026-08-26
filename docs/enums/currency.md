# Currency

```python
from rekep.enums import Currency

usd = Currency.from_str("USD")
assert usd.code == "USD"
assert usd.decimals == 0
assert usd.packed_code == "USD0"
```

Currency packs three uppercase letters and one decimal-count digit as
big-endian ASCII in an `int32`. `Currency.register` accepts another `CCCn`
value; the table lists the built-in zero-decimal members.

| Key | Packed code | Stored value |
| --- | --- | ---: |
| `UNKNOWN` | | 0 |
| `USD` | `USD0` | 1,431,520,304 |
| `EUR` | `EUR0` | 1,163,219,504 |
| `GBP` | `GBP0` | 1,195,528,240 |
| `JPY` | `JPY0` | 1,246,779,696 |
| `CHF` | `CHF0` | 1,128,810,032 |
| `CAD` | `CAD0` | 1,128,350,768 |
| `AUD` | `AUD0` | 1,096,107,056 |
| `NZD` | `NZD0` | 1,314,538,544 |
| `CNY` | `CNY0` | 1,129,208,112 |
| `HKD` | `HKD0` | 1,212,892,208 |
| `SGD` | `SGD0` | 1,397,179,440 |
| `SEK` | `SEK0` | 1,397,050,160 |
| `NOK` | `NOK0` | 1,313,819,440 |
| `DKK` | `DKK0` | 1,145,785,136 |
| `PLN` | `PLN0` | 1,347,178,032 |
| `CZK` | `CZK0` | 1,129,990,960 |
| `HUF` | `HUF0` | 1,213,548,080 |
| `MXN` | `MXN0` | 1,297,632,816 |
| `BRL` | `BRL0` | 1,112,689,712 |
| `ZAR` | `ZAR0` | 1,514,230,320 |
| `INR` | `INR0` | 1,229,869,616 |
| `KRW` | `KRW0` | 1,263,687,472 |
| `TWD` | `TWD0` | 1,415,005,232 |
| `XAU` | `XAU0` | 1,480,676,656 |
| `XAG` | `XAG0` | 1,480,673,072 |
| `XPT` | `XPT0` | 1,481,659,440 |
| `XPD` | `XPD0` | 1,481,655,344 |
| `XTS` | `XTS0` | 1,481,921,328 |
| `XXX` | `XXX0` | 1,482,184,752 |
