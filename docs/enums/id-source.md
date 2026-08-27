# IdSource

```python
from rekep.enums import IdSource

source = IdSource.from_fix("4")
assert source is IdSource.ISIN
assert source.is_registered
```

Identifier sources are banded by issuer, with the banding carried by each
member's rank and the stored value its readable mnemonic. The FIX code is the
value used by `SecurityIDSource <22>`.

| Key | Mnemonic | Stored value | Rank | FIX code |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` | | 0 | 0 | |
| `REGISTERED` | `REGISTRD` | 5928222864759345732 | 100 |  |
| `ISIN` | `ISIN` | 1230195022 | 110 | `4` |
| `CUSIP` | `CUSIP` | 289194330448 | 120 | `1` |
| `SEDOL` | `SEDOL` | 357644390220 | 130 | `2` |
| `COMMON` | `COMMON` | 74007878389582 | 140 | `G` |
| `VENDOR` | `VENDOR` | 94855665831762 | 200 |  |
| `RIC` | `RIC` | 5392707 | 210 | `5` |
| `BLOOMBERG` | `BLOOMBRG` | 4777280506750456391 | 220 | `A` |
| `LOCAL` | `LOCAL` | 327747322188 | 300 |  |
| `WERTPAPIER` | `WERTPAPR` | 6288522976769953874 | 310 | `B` |
| `DUTCH` | `DUTCH` | 293489361736 | 320 | `C` |
| `VALOREN` | `VALOREN` | 24278644001228110 | 330 | `D` |
| `SICOVAM` | `SICOVAM` | 23442976509673805 | 340 | `E` |
| `BELGIAN` | `BELGIAN` | 18653542378717518 | 350 | `F` |
| `QUIK` | `QUIK` | 1364543819 | 360 | `3` |
| `VENUE` | `VENUE` | 370529948997 | 400 |  |
| `EXCHANGE` | `EXCHANGE` | 4996817764179920709 | 410 | `8` |
| `CTA` | `CTA` | 4412481 | 420 | `9` |
| `OPRA` | `OPRA` | 1330664001 | 430 | `J` |
| `CLEARING` | `CLEARING` | 4849327045626908231 | 440 | `H` |
| `MARKETPLACE` | `MKTPLACE` | 5569638068027212613 | 450 | `M` |
| `OTHER` | `OTHER` | 340716438866 | 500 |  |
| `CURRENCY` | `CURRENCY` | 4851874686865130329 | 510 | `6` |
| `COUNTRY` | `COUNTRY` | 18946051244577369 | 520 | `7` |
| `ISDA_SPEC` | `ISDASPEC` | 5283641835197056323 | 530 | `I` |
| `ISDA_URL` | `ISDAURL` | 20639225918870092 | 540 | `K` |
| `CREDIT_LETTER` | `CRDTLTTR` | 4851014877479982162 | 550 | `L` |
