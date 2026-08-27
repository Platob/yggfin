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
| `UNKNOWN` |  | 0 | 0 |  |
| `REGISTERED` | `REGISTRD` | 5928222864759345732 | 100 |  |
| `ISIN` | `ISIN` | 5283647387192000512 | 110 | `4` |
| `CUSIP` | `CUSIP` | 4851875747901472768 | 120 | `1` |
| `SEDOL` | `SEDOL` | 6000277185909227520 | 130 | `2` |
| `COMMON` | `COMMON` | 4850180318139645952 | 140 | `G` |
| `VENDOR` | `VENDOR` | 6216460915950354432 | 200 |  |
| `RIC` | `RIC` | 5929344051689029632 | 210 | `5` |
| `BLOOMBERG` | `BLOOMBRG` | 4777280506750456391 | 220 | `A` |
| `LOCAL` | `LOCAL` | 5498687617769668608 | 300 |  |
| `WERTPAPIER` | `WERTPAPR` | 6288522976769953874 | 310 | `B` |
| `DUTCH` | `DUTCH` | 4923934415547006976 | 320 | `C` |
| `VALOREN` | `VALOREN` | 6215332864314396160 | 330 | `D` |
| `SICOVAM` | `SICOVAM` | 6001401986476494080 | 340 | `E` |
| `BELGIAN` | `BELGIAN` | 4775306848951684608 | 350 | `F` |
| `QUIK` | `QUIK` | 5860671076563943424 | 360 | `3` |
| `VENUE` | `VENUE` | 6216460988791652352 | 400 |  |
| `EXCHANGE` | `EXCHANGE` | 4996817764179920709 | 410 | `8` |
| `CTA` | `CTA` | 4851574166840672256 | 420 | `9` |
| `OPRA` | `OPRA` | 5715158366259511296 | 430 | `J` |
| `CLEARING` | `CLEARING` | 4849327045626908231 | 440 | `H` |
| `MARKETPLACE` | `MKTPLACE` | 5569638068027212613 | 450 | `M` |
| `OTHER` | `OTHER` | 5716273289605677056 | 500 |  |
| `CURRENCY` | `CURRENCY` | 4851874686865130329 | 510 | `6` |
| `COUNTRY` | `COUNTRY` | 4850189118611806464 | 520 | `7` |
| `ISDA_SPEC` | `ISDASPEC` | 5283641835197056323 | 530 | `I` |
| `ISDA_URL` | `ISDAURL` | 5283641835230743552 | 540 | `K` |
| `CREDIT_LETTER` | `CRDTLTTR` | 4851014877479982162 | 550 | `L` |
