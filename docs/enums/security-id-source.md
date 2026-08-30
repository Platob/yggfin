# SecurityIDSource

[`Ascii32`](ascii-codes.md){ .enum-base } — four bytes of printable ASCII packed left-justified into one `int32`, an open vocabulary, so a code it meets and can round-trip registers itself.

```python
from rekep.enums import SecurityIDSource

assert SecurityIDSource.from_fix("4") is SecurityIDSource.ISIN
assert SecurityIDSource.ISIN.into_fix() == "4"
assert SecurityIDSource.from_str("ISINNumber") is SecurityIDSource.ISIN
```

Which scheme a `SecurityID <48>` is issued under. The name is the spelling and
the code is what a column holds, because four bytes cannot carry
`FINANCIAL_INSTRUMENT_GLOBAL_IDENTIFIER`. The wire values are the dictionary's:
this vocabulary names `SecurityIDSource <22>` and reads tag 22's codes from it
rather than compiling a second copy.

A desk's own reference system is a scheme like any other and the dictionary
cannot know it, so a code it meets registers itself -- with no wire value,
since the field enumerates none for it.

| Key | Code | Stored value | FIX |
| --- | --- | ---: | --- |
| `UNKNOWN` |  | 0 |  |
| `CUSIP` | `CUSP` | 1,129,665,360 | `1` |
| `SEDOL` | `SEDL` | 1,397,048,396 | `2` |
| `QUIK` | `QUIK` | 1,364,543,819 | `3` |
| `ISIN` | `ISIN` | 1,230,195,022 | `4` |
| `RIC` | `RIC` | 1,380,532,992 | `5` |
| `ISO_CURRENCY` | `CCY` | 1,128,487,168 | `6` |
| `ISO_COUNTRY` | `CTRY` | 1,129,599,577 | `7` |
| `EXCHANGE_SYMBOL` | `EXCH` | 1,163,412,296 | `8` |
| `CTA` | `CTA` | 1,129,595,136 | `9` |
| `BLOOMBERG` | `BBG` | 1,111,639,808 | `A` |
| `WERTPAPIER` | `WKN` | 1,464,552,960 | `B` |
| `DUTCH` | `DUTC` | 1,146,442,819 | `C` |
| `VALOREN` | `VALO` | 1,447,119,951 | `D` |
| `SICOVAM` | `SICO` | 1,397,310,287 | `E` |
| `BELGIAN` | `BELG` | 1,111,837,767 | `F` |
| `COMMON` | `COMN` | 1,129,270,606 | `G` |
| `CLEARING_HOUSE` | `CLRH` | 1,129,075,272 | `H` |
| `ISDA_FPML_SPEC` | `ISDA` | 1,230,193,729 | `I` |
| `OPRA` | `OPRA` | 1,330,664,001 | `J` |
| `ISDA_FPML_URL` | `FPML` | 1,179,667,788 | `K` |
| `LETTER_OF_CREDIT` | `LOC` | 1,280,262,912 | `L` |
| `MARKETPLACE` | `MKTP` | 1,296,782,416 | `M` |
| `MARKIT_RED_ENTITY` | `RDEC` | 1,380,205,891 | `N` |
| `MARKIT_RED_PAIR` | `RDPC` | 1,380,208,707 | `P` |
| `CFTC_COMMODITY` | `CFTC` | 1,128,682,563 | `Q` |
| `ISDA_COMMODITY` | `ICRP` | 1,229,148,752 | `R` |
| `FIGI` | `FIGI` | 1,179,207,497 | `S` |
| `LEI` | `LEI` | 1,279,609,088 | `T` |
| `SYNTHETIC` | `SYNT` | 1,398,361,684 | `U` |
| `FIDESSA` | `FIDM` | 1,179,206,733 | `V` |
| `INDEX_NAME` | `INDX` | 1,229,866,072 | `W` |
| `UNIFORM_SYMBOL` | `UNIF` | 1,431,193,926 | `X` |
| `DIGITAL_TOKEN` | `DTI` | 1,146,374,400 | `Y` |
