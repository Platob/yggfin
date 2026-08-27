# Side

```python
from rekep.enums import Side

side = Side.from_fix("1")
assert side is Side.BUY
assert side.sign == 1
assert side.opposite is Side.SELL
```

Side packs a readable mnemonic as big-endian ASCII in one `int32`,
left-justified with trailing NULs like every other ASCII code, so the stored
integer orders exactly as the mnemonic does. `BID` and `ASK` are aliases of
`BUY` and `SELL`.

| Key | Mnemonic | Stored value | FIX code | Meaning |
| --- | --- | ---: | --- | --- |
| `UNKNOWN` |  | 0 |  | No side stated. |
| `BUY` | `BUY` | 1,112,889,600 | `1` | Buying and book bid. |
| `BID` | `BUY` | 1,112,889,600 | `1` | Alias of `BUY`. |
| `BUY_MINUS` | `BYMN` | 1,113,148,750 | `3` | Buy not above the last differing price. |
| `BORROW` | `BORR` | 1,112,494,674 | `G` | Borrowing collateral. |
| `SUBSCRIBE` | `SUBS` | 1,398,096,467 | `D` | Subscribing to a fund. |
| `SELL` | `SELL` | 1,397,050,444 | `2` | Selling and book ask. |
| `ASK` | `SELL` | 1,397,050,444 | `2` | Alias of `SELL`. |
| `SELL_PLUS` | `SLPL` | 1,397,510,220 | `4` | Sell not below the last differing price. |
| `SELL_SHORT` | `SHRT` | 1,397,248,596 | `5` | Selling stock not held. |
| `SELL_SHORT_EXEMPT` | `SHEX` | 1,397,245,272 | `6` | Exempt short sale. |
| `LEND` | `LEND` | 1,279,610,436 | `F` | Lending collateral. |
| `REDEEM` | `REDM` | 1,380,271,181 | `E` | Redeeming a fund holding. |
| `CROSS` | `CROS` | 1,129,467,731 | `8` | Both sides are the same participant. |
| `CROSS_SHORT` | `CRSH` | 1,129,468,744 | `9` | Cross with a short sell leg. |
| `CROSS_SHORT_EXEMPT` | `CRSE` | 1,129,468,741 | `A` | Cross with an exempt short leg. |
| `AS_DEFINED` | `ASDF` | 1,095,976,006 | `B` | Direction defined by the multileg instrument. |
| `OPPOSITE` | `OPPO` | 1,330,663,503 | `C` | Opposite of the multileg definition. |
| `UNDISCLOSED` | `UNDS` | 1,431,192,659 | `7` | Direction withheld. |
