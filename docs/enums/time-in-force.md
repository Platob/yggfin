# TimeInForce

```python
from rekep.enums import TimeInForce

lifetime = TimeInForce.from_fix("3")
assert lifetime is TimeInForce.IOC
assert not lifetime.rests
```

Time in force packs a readable mnemonic as big-endian ASCII in one `int32`,
right-justified with leading NULs like every other ASCII code -- so a
three-letter mnemonic stores as the plain integer of its own letters.
Ordering follows the declared lifetime rank rather than the packed integer.

| Key | Mnemonic | Stored value | FIX code | Meaning |
| --- | --- | ---: | --- | --- |
| `UNKNOWN` | | 0 | | Venue default. |
| `IMMEDIATE` | `IMMD` | 1,229,802,820 | | Ordering marker for non-resting instructions. |
| `IOC` | `IOC` | 4,804,419 | `3` | Trade what can immediately and cancel the rest. |
| `FOK` | `FOK` | 4,607,819 | `4` | Trade all immediately or none. |
| `SESSION` | `SESS` | 1,397,052,243 | | Ordering marker for session-valid instructions. |
| `DAY` | `DAY` | 4,473,177 | `0` | Good for the session. |
| `AT_OPEN` | `OPEN` | 1,330,660,686 | `2` | Opening auction only. |
| `AT_CLOSE` | `CLOS` | 1,129,074,515 | `7` | Closing auction only. |
| `GTX` | `GTX` | 4,674,648 | `5` | Good until crossing. |
| `GOOD_THROUGH_CROSSING` | `GTCR` | 1,196,704,594 | `8` | Valid through the next crossing phase. |
| `AT_CROSSING` | `ATCR` | 1,096,041,298 | `9` | Valid only during crossing. |
| `GFA` | `GFA` | 4,671,041 | `B` | Good for one auction. |
| `RESTING` | `REST` | 1,380,275,028 | | Ordering marker for cross-session instructions. |
| `GTC` | `GTC` | 4,674,627 | `1` | Good until cancelled. |
| `GTD` | `GTD` | 4,674,628 | `6` | Good until `Event.eunix`. |
| `GFT` | `GFT` | 4,671,060 | `A` | Good for a duration resolved into `Event.eunix`. |
| `GFM` | `GFM` | 4,671,053 | `C` | Good for the current month. |
