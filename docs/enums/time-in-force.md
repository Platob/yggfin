# TimeInForce

[`Ascii32`](ascii-codes.md){ .enum-base } — four bytes of printable ASCII packed left-justified into one `int32`, a closed set, so a stored value is a compiled code or it is `UNKNOWN`.

```python
from rekep.enums import TimeInForce

lifetime = TimeInForce.from_fix("3")
assert lifetime is TimeInForce.IOC
assert not lifetime.rests
```

Ordering follows the declared lifetime rank rather than the packed integer:
these codes sort by how long an order lives, which their spellings do not.

| Key | Mnemonic | Stored value | FIX code | Meaning |
| --- | --- | ---: | --- | --- |
| `UNKNOWN` |  | 0 |  | Venue default. |
| `IMMEDIATE` | `IMMD` | 1,229,802,820 |  | Ordering marker for non-resting instructions. |
| `IOC` | `IOC` | 1,229,931,264 | `3` | Trade what can immediately and cancel the rest. |
| `FOK` | `FOK` | 1,179,601,664 | `4` | Trade all immediately or none. |
| `SESSION` | `SESS` | 1,397,052,243 |  | Ordering marker for session-valid instructions. |
| `DAY` | `DAY` | 1,145,133,312 | `0` | Good for the session. |
| `AT_OPEN` | `OPEN` | 1,330,660,686 | `2` | Opening auction only. |
| `AT_CLOSE` | `CLOS` | 1,129,074,515 | `7` | Closing auction only. |
| `GTX` | `GTX` | 1,196,709,888 | `5` | Good until crossing. |
| `GOOD_THROUGH_CROSSING` | `GTCR` | 1,196,704,594 | `8` | Valid through the next crossing phase. |
| `AT_CROSSING` | `ATCR` | 1,096,041,298 | `9` | Valid only during crossing. |
| `GFA` | `GFA` | 1,195,786,496 | `B` | Good for one auction. |
| `RESTING` | `REST` | 1,380,275,028 |  | Ordering marker for cross-session instructions. |
| `GTC` | `GTC` | 1,196,704,512 | `1` | Good until cancelled. |
| `GTD` | `GTD` | 1,196,704,768 | `6` | Good until `Event.eunix`. |
| `GFT` | `GFT` | 1,195,791,360 | `A` | Good for a duration resolved into `Event.eunix`. |
| `GFM` | `GFM` | 1,195,789,568 | `C` | Good for the current month. |
