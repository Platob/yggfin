# MarketKind

```python
from rekep.enums import MarketKind

order_kind = MarketKind.from_fix("2", tag=40)
execution_kind = MarketKind.from_fix("F", tag=150)

assert order_kind is MarketKind.LIMIT_ORDER
assert execution_kind is MarketKind.TRADE
```

Market kinds share one normalized vocabulary across order, execution and quote
fields. FIX values are tag-scoped, so conversion always supplies the source
tag instead of treating a wire character as globally unique.

| Key | Stored value | Meaning |
| --- | ---: | --- |
| `UNKNOWN` | 0 | No market kind was resolved. |
| `MARKET` | 100 | Band floor for market-priced orders. |
| `MARKET_ORDER` | 110 | Execute at the available market price. |
| `MARKET_IF_TOUCHED` | 120 | Become a market order at its trigger. |
| `MARKET_TO_LIMIT` | 130 | Execute at market, then rest the remainder as a limit. |
| `LIMIT` | 200 | Band floor for limit-priced orders. |
| `LIMIT_ORDER` | 210 | Execute only at the limit or better. |
| `LIMIT_ON_CLOSE` | 220 | Limit order for the closing auction. |
| `LIMIT_OR_BETTER` | 230 | Limit price permitting price improvement. |
| `STOP` | 300 | Band floor for stop instructions. |
| `STOP_ORDER` | 310 | Become a market order at the stop. |
| `STOP_LIMIT` | 320 | Become a limit order at the stop. |
| `PEGGED` | 400 | Band floor for reference-priced instructions. |
| `PEGGED_ORDER` | 410 | Price follows a declared reference. |
| `PREVIOUSLY_QUOTED` | 420 | Execute against an earlier quote. |
| `PREVIOUSLY_INDICATED` | 430 | Execute against an earlier indication. |
| `EXECUTION` | 500 | Band floor for execution reports. |
| `ORDER_STATUS` | 510 | Report order state without a trade. |
| `TRADE` | 520 | Report an execution. |
| `TRADE_CORRECT` | 530 | Correct a prior execution. |
| `TRADE_CANCEL` | 540 | Cancel a prior execution. |
| `LOCKED` | 550 | Lock a trade for clearing. |
| `RELEASED` | 560 | Release a locked trade. |
| `CLEARING` | 600 | Band floor for clearing state. |
| `CLEARING_HOLD` | 610 | Hold before clearing. |
| `RELEASED_TO_CLEARING` | 620 | Release to clearing. |
| `ACTIVATION` | 700 | Band floor for activation state. |
| `TRIGGERED` | 710 | A condition activated the instruction. |
