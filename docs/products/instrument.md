# Instrument

One flat reference-data record for a tradable instrument. `symbolticker` is
the canonical stored key, and `xhash` is its framed XXH3-64 identity.

```python
from rekep import FixMsg, Instrument

line = (
    "8=FIX.4.4|35=d|49=VENUE|56=DESK|34=2|52=20260101-09:00:00.000|"
    "55=BTC-USD|48=BTCUSD|22=8|167=FXSPOT|15=USD|207=XCME|60=20260101-09:00:00.000|10=000"
)
for row in Instrument.from_fixmsgs([FixMsg.from_text(line)]):
    print(
        row.symbolticker,
        row.symbol,
        row.kind.name,
        row.currency.name,
        row.securityexchange,
    )
```

```text
XCME:BTC-USD BTC-USD CURRENCY USD XCME
```

`Symbol <55>` leads, under the `SecurityExchange <207>` that named it. A line
carrying no symbol falls to `SecurityID <48>` with its `SecurityIDSource <22>`
-- `XCME:ExchangeSymbol:BTCUSD` for the row above without its `55` -- and an
identifier without its source is no key at all.

`EUR/NOK`, `EURNOK`, and `EUR.NOK` share one FX spelling. The ticker also
classifies the instrument as currency and supplies `NOK` as its quote currency
when those facts are otherwise absent.

## Lineage

<div data-product-lineage data-product="instrument"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=d|49=VENUE|56=DESK|34=2|52=20260101-09:00:00.000|55=BTC-USD|48=BTCUSD|22=8|167=FXSPOT|15=USD|207=XCME|60=20260101-09:00:00.000|10=000"></div>

Written directly to `market.instruments` by
[parse FIX](../pipeline/tasks/parse-fix.md).
