# Instrument

One version of the facts known about a tradable instrument. `symbolticker`
is the canonical stored key, and `xhash` is its framed XXH3-64 identity.

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
XCME:ExchangeSymbol:BTCUSD BTC-USD CURRENCY USD XCME
```

`SecurityID <48>` with `SecurityIDSource <22>` leads, so the identifier above
is qualified by its scheme and `SecurityExchange <207>`. Without that pair the
ticker is `MIC:SYMBOL`, or just `SYMBOL` when the venue is unknown.

`EUR/NOK`, `EURNOK`, and `EUR.NOK` share one FX spelling. The ticker also
classifies the instrument as currency and supplies `NOK` as its quote currency
when those facts are otherwise absent.

## Lineage

<div data-product-lineage data-product="instrument"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=d|49=VENUE|56=DESK|34=2|52=20260101-09:00:00.000|55=BTC-USD|48=BTCUSD|22=8|167=FXSPOT|15=USD|207=XCME|60=20260101-09:00:00.000|10=000"></div>

Flattened into its table by the
[flatten instruments](../pipeline/tasks/flatten-instruments.md) task.
