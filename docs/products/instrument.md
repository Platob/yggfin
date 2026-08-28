# Instrument

One version of the facts known about a tradable instrument. `xhash` and
`code` derive from the exact symbol only, so one spelling is one lifecycle
across venues and two spellings never alias through an identifier.

```python
from rekep import FixMsg, Instrument

line = (
    "8=FIX.4.4|35=d|49=VENUE|56=DESK|34=2|52=20260101-09:00:00.000|"
    "55=BTC-USD|48=BTCUSD|22=8|167=FXSPOT|15=USD|207=XCME|60=20260101-09:00:00.000|10=000"
)
for row in Instrument.from_fixmsgs([FixMsg.from_text(line)]):
    print(row.symbol, row.code, row.kind.name, row.currency.name, row.exchange, row.security_id)
```

```text
BTC-USD BTC-USD CURRENCY USD XCME BTCUSD
```

An exact `AAA/BBB` symbol classifies as currency when no kind is declared and
supplies `BBB` as the price currency when that is absent. A symbol first seen
on a market log creates a synthetic minimal instrument; later facts enrich that
same lifecycle. There is no separate reference model.

## Lineage

<div data-product-lineage data-product="instrument"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=d|49=VENUE|56=DESK|34=2|52=20260101-09:00:00.000|55=BTC-USD|48=BTCUSD|22=8|167=FXSPOT|15=USD|207=XCME|60=20260101-09:00:00.000|10=000"></div>

Flattened into its table by the
[flatten instruments](../pipeline/tasks/flatten-instruments.md) task.
