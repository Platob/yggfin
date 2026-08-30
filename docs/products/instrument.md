# InstrumentUpdate

One current reference-data event for a tradable instrument. `Instrument` is
the nested FIX component of reference facts; `InstrumentUpdate` adds the event
envelope that `market.instruments` stores. `xhash` is the table key and the
framed XXH3-64 identity of `instrument.symbolticker`.

```python
from rekep import FixMsg, Instrument, InstrumentUpdate

line = (
    "8=FIX.4.4|35=d|49=VENUE|56=DESK|34=2|52=20260101-09:00:00.000|"
    "55=BTC-USD|48=BTCUSD|22=8|167=FXSPOT|15=USD|207=XCME|60=20260101-09:00:00.000|10=000"
)
update = next(InstrumentUpdate.from_fixmsgs([FixMsg.from_text(line)]))
instrument = Instrument.from_update(update)
print(
    instrument.symbolticker,
    instrument.symbol,
    instrument.kind.name,
    instrument.currency.name,
    instrument.securityexchange,
)
```

```text
XCME:BTC-USD BTC-USD CURRENCY USD XCME
```

`InstrumentUpdate.from_instrument` is the inverse for one row. Arrow batches
use `Instrument.from_update_arrow_batch` and
`InstrumentUpdate.from_instrument_arrow_batch`; both stay columnar.

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

`InstrumentUpdate.versioned` enriches observations without revising known
facts. [Parse instruments](../pipeline/tasks/parse-instruments.md) overwrites
the current row for the same `xhash`; the nested component remains the
readable reference record.
