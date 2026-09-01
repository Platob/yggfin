# InstUpdate

One current reference-data event for a tradable instrument. `Instrument` is
the nested FIX component of reference facts; `InstUpdate` adds the event
envelope that `market.instruments` stores. Its `fixed_size_binary[16]` `xhash`
is the table key: the direct XXH3-128 digest of UTF-8 `code`.

```python
from rekep import FixMsg, Instrument, InstUpdate

line = (
    "8=FIX.4.4|35=d|49=VENUE|56=DESK|34=2|52=20260101-09:00:00.000|"
    "55=BTC-USD|48=BTCUSD|22=8|167=FXSPOT|15=USD|207=XCME|60=20260101-09:00:00.000|10=000"
)
update = next(InstUpdate.from_fixmsgs([FixMsg.from_text(line)]))
instrument = Instrument.from_update(update)
print(update.altids, update.xhash == instrument.xhash)
print(
    instrument.symbolticker,
    instrument.symbol,
    instrument.kind.name,
    instrument.currency.name,
    instrument.securityexchange,
)
```

```text
{'code': 'XCME:BTC-USD', 'symbolticker': 'XCME:BTC-USD'} True
XCME:BTC-USD BTC-USD CURRENCY USD XCME
```

`InstUpdate.from_instrument` is the inverse for one row. Arrow batches
use `Instrument.from_update_arrow_batch` and
`InstUpdate.from_instrument_arrow_batch`; both stay columnar.

`Symbol <55>` leads, under the `SecurityExchange <207>` that named it. A line
carrying no symbol falls to `SecurityID <48>` with its `SecurityIDSource <22>`
-- `XCME:ExchangeSymbol:BTCUSD` for the row above without its `55` -- and an
identifier without its source is no key at all.

## Registry names

```python
import pyarrow

from rekep import Instrument
from rekep.fix import FixRegistry

rows = Instrument.from_fix_arrow(
    {
        "AMON.ISINCODE": pyarrow.array(["US0000000001", "GB0000000002"]),
        55: pyarrow.array(["SYNTH-A", None]),
        "207": pyarrow.array(["XNAS", "XLON"]),
        22: pyarrow.array(["4", "4"]),
        48: pyarrow.array(["US0000000001", "GB0000000002"]),
    },
    registry=FixRegistry(),
).to_pylist()

print([(row["symbolticker"], row["isincode"]) for row in rows])
```

```text
[('XNAS:SYNTH-A', 'US0000000001'), ('XLON:ISINNumber:GB0000000002', 'GB0000000002')]
```

`from_fix_arrow` resolves canonical names, folded aliases, numeric tags, and
ordered `fix:tags` through one cached registry plan. Canonical inputs lead and
the remaining spellings fill nulls. `Leg.from_fix_arrow` uses the same rule,
so ticker and ISIN derivation do not depend on a feed's preferred spelling.

`EUR/NOK`, `EURNOK`, and `EUR.NOK` share one FX spelling. The ticker also
classifies the instrument as currency and supplies `NOK` as its quote currency
when those facts are otherwise absent.

`InstUpdate.xhash`, `Instrument.xhash`, each `Leg.xhash`, and flat
`instrumentxhash` joins are the same sixteen-byte XXH3-128 identity derived
directly from UTF-8 `symbolticker`. They carry no event clock.

## Lineage

<div data-product-lineage data-product="instrument"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=d|49=VENUE|56=DESK|34=2|52=20260101-09:00:00.000|55=BTC-USD|48=BTCUSD|22=8|167=FXSPOT|15=USD|207=XCME|60=20260101-09:00:00.000|10=000"></div>

`InstUpdate.versioned` enriches observations without revising known
facts. [Parse instruments](../pipeline/tasks/parse-instruments.md) overwrites
the current row for the same event `xhash`; the nested component remains the
readable reference record.
