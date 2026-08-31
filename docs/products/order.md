# Order

One version of one order. `lastpx` is its `Price <44>` limit. `lastqty` is the
**remaining live quantity after that event**: new orders carry their initial
quantity, partial fills reduce it, and terminal orders carry zero.

```python
from rekep import FixMsg

line = (
    "8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|"
    "55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"
)
events = list(FixMsg.from_text(line).into_market_events(fix_version="4.4"))
for event in events:
    print(type(event).__name__, event.state.name, event.lastqty, event.lastpx)

order, execution = events
print(order.codesource, execution.codesource)
print(order.linkhashes == [execution.hash])
print(execution.linkhashes == [order.hash])
```

```text
Order FILLED 0.0 None
Execution FILLED 10.0 100.25
OrderID ExecID
True
True
```

One execution report produces both rows: the `Execution` is the evidence, the
`Order` is the resulting state. The order is authoritative for the remaining
quantity, so the execution is never subtracted twice. Each row carries the
other's exact event `hash` in `linkhashes`; the Execution's `parenthash` also
records the Order event it was built from.

```python
from rekep.enums import State

State.live_codes()   # the finite code set a storage scan pushes down
```

Pending replace and cancel requests leave acknowledged interest resting; a
replace confirmation publishes the amended live quantity and price.

## Lineage

<div data-product-lineage data-product="order"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"></div>

Flattened by the [flatten orders](../pipeline/tasks/flatten-orders.md) task;
folded into books by [Book](book.md).
