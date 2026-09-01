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

## Identifier changes

```python
from rekep import FixMsg
from rekep.market import Order


def order(body: str) -> Order:
    return next(
        event
        for event in FixMsg.from_text(body).into_market_events(fix_version="4.4")
        if isinstance(event, Order)
    )


base = "8=FIX.4.4|35=8|55=SYNTH|54=1|44=100|38=10|151=10|14=0|39=0|150=0|"
first = order(base + "11=CL-A|37=ORD-A|60=20260101-10:00:00|10=000").with_previous(None)
amended = order(
    base + "11=CL-B|41=CL-A|37=ORD-B|60=20260101-10:00:01|10=000"
).with_previous(first)

print(first.code, amended.code, first.xhash == amended.xhash)
print(first.version, amended.version)
```

```text
ORD-A ORD-A True
0 1
```

`OrigClOrdID <41>` is an explicit amendment edge, so both venue and client
identifiers may change without changing the lifecycle anchor. A stable
`GlobalOrderId`, `RootOrderId`, `RootOriginatorOrderId`, or `SecondaryOrderID`
provides the same evidence when a feed omits that edge:

```python
root = base + "30033=ROOT-X|"
root_first = order(
    root + "11=CL-1|37=ORD-1|60=20260101-10:01:00|10=000"
).with_previous(None)
root_moved = order(
    root + "11=CL-2|37=ORD-2|60=20260101-10:01:01|10=000"
).with_previous(root_first)

print(root_first.code, root_moved.code, root_first.xhash == root_moved.xhash)
```

```text
ORD-1 ORD-1 True
```

A reused `ClOrdID` alone cannot reconcile contradictory `OrderID` values.
Lookup keeps venue and client namespaces separate and reuses the first
lifecycle `code` after a proven match.

## Lineage

<div data-product-lineage data-product="order"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"></div>

Flattened by the [flatten orders](../pipeline/tasks/flatten-orders.md) task;
folded into books by [Book](book.md).
