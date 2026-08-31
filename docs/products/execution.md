# Execution

One fill, correction or cancellation reported against an order. `lastqty` and
`lastpx` are that report's own `LastQty <32>` and `LastPx <31>`, not the
order's running totals.

```python
from rekep import FixMsg

line = (
    "8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|"
    "55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"
)
execution = [
    event
    for event in FixMsg.from_text(line).into_market_events(fix_version="4.4")
    if type(event).__name__ == "Execution"
][0]
print(
    execution.lastqty,
    execution.lastpx,
    execution.execid,
    execution.orderid,
    execution.codesource,
    execution.state.name,
)
```

```text
10.0 100.25 E1 O1 ExecID FILLED
```

Missing identifiers may resolve against indexed live order names. Venue
rejection and expiry use `REJECTED`/`EXPIRED`; records this pipeline rejects
or expires use `INTERNAL_REJECTED`/`INTERNAL_EXPIRED`, so an audit query can
separate them.

## Lineage

<div data-product-lineage data-product="execution"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"></div>

Flattened by the
[flatten executions](../pipeline/tasks/flatten-executions.md) task.
