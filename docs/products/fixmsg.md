# FixMsg

One line transcribed under the [FIX registry](../fix/index.md): tags become
typed columns, and `unix` becomes the instant the message says it happened.

```python
from rekep import FixMsg, Message

line = (
    "8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|"
    "55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"
)
staged = Message.from_text(line, message=line)
row = FixMsg.from_message_batch([staged]).to_pylist()[0]

for name in ("protocolversion", "symbol", "side", "lastpx", "ordstatus", "unix", "unixsource"):
    print(f"{name:17} {row[name]!r}")
```

```text
protocolversion   '4.4'
symbol            'BTC-USD'
side              '1'
lastpx            100.25
ordstatus         '2'
unix              1767261600000000000
unixsource        'TransactTime'
```

!!! note "Batch transcribes; scalar lifts"

    `from_message_batch` is where a payload becomes columns.
    `FixMsg.from_text` lifts the same seven header fields the raw stage does
    and leaves the body in `entries` -- which is what
    `into_market_events` reads.

    ```python
    FixMsg.from_text(line).msgtype   # '8'
    FixMsg.from_text(line).symbol    # None -- body stays in entries
    ```

`unixsource` names which rung answered, so a transaction time and a print
time are never confused. The chain is in
[market lifecycle](../market/index.md#when-it-happened).

## Lineage

<div data-product-lineage data-product="fixmsg"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"></div>

Decode and encode arbitrary FIX on the [transcribe](../fix/transcribe.md) page,
which runs the same decoder this view does.
