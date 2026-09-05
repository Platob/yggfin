# FixMsg

One line transcribed under the [FIX registry](../fix/index.md): tags become
typed columns, and `unix` becomes the instant the message says it happened.

```python
from rekep import FixMsg, Message
from rekep.enums import Protocol

line = (
    "8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|"
    "55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"
)
staged = Message.from_text(line)
row = FixMsg.from_message_batch([staged]).to_pylist()[0]

row["protocol"] = Protocol.from_stored(row["protocol"]).code
row["altids"] = dict(row["altids"])
for name in (
    "protocol", "side", "lastpx", "priceinferred", "ordstatus", "unix",
    "unixsource", "code", "altids"
):
    print(f"{name:17} {row[name]!r}")
print(f"{'instrument.symbol':17} {row['instrument']['symbol']!r}")
```

```text
protocol          'FIX4.4'
side              '1'
lastpx            100.25
priceinferred     ''
ordstatus         '2'
unix              1767261600000000000
unixsource        'TransactTime'
code              'O1'
altids            {'orderid': 'O1', 'clordid': 'C1', 'execid': 'E1', 'code': 'O1', 'symbolticker': 'BTC-USD'}
instrument.symbol 'BTC-USD'
```

!!! note "One parsing boundary"

    `from_message_batch` is where raw `Message.body` becomes typed columns.
    `FixMsg.from_text` first builds that same raw envelope and passes through
    the batch parser. Session fields and components lift there; `entries`
    retains only ordered residual fields, and the raw body is not persisted.

    ```python
    FixMsg.from_text(line).msgtype              # '8'
    FixMsg.from_text(line).instrument.symbol    # 'BTC-USD'
    ```

`unixsource` names which rung answered, so a transaction time and a print
time are never confused. The chain is in
[market lifecycle](../market/index.md#when-it-happened).

`altids` keeps every code under its folded FIX field name. The direct XXH3-128
digest of `code` is the sixteen-byte lifecycle `xhash`.

`priceinferred` names price slots derived from another FIX price field. An
empty value means every stored price slot was explicit on the source message.

Malformed typed values and isolated transcription failures remain as rows with
a nullable `error`; see [best-effort rows](../fix/fixmsg.md#best-effort-rows).

## Lineage

<div data-product-lineage data-product="fixmsg"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"></div>

Decode and encode arbitrary FIX on the [transcribe](../fix/transcribe.md) page,
which runs the same decoder this view does.
