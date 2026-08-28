# Message

One log line, tokenized. Protocol-neutral: the header is lifted into columns,
every other field stays in `entries` in wire order.

```python
from rekep import Message

line = "8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|55=BTC-USD|54=1|10=000"
row = Message.from_text(line, message=line, sourceurl="s3://logs/capture.log", sourcerownum=1)

print(row.protocolcode, row.msgtype, row.msgseqnum, row.sendercompid, row.targetcompid)
print([(entry.tag, entry.value) for entry in row.entries])
```

```text
FIX 8 7 VENUE DESK
[(11, 'C1'), (55, 'BTC-USD'), (54, '1'), (10, '000')]
```

!!! warning "`message=` is what makes the syntax columns answer"

    `protocolcode`, `etype` and `direction` are read off the raw text, not
    off the pairs. Staged without it they stay unset and the FIX codec does
    not claim the row.

    ```python
    Message.from_text(line).protocolcode            # 'OTHER'
    Message.from_text(line, message=line).protocolcode  # 'FIX'
    ```

Seven header fields are lifted; `CheckSum <10>` is the boundary they are
lifted before, so it stays an entry:

```python
from rekep.text.message import SESSION_FIELDS

print([name for name, tag in SESSION_FIELDS])
```

```text
['beginstring', 'bodylength', 'msgtype', 'msgseqnum', 'sendercompid', 'targetcompid', 'sendingtime']
```

## Lineage

<div data-product-lineage data-product="message"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"></div>

<noscript>The lineage view requires JavaScript. `rekep fields load --target
schemas/rekep/message.yaml` prints the same columns.</noscript>

Next: [FixMsg](fixmsg.md) transcribes those entries under the registry.
