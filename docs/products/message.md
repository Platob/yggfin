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

    `protocolcode`, `eventtype` and `direction` are read off the raw text, not
    off the pairs. Staged without it, direction stays `UNKNOWN` and the FIX
    codec does not claim the row.

    ```python
    Message.from_text(line).protocolcode            # 'OTHER'
    Message.from_text(line, message=line).protocolcode  # 'FIX'
    ```

The standard header and trailer are lifted, in the order the FIX stage
declares them, so a reader who has the header does not have to walk `entries`
for it. Two fields stay entries and cannot do otherwise. `CheckSum <10>` is
the boundary every other lift is measured against. And `XmlData <213>` is a
message more often than it is a document — bridges write `key=value` pairs in
it, which the FIX stage expands in the place the tag sat — so it stays where
that expansion can still see it, with `XmlDataLen <212>` beside it because a
length and the value it measures are one token.

Which fields, and which tag each answers to, come from `rekep.fix.columns` —
the FIX stage's own declaration, with every tag read from the registry — so
neither stage writes a tag down twice:

```python
from rekep.text.message import SESSION_FIELDS

print([name for name, tag in SESSION_FIELDS])
```

```text
['beginstring', 'bodylength', 'msgtype', 'sendercompid', 'sendersubid', 'senderlocationid', 'targetcompid', 'targetsubid', 'targetlocationid', 'onbehalfofcompid', 'onbehalfofsubid', 'onbehalfoflocationid', 'delivertocompid', 'delivertosubid', 'delivertolocationid', 'msgseqnum', 'lastmsgseqnumprocessed', 'possdupflag', 'possresend', 'sendingtime', 'origsendingtime', 'onbehalfofsendingtime', 'applverid', 'cstmapplverid', 'applextid', 'messageencoding', 'securedatalen', 'securedata', 'signaturelength', 'signature']
```

## Lineage

<div data-product-lineage data-product="message"
     data-source="../../assets/product-lineage.json"
     data-registry-source="../../assets/fix-registry.json"
     data-sample="8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|151=0|14=10|39=2|150=F|60=20260101-10:00:00.000|10=000"></div>

<noscript>The lineage view requires JavaScript. `rekep fields load --target
schemas/rekep/message.yaml` prints the same columns.</noscript>

Next: [FixMsg](fixmsg.md) transcribes those entries under the registry.
