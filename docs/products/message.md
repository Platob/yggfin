# Message

One log line, tokenized. The header and registry version are lifted into
columns; every other field stays untyped in `entries` in wire order.

```python
from rekep import Message

line = "8=FIX.4.4|35=8|49=VENUE|56=DESK|34=7|52=20260101-10:00:00.000|11=C1|55=BTC-USD|54=1|10=000"
row = Message.from_text(
    line,
    plugin="VenueBridge",
    sourceurl="s3://logs/capture.log",
    sourcerownum=1,
)

print(row.protocol, row.msgtype, row.msgseqnum, row.sendercompid, row.targetcompid)
print(row.plugin, row.body == line.encode())
print([(entry.tag, entry.value) for entry in row.entries])
```

```text
FIX4.4 8 7 VENUE DESK
VENUEBRIDGE True
[(11, 'C1'), (55, 'BTC-USD'), (54, '1'), (10, '000')]
```

`plugin` is the bounded recording-source code carried into derived FIX and
market rows. Missing or overwide source names are `UNKNOWN`.

!!! note "`body` keeps the exact bytes"

    `from_text` retains its input as non-null binary `body`. Parsing uses a
    UTF-8 view; identity and writes keep the original bytes.

    ```python
    Message.from_text(line).protocol.code  # "FIX4.4"
    Message.from_text(line).body      # b"8=FIX.4.4|..."
    ```

## Four structured protocols

`protocol` says which grammar the payload is written in and carries the
registry version when one resolves. The keys decide the grammar -- never the
values, and never a MsgType. The column stores a packed
[`Protocol`](../enums/protocol.md) code, not text.

| code | what the payload's keys are |
| --- | --- |
| `FIX` | numbered tags only |
| `FIXML` | numbered tags and rendered names together |
| `UL` | rendered names only |
| `XML` | an XML document, bare or behind an `XmlApi` transport prefix |

A frame is FIXML whether the names arrive inline (`8=FIX.4.4|55=IBM|#SIDE=1`)
or inside an `XmlData <213>` document the FIX stage expands. The wire token
`35=UL` is a MsgType and stays one: it says what the message *is*, not how it
is spelled, so a numbered frame carrying it is `FIX`. A value full of digits
is still a value, and a `#A=1` quoted inside `Text <58>` is that field's text
rather than a second field.

A payload with no structure at all is `OTHER`, and the log prefix in front of
one never changes the answer: classification reads the message, which starts
where the prefix ends.

```python
xml = b'<Order ID="C1"><Leg Symbol="AAPL"/><Leg Symbol="MSFT"/></Order>'
row = Message.from_text(xml)

print(row.protocol)
print([(entry.comp, entry.key, entry.value) for entry in row.entries])
```

```text
XML
[('Order[0]', 'ID', 'C1'), ('Order[0].Leg[0]', 'Symbol', 'AAPL'), ('Order[0].Leg[1]', 'Symbol', 'MSFT')]
```

`Message.from_text(xml)` and `Message(body=xml)` use the same structured
reader.

XML attributes and leaf elements become ordered `entries`. Indexed element
paths are stored in `comp`, so repeated nested elements remain
component-compatible. A malformed document keeps the row with an empty entry
list and a bounded diagnostic in `reason`.

The standard header and trailer are lifted, in the order the FIX stage
declares them, so a reader who has the header does not have to walk `entries`
for it. Two fields stay entries and cannot do otherwise. `CheckSum <10>` is
the boundary every other lift is measured against. And `XmlData <213>` is a
message more often than it is a document — FIXML writes `NAME=VALUE` pairs in
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
