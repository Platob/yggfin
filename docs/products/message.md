# Message

One raw text record. `Message` preserves the source position, captured log
header, and exact binary body; it does not inspect the body.

```python
from rekep import Message

line = b"8=FIX.4.4|35=D|11=C1|10=000"
row = Message.from_text(
    line,
    sourceurl="s3://logs/capture.log",
    sourcerownum=17,
    timestamp="2026-01-01 10:00:00.000_000",
    threadname="fix-reader",
    plugin="VenueBridge",
    level="INFO",
)

assert row.body == line
assert row.sourceurl == "s3://logs/capture.log"
assert row.sourcerownum == 17
```

The persisted contract is deliberately small:

| column | meaning |
| --- | --- |
| `sourceurl` | absolute URI of the source text object |
| `sourcerownum` | 1-based physical row number in that object |
| `timestamp` | timestamp spelling captured from the log header |
| `threadname` | thread spelling captured from the log header |
| `plugin` | plugin spelling captured from the log header |
| `level` | severity spelling captured from the log header |
| `body` | exact bytes after the matched header prefix |

`sourceurl` and `sourcerownum` are the raw table identity. Header captures are
nullable because a text row may not match the configured header. `plugin` and
`timestamp` remain source spellings rather than enum or event values.

There is no `protocol`, `direction`, `msgtype`, `eventtype`, `entries`, FIX
session header, event envelope, or payload-derived hash in a `Message`.
[`FixMsg`](fixmsg.md) owns UTF-8 repair, protocol classification, tokenization,
session-field lifting, typed values, diagnostics, clocks, and event identity.

## Text source

`parse_messages` obtains these columns from yggdryl text media. Its native
`url`, `rownum`, and `body` columns become `sourceurl`, `sourcerownum`, and
`body`; named `rowheader` captures supply the remaining columns. A caller may
provide any Arrow filesystem through `IOBase.from_fs`.

Next: [FixMsg](fixmsg.md) interprets the raw body under the FIX registry.
