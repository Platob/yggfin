# Message

One `Message` is one physical text record. It keeps the source position,
captured log header, and exact binary body without interpreting the body.

```python
from rekep import Message

row = Message.from_text(
    b"8=FIX.4.4|35=D|11=C1|10=000",
    sourceurl="s3://logs/capture.log.gz",
    sourcerownum=17,
    timestamp="2026-01-01 10:00:00.000_000",
    threadname="reader",
    plugin="venue",
    level="INFO",
)

assert row.body == b"8=FIX.4.4|35=D|11=C1|10=000"
assert (row.sourceurl, row.sourcerownum) == (
    "s3://logs/capture.log.gz",
    17,
)
```

| column | contract |
| --- | --- |
| `sourceurl` | URI reported by Yggdryl for the source object |
| `sourcerownum` | 1-based physical row number in that object |
| `timestamp` | timestamp text captured from the header |
| `threadname` | thread text captured from the header |
| `plugin` | plugin text captured from the header |
| `level` | severity text captured from the header |
| `body` | bytes after the matched header prefix |

`sourceurl` and `sourcerownum` form the Iceberg merge key. Header captures are
nullable because a line may not match the configured expression.

## Text source

```json
{
  "parameters": {
    "filesystem": "file:data/capture"
  }
}
```

For AWS S3, use
`s3://example-bucket/capture?region=eu-west-1` as `filesystem`.

The task passes the URI to `IOBase.from_uri`. `TextOptions` enables row numbers,
applies `rekep.times.MESSAGE_HEADER`, disables type guessing, and leaves
Yggdryl to traverse and decompress the selected text resources.
