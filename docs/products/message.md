# Message

One `Message` is one physical text record. It keeps the source position,
captured log header, and exact binary body without interpreting the body.

```python
import datetime

from rekep import Message

row = Message.from_text(
    b"8=FIX.4.4|35=D|11=C1|10=000",
    url="s3://logs/capture.log.gz",
    rownum=17,
    timestamp="2026-01-01 10:00:00.123_456",
    threadname="reader",
    branch="venue",
    level="INFO",
)

assert row.body == b"8=FIX.4.4|35=D|11=C1|10=000"
assert (row.url, row.rownum) == (
    "s3://logs/capture.log.gz",
    17,
)
assert row.timestamp == datetime.datetime(
    2026, 1, 1, 10, 0, 0, 123456, tzinfo=datetime.UTC
)
```

| column | contract |
| --- | --- |
| `url` | URI reported by Yggdryl for the source object |
| `rownum` | 1-based physical row number in that object |
| `timestamp` | nullable Arrow `timestamp[us, UTC]`; an offset-free header means UTC |
| `timepartition` | `timestamp[us, UTC]` copied from `timestamp`; Iceberg partitions it with the `hour` transform |
| `threadname` | thread text captured from the header |
| `branch` | branch text captured from the header |
| `level` | severity text captured from the header |
| `body` | bytes after the matched header prefix |

`url` and `rownum` form the Iceberg merge key. `timepartition` is nullable
because its source is nullable. Yggdryl derives it during Arrow application;
PyIceberg applies the UTC hourly partition transform. Other header captures are
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
applies `rekep.times.MESSAGE_HEADER`, disables general type guessing, and leaves
Yggdryl to traverse and decompress the selected text resources. The Message
boundary alone normalizes a matched timestamp to UTC microseconds.

The downstream [`parse_fix`](../pipeline/tasks/parse-fix.md) task interprets
`body` through Yggdryl while preserving this row's source identity and header
columns.
