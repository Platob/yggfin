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
assert row.timestamp == datetime.datetime(2026, 1, 1, 10, 0, 0, 123456, tzinfo=datetime.UTC)
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
| `mimetype` | media type Yggdryl's shallow scan infers for the body |
| `msgtype` | raw `MsgType` the body's frame spells, or `unknown` |
| `msgdirection` | `SENT` or `RECV`, from the verbs beside the frame, or the declared default |
| `msghash` | `fixed_size_binary[16]` XXH3-128 digest of the exact body bytes |
| `body` | bytes after the matched header prefix |

`url` and `rownum` form the Iceberg merge key. `timepartition` is nullable
because its source is nullable. Yggdryl derives it during Arrow application;
PyIceberg applies the UTC hourly partition transform. Other header captures are
nullable because a line may not match the configured expression.

## Classification without interpretation

The three `msg*` columns and `mimetype` name what the body *is* without parsing
it. `yggdryl.fix.classify_arrow_array` answers all three in one native pass over
the payload column -- the same shallow scan and the same direction verbs the FIX
reader itself uses, so the classifying stage and the parsing one cannot
disagree. All four are non-null: a record that names nothing carries
`application/octet-stream` and `unknown`.

`direction` is the task parameter naming what an unmarked line took. A session's
own log is written by the side doing the sending, so `sent` is the default; a
capture taken from the other side sets `recv`, and one whose silence really
means nothing sets `unknown`. Any verb a line does carry beats it.

`msghash` is a digest holder: `body` stays an ordinary column and the
declaration marks one field as holding the digest over it, which Yggdryl fills
during Arrow application. [Quality](../fix/quality.md) covers what each value
means and how `parse_fix` uses them.

```python
from rekep import Message

field = Message.field()
assert field["msghash"].digest.is_holder()
assert field["msghash"].digest.sources == ["body"]
assert field["msghash"].digest.algorithm == "xxh3-128"
assert field["msgtype"].nullable is False
```

This is still not interpretation: the body is stored exactly as captured, and
[`parse_fix`](../pipeline/tasks/parse-fix.md) owns the protocol pass.

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
