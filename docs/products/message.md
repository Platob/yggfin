# logs.messages

One row is one physical text line: source identity, the ULBridge header, an
exact-body digest, and the uninterpreted bytes.

## Schema

12 columns, keyed by `(url, rownum)` and partitioned by the UTC hour of
`timepartition`.

| column | Arrow type | null | contract |
| --- | --- | --- | --- |
| `url` | `string` | no | Yggdryl URL of the source object; primary key |
| `rownum` | `int64` | no | 1-based physical line; primary key |
| `timestamp` | `timestamp[us, UTC]` | yes | clock captured from the row header |
| `timepartition` | `timestamp[us, UTC]` | yes | derived from `timestamp`; Iceberg hour transform |
| `threadId` | `int64` | yes | bridge thread |
| `sessionUid` | `string` | yes | bridge session instance |
| `msgCtxId` | `string` | yes | bridge message context |
| `seqNum` | `int64` | yes | bridge context sequence |
| `plugin` | `string` | yes | plugin that wrote the line |
| `level` | `string` | yes | log severity |
| `bodyhash` | `fixed_size_binary[16]` | yes | XXH3-128 over exact `body` bytes |
| `body` | `binary` | no | bytes after the matched prefix, or the full unmatched line |

```python
from rekep import Message

field = Message.field()
assert [member.name for member in field][-2:] == ["bodyhash", "body"]
assert field["bodyhash"].digest.sources == ["body"]
assert field["timepartition"].partition.sources == ["timestamp"]
assert field["timepartition"].iceberg["partition_key"] == "hour"
```

`bodyhash` is not the parsed FIX `msghash`. Different names preserve both
identities when the raw row passes through the codec.

## Native read

`Message.text_options()` binds the field contract to Yggdryl's text reader.
The task receives final `Message` batches directly; it does not classify the
body or loop over rows in Python.

```python
from rekep import Message
from yggdryl import IOBase

source = IOBase.from_uri("file:data/ulbridge.log")
reader = source.read_arrow_reader(options=Message.text_options())
assert reader.schema == Message.field().into_arrow_schema()
```

The header expression accepts this shape:

```text
2026-08-14 14:46:39.769 [250-e7256476:9effef3e6a:72504] [ULBridge] (INFO) body
```

The `-sessionUid:msgCtxId:seqNum` group is optional. An unmatched line is not
dropped: all header captures stay null and the full line is `body`.

## Scalar construction

`Message.from_text` is useful for tests and one-off rows; production ingestion
uses the native reader.

```python
import datetime

from rekep import Message

row = Message.from_text(
    b"8=FIX.4.4|35=D|10=000|",
    url="file:///capture.log",
    rownum=17,
    timestamp="2026-08-14 14:46:39.769",
    threadId="250",
    seqNum="72504",
)

assert row.timestamp == datetime.datetime(
    2026, 8, 14, 14, 46, 39, 769000, tzinfo=datetime.UTC
)
assert row.threadId == 250
assert row.body == b"8=FIX.4.4|35=D|10=000|"
```

## Source binding

`filesystem` reaches `IOBase.from_uri` unchanged. It can name a local file or
directory, an injected Arrow filesystem, or an object-store prefix. Recursive
discovery, decompression, header capture, and physical-line batching all happen
in Yggdryl. Production never stages a remote object locally.
