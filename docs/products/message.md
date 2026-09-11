# logs.messages

`logs.messages` is the replay boundary. One row is one physical line from one
leaf object, with the matched ULBridge header typed and the remaining body
kept byte-for-byte.

## Complete schema

| # | column | Arrow type | null | contract |
| -: | --- | --- | :---: | --- |
| 1 | `url` | `string` | no | canonical source URI; primary-key member |
| 2 | `rownum` | `int64` | no | 1-based physical line number; primary-key member |
| 3 | `timestamp` | `timestamp[us, UTC]` | yes | UTC header timestamp |
| 4 | `timepartition` | `timestamp[us, UTC]` | yes | derived from `timestamp`; Iceberg hour partition |
| 5 | `threadId` | `int64` | yes | bridge thread identifier |
| 6 | `sessionUid` | `string` | yes | bridge session identifier |
| 7 | `msgCtxId` | `string` | yes | message-context identifier |
| 8 | `seqNum` | `int64` | yes | context sequence number |
| 9 | `pluginid` | `string` | yes | plugin that logged the line; names the dialect where it names a branch |
| 10 | `level` | `string` | yes | header severity spelling |
| 11 | `bodyhash` | `fixed_size_binary[16]` | yes | XXH3-128 of exact `body` bytes |
| 12 | `body` | `binary` | no | every byte after the matched header |

The reviewed table contract is
[`schemas/rekep/message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json):
the Iceberg schema, partition spec and sort order this table is created with.
The digest and derived-partition rules above are declared in `Message.into_field()`,
which an Iceberg schema has no place for.

## Header transcription

```text
2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218]
[OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4|35=8|...
```

becomes:

| column | value |
| --- | --- |
| `timestamp` | `2026-08-14T14:46:39.769000Z` |
| `threadId` | `15255` |
| `sessionUid` | `e7254b12` |
| `msgCtxId` | `9f03166699` |
| `seqNum` | `40218` |
| `pluginid` | `OMS_X1_TradeCapture` |
| `level` | `INFO` |
| `body` | `Receiving : 8=FIX.4.4\|35=8\|...` as bytes |

`url` and `rownum` come from traversal rather than the header. A line whose
header does not match still has those two columns and its complete body;
header-derived columns are null.

## Read with the same parser as the task

```python
from rekep import IOBase, Message

source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
reader = source.read_arrow_reader(options=Message.text_options())
first = next(iter(reader)).slice(0, 1)

assert first.schema.equals(Message.into_field().into_arrow_schema(), check_metadata=True)
assert first.column("rownum")[0].as_py() == 1

reader.close()
source.close()
```

## Operational behavior

- Directories and object-store prefixes are traversed recursively in natural
  path order; one leaf is open at a time.
- gzip and zstd are decompressed while streaming. Concatenated gzip members
  remain subject to the decoder support documented on the task page.
- `bodyhash` is computed during field application, not in a Python row loop.
- The writer merges on `(url, rownum)`, so a replay writes nothing new.
- Remote objects remain remote; local staging is not part of the production
  path.
