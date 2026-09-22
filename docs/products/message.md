# logs.messages

`logs.messages` is the replay boundary. One row is one physical line from one
leaf object, with the matched ULBridge header typed and the whole line kept as
the read decoded it. Its sole key is `curruuid`, the source-line identity the
native read states; `currhashcode` remains the exact-content code for audit and
comparison, not a second identity declaration.

## Complete schema

| # | column | Arrow type | null | contract |
| -: | --- | --- | :---: | --- |
| 1 | `currunix` | `timestamp[us, UTC]` | no | the instant the read settles over the line; Iceberg hour partition |
| 2 | `curruuid` | `fixed_size_binary[16]` | no | the only primary key; the line's own identity as the read states it; a parsed message names it as its `srcuuids` |
| 3 | `currhashcode` | `int64` | no | the line's own content code, as the read states it; a table stores the same eight bytes signed |
| 4 | `sourceurl` | `string` | no | canonical source URI of this raw line |
| 5 | `rownum` | `int64` | no | 1-based physical line number |
| 6 | `body` | `string` | no | the whole line as retained, row header included |
| 7 | `msgthreadid` | `int64` | yes | bridge thread identifier |
| 8 | `msgsessionid` | `string` | yes | bridge session instance; native FIX input under the same meaning |
| 9 | `msgctxid` | `string` | yes | message-context identifier; native FIX input under the same meaning |
| 10 | `msgseqnum` | `int64` | yes | context sequence; fills a message that stated no `MsgSeqNum` |
| 11 | `msgpluginid` | `string` | yes | plugin that wrote the raw line; native FIX input under the same meaning |
| 12 | `loglevel` | `string` | yes | header severity spelling |

The reviewed table contract is
[`schemas/rekep/message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json):
the Iceberg schema, partition spec and sort order this table is created with.
Nothing above is derived from anything else. The layout is the hour of
`currunix` alone, exactly as both FIX tables are laid out by the hour of
theirs, and the row already carries that instant, so no column beside it holds
a second copy of the fact. `curruuid` is Iceberg field 2 and required, because
the read stamps it on every row it produces. A `logs.messages` written under
the older shape is not evolved into this one -- whether its columns predate
the native event layout or only its identifier is still `currhashcode` -- it
is recreated and reingested from the capture.

`sourceurl`, `rownum`, `msgthreadid`, `loglevel` and `body` are raw to this
product alone. `msgsessionid`, `msgctxid`, `msgseqnum` and `msgpluginid` are
FixMsg columns in their own right, named for the fields a parse fills from
them -- 65032, 65008, 34 and 65009 -- so a stored row goes on through the
codec without one spelling being translated into another. `currunix`,
`curruuid` and `currhashcode` stand on both shapes and mean the row they sit
on: here the line, there the settled event. The emitted `srcuuids` value is
the join back to this row's `curruuid` for every raw fact.

## Header transcription

```text
2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218]
[OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4|35=8|...
```

becomes:

| column | value |
| --- | --- |
| `currunix` | `2026-08-14T14:46:39.769000Z`, settled from the header's `mtime` capture |
| `msgthreadid` | `15255` |
| `msgsessionid` | `e7254b12` |
| `msgctxid` | `9f03166699` |
| `msgseqnum` | `40218` |
| `msgpluginid` | `OMS_X1_TradeCapture` |
| `loglevel` | `INFO` |
| `body` | `2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218] [OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4\|35=8\|...` as text, the whole line |
| `curruuid` | the sixteen bytes the read stamps the line with |

`mtime` is the one capture that fills no column of its own: it is the record
clock the read settles `currunix` from, and the row keeps the settled instant
rather than a second reading of it. `sourceurl` and `rownum` come from
traversal rather than the header, and `currhashcode` from the read beside the
identity above. A line whose header does not match still lands: it settles at
the epoch pin, under the identity and the code the read stated over it, with
its whole text, and every capture null.

## Read with the same parser as the task

```python
from rekep import IOBase, Message

source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
reader = source.read_arrow_reader(options=Message.text_options())
first = next(iter(reader)).slice(0, 1)

assert first.schema.equals(Message.read_field().into_arrow_schema(), check_metadata=True)
assert first.column("rownum")[0].as_py() == 1

reader.close()
source.close()
```

The read answers `Message.read_field()`, which is the contract above with
`currhashcode` widened to the `uint64` a content code is stated as; the same
eight bytes land in the stored `int64`.

## Operational behavior

- Directories and object-store prefixes are traversed recursively in natural
  path order; one leaf is open at a time.
- gzip and zstd are decompressed while streaming. Concatenated gzip members
  remain subject to the decoder support documented on the task page.
- `currhashcode` arrives with the read, which states it over every line:
  nothing here computes it, during field application or in a Python row loop.
- Only the lines whose `currunix` falls in the run's window are written; a line
  the header could not date sits at the epoch pin, which is in every window, so
  no window loses it.
- The writer replaces on `curruuid` within the line's hour partition. A new
  key takes the append commit path; a replay rewrites only files that contain
  a matching key.
- `curruuid` carries the line's place in the read and its source, not only its
  bytes, so the bundled capture's three exact repeated lines answer distinct
  identities and its 144 physical lines land as 144 rows. The same bytes read
  from two URIs answer two identities under one `currhashcode`, which stays out
  of the key.
- This `currhashcode` is the code of the *line* and not the message's own,
  which covers the settled event and is a column of both FIX tables. Two
  different lines can state one message, so the two codes answer different
  questions.
- Remote objects remain remote; local staging is not part of the production
  path.
