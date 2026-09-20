# logs.messages

`logs.messages` is the replay boundary. One row is one physical line from one
leaf object, with the matched ULBridge header typed and the whole line kept
byte for byte -- and it is keyed on `bodyhash`, the digest of those bytes, so
identical lines are one row whatever session carried them and however often
the capture is re-read.

## Complete schema

| # | column | Arrow type | null | contract |
| -: | --- | --- | :---: | --- |
| 1 | `sourceurl` | `string` | no | canonical source URI, filling `sourceurl` (65026) downstream |
| 2 | `rownum` | `int64` | no | 1-based physical line number |
| 3 | `timestamp` | `timestamp[us, UTC]` | yes | UTC header timestamp |
| 4 | `timepartition` | `timestamp[us, UTC]` | yes | derived from `timestamp`; Iceberg hour partition |
| 5 | `threadId` | `int64` | yes | bridge thread identifier |
| 6 | `msgsessionid` | `string` | yes | bridge session instance, filling `msgsessionid` (65032) downstream |
| 7 | `msgctxid` | `string` | yes | message-context identifier, filling `msgctxid` (65008) downstream |
| 8 | `msgseqnum` | `int64` | yes | context sequence number, filling `MsgSeqNum` (34) where a frame stated none |
| 9 | `pluginid` | `string` | yes | plugin that wrote the line; rides in front of a FIX row under this name, the row's own column is `msgpluginid` |
| 10 | `level` | `string` | yes | header severity spelling |
| 11 | `bodyhash` | `fixed_size_binary[16]` | no | XXH3-128 of the exact `body` bytes; the primary key |
| 12 | `body` | `binary` | no | the whole line as retained, row header included |
| 13 | `curruuid` | `fixed_size_binary[16]` | no | the line's own identity as the read states it; a message parsed out of the line names it as its `srcuuids` |

The reviewed table contract is
[`schemas/rekep/message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json):
the Iceberg schema, partition spec and sort order this table is created with.
The digest and derived-partition rules above are declared in
`Message.into_field()`, which an Iceberg schema has no place for. `curruuid`
is Iceberg field 13 and declared last, because a table that already exists
takes a new column at its end.

Every header capture is named for the FIX column it fills when the stored row
goes on through the codec, so `parse_fix_bronze` needs no renaming pass of its
own: `msgsessionid`, `msgctxid` and `msgseqnum` fold onto the row's columns of
those names, and `sourceurl`, which traversal fills rather than the header,
onto the dictionary's own. `pluginid` does not fold: the row's own column for
the plugin is `msgpluginid`, and this capture rides in front of the row under
the spelling the bridge's header brackets it with.
`msgsessionid` is the session *instance* the bridge handled the line on, and
not what the message itself says about the counterparty session it
names for itself.

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
| `msgsessionid` | `e7254b12` |
| `msgctxid` | `9f03166699` |
| `msgseqnum` | `40218` |
| `pluginid` | `OMS_X1_TradeCapture` |
| `level` | `INFO` |
| `body` | `2026-08-14 14:46:39.769 [15255-e7254b12:9f03166699:40218] [OMS_X1_TradeCapture] (INFO) Receiving : 8=FIX.4.4\|35=8\|...` as bytes, the whole line |
| `curruuid` | the sixteen bytes the read stamps the line with |

`sourceurl` and `rownum` come from traversal rather than the header, and
`curruuid` from the read itself. A line whose header does not match still has
those three columns and its complete bytes; header-derived columns are null.

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
- Only the lines whose `timepartition` falls in the run's window are written;
  a line with no clock is in every window.
- The writer replaces on `bodyhash`, the digest of the whole line, within the
  line's hour partition, so a line printed twice inside one hour is one row and
  a replay of a window lands the same lines once. Over the bundled capture, 144
  lines are 141 rows: 3 repeat another line byte for byte.
- `bodyhash` is the digest of the *line* and not the message's own
  `currhashcode`, which covers the settled event and is a column of both FIX
  tables. Two different lines can state one message, so the two answer
  different questions.
- Remote objects remain remote; local staging is not part of the production
  path.
