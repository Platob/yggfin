# logs.messages

`logs.messages` is the replay boundary. One row is one physical line from one
leaf object, with the matched ULBridge header typed and the line past it kept
as `body`, as the read decoded it. Its sole key is `curruuid`, the line
identity the native read states; `currhashcode` remains the line's content
code for audit and comparison, not a second identity declaration.

## Complete schema

| # | column | Arrow type | null | contract |
| -: | --- | --- | :---: | --- |
| 1 | `currunix` | `timestamp[us, UTC]` | no | the instant the read settles over the line; Iceberg hour partition |
| 2 | `curruuid` | `fixed_size_binary[16]` | no | the only primary key; the line's own identity as the read states it; a parsed message names it as its `srcuuids` |
| 3 | `currhashcode` | `int64` | no | the line's own content code, as the read states it; a table stores the same eight bytes signed |
| 4 | `crosscode` | `string` | yes | the object the line was read from: the canonical text of the identifier the read was addressed under -- the URL where that is a location, the name itself under a `urn:` or an `arn:`, null for a buffer nothing addressed |
| 5 | `seqnum` | `int64` | yes | the line's row number in that object, counted from 1; null where zero |
| 6 | `body` | `string` | no | the line past its row header, what the bridge printed after the bracket; empty where the header consumed the line, the whole text where it did not match |
| 7 | `msgthreadid` | `int64` | yes | bridge thread identifier |
| 8 | `msgsessionid` | `string` | yes | bridge session instance; native FIX input under the same meaning |
| 9 | `msgctxid` | `string` | yes | message-context identifier; native FIX input under the same meaning |
| 10 | `msgseqnum` | `int64` | yes | context sequence; fills a message that stated no `MsgSeqNum` |
| 11 | `msgpluginid` | `string` | yes | plugin that wrote the line; native FIX input under the same meaning |
| 12 | `loglevel` | `string` | yes | header severity spelling |

The reviewed table contract is
[`schemas/rekep/message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json):
the Iceberg schema, partition spec and sort order this table is created with.
Nothing above is derived from anything else. The layout is the hour of
`currunix` alone, exactly as both FIX tables are laid out by the hour of
theirs, and the row already carries that instant, so no column beside it holds
a second copy of the fact. `curruuid` is Iceberg field 2 and required, because
the read stamps it on every row it produces; `crosscode` and `seqnum` are
optional, because a buffer names no object and a row number of zero is no
place. A `logs.messages` written under yggdryl 0.1.9 or earlier is not evolved
into this shape: two of its columns were replaced, every field id was
renumbered, and every `curruuid` and `currhashcode` differ under 0.1.10, so
it is dropped and replayed from the capture.

`msgthreadid`, `loglevel` and `body` are columns of this product alone.
`msgsessionid`, `msgctxid`, `msgseqnum` and `msgpluginid` are FixMsg columns
in their own right, named for the fields a parse fills from them -- 65032,
65008, 34 and 65009 -- so a stored row goes on through the codec without one
spelling being translated into another. `currunix`, `curruuid`,
`currhashcode`, `crosscode` and `seqnum` stand on both shapes and mean the row
they sit on: here the line, the object it was read from and its row number
there; on a FIX row the settled event, the chain it belongs to and its step
in it. The emitted `srcuuids` value is the join back to this row's `curruuid`
for every fact of the line.

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
| `body` | `Receiving : 8=FIX.4.4\|35=8\|...` as text, the line past the bracket |
| `crosscode` | the URL of the object the line was read from |
| `seqnum` | the line's row number in that object |
| `curruuid` | the sixteen bytes the read stamps the line with |

`mtime` is the one capture that fills no column of its own: it is the record
clock the read settles `currunix` from, at nanoseconds UTC whatever width the
fraction is written at, and the row keeps the settled instant rather than a
second reading of it. `crosscode` and `seqnum` are the read's own rather than
the header's: the identifier the read was addressed under and the line's
number in it. `currhashcode` digests that object, the captures above except
the clock, the row number and then the body, and `curruuid` is derived from
`currunix` and that code. A line whose header does not match still lands:
every capture null, its whole text as `body`, and `currunix` the modification
time of the object it was read from -- a file's mtime, an S3 object's
`LastModified` -- under the identity the read derives from that instant. A
copy of a capture written at another time therefore states another identity
for every such line, so a capture is replayed from where it was read, never
from a copy. The epoch dates a line only where the handle has no clock at
all, an in-memory buffer.

## Read with the same parser as the task

```python
from rekep import IOBase, Message

source = IOBase.from_uri("file:python/tests/data/ulbridge.log")
reader = source.read_arrow_reader(options=Message.text_options())
first = next(iter(reader)).slice(0, 1)

assert first.schema.equals(Message.read_field().into_arrow_schema(), check_metadata=True)
assert first.column("seqnum")[0].as_py() == 1

reader.close()
source.close()
```

The read answers `Message.read_field()`: the contract above at the read's own
types, projected off the native row -- `currunix` at nanoseconds UTC,
`curruuid` as a `uuid`, `currhashcode` and `seqnum` unsigned, `crosscode` a
string. The storage boundary, `rekep.fields.stored_arrow_reader`, views the
two unsigned columns into `int64` and casts the rest -- nanoseconds to
microseconds, the uuid to its sixteen bytes -- so the same bytes land in the
table.

## Operational behavior

- Directories and object-store prefixes are traversed recursively in natural
  path order; one leaf is open at a time.
- gzip and zstd are decompressed while streaming. Concatenated gzip members
  remain subject to the decoder support documented on the task page.
- `currhashcode` arrives with the read, which states it over every line:
  nothing here computes it, during field application or in a Python row loop.
- The run's window is pushed into the read as
  `where_within("currunix", window)`: the decode cuts every line and the
  record surface answers the clause over the rows they become, so the read
  answers only the window's lines and nothing is filtered after it: the two
  bounds, and nothing else. A line the header could not date carries its
  object's modification time and belongs to the window that instant falls in;
  the epoch dates only a line read from a handle with no clock, and that line
  is in the window that covers 1970.
- The writer replaces on `curruuid` within the line's hour partition. A new
  key takes the append commit path; a replay rewrites only files that contain
  a matching key.
- `currhashcode` digests the object a line was read from and its row number
  beside its header and body, and `curruuid` is derived from that code and
  the line's instant, so the bundled capture's three exact repeated lines
  answer three codes and three identities, and its 144 physical lines land as
  144 rows under 144 distinct codes. The same bytes read from two URIs answer
  two codes and two identities. The code stays out of the key.
- This `currhashcode` is the code of the *line* and not the message's own,
  which covers the settled event and is a column of both FIX tables. Two
  different lines can state one message, so the two codes answer different
  questions.
- Remote objects remain remote; local staging is not part of the production
  path.
