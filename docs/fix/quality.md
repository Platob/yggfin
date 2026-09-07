# Quality

What the two stages assert about a record, and where each assertion is stored
so a reader can check it after the fact rather than trusting the pipeline.

## Classification

`classify_arrow_array(column, direction)` answers three questions about a whole
payload column natively and returns three `utf8` arrays the length of the
input: what the record is, what message type it declares, and which way it
moved. It resolves nothing against a dictionary and builds no message, which is
what makes it cheap enough to run over every line of a capture -- it is the same
shallow scan the byte readers run, three times over the column rather than once,
so it is a scan-cost pass and not a parse.

[`parse_messages`](../pipeline/tasks/parse-messages.md) runs it once per batch
and stores the answers as `mimetype`, `msgtype` and `msgdirection`. A record
that names nothing is `application/octet-stream` and `unknown` rather than null.

The media type is decided by what the record carries, not by what the log says
it is:

| media type | what proves it |
| --- | --- |
| `text/fix` | numeric `tag=value` entries |
| `text/ullink` | `#`-marked keys, or a raw `MSGTYPE=` key, with no numeric tag |
| `text/fixul` | both, or a pair-shaped `XmlData(213)` inside a numeric frame |
| `text/fixml` | an `XmlData(213)` payload opening with `<` |
| `application/xml`, `application/json` | no frame, and the whole line opens as a document |
| `text/key-value` | no frame, but at least one `key=value` pair somewhere in the line |
| `application/octet-stream` | anything else |

A frame beats a document, because an `XmlData` payload is part of a frame
rather than a document of its own; a document beats the pair rules, because an
attribute inside a tag is not a field. Unrelated log attributes therefore do
not turn a random line into Ullink.

A raw `MSGTYPE=` found anywhere in the line beats numeric tag 35, because a
bridge writes its own type in front of a frame it is relaying. FIX's
user-defined range routes through one name: `35=UL` and `35=U7` both answer
`UDF`. The value is not validated to a message type's width -- a line may carry
anything, and a classifier must not refuse what it was asked to read.

```python
import pyarrow

from yggdryl.fix import classify_arrow_array

lines = [
    b"sending >> 8=FIX.4.4|35=D|55=TTF|10=203|",
    b"#MSGTYPE=D|#SYMBOL=TTF|#SIDE=1",
    b"8=FIX.4.4|35=D|#SYMBOL=TTF|10=0|",
    b"8=FIX.4.4|35=n|213=<FIXML><Order/></FIXML>|10=0|",
    b"level=INFO worker=3 took=12ms",
    b"no level printed by this driver",
    b"MSGTYPE=8|8=FIX.4.4|35=D|10=0|",
    b"8=FIX.4.2|35=UL|10=044|",
]
mimetype, msgtype, msgdirection = classify_arrow_array(pyarrow.array(lines), "sent")

assert mimetype.to_pylist() == [
    "text/fix",
    "text/ullink",
    "text/fixul",
    "text/fixml",
    "text/key-value",
    "application/octet-stream",
    "text/fixul",
    "text/fix",
]
# The bridge's own MSGTYPE outranks tag 35, and `U*` routes through one name.
assert msgtype.to_pylist()[-2:] == ["8", "UDF"]
assert msgtype.to_pylist()[4:6] == [None, None]
assert set(msgdirection.to_pylist()) == {"SENT"}
```

Try any single line against the same scan in the [decoder](decode.md#try-it).

## Direction

Two readings, spelled `SENT` and `RECV`, and the absence of one. They are what
`msgdirection` holds on a raw record -- where an absent reading is filled with
`unknown`, because the column is non-null -- and what column `385` holds on a
parsed one, where it stays null.

The verb is read **in front of the payload**, never inside it. Where the
message starts is where the transport's own prose stops, so a `sent` inside a
`Text(58)`, a bridge value spelled `OUT=1`, and an XML payload's own wording
never become a direction.

| direction | verbs |
| --- | --- |
| `SENT` | `sending`, `sent`, `send`, `outbound`, `outgoing`, `out` |
| `RECV` | `receiving`, `received`, `receive`, `recv`, `inbound`, `incoming`, `in` |

Matched case-insensitively, longest first within a direction so `received` is
not read as `receive`. A verb must open at the start of the prefix or after
whitespace, `[`, `(` or `<`, and close at the end of it or before whitespace,
`]`, `)`, `>`, `:` or `,`. The bare `in` and `out` are *chosen* only where the prefix
starts with one or a bracket opens it, and `]`, `)` or `:` closes it, because
that is the one shape a marker has and none of the shapes the same letters have
otherwise:
`direct:out` is a route endpoint and `MCFID-IN-XPAR` is a session name. They
still count against an opposite verb, which is what makes `sending in session
3` answer nothing rather than `SENT`.

A prefix carrying both verbs and one carrying neither both answer nothing:
there is no verb the reading can prefer, and inventing one would be a guess.
Arrows and bridge nouns are not verbs either -- `>>`, `<<` and `toBridge` mark
nothing, so the fixture's bridge lines take the default.

What fills that silence is the `direction` parameter, and its default is
`sent`: a session's own log is written by the side doing the sending, so its
unmarked lines are the ones it sent and its inbound lines are the ones it
bothered to mark. A capture taken from the other side sets `recv`, and one
whose silence really means nothing sets `unknown`, which leaves the reading
null. Any verb a line does carry beats the default.

```python
import pyarrow

from yggdryl.fix import classify_arrow_array

lines = [
    b"sending >> 8=FIX.4.4|35=D|10=0|",
    b"recv 8=FIX4^A35=0^A10=017^A on session 3",
    b"[OUT] 8=FIX.4.4|35=D|10=0|",
    b"8=FIX.4.4|35=8|58=sent earlier|10=1|",
    b"sending in session 3 8=FIX.4.4|35=D|10=0|",
    b"toBridge #SYMBOL=TTF|#SIDE=1",
]


def read(default):
    """Just the direction each line answers, for one default."""
    return classify_arrow_array(pyarrow.array(lines), default)[2].to_pylist()


# A verb is a verb whatever the default is.
assert read("sent")[:3] == ["SENT", "RECV", "SENT"]
assert read("recv")[:3] == ["SENT", "RECV", "SENT"]

# The last three carry none, so they take the default -- or nothing.
assert read("sent")[3:] == ["SENT", "SENT", "SENT"]
assert read("recv")[3:] == ["RECV", "RECV", "RECV"]
assert read("unknown")[3:] == [None, None, None]
```

## What `parse_fix` reads, and what it leaves

[`parse_fix`](../pipeline/tasks/parse-fix.md) reads `logs.messages` under
`msgtype != 'unknown'`. A record that named no `MsgType` carries no FIX frame,
so it stays in `logs.messages` rather than becoming a row of nulls in
`fix.messages`. The classification is **read**, never recomputed: the raw layer
decided it once, and the filter is an Iceberg predicate pushed into scan
planning.

Direction is carried the same way. The task renames `msgdirection` to
`direction`, which is the per-row parameter the reader already takes, so column
`385` is the reading the raw layer made rather than a second, independent one.
A `385` the frame itself carried is kept in `entries` and does not overwrite
the column -- the wire states what one side called it, and the column states
which way the capture saw it move.

## Two digests

A row carries two, and they answer different questions.

`msghash` is on the raw `Message` contract: a `fixed_size_binary[16]` XXH3-128
digest of the exact `body` bytes, declared as a holder and filled
during Arrow application.

```python
from rekep import Message

field = Message.field()["msghash"]
assert field.digest.is_holder()
assert field.digest.sources == ["body"]
assert field.digest.algorithm == "xxh3-128"
```

The holder is the only field that states anything: `body` stays an ordinary
column, and a schema declares one holder rather than marking every field that
contributes to it. `parse_fix` carries the column through unchanged.

Column `30001` is the other one, derived over the message rather
than over the bytes -- it is `FixMsg.digest()`, one of the
[seven fields this crate adds](registry.md#the-fields-this-crate-adds). It is
XXH3-128 over the entries in arrival order, each value fed as its length and
then its bytes, with the session envelope left out. Twenty-four tags, in three
groups: how the message was written down (`BeginString`, `BodyLength`,
`CheckSum`, `SignatureLength`, `Signature`, `XmlDataLen`, `XmlData`); who wrote
it and who relayed it (`SenderCompID`, `TargetCompID`, `OnBehalfOfCompID`,
`DeliverToCompID` and each one's sub- and location- variant); and which delivery
this was (`MsgSeqNum`, `SendingTime`, `OrigSendingTime`, `PossDupFlag`,
`PossResend`). None of them is the message. `MsgType` stays in,
because a message type is what a message is rather than how it travelled. The
crate's own derived fields are excluded too, for the plainer reason that a
value cannot cover itself.

So the same order relayed through two sessions is two `msghash` values and one
`30001`:

```python
import pyarrow

from rekep import Message
from yggdryl import IOBase
from yggdryl.fix import FixRegistry, parse_arrow_reader

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.with_crate_fields()

schema = pyarrow.schema(
    [
        pyarrow.field("url", pyarrow.string()),
        pyarrow.field("rownum", pyarrow.int64()),
        pyarrow.field("body", pyarrow.binary()),
    ]
)
batch = pyarrow.RecordBatch.from_arrays(
    [
        pyarrow.array(["file:data/capture"] * 2),
        pyarrow.array([1, 2]),
        pyarrow.array(
            [
                b"sending >> 8=FIX.4.4|9=64|35=D|11=ORD-1|55=TTF|10=203|",
                b"recv 8=FIX.4.2|9=99|35=D|34=7|49=XPAR|56=BUYSIDE|11=ORD-1|55=TTF|10=000|",
            ]
        ),
    ],
    schema=schema,
)

raw = Message.apply_arrow_batch(batch)
fixes = parse_arrow_reader(
    pyarrow.RecordBatchReader.from_batches(schema, [batch]), registry, "body"
).read_all()

body = raw.column("msghash").to_pylist()
message = fixes.column("30001").to_pylist()

assert len(body[0]) == len(message[0]) == 16
assert body[0] != body[1]  # two captured lines
assert message[0] == message[1]  # one order
```

`msghash` is the byte identity of what was captured: the same bytes digest the
same however the row reached a table, so it recognizes a truncated upload, a
stale cache or a duplicated row, and it joins a parsed row back to the raw
record it came from. `30001` is the value identity of what the message said:
it is what `dedup` compares, what joins a relayed message to its original, and
what recognizes a redelivery. A consumer that needs to tell two deliveries
apart reads the sequence number and the time, which are columns of their own
beside it.

Neither is cryptographic. xxHash detects accidental change and never withstands
an adversary who is allowed to choose the input.

## Anomalies

`FixMsg.anomalies()` reports what a message says about itself that does not add
up. All real, none fatal: a reader that refused any of them would drop a row a
monitor exists to see, and one that repaired them would state something the
wire did not.

| anomaly | what it means |
| --- | --- |
| would not type | a value arrived and the declared type could not read it, so the row holds null and the text is still in `entries` |
| states *n* occurrences and holds *m* | a repeating group's counter disagrees with the group it introduces |
| decoded lossily and is not text | an entry's text carries the replacement character, so it is a lossy decode of the bytes that arrived |

They are derived on demand by comparing the row against the entries, not stored:
there is no error channel on a message, nothing to keep in step with an edit,
and a caller who never asks pays nothing. At most one per entry, in that
order -- a lossy decode says the text is not the authority, which makes any
further reading of it meaningless; then the miscount, which is about the group
rather than this value; then the value's own typing.

```python
from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
reader = FixReader(registry)

stated = reader.text("MSGTYPE=D|#NOPARTYIDS=2|#NOPARTYIDS[0]=PARTYID=BROKERX|ORDERQTY=notanumber|")
# A field the dictionary placed is named by its canonical name; one it did
# not is named by the key the wire actually carried.
assert stated.anomalies() == [
    "nopartyids (453) states 2 occurrences and holds 1",
    'ORDERQTY (38) would not type from "notanumber"',
]
assert stated.by_tag(38).is_null()
assert stated.entries()[-1] == (38, None, "ORDERQTY", "notanumber")

lossy = reader.bytes(b"8=FIX.4.4|35=D|58=\xff\xfe|10=0|")
assert lossy.anomalies() == ["58 (58) decoded lossily and is not text"]
assert lossy.entries()[2] == (58, None, "58", "\ufffd\ufffd")
```

A miscount is stated rather than corrected: a filtered occurrence and a
truncated capture both land there, and renumbering either would hide which one
happened. A lossy entry matters on the way back out --
[re-emitting](encode.md) from it would write the replacement character onto the
wire, which is not what arrived.

## Sequential deduplication

`parse_fix` reads with `dedup=True`. A record whose `30001` digest equals the
previous emitted record's is dropped, and the comparison carries across
source-batch boundaries, so a duplicate split by a batch edge is still caught.

It is off by default in the reader, and it must be: with it on, the output row
count no longer equals the input line count, so a batch stops aligning with its
source by position and cannot be joined back to it. That is the one exception to
the row-in/row-out correspondence every other path keeps. What went is visible
rather than counted -- the stage result's `read` is the input count and
`written` the output one, and `msghash` on the rows that survived still names
each body exactly -- so a drop is recoverable from `logs.messages`, which keeps
every line.

```python
import pyarrow

from yggdryl import IOBase
from yggdryl.fix import FixRegistry, parse_arrow_reader

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
schema = pyarrow.schema(
    [pyarrow.field("rownum", pyarrow.int64()), pyarrow.field("body", pyarrow.binary())]
)
rows = [
    b"8=FIX.4.4|35=D|11=A|10=0|",
    b"8=FIX.4.4|35=D|11=A|10=0|",
    b"8=FIX.4.4|35=D|11=B|10=0|",
    b"8=FIX.4.4|35=D|11=A|10=0|",
]
batch = pyarrow.RecordBatch.from_arrays(
    [pyarrow.array([1, 2, 3, 4]), pyarrow.array(rows)], schema=schema
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])

kept = parse_arrow_reader(source, registry, "body", dedup=True).read_all()
# The republished line goes; the one that comes back after B does not, because
# it is no longer adjacent to its twin.
assert kept.column("rownum").to_pylist() == [1, 3, 4]
```

It is deliberately *sequential*, not global. A capture tool publishes the same
line twice adjacently, and that is the duplication worth removing at read time.
Two identical heartbeats an hour apart are two events: non-adjacent identity is
a question about a *window* -- how wide, measured how -- and that policy belongs
to the caller. The implementation follows from the same choice: one `u128` of
state whatever the stream's length, never a set, because a set over a day's
capture grows without bound.

## Coverage: what `unmapped` is for

`unmapped` holds every pair the dictionary did not place. It is the coverage
metric: a venue field nobody has declared yet shows up there with the exact
text the bridge wrote, and a query over it names the gap.

```python
import pyarrow
import pyarrow.compute

from yggdryl import IOBase
from yggdryl.fix import FixRegistry, parse_arrow_reader

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.with_crate_fields()

schema = pyarrow.schema([pyarrow.field("body", pyarrow.binary())])
batch = pyarrow.RecordBatch.from_arrays(
    [
        pyarrow.array(
            [
                b"toBridge #ISINCODE=XX0000084733|#CFICODE=FXXXSX|#SYMBOL=TTF|"
                b"#SIDE=1|#ORDERQTY=1200|#UNKNOWNVENUEFIELD=Z9",
                b"toBridge #ISINCODE=XX0000084733|#SYMBOL=TTF|#SIDE=1|"
                b"#ORDERQTY=1200|#UNKNOWNVENUEFIELD=Z9",
            ]
        )
    ],
    schema=schema,
)
fixes = parse_arrow_reader(
    pyarrow.RecordBatchReader.from_batches(schema, [batch]), registry, "body"
).read_all()


def unmapped_keys(table):
    """Every wire key this dictionary did not place, and how often."""
    flat = pyarrow.compute.list_flatten(table.column("unmapped").combine_chunks())
    return pyarrow.TableGroupBy(pyarrow.table({"key": flat.field("key")}), "key").aggregate(
        [([], "count_all")]
    )


assert unmapped_keys(fixes).to_pylist() == [
    {"key": "ISINCODE", "count_all": 2},
    {"key": "UNKNOWNVENUEFIELD", "count_all": 2},
]
# The two the dictionary does place are columns instead, whether the venue
# spells them as a standard field's name or not.
assert fixes.column("55").to_pylist() == ["TTF", "TTF"]
assert fixes.column("461").to_pylist() == ["FXXXSX", None]
```

An empty `unmapped` across a capture means the dictionary is complete for that
venue. A growing one is the work list -- add the definition to the registry
([Registry](registry.md#adding-a-definition)) and the column appears on the
next run.

## Replay

Both tables merge on `(url, rownum)`. Replaying the same capture reads every
row, writes none, and creates no data file or snapshot:

```text
parse_messages  14 read, 14 written,  0 skipped     first run
parse_fix        4 read,  4 written,  0 skipped

parse_messages  14 read,  0 written, 14 skipped     replay
parse_fix        4 read,  0 written,  4 skipped
```

Ten of the fourteen fixture records name no `MsgType`, which is why `parse_fix`
reads four.
