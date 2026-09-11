# Capture columns

A capture row is a line plus everything the log said about it before the
payload started: where it came from, when it was written, which plugin wrote
it, which session it belonged to. The codec reads those facts as columns, and
this page is the rule that decides what each one does.

The short version: a capture column whose folded name a FIX column already
takes **fills that column**; every other capture column is **carried in front**
of the FIX columns, untouched.

## The row a text read answers

`Message.text_options()` states the ULBridge row header once, and every named
group in it becomes a column:

```python
from rekep import Message

names = [member.name for member in Message.into_field()]

assert names[:4] == ["url", "rownum", "timestamp", "timepartition"]
assert names[4:10] == ["threadId", "sessionUid", "msgCtxId", "seqNum", "pluginid", "level"]
assert names[-2:] == ["bodyhash", "body"]
```

Those names are the contract. `pluginid` is spelled the way the dictionary
spells tag 65009 because that is what makes the value land in the FIX column
rather than beside it; `threadId` and `sessionUid` are spelled the way the
bridge spells them because no FIX field claims either.

## Carried, or folded in

Run the same capture twice — once with the header group named `pluginid`, once
named `plugin` — and the difference is one column:

| capture column | folded name a FIX column takes? | result |
| --- | --- | --- |
| `url`, `rownum`, `threadId`, `sessionUid`, `seqNum`, `level`, `bodyhash`, `body` | no | carried, in capture order, in front of the FIX columns |
| `timestamp` | yes, tag 65003 | fills it; not carried a second time |
| `msgCtxId` | yes, tag 65008 | fills it; not carried a second time |
| `pluginid` | yes, tag 65009 | fills it; not carried a second time |
| `plugin` | no | carried as an opaque column, and `pluginid` stays null |

```python
from rekep.fix import fix_message_field

columns = [member.name for member in fix_message_field()]

assert columns[:6] == ["url", "rownum", "timepartition", "threadId", "sessionUid", "seqNum"]
assert "pluginid" in columns
assert columns.count("pluginid") == 1
assert "timestamp" in columns and columns.count("timestamp") == 1
```

Two columns of one name is not a schema, so the FIX column wins and the capture
value is not lost — it is what fills it. A capture column named after any other
field fills that field too, where the frame did not state it: `seqNum` keeps its
own column *and* fills `msgseqnum` when tag 34 is absent, because `seqNum` is a
declared bridge alias of `MsgSeqNum` rather than its folded name.

A filled column never becomes an entry. The arrival record is what the wire
carried; a capture column is what the log said about it.

## A message always wins

Where the message itself states the field, the message's value stands and the
capture column is ignored for it. That ordering is what makes replay stable: a
row re-read from `logs.messages` produces the same `fix.messages` row whether
or not the capture happened to repeat something the frame already said.

## The dialect a row is read under

`pluginid` does one thing more than fill a column. Where its text is the name
of a branch the registry declares, or one of that branch's aliases, it is also
the dialect the row is read under — outranking the codec's own `branch` pin.
Any other `pluginid` leaves the pin standing.

| pin | `pluginid` capture | branch the row is read under |
| --- | --- | --- |
| `"ulbridge"` | `OMS_X1_TradeCapture` | `ulbridge` — the pin |
| none | `OMS_X1_TradeCapture` | standard — no pin, no branch named |
| none | `ulbridge` | `ulbridge` — the capture named it |
| `"ulbridge"` | `ulbridge` | `ulbridge` |

```python
from rekep import TextLine
from rekep.fix import FixCodec, fix_registry

wire = b"8=FIX.4.4|35=D|55=AAPL|10=0|"
plain = FixCodec(fix_registry(), capture_names=["pluginid"])
pinned = FixCodec(fix_registry(), branch="ulbridge", capture_names=["pluginid"])

unpinned, = plain.parse_text_line(TextLine(1, wire, ["OMS_X1_TradeCapture"]))
by_pin, = pinned.parse_text_line(TextLine(2, wire, ["OMS_X1_TradeCapture"]))
by_capture, = plain.parse_text_line(TextLine(3, wire, ["ulbridge"]))

assert unpinned.branch == ""
assert by_pin.branch == "ulbridge"
assert by_capture.branch == "ulbridge"
```

## Naming the captures of a decoded line

A `TextLine` answers its row-header captures by position, not by name, because
the expression that produced them is the reader's and not the codec's.
`capture_names` is what says which capture is which — once, for the whole run.
The read already knows them, so a codec over rekep's own capture takes them
from the options rather than restating them:

```python
from rekep import Message
from rekep.fix import fix_codec

names = Message.text_options().capture_names
codec = fix_codec()

assert names == (
    "timestamp",
    "threadId",
    "sessionUid",
    "msgCtxId",
    "seqNum",
    "pluginid",
    "level",
)
assert codec.registry is not None
```

Stated by hand, it is the same list in the same order:

```python
from rekep import TextLine
from rekep.fix import FixCodec, fix_registry

line = TextLine(1, b"8=FIX.4.4|35=D|55=AAPL|10=0|", ["OMS_X1_TradeCapture", "FIX.4.4"])
codec = FixCodec(
    fix_registry(),
    branch="ulbridge",
    capture_names=["pluginid", "beginstring"],
)
message, = codec.parse_text_line(line)

assert line.index == 1
assert line.captures == (b"OMS_X1_TradeCapture", b"FIX.4.4")
assert message.by_name("pluginid").as_py() == "OMS_X1_TradeCapture"
assert message.by_name("beginstring").as_py() == "FIX.4.4"

unnamed, = FixCodec(fix_registry(), branch="ulbridge").parse_text_line(line)

assert unnamed.get_by_name("pluginid") is None
```

`parse_text_arrow_reader` needs none of this: a batch already names its columns,
so the schema is the naming. `capture_names` is for the row-at-a-time doors,
`parse_text_line` and `parse_text_lines`.

A `direction` capture is named so that it cannot silently fill a field of that
name. It is read only by `parse_text_arrow_reader`, which has a `msgdirection`
column to put it in.

## The three capture names that matter here

| capture | fills | because |
| --- | --- | --- |
| `timestamp` | tag 65003, the clock a capture is ordered by | a log's own clock is closer to the market than the frame's |
| `msgCtxId` | tag 65008, the bridge message context | only the log states it |
| `pluginid` | tag 65009, and the dialect | only the log states which plugin wrote the line |

`timestamp` is why `fix.messages` has a non-null clock on a row whose payload
was prose: the row's own clock stamps the message, else the first clock the
message carries, else the epoch. `unixpartition` follows it, and
`timepartition` is rekep's own hourly partition over the same instant.

## When the ULBridge header changes

The header expression lives in `rekep.times.MESSAGE_HEADER` and its named
groups are the capture columns. Renaming a group renames a column in
`logs.messages`, which changes both published contracts — so the two move
together:

```python
from rekep.times import MESSAGE_HEADER

assert "(?P<pluginid>" in MESSAGE_HEADER
assert "(?P<msgCtxId>" in MESSAGE_HEADER
assert "(?P<timestamp>" in MESSAGE_HEADER
```

Regenerate `schemas/rekep/message.json` and `schemas/rekep/fix-message.json`
after any such change; `python/tests/test_schemas.py` fails until both agree
with the runtime declarations.
