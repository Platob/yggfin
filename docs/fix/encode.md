# Encode

This project reads FIX; it does not send it. The encoder is here because a
decoder is only debuggable against a frame you can build: to reproduce a
support case, to check a mapping, or to see what the dictionary does with a
message that does not exist yet.

`FixMsg.to_bytes` is the whole encoding surface.

```text
FixMsg.to_bytes(separator: int = 1) -> bytes
```

`separator` is a byte value rather than a string: `1` is `SOH` and the default,
`ord("|")` is the other spelling a reader infers.

The bytes come from the message's `entries`, never from its columns. Each entry
is written `key=value` followed by the separator -- after the last pair as well
-- in arrival order. Three consequences, and together they are the contract:

- The key is the spelling the frame used, so a numeric tag comes back numeric
  and a symbolic bridge key comes back symbolic.
- The value is the text that arrived, not the row's reading of it. A row that
  reads `54=1` as a side cannot say whether the wire carried `1` or a symbolic
  spelling, so the arrival record is what is re-emitted.
- A `FixMsg` built from a schema and a value carries no arrival record, and
  encodes to `b""`.

```python
import pyarrow

from yggdryl import Field, IOBase
from yggdryl.fix import FixMsg, FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
reader = FixReader(registry)

message = reader.text(
    "2026-08-14 00:05:01.147 INFO [session] SENT "
    "8=FIX.4.4^A35=D^A55=TTF^A54=1^A60=20260814-00:05:01.147^A10=000^A queued"
)
wire = message.to_bytes()

assert wire == (b"8=FIX.4.4\x0135=D\x0155=TTF\x0154=1\x0160=20260814-00:05:01.147\x0110=000\x01")
assert message.to_bytes(ord("|")) == (
    b"8=FIX.4.4|35=D|55=TTF|54=1|60=20260814-00:05:01.147|10=000|"
)

again = reader.bytes(wire)
assert again.by_tag(55).as_py() == "TTF"
assert again.by_tag(60).as_py().isoformat() == "2026-08-14T00:05:01.147000+00:00"
assert again.to_bytes() == wire

schema = pyarrow.schema([pyarrow.field("35", pyarrow.utf8())])
assembled = FixMsg(Field.from_arrow_schema(schema, name="FixMsg"), {"35": "D"}, registry)
assert assembled.to_bytes() == b""
```

The log's prefix, the `^A` spelling and the trailing ` queued` all fall away:
what comes back is the frame, unescaped, with everything the capture wrapped
around it gone. [Decode](decode.md#locating-the-frame) is that scan.

## What the frame owes the standard

| tag | field | obligation |
| --- | --- | --- |
| 8 | `BeginString` | always first |
| 9 | `BodyLength` | always second; the bytes after `9=...<SOH>` up to and including the separator before `10=` |
| 10 | `CheckSum` | always last; the low byte of the sum of every preceding byte, three digits, zero-padded |

Yggdryl computes none of the three, and checks none of them. `to_bytes` writes
the entries and nothing else: no `9=` appears unless one arrived, and a `10=`
that arrived is copied whatever it sums to. There is no checksum arithmetic
anywhere in the crate. Tag 10 is where the scan stops -- whatever a log wrote
after the checksum is prose -- but the pair itself is kept: it is a trailer
column like any other, so a parsed row carries `10` typed, and `entries` carries
it verbatim. What nothing does is *verify* it. A frame that has to
satisfy a counterparty is the caller's arithmetic; the builder below does it in
the browser.

Order is what actually bites, and it bites on the way back in: the frame opens
at the first `8=` pair, so a pair written before `BeginString` is prefix, and it
is gone.

```python
from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
reader = FixReader(registry)

built = reader.pairs([("8", "FIX.4.4"), ("35", "D"), ("54", "1"), ("10", "000")])
assert built.to_bytes(ord("|")) == b"8=FIX.4.4|35=D|54=1|10=000|"
assert built.anomalies() == []

out_of_order = reader.pairs([("35", "D"), ("8", "FIX.4.4"), ("55", "TTF")])
assert out_of_order.to_bytes() == b"35=D\x018=FIX.4.4\x0155=TTF\x01"
assert [entry[0] for entry in reader.bytes(out_of_order.to_bytes()).entries()] == [8, 55]

framed = reader.pairs(
    [("8", "FIX.4.4"), ("9", "57"), ("35", "D"), ("55", "TTF"), ("54", "1"), ("10", "000")]
)
bare = reader.pairs([("8", "FIX.4.2"), ("35", "D"), ("55", "TTF"), ("54", "1"), ("10", "231")])
assert framed.digest() == bare.digest()
```

The last two messages there are the other side of the same rule.
`FixMsg.digest()` is over the entries with the session envelope excluded --
`BeginString`, `BodyLength` and `CheckSum` among them -- so a message
re-emitted with a corrected length and checksum digests equal to the one that
arrived. It is the digest [`dedup` compares](quality.md#sequential-deduplication),
and it is not `msghash`, which is over the raw body bytes.

## Which separator to write

A capture spells the separator six ways: the `SOH` byte itself, `|`, and the
four printed stand-ins `^A`, `\x01`, `<SOH>` and `{SOH}`. The reader
[recognizes all six](decode.md#locating-the-frame). `to_bytes` writes a single
byte, so it writes the first two; the four printed spellings are read and never
written, and a log that wants one gets it with a `replace` after the fact.

`SOH` is the right default because it is the only separator that cannot appear
inside a FIX value. `|` is printable, so a frame carrying a `Text` field
re-emitted with `|` loses the tail of that value silently -- no anomaly, no
refusal, just a shorter string.

```python
from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
reader = FixReader(registry)
noisy = reader.pairs([("8", "FIX.4.4"), ("35", "D"), ("58", "a|b")])

assert reader.bytes(noisy.to_bytes()).by_tag(58).as_py() == "a|b"
assert reader.bytes(noisy.to_bytes(ord("|"))).by_tag(58).as_py() == "a"

printed = noisy.to_bytes().replace(b"\x01", b"^A")
assert printed == b"8=FIX.4.4^A35=D^A58=a|b^A"
assert reader.bytes(printed).entries() == noisy.entries()

exotic = noisy.to_bytes(ord("~"))
assert len(reader.bytes(exotic).entries()) == 1
assert reader.fixtext(exotic, ord("~")).entries() == noisy.entries()
```

Any other byte encodes, and nothing reads it back on its own: `text` and
`bytes` infer `SOH` or `|` from the frame, and only `fixtext(body, separator)`
is told. Over a batch the byte is a row's own `sep` column rather than an
argument to the stream.

## Typing on the way out

A value is text on the wire whatever the dictionary declares. Three declared
types have a FIX spelling no generic cast reads, and those three are the ones
the reader rewrites before typing -- the
[decode cast table](decode.md#typing-and-what-a-bad-value-does) read the other
way:

| declared type | write |
| --- | --- |
| `bool` | `Y` or `N`; `y` and `n` also read |
| `date32`, `date64` | `YYYYMMDD` |
| `timestamp[us, UTC]` | `YYYYMMDD-HH:MM:SS.sss` |

Everything else reaches the field's own coercion as text. A
`decimal128(20, 8)` takes plain digits with an optional `.`; a thousands
separator does not read. Precision past the declared unit does not read either
-- nine fractional digits into a `timestamp[us, UTC]` is a null and an anomaly,
never a rounded instant.

```python
from datetime import date, datetime, timezone

import pyarrow

from yggdryl.fix import FixReader, FixRegistry

spellings = FixRegistry.from_fields(
    [
        pyarrow.field("PossDupFlag", pyarrow.bool_(), True, {"fix:tag": "43"}),
        pyarrow.field("MaturityDate", pyarrow.date32(), True, {"fix:tag": "541"}),
        pyarrow.field("SendingTime", pyarrow.timestamp("us", "UTC"), True, {"fix:tag": "52"}),
    ]
)
reader = FixReader(spellings)

typed = reader.pairs([("43", "Y"), ("541", "20260814"), ("52", "20260814-00:05:01.147")])
assert typed.by_tag(43).as_py() is True
assert typed.by_tag(541).as_py() == date(2026, 8, 14)
sent = datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=timezone.utc)
assert typed.by_tag(52).as_py() == sent

precise = reader.pairs([("52", "20260814-00:05:01.147123456")])
assert precise.by_tag(52).is_null()
assert precise.anomalies() == ['52 (52) would not type from "20260814-00:05:01.147123456"']
```

Two things do not round-trip. The spellings `""`, `null` and `<null>` are read
as [nothing sent](decode.md#nothing-is-skipped) -- case-insensitively, and
before typing -- so they produce no entry and are not re-emitted at all. And a
`data` field's entry is a lossy decode of bytes that were never text, so
re-emitting one writes `U+FFFD` where the bytes were; `anomalies()` names
exactly those entries, while the row still holds the real bytes.

```python
import pyarrow

from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
assert FixReader(registry).pairs([("8", "FIX.4.4"), ("58", "null")]).to_bytes() == (
    b"8=FIX.4.4\x01"
)
kept = FixReader(registry, null_values=[]).pairs([("8", "FIX.4.4"), ("58", "null")])
assert kept.to_bytes() == b"8=FIX.4.4\x0158=null\x01"

binary = FixRegistry.from_fields(
    [pyarrow.field("RawData", pyarrow.binary(), True, {"fix:tag": "96"})]
)
lossy = FixReader(binary).fixtext(b"96=a\xffb\x01")
assert lossy.by_tag(96).as_py() == b"a\xffb"
assert lossy.anomalies() == ["96 (96) decoded lossily and is not text"]
assert lossy.to_bytes() == b"96=a\xef\xbf\xbdb\x01"
```

## The version a message is read at

`FixReader` and `parse_arrow_reader` take the same two version arguments, and
they are not symmetric.

`source_version` is the version the arriving rows are written in. It pins what
the reader otherwise infers, in the specification's own order: `ApplVerID`
(1128), then `BeginString` (8) with `FIXT.1.1` falling through, then the
branch's default, then the dictionary's newest. It is a filter on the
dictionary read -- a field's `fix:lineage` says what that field was called and
typed at each version, and the message's field is the
[registry's field](registry.md#resolution) projected through it. A dictionary
that carries no lineage projects to itself; `config/fix` dates 1,564 of its
6,203 definitions, so the knob is visible on those.

`LastQty` (32) is one of them: it was `LastShares` typed `int` from FIX 2.7,
kept that name with a `Qty` type at 4.2, and became `LastQty` at 4.3.

```python
from yggdryl import IOBase
from yggdryl.fix import FixReader, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
frame = [("8", "FIX.4.4"), ("35", "8"), ("32", "500")]

old = FixReader(registry, source_version="FIX.4.0").pairs(frame)
new = FixReader(registry, source_version="FIX.4.4").pairs(frame)


def column(message, tag):
    member = next(held for held in message.field.unnest_fields() if held.fix.tag == tag)
    return member.name, str(member.dtype)


assert column(old, 32) == ("lastshares", "int32")
assert column(new, 32) == ("lastqty", "float64")
assert old.to_bytes() == new.to_bytes()

targeted = FixReader(registry, source_version="FIX.4.4", target_version="FIX.4.0").pairs(frame)
assert targeted.field == new.field
assert targeted.to_bytes() == new.to_bytes()
```

The bytes are identical either way, and that is the point: a version changes
what the message is read as, never what it said.

`target_version` is declared as the version built messages are expressed in.
Nothing in the build path reads it today -- the same frame parsed with it set
and unset answers the same field, the same values and the same bytes -- so
`source_version` is the one that standardizes a read.

## Build one

Pick fields by tag or by any name the dictionary resolves -- a canonical name
or an alias. The builder orders the frame the way the standard requires and
computes `BodyLength` and `CheckSum`, which is exactly the arithmetic
`to_bytes` leaves to the caller. A spelling it refuses is a spelling `parse_fix`
would leave in [`unmapped`](quality.md#coverage-what-unmapped-is-for).

<div data-fix="encode">Loading the dictionary…</div>

It resolves against the same
[generated dump](registry.md#the-dump-these-pages-read) of `config/fix` the
decoder reads, in the tab. Paste the result into [Decode](decode.md#try-it) to
see the round trip.
