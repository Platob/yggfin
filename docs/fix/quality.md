# FIX quality and audit

Quality is represented in rows rather than hidden in parser control flow.

| signal | question |
| --- | --- |
| `bodyhash` | did the exact captured bytes change? |
| `hashcode` | did the settled event change? |
| `curruuid` | which event is this, whichever hop logged it? |
| `fixentries` | exactly which pairs arrived, in what order? |
| `fixentries` entries of `tag` 0 | which pairs had no registry definition? |
| typed null with an arrival | which value failed translation or conversion? |

## Distinct digests

`bodyhash` includes log prose and framing because it hashes `Message.body`, and
it is what `logs.messages` is keyed on: identical bytes are one line whatever
session carried them.

`hashcode` is the event's content code — XXH3-64 over its facts, its text, its
metadata, the stated header cells and the entry tree, and never the row's
storage, so a message read back out of a row is the same message. Two lines
carrying the same frame under the same clock answer one code while their bodies
differ, which is exactly what makes a message logged at three hops one event.
`curruuid` is the identity over that code and the instant the event settled on,
and it is what `fix.messages` is keyed on.

`crosshashcode` digests `crosscode` alone — the first chain identifier the
message states — so every event of one chain shares it, and `crossuuid` is the
identity over that.

## Stated claims a row disagrees with

A message that states its own `BodyLength(9)` or `CheckSum(10)` states a claim
about bytes the reader can check, and the arrival record keeps both the claim
and what arrived. Nothing is erased on a mismatch: the frame is read, the
stated pair lands in its column, and the entries beside it are what the wire
carried.

```python
from rekep.fix import fix_codec, fix_registry

codec = fix_codec(fix_registry())
message = next(iter(codec.parse_line(b"8=FIX.4.4|9=999|35=D|55=AAPL|10=000|")))

# The stated length is nine hundred and ninety-nine; the frame is not.
assert message.by_name("bodylength").as_py() == 999
assert message.by_name("symbol").as_py() == "AAPL"
assert message.by_tag(35).as_py() == "D"
```

## Registry coverage

A pair no dictionary explains is an entry of `tag` 0 inside the arrival
record, under the raw key the wire spelled, so coverage is read off
`fixentries` — the group named after itself, counted by `nofixentries` — rather
than off a second column:

```python
# `fixed` is a pyarrow.Table read from fix.messages.
needs_dictionary_work = [
    (
        row["sourceurl"],
        row["rownum"],
        [entry["tagkey"] for entry in row["fixentries"] or [] if entry["tagnum"] == 0],
    )
    for row in fixed.select(("sourceurl", "rownum", "fixentries")).to_pylist()
]
```

Investigate those keys, add definitions to an explicit registry, replay
`parse_fix`, and review the schema diff before publishing it as the next
bundle.

## Replay guarantees

`(sourceurl, rownum)` joins a fixed row back to the line it came from, and
`curruuid` tells two messages of one line apart and folds one message logged at
three hops — which is why the fixed table's key is `curruuid` alone. A replay
of a window lands the same
messages over the ones it landed and leaves no duplicate, because a message
that stated no clock of its own is dated by what the capture recorded, or by
the codec's `UNDATED` floor where it recorded nothing, and never by the instant
the parse ran: the identity is the same one every time that line is read.
