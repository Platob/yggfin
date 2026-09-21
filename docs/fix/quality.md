# FIX quality and audit

Quality is represented in rows rather than hidden in parser control flow.

| signal | question |
| --- | --- |
| a line's `currhashcode` | did the exact captured line change? |
| `currhashcode` | did the settled event change? |
| `curruuid` | which event is this, whichever hop logged it, and which instant the walk settled it on? |
| `fixentries` | which pairs or groups were not fully represented in lifted columns? |
| `fixentries` entries of `tag` 0 | which pairs had no registry definition? |
| typed null with an arrival | which value failed translation or conversion? |

## Distinct digests

A line's `currhashcode` codes the whole line, row header included, because
the read states it over `Message.body` as it retains it, and it is what
`logs.messages` is keyed on: identical lines are one row whatever session
carried them.

`currhashcode` is the event's content code -- XXH3-64 over its facts, its text,
its metadata, the stated header cells and the entry tree, and never the row's
storage, so a message read back out of a row is the same message. Two lines
carrying the same frame under the same clock answer one code while their bytes
differ, which is exactly what makes a message logged at three hops one event.
`curruuid` is the identity over that code and the instant the event settled
on, and it is what both FIX tables are keyed on: the parse settles a bronze
row's, and the walk settles it again where it dates the message by its
`TransactTime`.

`crosshashcode` digests `crosscode` alone. The first non-empty business
identifier wins in this order: `OrderID`, `ClOrdID`, `OrigClOrdID`, `QuoteID`,
`QuoteReqID`, `MDReqID`. Capture session and context are available separately
as `identifiers["msgsectxid"]` when both exist and never alter the content
identity. `crossuuid` is the identity over the chosen business code.

## Stated claims a row disagrees with

A message that states its own `BodyLength(9)` or `CheckSum(10)` states a claim
about bytes the reader can check. The frame is read and the stated pair lands
in its column. An unrepresentable or conflicting pair remains residual; a
successfully lifted value is not duplicated in `fixentries`.

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

A pair no dictionary explains is an entry of `tag` 0 inside residual
`fixentries`, under the key the wire spelled. `nofixentries` counts this
residual tree:

```python
# `fixed` is a pyarrow.Table read from fix.silver or fix.bronze.
needs_dictionary_work = [
    (
        row["sourceurl"],
        row["rownum"],
        [entry["name"] for entry in row["fixentries"] or [] if entry["tag"] == 0],
    )
    for row in fixed.select(("sourceurl", "rownum", "fixentries")).to_pylist()
]
```

Investigate those keys, add definitions to an explicit registry, replay
`parse_fix_bronze` and then `parse_fix_silver` under it, and review the schema
diff before publishing it as the next bundle.

## Replay guarantees

`srcuuids` joins a fixed row back to the stored line it was parsed out of --
the line's own `curruuid`, as `logs.messages` holds it -- with `sourceurl` and
`rownum` beside it, and `curruuid` tells two messages of one line apart and
folds one message logged at three hops, which is why both FIX tables are keyed
on `curruuid` alone. A replay of a window lands the same rows in `fix.bronze`
and, walked, the same rows in `fix.silver`, and leaves no duplicate: a message
that stated no clock of its own is dated by the codec's `UNDATED` floor in the
parse and by the `TransactTime(60)` it states in the walk, never by the instant
either ran, so the identity is the same one every time that line is read.
