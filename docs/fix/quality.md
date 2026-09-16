# FIX quality and audit

Quality is represented in rows rather than hidden in parser control flow.

| signal | question |
| --- | --- |
| `bodyhash` | did the exact captured bytes change? |
| `msghash` | did the parsed message change? |
| `fixentries` | exactly which pairs arrived, in what order? |
| `fixentries` entries of `tagnum` 0 | which pairs had no registry definition? |
| typed null with an arrival | which value failed translation or conversion? |
| `FixMsg.anomalies()` | which internal protocol claims disagree? |

## Distinct digests

`bodyhash` includes log prose and framing because it hashes `Message.body`.
`msghash` covers the settled instant and the canonical named message content,
so two lines carrying the same frame under the same clock answer one identity
while their bodies differ — and a message stating another session, another
direction or another sequence number is another message rather than the same
one seen twice. `msgphash` hashes the `code` alone, so every message of one
chain shares it.

## Anomalies

```python
from rekep.fix import fix_codec, fix_registry

codec = fix_codec(fix_registry())

for message in codec.parse_line(b"8=FIX.4.4|9=999|35=D|55=AAPL|10=000|"):
    for anomaly in message.anomalies():
        print(anomaly)
```

Anomalies are message-level checks such as stated lengths, checksums, and group
counts. They do not erase the message or its arrivals.

## Registry coverage

A pair no dictionary explains is an entry of `tagnum` 0 inside the arrival
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
`msghash` tells two messages of one line apart — which is why the fixed table's
key is `(sourceurl, rownum, msghash)`. A replay writes no duplicate and no
empty snapshot, because a message that stated no clock of its own is dated by
what the capture recorded, or by the codec's `UNDATED` floor where it recorded
nothing, and never by the instant the parse ran: the identity is the same one
every time that line is read.
