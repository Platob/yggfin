# FIX quality and audit

Quality is represented in rows rather than hidden in parser control flow.

| signal | question |
| --- | --- |
| `bodyhash` | did the exact captured bytes change? |
| `uuid` | did the parsed message change? |
| `nofixentries` | exactly which pairs arrived, in what order? |
| `nofixentries` entries of tag 0 | which pairs had no registry definition? |
| typed null with an arrival | which value failed translation or conversion? |
| `FixMsg.anomalies()` | which internal protocol claims disagree? |

## Distinct digests

`bodyhash` includes log prose and framing because it hashes `Message.body`.
`uuid` hashes the named message content while excluding session-envelope
tags. Two relayed copies can therefore have different bodies but the same
parsed identity; a corrected dictionary can change typed columns without
changing either source digest.

## Anomalies

```python
from rekep.fix import FixCodec, fix_registry

codec = FixCodec(fix_registry())
message = codec.transform_line(b"8=FIX.4.4|9=999|35=D|55=AAPL|10=000|")

for anomaly in message.anomalies():
    print(anomaly)
```

Anomalies are message-level checks such as stated lengths, checksums, and group
counts. They do not erase the message or its arrivals.

## Registry coverage

A pair no dictionary explains is an entry of tag 0 inside the arrival record,
under the raw key the wire spelled, so coverage is read off `nofixentries`
rather than off a second column:

```python
# `fixed` is a pyarrow.Table read from fix.messages.
needs_dictionary_work = [
    (row["url"], row["rownum"], [entry["key"] for entry in row["nofixentries"] or [] if entry["tag"] == 0])
    for row in fixed.select(("url", "rownum", "nofixentries")).to_pylist()
]
```

Investigate those keys, add definitions to an explicit registry, replay
`parse_fix`, and review the schema diff before publishing it as the next
bundle.

## Replay guarantees

`(url, rownum)` joins a fixed row back to the line it came from, and `uuid`
tells two messages of one line apart. A replay writes no duplicate and no
empty snapshot, because a message that stated no clock of its own is dated
by its capture instant rather than by the run: the identity is the same one
every time that line is read.
