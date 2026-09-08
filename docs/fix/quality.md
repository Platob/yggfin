# FIX quality and audit

Quality is represented in rows rather than hidden in parser control flow.

| signal | question |
| --- | --- |
| `bodyhash` | did the exact captured bytes change? |
| `msghash` | did the parsed business record change? |
| `nofixentries` | exactly which pairs arrived, in what order? |
| `nounmappedfixentries` | which pairs had no registry definition? |
| typed null with an arrival | which value failed translation or conversion? |
| `FixMsg.anomalies()` | which internal protocol claims disagree? |

## Distinct digests

`bodyhash` includes log prose and framing because it hashes `Message.body`.
`msghash` hashes the parsed arrival record while excluding session-envelope
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

```python
import pyarrow.compute as pc

# `fixed` is a pyarrow.Table read from fix.messages.
unmapped_rows = pc.greater(pc.list_value_length(fixed["nounmappedfixentries"]), 0)
needs_dictionary_work = fixed.filter(unmapped_rows)
```

Investigate the original keys in `nounmappedfixentries`, add definitions to an
explicit registry, replay `parse_fix`, and review the schema diff before
publishing it as the next bundle.

## Replay guarantees

With `dedup=false`, `(url, rownum)` keeps raw and fixed products aligned. A
replay writes no duplicate and no empty snapshot. Turning deduplication on
drops only adjacent equal `msghash` values and explicitly gives up positional
row alignment; use it only for a product whose contract allows that.
