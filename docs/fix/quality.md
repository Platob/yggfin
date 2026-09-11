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
from rekep.fix import fix_codec

message, = fix_codec().parse_line(b"8=FIX.4.4|35=D|453=3|448=BROKER|10=000|")

assert message.anomalies() == ["parties (453) states 3 occurrences and holds 1"]
assert message.by_path("Parties[0].PartyID").as_py() == "BROKER"
```

Anomalies are message-level checks such as stated lengths, checksums, and group
counts. They do not erase the message or its arrivals: the row above still
carries the one party that arrived, and the record still carries both pairs.

## Registry coverage

```python
import pyarrow
import pyarrow.compute as pc

from rekep.fix import fix_codec, fix_message_field

# In a pipeline `fixed` is read from fix.messages; here it is one parsed line.
codec = fix_codec()
source = pyarrow.RecordBatchReader.from_batches(
    pyarrow.schema([pyarrow.field("body", pyarrow.large_binary())]),
    [
        pyarrow.RecordBatch.from_pylist(
            [{"body": b"MSGTYPE=executionreport|SYMBOL=HOLN|VENUEPRIVATEKEY=x|"}],
            schema=pyarrow.schema([pyarrow.field("body", pyarrow.large_binary())]),
        )
    ],
)
fixed = codec.parse_text_arrow_reader(source).read_all()

unmapped_rows = pc.greater(pc.list_value_length(fixed["nounmappedfixentries"]), 0)
needs_dictionary_work = fixed.filter(unmapped_rows)

assert needs_dictionary_work.num_rows == 1
assert [entry["key"] for entry in needs_dictionary_work["nounmappedfixentries"][0].as_py()] == [
    "VENUEPRIVATEKEY"
]
assert len(list(fix_message_field())) == 114
```

Investigate the original keys in `nounmappedfixentries`, add definitions to an
explicit registry, replay `parse_fix`, and review the schema diff before
publishing it as the next bundle.

## Replay guarantees

`(url, rownum)` keeps raw and fixed products joinable, and
`(url, rownum, msghash)` is what identifies a fixed row: a bridge
configuration line states one message per MBean, so the line's own identity is
a key prefix rather than the whole key. A replay writes no duplicate and no
empty snapshot. Nothing drops a row unasked — `parse_fix` has no deduplication
switch, and a product that wants adjacent republications collapsed does it
downstream, where giving up positional row alignment is a stated choice rather
than a parser flag.

`msghash` is a digest of the arrival record, so it changes when the parse
changes. An upgrade that changes how a line is framed changes the digest, and
with it the identity of an already-stored row: a replay after such an upgrade
inserts rather than matches. Replay into a fresh table, or accept both
generations, rather than expecting the merge to reconcile them.
