# Quality

The two stages retain source position, exact bytes, parsed meaning, and
dictionary coverage as separate facts. No pre-parser classification decides
whether a row is allowed into `fix.messages`.

## One row in, one row out

`parse_messages` stores every physical line. With the task default
`dedup=false`, `parse_fix` emits one codec row for each stored line, including
prose, an empty body, or a malformed frame. `(url, rownum)` therefore identifies
the same position in both tables.

The codec guarantees non-null `beginstring`, `msghash`, `timestamp`, and
`unixpartition`. Missing body facts use a resolved FIX version, an empty-record
digest, the source timestamp or Unix epoch, and its derived partition.

## Two digests

| digest | source | answers |
| --- | --- | --- |
| `bodyhash` | exact raw `body` bytes | did this physical capture change? |
| `msghash` | parsed `nofixentries`, excluding the session envelope | did the message meaning change? |

```python
from rekep import Message
from yggdryl.fix import fix_crate_fields

bodyhash = Message.field()["bodyhash"]
msghash = {field.name: field for field in fix_crate_fields()}["msghash"]

assert bodyhash.digest.sources == ["body"]
assert msghash.digest.sources == ["nofixentries"]
assert bodyhash.digest.algorithm == msghash.digest.algorithm == "xxh3-128"
```

The codec's folded-name rule is why the raw holder is called `bodyhash`.
Calling it `msghash` would fill the fixed field from source and erase the
distinction. Neither digest is cryptographic.

## Source facts fill fixed fields

`timestamp` from the log header outranks clocks inside the message. `msgCtxId`
fills `msgctxid`; `seqNum` can fill `msgseqnum`; and `plugin` can fill the
sender or target plugin session. A source fill is not appended to
`nofixentries`, because it was context around the protocol record rather than a
pair inside it.

Direction comes from a source `direction` column when present, otherwise from
a recognized verb before the payload, otherwise the codec's stream default.
It fills folded `msgdirection` (tag 385) and selects which plugin-session side
the source `plugin` supplies.

## Arrival and coverage

`nofixentries` preserves every parsed pair in arrival order. It keeps original
text when a declared type cannot read a value and is the source used by
`FixMsg.to_bytes()`.

`nounmappedfixentries` is the subset no registry field resolved. Group it by
`key` to find missing venue definitions; an empty list means the selected
registry covered every pair in that row. Adding a definition changes future
typed output without rewriting what the source originally said.

`FixMsg.anomalies()` reports type failures, repeating-group count mismatch, and
lossy text decoding for one object. These are observations, not reasons to drop
the row.

## Deduplication

`dedup=true` drops only a row whose `msghash` equals the previously emitted
row, carrying the comparison across batch boundaries. It is sequential and
bounded, not a global set. Enabling it intentionally breaks positional row
alignment and is therefore explicit; the pipeline default is false.

## Replay

Iceberg insertion is keyed by `(url, rownum)` in both tables. Replaying the
same source reads and parses all rows, writes none, and creates no data file or
snapshot. The stage result reports the skipped count rather than hiding it.
