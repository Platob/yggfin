# Parse messages

`tasks/parse_messages/parse_messages.ipynb` streams text files through
`TextFile`/`TextFiles` into `logs.messages`. It reads only the registry's
MsgType event metadata; field and protocol interpretation belong to
[`parse_fix`](parse-fix.md).

## Run this step

```bash
uv run --project python --with papermill rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter source=python/tests/data/app_messages_sample.txt \
  --output parse_messages.executed.ipynb
```

The package, a FIX registry and a catalog have to exist first:
[deploy from scratch](../operations/deploy.md).

Each [`Message`](../../products/message.md) row carries the recording time in
`unix` with its `unix_partition`, `source_url` and the 1-based physical
`source_rownum`, `thread_name` and `plugin_code` from the configured header,
the unsplit payload in `message`, the syntax-only `protocol_code`, residual
ordered `entries` with repeated keys retained, the unambiguous `MsgType` and
registry-mapped `etype`, and `hash` — the XXH3-64 identity of the exact UTF-8
payload.

Registry names, protocol versions, components and typed values do not belong
to this stage. A leading `#` is removed from each key; values stay text.

## Why the table is retained

`logs.messages` is the protocol-neutral source for later parsers: a field or
protocol rule can change without reopening compressed logs or re-listing the
source prefix, and rerunning a protocol parser uses the retained `entries`
rather than splitting `message` again. Only a MsgType `event_types` change
requires a rebuild, because it changes stored `etype`.

Identity hashes `message` alone — no composite frame, source path or row
number — so identical payloads share an identity across captures. The table
sorts by `(unix, hash)` and partitions from the recording time, so a later
parser may move its own event time without changing which source interval owns
the row.

## Reading

Remote compressed captures stream directly through Arrow. `spill: true` copies
their raw compressed bytes to a uniquely owned temporary local file before
decoding, deleted when that stream finishes. The expanded capture is never
written or collected in memory, and plain remote and local captures are never
copied.

Keep a directly opened `TextFile` in a `with` block: exhausting its reader
releases the temporary spill, and closing the owner releases it immediately
when a caller stops early.

A log is read once at a time. A `TextFile` has one stream, so a second reader
while the first is live is refused rather than served — both would read
through one handle, and the second rewinding it under the first splices a
record across a read boundary. Close the reader, or open a second `TextFile`.
Closing either class under a live reader fails that reader, so a capture read
short is never reported as one read whole.

Physical lines are scanned through a fixed-size native buffer. A long line or
folded continuation grows one mutable record buffer instead of recopying its
prefix on every compressed read.

## Filters

All of them run **before** entries are split.

| setting | what it does |
| --- | --- |
| `include_regexes` | admits a payload when any Arrow RE2 pattern matches |
| `exclude_regexes` | then removes a payload when any pattern matches |
| `include_msgtypes` | admits only exact discriminators when non-empty; a row without one survives an empty list |
| `exclude_msgtypes` | removes exact values after that inclusion |
| `technical_plugins` | omits exact plugin codes, case-insensitively, before persistence |

Empty lists keep everything, so heartbeats (`0`) and test requests (`1`) are
retained unless excluded. Regex matching sees the complete folded message and
runs with the `[start, end)` recording-time filter.

Because the MsgType filters read the raw text, they take the first `35=` or
`MsgType=` token before the first checksum-shaped token *anywhere in the line*.
A payload carrying a `10=` sequence inside an earlier field value therefore
reads as having no discriminator here, while the stored `MsgType` column —
whose boundary is the first checksum-*keyed entry* — still holds it. Filter on
`MsgType` after the fact where that matters.

`technical_plugins` is source policy belonging to the task, not to the FIX
dictionary. Rebuild `logs.messages` after changing it when previously
persisted rows must go.

## Batching and bounds

```yaml
batch_row_size: 65536
batch_byte_size: 67108864       # closes a batch around unusually large diagnostics
max_row_byte_size: 67108864     # bounds one record: header line plus continuations
duration_ns: null               # closes a non-empty batch when its window changes
```

With `start`, windows begin exactly there; otherwise the first retained `unix`
is truncated to the duration. A busy window still produces multiple
`batch_row_size` batches. One logical record is never split across batches, so
a batch holding one is as large as that record. Gaps produce no empty batches,
and input must be ordered by these windows.

A newline is the writer's promise that a record ended — and a capture cut
mid-write, a binary blob logged by accident or a runaway diagnostic never makes
it. Without `max_row_byte_size` one record would hold the rest of the file.
Bytes past the bound are read in `read_byte_size` pieces and dropped rather
than held, and the row says how many it lost:

```python
from rekep import TextFile

log = TextFile.from_path("app.log")
table = log.into_arrow_table(max_row_byte_size=1 << 20)
table.filter(table.column("reason").is_valid()).select(["source_rownum", "reason"])
```

```text
source_rownum  reason
1              row truncated at max_row_byte_size; dropped bytes: 66060331
```

Every dropped byte reaches a `reason` or the read is refused: a bound so small
it cuts a line before the header can match leaves no row to carry what it
dropped, and raises rather than reading a whole log as no rows at all.

Both byte bounds cap at 2,147,483,647 — what one Arrow 32-bit binary offset
addresses over a whole array, and so over the batch as well as the record
inside it.

## Contract

Only payloads with a discriminator, a FIX BeginString, or at least two
pipe/SOH/caret/caret-A/semicolon/hash-delimited assignments enter the generic
key/value splitter. A piped bridge without MsgType keeps its arguments and is
`MISC`; an ordinary long log message with an incidental `A=1` skips the
allocation.

The standard header — `BeginString`, `BodyLength`, `MsgType`, `MsgSeqNum`,
`SenderCompID`, `TargetCompID`, `SendingTime` — is lifted out of `entries`
into columns of its own, beside `protocol_code` and the early `etype`. A
lifted column is read back out of `entries` wherever it is empty, so a row
that carried the field only in the list still answers.

`parse_fix` refuses a source missing `MsgType`, `entries` or `protocol_code`
rather than reporting an empty successful run. There is no migration: a table
without those columns is rebuilt.

Keep custom `protocols` aligned with `parse_fix.yml`; null uses the shipped
default rules in both stages.
