# Parse messages

`tasks/parse_messages/parse_messages.ipynb` streams text files through
`TextFile` or `TextFiles` and writes `logs.messages`. It reads only the
registry's MsgType event metadata; field and protocol interpretation stay in
`parse_fix`.

## Run this step

Set `source` in the adjacent YAML to an existing log path or override it from
the repository root:

```bash
uv run --project python --with papermill rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter source=python/tests/data/app_messages_sample.txt \
  --output parse_messages.executed.ipynb
```

The command writes the executed notebook beside the repository root and the
parsed rows to the configured `logs.messages` Iceberg table.

Each `Message` row contains:

- the recording time in `unix`, with its `unix_partition`;
- `source_url` and the 1-based physical `source_rownum`;
- `thread_name` and `plugin_code` from the configured header;
- the unsplit payload in `message`;
- the protocol syntax in `protocol_code`, without field interpretation;
- residual ordered `entries` parsed from key/value syntax, with repeated keys retained;
- the unambiguous `MsgType` spelling and the registry-mapped `etype`;
- `hash`, the XXH3-64 identity of the exact UTF-8 `message` payload.

Registry names, protocol versions, components and typed values do not belong
to this stage. A leading `#` is removed from each key; values remain text.
MsgType metadata decides the event kind, and `parse_fix` owns dictionary
interpretation.

Exact `include_msgtypes` and `exclude_msgtypes` filters run before argument
splitting. Both default to empty, so heartbeats (`0`), test requests (`1`) and
other administrative messages are retained unless the caller excludes them.
`technical_plugins` drops named operational sources such as Jolokia before
persistence.

## Why the table is retained

`logs.messages` is the protocol-neutral source for later parsers. A field or
protocol rule can change without reopening compressed logs or listing the
source object-store prefix again, and re-running a protocol parser uses the
retained `entries` rather than splitting `message` again.

A change to MsgType `event_types` is different because it changes stored
`Message.etype`, so it requires rebuilding this table.

The row identity hashes only `message`, without a composite frame, source path,
or row number. Identical payloads therefore share an identity across captures.
The table is sorted by `(unix, hash)` and partitioned from the recording time;
a later parser may move its own event time without changing which source
interval owns the row.

Remote compressed captures stream directly through Arrow by default. Set
`spill: true` to copy their raw compressed bytes to a uniquely owned temporary
local file before decoding; it is deleted when that stream finishes.

The expanded capture is never written or collected in memory, and plain remote
and local captures are never copied. Callers that explicitly request a
persistent `ArrowFileIO` spill get its deterministic, remote-size-validated
cache behavior.

Keep a directly opened `TextFile` in a `with` block. Exhausting its Arrow
reader releases the temporary spill; when a caller stops early, closing the
owning `TextFile` performs that release immediately.

A log is read once at a time. A `TextFile` has one stream, so asking it for a
second reader while the first is live is refused rather than served: both would
read through that one handle, and the second rewinding it under the first
splices a record across a read boundary. Close the reader, or open a second
`TextFile`. Closing a `TextFile` or a `TextFiles` under a live reader fails
that reader, so a capture read short is never reported as one read whole.

Physical lines are scanned through a fixed-size native buffer. A long line or
folded continuation grows one mutable record buffer instead of recopying its
entire prefix on every compressed read; `max_row_byte_size` bounds that buffer,
and the row and byte bounds limit the other records held beside it.

## Configuration

The adjacent `parse_messages.yml` selects the source, FIX dictionary, filename
pattern, header regex, timezone, protocol rules, spill policy, catalog, branch
and batch sizes. Keep custom `protocols` aligned with `parse_fix.yml`; this
stage stores the classification that projected FIX conversion consumes. Null
uses the shipped default rules in both stages.

`include_regexes` admits a payload when any Arrow RE2 pattern matches;
`exclude_regexes` then removes a payload when any pattern matches. Empty lists
keep every payload. Matching sees the complete folded message and happens
together with the `[start, end)` recording-time filter before entries and
message identities are parsed.

`include_msgtypes` admits only exact discriminator values when non-empty;
`exclude_msgtypes` removes exact values after that inclusion. Rows without a
discriminator survive an empty include list. Empty lists retain every MsgType.

Both filters run before entries are split, so they read the discriminator off
the raw text rather than off the parsed arguments: the first `35=` or
`MsgType=` token before the first checksum-shaped token *anywhere in the line*.
A payload carrying a `10=` sequence inside an earlier field value therefore
reads as having no discriminator here while the stored `MsgType` column, whose
boundary is the first checksum-keyed entry, still holds it. Filter on
`MsgType` after the fact where that matters.

`technical_plugins` names exact plugin codes to omit case-insensitively from
the parsed stream before it is written. This source policy belongs to the task,
not to the FIX dictionary; null or an empty list retains every plugin. Rebuild
`logs.messages` after changing it when previously persisted rows must be removed.

`duration_ns` closes a non-empty batch when its recording-time window changes.
With `start`, windows begin exactly there; otherwise the first retained `unix`
is truncated to the duration. A busy window can still produce multiple
`batch_row_size` batches, and `batch_byte_size` closes a batch around
unusually large diagnostics.

One logical record is never split across batches, so a batch holding one is as
large as that record. Gaps do not produce empty batches, and input must be
ordered by these windows.

`max_row_byte_size` bounds one record: the header line plus every continuation
folded into it. A newline is the writer's promise that a record ended, and a
capture cut mid-write, a binary blob logged by accident or a runaway diagnostic
never makes it -- so without the bound one record holds the rest of the file.
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
that it cuts a line before the header can match leaves no row to carry what it
dropped, and raises rather than reading a whole log as no rows at all.

`batch_byte_size` and `max_row_byte_size` are both capped at 2,147,483,647 --
what one Arrow 32-bit binary offset addresses over a whole array, and so over
the batch as well as the record inside it.

Only payloads with a discriminator, a FIX BeginString, or at least two
pipe/SOH/caret/caret-A/semicolon/hash-delimited assignments enter the generic key/value
splitter. A piped bridge without MsgType keeps its arguments and is `MISC`; an
ordinary long log message with an incidental `A=1` skips the allocation.

Message contract version 3 lifts the standard header -- `BeginString`,
`BodyLength`, `MsgType`, `MsgSeqNum`, `SenderCompID`, `TargetCompID`,
`SendingTime` -- out of `entries` into columns of its own, beside the
`protocol_code` and early `etype` version 2 added. Rebuild an existing
`logs.messages` table when `MsgType`, `entries` or `protocol_code` is absent;
`parse_fix` refuses that older physical schema rather than reporting an empty
successful run.

Tables created from the former task-level `static_values` declaration also
need those required columns removed or a fresh table before the narrower task
contract can write them.
