# Parse FIX

`tasks/parse_fix/parse_fix.ipynb` transcribes stored `Message` arguments into
`FixMsg` rows. Its primary Iceberg scan applies the recording-time window and
`eventtype` in `EventType.ranked_at_least(INTENT)` before any FIX dictionary
work begins.

The complementary scan -- `Not` of the market code set -- retains everything
else: terminal operational rows, unrecognized rows, and any stored code no
compiled member spells. Together the two scans partition the table, so no row
silently matches neither.

## Run this step

After `parse_messages` has populated `logs.messages`, run from the repository
root:

```bash
uv run --project python --with papermill rekep task run \
  tasks/parse_fix/parse_fix.yml \
  --output parse_fix.executed.ipynb
```

The package, a FIX registry and a catalog have to exist first:
[deploy from scratch](../operations/deploy.md).

To replay only one half-open recording interval, add
`--parameter start=2026-08-21T10:00:00Z` and
`--parameter end=2026-08-21T11:00:00Z` before `--output`.

`parse_messages` has already opened the dictionary for MsgType event metadata.
This stage opens the same dictionary for full transcription. For each Arrow
batch it:

1. reads the stored protocol classification and ordered `entries`;
2. infers the FIX application version;
3. resolves names, tags, types and configured value spellings;
4. lifts declared fields and structured components;
5. derives the venue, transaction time and identities.

`Message.eventtype`, `Message.MsgType`, and `Message.protocolcode` pass through
this conversion; the FIX stage does not classify the message a second time.

Repeated tags and wire order remain in `entries`. A resolved entry records the
canonical FIX key, its numeric tag, its value, and either its component path or
vendor namespace. Fields promoted for filtering use the registry spelling as
their physical column name: `MsgType`, `MsgSeqNum`, `OrigClOrdID`,
`TransactTime`, and so on.

## Routing

The task writes orders, quotes, executions, books and instruments to
`fix.market`. Recognized operational traffic, rows without MsgType, and unknown
events on a recognized transport go to `fix.misc`; an unknown event on an
unrecognized transport goes to `fix.unknown`.

Market and terminal predicates are pushed independently, so neither stream
sees the other's rows. Registry-declared technical MsgTypes are excluded by the
scan, and plugin filtering already happened in `parse_messages`, so those rows
never enter this source table. The raw `message` column is projected out for
both streams: stored `entries` already carry what transcription needs.

The source interval is filtered on `Message.unix`, the recording clock. The
resulting `FixMsg.unix` may instead come from a regulatory timestamp,
`TransactTime`, market-data entry time, sending time, or finally the recording
clock. Output tables are sorted by `(unix, MsgSeqNum, hash)`.

## Instruments

After routing, the notebook resumes recent Instrument state and derives new
versions from the sorted market stream. Normalized Instrument rows are written
back to `fix.market` with the package-owned user-defined `MsgType` `U1`,
then `flatten_instruments` writes the Instrument table.

## Configuration

The adjacent `parse_fix.yml` owns full-transcription settings:
`fix_dictionary`, `null_values`, protocol rules, and declared `fields`. It
also selects the catalog, branch, source interval and batch sizes. A
dictionary, field, or protocol-rule change reruns this stage against retained
`Message` rows, resolving the stored arguments without tokenizing the payload.

Keep its `fix_dictionary` aligned with `parse_messages.yml`. MsgType event
metadata is read by `parse_messages` because `eventtype` is part of `Message`, so
changing that metadata requires rebuilding `logs.messages`, while other
dictionary changes can rerun only this stage.

The projected conversion requires the `MsgType`, `entries` and
`protocolcode` columns of the [Message contract](../../contracts/index.md),
and refuses a source without them rather than reporting an empty run.
