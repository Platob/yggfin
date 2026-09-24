# Run tasks

Every task is a module of `rekep.tasks`, and `rekep tasks <name> run` runs one
in this process: it resolves the parameters, calls the module's `run`,
validates the returned stage result, and writes it to stdout as one compact
JSON line. Logs and tracebacks go to stderr, and a failure exits 1.

```bash
uv run --project python rekep tasks list
uv run --project python rekep tasks parse_messages show
uv run --project python rekep tasks parse_messages --help
```

`list` names every task, in the order the graph runs them, with its summary
and the tables it writes. `show` prints the parameters a run would take, under
the same overrides a run is given, and `--help` lists the defaults.

## Ingestion and optional dbt

```bash
uv run --project python rekep tasks parse_messages run \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep tasks parse_fix_raw run \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep tasks parse_fix_refined run \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep tasks build_dbt run
```

The order is required: `parse_fix_raw` reads `logs.messages` rather than
source files, `parse_fix_refined` reads `fix.raw` rather than either, and
[`build_dbt`](../tasks/build-dbt.md) reads `fix.refined` and nothing before
it.

## Market events from one book snapshot

This example assumes deployed refined rows containing valid market messages
in the named September window. The bundled August ingestion fixture contains
an incomplete AE side and is not a book demo. Capture the book result:

```bash
uv run --project python rekep tasks parse_books run \
  --parameter 'start="2026-09-21T10:00:00Z"' --parameter 'end="2026-09-21T10:00:10Z"' \
  --result-file /tmp/rekep-books.json
BOOK_SNAPSHOT=$(python -c 'import json; print(json.load(open("/tmp/rekep-books.json"))["snapshot_id"])')
pids=()
for KIND in orders quotes executions; do
  uv run --project python rekep tasks "parse_${KIND}" run \
    --parameter 'start="2026-09-21T10:00:00Z"' --parameter 'end="2026-09-21T10:00:10Z"' \
    --parameter "snapshot_id=$BOOK_SNAPSHOT" &
  pids+=("$!")
done
status=0
for pid in "${pids[@]}"; do
  wait "$pid" || status=1
done
test "$status" -eq 0
```

All three readers now use the same source snapshot even if another writer
advances `market.books`. The shell checks every child process's exit status; Airflow manages their
failures separately when scheduled. A standalone task
with `snapshot_id=null` resolves one current snapshot for itself; it does not
promise the same one another independently started task resolves. Zero means
an empty source with no head.

Market time filtering is strict `[start, end)` on both source and output;
unresolved epoch rows are not included automatically. Book creation starts
without pre-window depth and does not rerun refined lifecycle. Orders and
quotes flatten deltas; executions flatten native execution leaves. See the
[book task](../tasks/parse-books.md) for these limits.

## One window

The three original ingestion tasks cover one window, `[start, end)`. Each bound is an
instant or a date -- a date as `end` is the end of that day -- and a task
given neither takes the last day up to now, which is the window a nightly run
means. The sample capture is dated 2026-08-14, so the runs above name that
day; a run without the two parameters covers the last day, which the lines
the header dates fall outside, so it reads none of them -- only a line the
header did not match, which the file's own modification time dates, could
fall in it. `parse_messages` and `parse_fix_raw` read the window off
`currunix`, the event clock, so the two are run over the same one: the text
read settles it over a line, off the header's `mtime` capture or off the
modification time of the object a line the header did not match was read
from, and the window is the two bounds and nothing else -- the epoch, which
dates a line only where its handle has no clock at all, is in the window that
covers 1970. `parse_fix_refined` reads it off the same
column -- a `fix.raw` row is already an event, dated by what its message
stated -- and the rows the parse could not date, which sit at the codec's
pin, by the `TransactTime` the walk dates them with: a day's run walks the
day's events, dated or pinned, so it is run over the same window again.

## One override

`--parameter NAME=VALUE` reads VALUE as JSON first, then plain text. Quote JSON
strings so a URI is not mistaken for syntax:

```bash
uv run --project python rekep tasks parse_messages run \
  --parameter 'filesystem="file:/srv/capture/2026-08-14"'
```

Point the FIX parse at a candidate dictionary:

```bash
uv run --project python rekep tasks parse_fix_raw run \
  --parameter 'registry="file:///srv/fix"'
```

`parse_fix_refined` takes the same `registry`, and the walk reads each row
back with the dictionary that wrote it, so a candidate is given to both.
There is no version left to pin: what a message was read at is what its own
`beginstring` said, and native `FixCodec` validates every keyword it receives.
`codec_options: null` delegates native defaults; an object is forwarded
unchanged. Useful pins include `batch_row_size`, `include_msgtypes`,
`exclude_msgtypes`, `threads`, `official_time_delay_ms`, and `snapshot_ns`.
`version` is no longer a task parameter, so a `--parameter 'version="4.4"'`
names nothing the task declares, and the CLI and Airflow's operator both
refuse it before anything runs:
`parse_fix_raw takes no version; it takes messages, registry, codec_options, start, end, catalog`.
There is no switch on the walk: the parsed rows without their chains are
`fix.raw`, which `parse_fix_raw` publishes on its own.

## Parameters file

Use a file for nested catalog settings and keep secrets out of it:

```json
{
  "catalog": {
    "name": "rekep",
    "properties": {
      "type": "glue",
      "warehouse": "s3://market-warehouse/rekep",
      "glue.region": "eu-west-1",
      "s3.region": "eu-west-1"
    }
  }
}
```

```bash
uv run --project python rekep tasks parse_messages run \
  --parameters-file /run/rekep/aws.json \
  --parameter 'filesystem="s3://market-capture/ulbridge/2026/08/14?region=eu-west-1"'
```

Precedence is the shipped defaults, then the parameters file, then each
`--parameter`. An override replaces a parameter whole, so a `catalog` override
is the whole mapping, and a name the task does not declare is refused. `show`
takes the same options and prints what the run would take. Airflow uses the
shipped defaults, operator parameters, DAG-run Params, then the run's data
interval for `start` and `end`, unless the run's conf names them.

## Capture a result

```bash
uv run --project python rekep tasks parse_fix_raw run \
  --result-file /run/rekep/parse-fix-raw-result.json
```

The result file is published atomically. It contains counts and locations,
never table rows. A non-zero exit means no valid result was published.

## Replay

For the three original ingestion stages, run the same commands again over
the same window. All three stages
read the same rows, land them over the rows the first run landed, and report
them as written: the table holds each line and each event once, and the
replay is one more snapshot per table. Running a window again after a
registry or parser change is therefore the rebuild -- the new reading of every
message lands over the old one on the same `curruuid` key. A reading that
changes an event's `curruuid` is a new key, and the old row stays: delete the
window first, or use a new target table, when the identity itself changes.
The walk re-settles the identity of a message it dates, which is why
`fix.refined` is written from `fix.raw` and never in place: a walked row's
key is not always the key of the parsed row it restates, and a walk landing
over the parsed rows would leave the old identity beside the new one.


The four market tasks use exact window replacement instead: old rows matching
`[start, end)` and the new staged rows publish in one snapshot. Changed native
identities or events absent on rerun cannot leave stale rows inside that
window. Empty output clears it, outside rows survive, and failures leave the
previous committed state visible. Rerun the fan-out against the new book
snapshot after a successful book replacement.
