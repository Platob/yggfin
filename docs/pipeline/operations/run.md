# Run tasks

`rekep task run` loads one task JSON document, resolves its adjacent Marimo
application, replaces the `parameters` cell, validates the returned stage
result, and writes one compact JSON result to stdout. Logs and tracebacks go to
stderr.

## Whole pipeline

```bash
uv run --project python rekep task run tasks/parse_messages/parse_messages.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep task run tasks/parse_fix_bronze/parse_fix_bronze.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep task run tasks/parse_fix_silver/parse_fix_silver.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep task run tasks/build_dbt/build_dbt.json
```

The order is required: `parse_fix_bronze` reads `logs.messages` rather than
source files, `parse_fix_silver` reads `fix.bronze` rather than either, and
[`build_dbt`](../tasks/build-dbt.md) reads `fix.silver` and nothing before
it.

## One window

The three streaming tasks cover one window, `[start, end)`. Each bound is an
instant or a date -- a date as `end` is the end of that day -- and a task
given neither takes the last day up to now, which is the window a nightly run
means. The sample capture is dated 2026-08-14, so the runs above name that
day; a run without the two parameters would read every line and write none,
because none falls in the last day. `parse_messages` and `parse_fix_bronze`
read the window off the capture clock, so the two are run over the same one.
`parse_fix_silver` reads it off the event clock `currunix` -- a bronze row is
already an event, dated by what its message stated -- and the rows the parse
could not date, which sit at the codec's pin, by the `TransactTime` the walk
dates them with: a day's run walks the day's events, dated or pinned, so it is
run over the same window again.

## One override

`--parameter NAME=VALUE` reads VALUE as JSON first, then plain text. Quote JSON
strings so a URI is not mistaken for syntax:

```bash
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameter 'filesystem="file:/srv/capture/2026-08-14"'
```

Point the FIX parse at a candidate dictionary:

```bash
uv run --project python rekep task run \
  tasks/parse_fix_bronze/parse_fix_bronze.json \
  --parameter 'registry="file:///srv/fix"'
```

`parse_fix_silver` takes the same `registry`, and the walk reads each row
back with the dictionary that wrote it, so a candidate is given to both.
There is no version left to pin: what a message was read at is what its own
`beginstring` said, and `fix_codec` refuses by name any keyword that is not one
of its seven pins. `version` is no longer a task parameter either, so a
`--parameter 'version="4.4"'` names nothing the document declares -- the CLI
carries it into an unused definition and Airflow's operator fails the task
outright. There is no switch on the walk: the parsed rows without their
chains are `fix.bronze`, which `parse_fix_bronze` publishes on its own.

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
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameters-file /run/rekep/aws.json \
  --parameter 'filesystem="s3://market-capture/ulbridge/2026/08/14?region=eu-west-1"'
```

Precedence is task defaults, parameters file, then repeated command-line
parameters. Airflow uses task defaults, operator parameters, DAG-run Params,
then the run's data interval for `start` and `end`, unless the run's conf
names them.

## Capture a result

```bash
uv run --project python rekep task run \
  tasks/parse_fix_bronze/parse_fix_bronze.json \
  --result-file /run/rekep/parse-fix-bronze-result.json
```

The result file is published atomically. It contains counts and locations,
never table rows. A non-zero exit means no valid result was published.

## Replay

Run the same three commands again, over the same window. All three stages
read the same rows, land them over the rows the first run landed, and report
them as written: the table holds each line and each event once, and the
replay is one more snapshot per table. Running a window again after a
registry or parser change is therefore the rebuild -- the new reading of every
message lands over the old one on the same `curruuid` key. A reading that
changes an event's `curruuid` is a new key, and the old row stays: delete the
window first, or use a new target table, when the identity itself changes.
The walk re-settles the identity of a message it dates, which is why
`fix.silver` is written from `fix.bronze` and never in place: a silver key is
not always its bronze twin's, and a walk landing over the parsed rows would
leave the old identity beside the new one.
