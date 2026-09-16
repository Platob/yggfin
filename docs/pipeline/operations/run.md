# Run tasks

`rekep task run` loads one task JSON document, resolves its adjacent Marimo
application, replaces the `parameters` cell, validates the returned stage
result, and writes one compact JSON result to stdout. Logs and tracebacks go to
stderr.

## Whole pipeline

```bash
uv run --project python rekep task run tasks/parse_messages/parse_messages.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep task run tasks/parse_fix/parse_fix.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep task run tasks/build_dbt/build_dbt.json
```

The order is required: `parse_fix` reads `logs.messages` rather than source
files, and [`build_dbt`](../tasks/build-dbt.md) reads `fix.messages` rather
than either.

## One window

The two streaming tasks parse one window, `[start, end)`. Each bound is an
instant or a date -- a date as `end` is the end of that day -- and a task
given neither takes the last day up to now, which is the window a nightly run
means. The sample capture is dated 2026-08-14, so the runs above name that
day; a run without the two parameters would read every line and write none,
because none falls in the last day. `parse_fix` reads the stored rows of the
window it is given, so the two stages are run over the same one.

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
  tasks/parse_fix/parse_fix.json \
  --parameter 'registry="file:///srv/fix"'
```

There is no version left to pin: what a message was read at is what its own
`beginstring` said, and `fix_codec` refuses by name any keyword that is not one
of its seven pins. `version` is no longer a task parameter either, so a
`--parameter 'version="4.4"'` names nothing the document declares — the CLI
carries it into an unused definition and Airflow's operator fails the task
outright. To publish parsed and enriched rows without naming the event chains,
turn the last stage off instead:

```bash
uv run --project python rekep task run \
  tasks/parse_fix/parse_fix.json \
  --parameter 'lifecycle=false'
```

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
  tasks/parse_fix/parse_fix.json \
  --result-file /run/rekep/parse-fix-result.json
```

The result file is published atomically. It contains counts and locations,
never table rows. A non-zero exit means no valid result was published.

## Replay

Run the same two commands again, over the same window. Both stages read the
same rows, land them over the rows the first run landed, and report them as
written: the table holds each line and each message once, and the replay is
one more snapshot. Running a window again after a registry or parser change
is therefore the rebuild -- the new reading of every message lands over the
old one on the same `(sourceurl, rownum, msghash)` key. A reading that changes
a message's `msghash` is a new key, and the old row stays: delete the window
first, or use a new target table, when the identity itself changes.
