# Run tasks

`rekep task run` loads one task JSON document, resolves its adjacent Marimo
application, replaces the `parameters` cell, validates the returned stage
result, and writes one compact JSON result to stdout. Logs and tracebacks go to
stderr.

## Whole pipeline

```bash
uv run --project python rekep task run tasks/parse_messages/parse_messages.json
uv run --project python rekep task run tasks/parse_fix/parse_fix.json
```

The order is required: `parse_fix` reads `logs.messages` rather than source
files.

## One override

`--parameter NAME=VALUE` reads VALUE as JSON first, then plain text. Quote JSON
strings so a URI is not mistaken for syntax:

```bash
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameter 'filesystem="file:/srv/capture/2026-08-14"'
```

Pin FIX translation to a version:

```bash
uv run --project python rekep task run \
  tasks/parse_fix/parse_fix.json \
  --parameter 'version="4.4"' \
  --parameter 'dedup=false'
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
then data-interval values for tasks that declare them.

## Capture a result

```bash
uv run --project python rekep task run \
  tasks/parse_fix/parse_fix.json \
  --result-file /run/rekep/parse-fix-result.json
```

The result file is published atomically. It contains counts and locations,
never table rows. A non-zero exit means no valid result was published.

## Replay

Run the same two commands again. Both stages read the same number of rows,
report those rows as skipped, write zero, and create no empty Iceberg snapshot.
Changing the registry or parser while retaining `(url, rownum)` is a controlled
rebuild: use a new target table or an explicit overwrite procedure when typed
rows must be replaced rather than skipped.
