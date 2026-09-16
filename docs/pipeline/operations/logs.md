# Logs and task results

Every application uses `rekep.logs.Stage`. Human-readable lifecycle records go
to stderr; one machine-readable result goes to stdout and, when requested, an
atomic result file.

## Result schema

| field | type | meaning |
| --- | --- | --- |
| `task` | string | stage name |
| `read` | integer | source rows consumed |
| `written` | integer | new target rows committed |
| `skipped` | integer | read rows not inserted |
| `sources` | object | logical source names to masked locations |
| `targets` | object | logical target names to table identifiers |
| `window` | object | `start` and `end`, each epoch nanoseconds or null |
| `elapsed_ms` | integer | wall-clock stage duration |

Anything else a task knows keeps its own name beside those fields. `parse_fix`
returns one such key, `messages`: a row is a message and not a line, so what
the codec answered is counted separately from the lines it was handed.
`build_dbt` returns `models`, `tests` and `rows`, because its unit of work is a
dbt node: `read` and `skipped` count nodes there, and `written` and `rows`
count the rows its models committed.

The closing INFO record and returned JSON agree on every field. A result is
small enough for Airflow XCom because it contains no rows or schemas.

## Example

```json
{
  "task": "parse_fix",
  "read": 111,
  "written": 71,
  "skipped": 0,
  "sources": {"messages": "logs.messages"},
  "targets": {"fix": "fix.messages"},
  "window": {"start": null, "end": null},
  "elapsed_ms": 208,
  "messages": 71
}
```

`window` is always an object. A task that declares no interval reports the
open one, `{"start": null, "end": null}`; it is never `null`, and
`Stage.validated` — which both the runner and the operator call before a
result is published or pushed to XCom — refuses anything that is not a
mapping of exactly `start` and `end`.

## Monitoring rules

- A first immutable-capture run normally has `read == written`.
- A complete replay normally has `read == skipped` and `written == 0` — except
  in `parse_fix`, which counts messages and not lines, where a complete replay
  has `skipped == messages` and `written == 0`.
- `parse_fix.read` should equal the selected `logs.messages` row count, and
  its own `messages` key what the codec answered: a line carries none, one or
  several messages.
- A successful zero-row run is not a failure.
- Missing result JSON, non-zero child exit, or mismatched task name fails the
  operator.

Airflow attaches `task`, `read`, `written`, and `skipped` to each emitted Asset
event. Attempt parameter files may contain catalog details, so the operator
creates them mode `0600` in a private directory and removes the directory on
success, failure, or kill.
