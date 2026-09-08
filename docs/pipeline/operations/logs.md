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
| `window` | object or null | scheduler interval where declared |
| `elapsed_ms` | integer | wall-clock stage duration |

The closing INFO record and returned JSON agree on every field. A result is
small enough for Airflow XCom because it contains no rows or schemas.

## Example

```json
{
  "task": "parse_fix",
  "read": 111,
  "written": 111,
  "skipped": 0,
  "sources": {"messages": "logs.messages"},
  "targets": {"fix": "fix.messages"},
  "window": null,
  "elapsed_ms": 208
}
```

## Monitoring rules

- A first immutable-capture run normally has `read == written`.
- A complete replay normally has `read == skipped` and `written == 0`.
- `parse_fix.read` should equal the selected `logs.messages` row count when
  `dedup=false`.
- A successful zero-row run is not a failure.
- Missing result JSON, non-zero child exit, or mismatched task name fails the
  operator.

Airflow attaches `task`, `read`, `written`, and `skipped` to each emitted Asset
event. Attempt parameter files may contain catalog details, so the operator
creates them mode `0600` in a private directory and removes the directory on
success, failure, or kill.
