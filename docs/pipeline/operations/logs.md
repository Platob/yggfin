# Logs and task results

Every application uses `rekep.logs.Stage`. Human-readable lifecycle records go
to stderr; one machine-readable result goes to stdout and, when requested, an
atomic result file.

## Result schema

| field | type | meaning |
| --- | --- | --- |
| `task` | string | stage name |
| `read` | integer | source rows consumed |
| `written` | integer | target rows the run carried into the table |
| `skipped` | integer | what a run had and did not write: a restatement of a key the write folded, or a row the write left out; the window is pushed into every read, so nothing is read and then found outside it |
| `sources` | object | logical source names to masked locations |
| `targets` | object | logical target names to table identifiers |
| `window` | object | `start` and `end`, each epoch nanoseconds or null |
| `elapsed_ms` | integer | wall-clock stage duration |

Anything else a task knows keeps its own name beside those fields.
`parse_fix_raw` returns one such key, `messages`: a row is a message and
not a line, so what the codec answered is counted separately from the lines
it was handed, and its `skipped` is counted against those messages -- a
restatement of an identity the key already folded -- rather than against the
lines. `parse_fix_refined` returns nothing beside the contract: the walk
answers one row per event, folding every observation of it. `build_dbt` returns `models`, `tests` and
`rows`, because its unit of work is a dbt node: `read` and `skipped` count
nodes there, and `written` and `rows` count the rows its models committed.

The closing INFO record and returned JSON agree on every field. A result is
small enough for Airflow XCom because it contains no rows or schemas.

## Example

```json
{
  "task": "parse_fix_raw",
  "read": 144,
  "written": 49,
  "skipped": 30,
  "sources": {"messages": "logs.messages"},
  "targets": {"raw": "fix.raw"},
  "window": {"start": 1786665600000000000, "end": 1786752000000000000},
  "elapsed_ms": 208,
  "messages": 79
}
```

`window` is always an object. A streaming task reports the window it ran, as
epoch nanoseconds -- the bounds it was given, or the last day up to now when
it was given none; `build_dbt` declares no window and reports the open one,
`{"start": null, "end": null}`. It is never `null`, and
`Stage.validated` -- which both the runner and the operator call before a
result is published or pushed to XCom -- refuses anything that is not a
mapping of exactly `start` and `end`.

## Monitoring rules

- `parse_messages` reads the lines its window covers and no other, so its
  `read` is that count and its `skipped` is 0 unless the write skipped; a
  window the capture falls outside reads 0. Two lines that repeat another
  byte for byte are two rows under two codes, because a line's
  `currhashcode` digests the object it was read from and its row number
  beside its body.
- A replay of the same window reports the same numbers as the run it repeats:
  what a run writes is what it carried, and the table holds each row once
  either way. A run whose `read` fell is one whose window covers fewer
  lines.
- `parse_fix_raw.read` should equal the selected `logs.messages` row count,
  and its own `messages` key what the codec answered: a line carries none, one
  or several messages. `written` is the events those messages settled on.
- `parse_fix_refined.read` should equal the `fix.raw` rows of the window and
  the hour before it, dated or at the pin, and `written` what the walk
  restated: one row per event it folded, under identities that need not be
  the parsed rows'.
- A successful zero-row run is not a failure.
- Missing result JSON, non-zero child exit, or mismatched task name fails the
  operator.

Airflow attaches `task`, `read`, `written`, and `skipped` to each emitted Asset
event. Attempt parameter files may contain catalog details, so the operator
creates them mode `0600` in a private directory and removes the directory on
success, failure, or kill.
