# Parse FIX

One task document runs the shared application for one category. Airflow injects
`market`, `misc`, and `unknown` into three parallel runs; each scans only its
category and writes one typed `FixMsg` table.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml --parameter category=market
```

The step is `tasks/parse_fix/parse_fix.py`; `parse_fix.yml` configures it, and
`category` is the only value that differs between its three runs.

Run every category in parallel:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml --parameter category=market &

uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml --parameter category=misc &

uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml --parameter category=unknown &

wait
```

Add the same half-open replay interval to every task when needed:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml \
  --parameter category=market \
  --parameter start=2026-08-21T10:00:00Z \
  --parameter end=2026-08-21T11:00:00Z
```

Deploy the catalog first: [deploy from scratch](../operations/deploy.md).

## Category scans

```text
logs.messages -+-> market predicate  -> transcribe -> fix.market
               +-> misc predicate    -> transcribe -> fix.misc
               `-> unknown predicate -> transcribe -> fix.unknown
```

The predicates are mutually exclusive and execute in Iceberg before FIX
transcription:

| Category run | Selection | Target |
| --- | --- | --- |
| `parse_fix_market` | `eventtype` ranked at least `INTENT` | `fix.market` |
| `parse_fix_misc` | not market, and either `eventtype == MISC` or a configured protocol | `fix.misc` |
| `parse_fix_unknown` | not market, not `MISC`, and no configured protocol | `fix.unknown` |

MsgTypes in `exclude_msgtypes` are removed before those predicates. A null
MsgType remains eligible for best-effort transcription. Only
`parse_fix_market` has downstream consumers; the other two tables are
terminal audit products.

## Transcription

A market row keeps filterable values flat and the reference component nested:

```yaml
protocol: FIX4.4
msgtype: D
unix: 1787306400123000000
unixsource: TransactTime
creaunix: 1787306399000000000
expunix: 1787308200000000000
creationtime: 2026-08-21T09:59:59Z
expiretime: 2026-08-21T10:30:00Z
sendingtime: 2026-08-21T09:59:59Z
instrument:
  symbol: TTF
  securityexchange: XPAR
entries:
  - {tag: 60, key: TransactTime, value: "20260821-10:00:00.123"}
unmap: null
```

`protocol` carries the protocol and resolved version. Repeated tags and
unpromoted fields remain in `entries`; unknown names move to `unmap`. A bad
field fills the row's `error` and the remaining fields are transcribed on a
best-effort basis.

## Task parameters

The one task document owns the shared parsing and commit rules:

```yaml
fix_dictionary: null # The repository's data/fix; set a path or URL to override it.
null_values: ["", "null", "<null>", "n/a", "none"]
exclude_msgtypes: ["0", "1"] # Heartbeat and TestRequest stay in logs.messages.
protocols: null
fields: null
commit_batch_num: 8
commit_row_size: null # Optional earlier row cap.
```

Custom field rules use the same shape:

```yaml
fields:
  rules:
    - field: "9999"
      type: timestamp[us]
    - field: Side
      values: {BUYSIDE: "1", SELLSIDE: "2"}
```

FIX dates, times and timestamps are stored as `timestamp[us]`; date-only
values land at midnight. An evidence-free UL row uses the selected registry's
newest application version. Keep `fix_dictionary` and custom protocol rules
aligned with `parse_messages.yml`.

Each category run buffers at most eight input RecordBatches before a storage commit.
The source interval uses the recording clock; the resulting `unix` uses the
best FIX clock. Consumers request event ordering when needed, so the FIX tasks
do not add a physical write sort.
