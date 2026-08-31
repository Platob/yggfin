# Parse FIX

`parse_fix` transcribes `logs.messages` through the FIX registry and writes
typed `FixMsg` rows.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml \
  --output parse_fix.executed.ipynb
```

Add a half-open replay interval when needed:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml \
  --parameter start=2026-08-21T10:00:00Z \
  --parameter end=2026-08-21T11:00:00Z \
  --output parse_fix.executed.ipynb
```

Deploy the catalog first: [deploy from scratch](../operations/deploy.md).

## Transcription

```text
logs.messages
  -> resolve protocol, tags, values and components
  -> derive transaction and creation clocks
  -> nest Instrument and repeating groups
  -> fix.market | fix.misc | fix.unknown
```

A market row keeps filterable values flat and the reference component nested:

```yaml
protocol: FIX4.4
msgtype: D
unix: 1787306400123000000
unixsource: TransactTime
creaunix: 1787306399000000000
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

## Rules

The adjacent task document owns estate-specific readings:

```yaml
fix_dictionary: data/fix
null_values: ["", "null", "<null>", "n/a"]
fields:
  rules:
    - field: "9999"
      type: timestamp[us]
    - field: Side
      values: {BUYSIDE: "1", SELLSIDE: "2"}
```

FIX dates, times and timestamps are stored as `timestamp[us]`; date-only
values land at midnight. Keep `fix_dictionary` and custom protocol rules
aligned with `parse_messages.yml`.

Market event codes route to `fix.market`. Known non-market traffic routes to
`fix.misc`; an unknown event on an unknown protocol routes to `fix.unknown`.
The source scan uses recording time, while output `unix` uses the best FIX
clock and each output table sorts by `hash`.
