# Parse instruments

`parse_instruments` versions the nested `Instrument` components in
`fix.market` into `market.instruments`.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_instruments/parse_instruments.yml
```

`tasks/parse_instruments/parse_instruments.py` is the application;
`parse_instruments.yml` beside it holds the parameters.

Deploy the catalog first: [deploy from scratch](../operations/deploy.md).

## Version contract

```yaml
source: fix.market
target: market.instruments
batch_row_size: 65536
commit_batch_num: 8
commit_row_size: null # Optional earlier row cap.
```

`InstUpdate.versioned` applies one rule:

```text
same vhash                   -> no write
observation adds facts       -> enriched replacement under the same xhash
observation is less complete -> no write
```

`code` is the exact `symbolticker`; `altids` retains it under both names.
Later reference facts keep the same sixteen-byte `xhash`. Each batch finds
the current row by its bounded set of codes, then overwrites by the declared
`xhash` primary key. A replay that changes nothing commits no snapshot.

The `[start, end)` interval uses `FixMsg.unix`. Rows with a non-null `error`
remain in `fix.market` for audit and are excluded here. Keep `fix_dictionary`
aligned with `parse_fix.yml`; it resolves identifiers still present in
residual entries. Airflow runs this task beside `parse_market` because both
only read `fix.market`.
