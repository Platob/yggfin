# Parse instruments

`parse_instruments` versions the nested `Instrument` components in
`fix.market` into `market.instruments`.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_instruments/parse_instruments.yml \
  --output parse_instruments.executed.ipynb
```

Deploy the catalog first: [deploy from scratch](../operations/deploy.md).

## Version contract

```yaml
source: fix.market
target: market.instruments
batch_row_size: 65536
commit_row_size: 250000
```

`InstrumentUpdate.versioned` applies one rule:

```text
same vhash                   -> no write
observation adds facts       -> enriched replacement under the same xhash
observation is less complete -> no write
```

`code` is the exact `symbolticker` and `codesource` is `SymbolTicker`.
Later reference facts keep the same sixteen-byte `xhash`. Each batch finds
the current row by its bounded set of codes, then overwrites by the declared
`xhash` primary key. A replay that changes nothing commits no snapshot.

The `[start, end)` interval uses `FixMsg.unix`. Rows with a non-null `error`
remain in `fix.market` for audit and are excluded here. Keep `fix_dictionary`
aligned with `parse_fix.yml`; it resolves identifiers still present in
residual entries. Airflow runs this task beside `parse_market` because both
only read `fix.market`.
