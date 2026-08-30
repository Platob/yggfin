# Parse instruments

`tasks/parse_instruments/parse_instruments.ipynb`
reads the checked FIX market messages `parse_fix` wrote and versions the
reference data they carry into `market.instruments`. One row per canonical
`symbolticker`, replaced when an observation adds a fact the stored record
does not hold.

## Run this step

After `parse_fix` has populated `fix.market`, from the repository root:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_instruments/parse_instruments.yml \
  --output parse_instruments.executed.ipynb
```

The package, a FIX registry and a catalog have to exist first:
[deploy from scratch](../operations/deploy.md).

## What a version is

The rule is `rekep.market.versioned`, in the package rather than in this
notebook, so the answer does not depend on which job writes the table:

- an observation whose `vhash` matches the stored record states the same
  facts and writes nothing;
- anything else is the **stored record enriched** with what the observation
  adds — earlier facts kept, a new `hash` under the same `symbolticker`;
- an observation that adds nothing to a fuller stored record is dropped
  rather than written back thinner.

`xhash` is the digest of `symbolticker` alone, so a version never moves the
lifecycle a join points at. Reference data learnt later — a tick size, a
maturity — is not part of the key, deliberately.

The write is an overwrite keyed by `symbolticker`, not an append: a ticker
holds one row and a version replaces it. A window that changed nothing
commits no snapshot at all, which is what makes a replay free.

## Reading the stored table back

Observations arrive in batches of `batch_row_size`. Each batch's tickers are
looked up in one `IN` predicate against the table, so the lookup is bounded by
the batch rather than by the table — and the predicate is on the primary key.

## Configuration

The adjacent `parse_instruments.yml` selects the source, the target, the
catalog, branch, `[start, end)` interval, and both batch sizes. Keep its
`fix_dictionary` aligned with `parse_fix.yml`: the registry is what resolves
identifiers out of residual entries.

The interval is read off `FixMsg.unix` — the transaction clock `parse_fix`
resolved and the column `fix.market` is sorted and partitioned on. This stage
asks the FIX stage for nothing but its table, so a rule change here reruns
against captures already parsed.

Airflow runs this task and `parse_market` side by side, on the same
`routed.market` count: both read `fix.market` and neither writes what the
other reads.
