# Flatten instruments

`tasks/flatten_instruments/flatten_instruments.ipynb`
reads sorted `logs.market` rows and writes `market.instruments`.

`parse_logs` already created, enriched, versioned, and snapshotted every
normalized Instrument lifecycle row. This notebook filters those rows by
`etype` and the internal driver marker, converts each `Log` back to its exact
`Instrument`, and appends the flat audit table. It does not parse FIX or
version the lifecycle a second time.

The adjacent `flatten_instruments.yml`
selects the interval, catalog, and commit size. The notebook runs independently
after `parse_logs`.
