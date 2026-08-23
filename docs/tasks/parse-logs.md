# Parse logs

`tasks/parse_logs/parse_logs.ipynb` streams text files
through `TextFile` or `TextFiles`, categorizes each parsed `Log`, and fans out
one Arrow pass to:

- `logs.market` for order, quote, execution, book, and instrument traffic;
- `logs.misc_logs` for recognized operational traffic;
- `logs.unknown_logs` for unmatched formats.

The adjacent `parse_logs.yml` selects
the capture, rules, offline FIX registry, catalog, batch size, commit size, and
optional `[start, end)` interval. Unknown lines are retained. Replays use the
declared `Log` key when `merge_by` is enabled.

After the raw append, the notebook reads the prior interval's latest
`market.instruments` snapshots and the newly sorted `logs.market` slice. It
versions and snapshots instrument facts once, then appends them to
`logs.market` as normalized `etype=INSTRUMENT` rows. Their lifecycle envelope
and registry-shaped ordered pairs round-trip directly between `Instrument`
and `Log`.

Input is a text file, folder, or supported Arrow filesystem URI. Output is
only categorized `Log` tables; the Instrument table is read-only recovery
state from a completed earlier interval.
