# Parse FIX

`tasks/parse_fix/parse_fix.ipynb` streams text files
through `TextFile` or `TextFiles`, categorizes each parsed `FixMessage`, and fans out
one Arrow pass to:

- `fixmessage.market` for order, quote, execution, book, and instrument traffic;
- `fixmessage.misc` for recognized operational traffic;
- `fixmessage.unknown` for unmatched formats.

The adjacent `parse_fix.yml` selects
the capture, rules, offline FIX registry, catalog, batch size, commit size, and
optional `[start, end)` interval. Unknown lines are retained. Replays use the
declared `FixMessage` key when `merge_by` is enabled.

After the raw append, the notebook reads the prior interval's latest
`market.instruments` snapshots and the newly sorted `fixmessage.market` slice. It
versions and snapshots instrument facts once, then appends them to
`fixmessage.market` as normalized `etype=INSTRUMENT` rows. Their lifecycle envelope
and registry-shaped ordered pairs round-trip directly between `Instrument`
and `FixMessage`.

Input is a text file, folder, or supported Arrow filesystem URI. Output is
only categorized `FixMessage` tables; the Instrument table is read-only recovery
state from a completed earlier interval.
