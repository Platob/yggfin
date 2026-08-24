# Parse market

`tasks/parse_market/parse_market.ipynb` reads
`fixmessage.market` in `(unix, msg_seq_num, hash)` order and folds it through
`BookIterator`.
It writes only `market.books`.

For a bounded interval the reader includes recovery history before `start` and
late input after `end`, then writes only `[start, end)`. The scan starts at the
hour containing `start` minus one hour and stops after the hour containing
`end` plus 15 minutes. Prior Book snapshots restore live orders.

`parse_market` never reads `market.instruments`. Normalized instrument
lifecycle rows already share the sorted `fixmessage.market` input; `BookIterator`
indexes them by `etype` and folds the remaining rows. Snapshot generation,
terminal-state handling, one-day inactivity expiry, and internal rejection
reasons belong to the shared event and book models rather than the notebook.

The adjacent `parse_market.yml`
sets snapshot cadence, lateness, live-order age, side bound, catalog, and
commit size.
