# Parse market

`tasks/parse_market/parse_market.ipynb` reads
`fix.market` in `(unix, msgseqnum, hash)` order. With the default
`books: true`, it folds that stream through `BookIterator`, writes only
`market.books`, and leaves the two flatten notebooks to publish orders and
executions.

## Run this step

After `parse_fix` has populated `fix.market`, run the configured book mode from
the repository root:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_market/parse_market.yml \
  --output parse_market.executed.ipynb
```

The package, a FIX registry and a catalog have to exist first:
[deploy from scratch](../operations/deploy.md).

Bypass Books and write FIX-carried Orders and Executions directly with:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_market/parse_market.yml \
  --parameter books=false \
  --output parse_market.direct.executed.ipynb
```

With `books: false`, it does not construct or write Books.
`FixMsg.into_market_arrow_batches()` adapts the input stream into bounded,
typed Order and Execution batches for `order_target` and `execution_target`.
Either direct target may be null, but at least one is required.

Flat messages use the selected FIX dictionary's dispatch, field tags, and
lifecycle mappings through Arrow kernels. Repeating groups and uncommon or
custom shapes fall back to the same scalar translator without changing output.

Direct rows are exactly the events carried by FIX: the mode does not create
book snapshots, book-generated expiry or rejection rows, lifecycle completion
from resting state, or a carrying `Book.hash` parent.

Use empty direct targets, or targets dedicated to this mode, when switching an
existing deployment: merge-by cannot remove older book-normalized rows whose
hashes differ from their raw FIX versions.

For a bounded interval the reader scans a wider source range, then filters
each emitted Book or event back to `[start, end)`. The scan starts at the hour
containing `start` minus one hour and stops after the hour containing `end`
plus 15 minutes. In book mode, prior Book snapshots use that recovery history
to restore live orders.

`parse_market` never reads `market.instruments`. `BookIterator` translates the
sorted `fix.market` stream and uses the transient Instrument facts carried by
each market event.

Snapshot generation, terminal-state handling, one-day inactivity expiry, and
internal rejection reasons belong to the shared event and book models rather
than the notebook. Direct mode skips captured Instrument events and keeps the
instrument facts carried by each translated FIX message.

The adjacent `parse_market.yml` sets the FIX dictionary, mode, all three
targets, snapshot cadence, lateness, live-order age, side bound, catalog, and
commit size. The same dictionary controls both direct and book translation.
Switching only `books` to `false` selects the configured direct targets.

The result's `read` mapping reports every attempted Book, Order, and Execution.
In book mode the Order and Execution counts come from the selected Books'
nested deltas; in direct mode they are the translated rows written here.

The separate `flatten` mapping is downstream work: it carries those nested
counts in book mode and stays zero in direct mode, where no flatten task is
needed. A positive `commit_row_size` bounds direct-event Arrow batches; zero
retains the explicit whole-stream atomic-commit behavior and drains each event
type only at the end.
