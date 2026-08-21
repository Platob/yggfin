# Market

`rekep.market` is a set of declarations for what happens in a market — an
order, an execution, a side of a book, a book — written as a **history rather
than a state**. Nothing is ever updated in place: every version of every thing
is its own immutable row, keyed by the sixteen bytes of its own content and
linked to the version before it.

```python
from rekep.market import Book, BookSide, Execution, Instrument, Order, State

Order.FIELD.into_arrow_schema()        # what the data is
Order.FIELD.into_iceberg_schema()      # the same thing, in Iceberg's terms
Order.FIELD.primary_keys()             # ['unix', 'h128']
```

They are ordinary [`@field` declarations](types.md), so everything the rest of
this package does with a shape works here: casting a nearly-right batch onto
it, publishing it as a [contract](contracts.md), creating an
[Iceberg dataset](iceberg.md) from it.

## The envelope

Every shape here is an `Event`, and the envelope is what turns a stream of rows
into a history.

=== "One thing, many versions"

    ```python
    order.xh128        # the order, across every amendment of it
    order.h128         # this version of it
    order.version      # 0, 1, 2, ... within the lifecycle
    order.state        # where the lifecycle stands
    ```

    `xh128` is the thing and `h128` is the version. An order amended four
    times is four rows sharing one `xh128`, each with its own `h128`. That is
    what lets the store be append-only, and what lets a reader ask what was
    known at any moment instead of only what is true now.

    `h128` is a digest of what the event **says**, never of when it was
    recorded — so the same version arriving down two feeds is one row with one
    key, and re-reading yesterday's capture writes nothing.

=== "Four clocks"

    ```python
    event.unix         # when it happened          -- and the only one in the key
    event.cunix        # when it was created, upstream
    event.runix        # when it was written down here
    event.eunix        # when it stops being true  -- an expiry, or None
    ```

    Separate because they answer different questions and disagree constantly:
    by microseconds on a good day, and by hours when something upstream has
    gone wrong. All four are whole nanoseconds since the epoch as `int64`, like
    [`Log.recorded_at_unix`](logs.md) and for the same reason — a timestamp
    width or zone that a downstream is picky about is a conversion per row.

    `date` is `unix` as a calendar day, denormalised, and it is what the data
    is partitioned on.

=== "The version before"

    ```python
    event.prev_h128    # the version this one replaced
    event.prev_state   # the state it moved out of
    event.prev_unix    # when that was
    ```

    Three columns, so "what changed" and "how long did it sit there" are read
    from one row rather than joined out of a window function over the whole
    lifecycle — which is a shuffle of the table for a question asked once a
    tick.

    `parent_h128` is the other direction: every event this one was built from,
    as a list. A book has two parents, a spread has as many legs as it has.

## Codes

A state, a side, a validity and a kind are each stored as an `int32` and read
back through a `Ranged` enum. The split is the point.

=== "The column is the integer"

    ```python
    from rekep.market import State

    State.from_code(410)         # State.FILLED
    State.from_code(440)         # State.DONE   -- a code from a later release
    State.from_code(99999)       # State.UNKNOWN
    ```

    A code this build has never seen is still stored, compared and partitioned
    like one it knows, and `from_code` degrades it to its **band** rather than
    raising. The floor of every band is itself a member, so the degradation
    lands on something true: a state added at 440 is still terminal here.

=== "The bands are ordered"

    ```python
    State.OPEN                   # 200  -- live
    State.TERMINAL               # 400  -- and everything from here up is over
    State.FILLED.is_terminal     # True
    ```

    Every member sits in a hundred, and the hundreds are ordered by how far
    through its life the thing is. So the questions a query actually asks are
    each **one range predicate**:

    ```sql
    WHERE state >= 400                      -- done, whatever the reason
    WHERE state >= 200 AND state < 400      -- live
    WHERE kind  >= 300                      -- shares actually moved
    WHERE action >= 200                     -- liquidity was removed
    ```

    A set of `IN` literals cannot prune like that: a manifest knows the minimum
    and maximum of a file, not which values are in it. Ordering the codes is
    what turns a filter into a skipped file.

=== "The wire character is on the member"

    ```python
    State.from_fix("2")          # State.FILLED     -- FIX OrdStatus <39>
    State.FILLED.into_fix()      # '2'
    Side.BID is Side.BUY         # True
    Side.SELL.sign               # -1
    ```

    The FIX character lives on the member, because that is the only place it
    cannot drift from the value beside it. `BID` and `ASK` are the same codes
    as `BUY` and `SELL` — a bid *is* a buy, and two spellings of one direction
    would split every filter in half.

A value, once given, is never reused: `python/tests/market/test_enums.py`
pins the whole of `State` and `Side` against a literal table, so renumbering a
member fails the build rather than quietly rewriting what stored rows mean.
Everything from `Ranged.PRIVATE` (9000) up is left to venues and vendors.

## Identifiers

Every identifier is sixteen fixed bytes — `fixed_size_binary[16]` in Arrow,
`fixed[16]` in Iceberg — and a `uuid.UUID` in Python, which is the standard
16-byte value with a canonical text form.

=== "Building one"

    ```python
    from rekep.market import Order

    Order.h128_of("XNAS", "AAPL", "cl-1")      # namespaced by the shape
    ```

    The class name goes in front of the parts, so an `Order` and a `Book` built
    from the same symbol and time cannot land on one identifier — a collision
    no hash width prevents, because the inputs really are equal.

=== "Building a column of them"

    ```python
    Order.h128_arrow("XNAS", batch.column("symbol"), batch.column("client_order_id"))
    ```

    The same identifiers, in kernels: one join over every column at once, then
    one digest per row read straight out of the joined buffer instead of out of
    Python strings. About [six times faster](#benchmarks) per row than building
    them one at a time. A scalar argument broadcasts.

=== "Why sixteen, and why fixed"

    - **Wide enough to be an identifier and not a hash.** A 64-bit digest
      collides once in a few billion rows by birthday, which a day of ticks
      reaches. An identifier that collides silently merges two lifecycles, and
      nothing downstream can tell that it happened.
    - **Fixed, so it costs no offsets.** One flat buffer, no indirection per
      row, so a join or a group-by on it is a memcmp over contiguous memory.
      The same identifier as text is 32–36 bytes plus a 4-byte offset each.
    - **`fixed[16]`, not Iceberg's `uuid`.** pyiceberg reads `uuid` back as
      `extension<arrow.uuid>`, and Iceberg's Spark runtime surfaces it as a
      *string*; `fixed[16]` comes back as the same sixteen bytes in both.

Parts are hashed **behind their own byte lengths** — `4:AAPL:4:cl-1` — which is
what makes the encoding injective. A separator alone stops `("AB", "C")` and
`("A", "BC")` colliding, but not a part that contains the separator; and a raw
identifier used as a part contains any given byte about six times in a hundred.

## Orders and executions

`MarketEvent` adds four deliberately abstract slots — a side, a price, a
quantity, an instrument — and each subclass says what it puts in them.

| shape | `px` is | `qty` is |
| --- | --- | --- |
| `Order` | the limit | the quantity asked for |
| `Execution` | what traded (FIX `LastPx <31>`) | how much (`LastQty <32>`) |
| `BookSide` | the best level's price | its size |
| `Book` | the mid | the size at the touch |

Both are nullable, and that is not laziness: a market order has no price, an
empty book side has neither, and `0.0` is a real price — negative ones are
too, as an oil settlement in April 2020 reminded everybody.

```python
Order.FIELD.field("tif").fix["tag"]      # '59'   -- TimeInForce
Order.FIELD.field("px").fix["name"]      # 'Price'
```

Around forty columns carry the FIX field they came from under the `fix:` keys
`Field.fix` already reads, and every one of them is checked against the
published dictionary in `data/fix.zip` by `python/tests/market/test_fix.py` —
both the tag and the datatype — so a tag typed from memory fails the build
instead of mislabelling a column forever.

`Order` carries what was asked for (`kind`, `tif`, `stop_px`, `display_qty`)
and how far it got (`filled_qty`, `leaves_qty`, `avg_px`). `Execution` carries
`order_xh128`, a flat single-valued link to the order's lifecycle beside the
generic `parent_h128` list — because no engine below joins on an array without
exploding it first.

## Books

A `BookSide` carries **both the state and the delta**: `alive` is every live
level, `updates` is the changes that produced this version.

A feed sends one or the other. Keeping only snapshots loses what moved; keeping
only increments makes every reader replay from the last snapshot before it can
answer anything. Carrying both means a consumer reads state without replaying
and reconstructs causation without a second stream.

=== "Deriving the flat columns"

    ```python
    from rekep.market import Book, BookSide

    sides = BookSide.summarise_arrow(batch)   # px, qty, depth, total_qty
    books = Book.summarise_arrow(batch)       # both sides, then px, spread, micro_px, imbalance
    ```

    Everything in kernels, no row looked at in Python. `Book.summarise_arrow`
    derives each nested side from its own levels *first* and then the prices
    across them, because the second needs the first.

    A row whose `alive` is null is an increment that was never resolved to a
    state, and is left exactly as it was found rather than derived into nulls.

=== "Why they are columns at all"

    ```sql
    WHERE spread < 0                -- crossed: skips files
    WHERE bid.alive[0].px > 100     -- reads every file, then throws rows away
    ```

    Iceberg writes **no lower or upper bounds for any field under a list or a
    map** at all, so nothing about a level in `alive` can ever prune. Doris
    pushes a predicate down only for a top-level scalar column, and never for
    a struct, array or map column. Deriving these once at write time is what
    makes the same query cheap on all three engines instead of only on Spark —
    and it costs [260–1050 ns a row against 0.4 ns to read back](#benchmarks).

=== "What is not stored"

    ```python
    best_bid = book.px - book.spread / 2
    best_ask = book.px + book.spread / 2
    ```

    **The flat pair `(px, spread)` *is* the best bid and offer**, exactly, so
    neither is duplicated as a column of its own. There is no `crossed` flag
    either: `spread < 0` is crossed, `spread == 0` is locked, and both are one
    range predicate on a column an engine already has statistics for.

A `Book` holds two whole `BookSide` events, identity included, so it says
exactly which version of each side it was built from — `bid.h128` and
`ask.h128`, which are also what `parent_h128` holds. A snapshot that kept only
the levels would be a number nobody can reproduce.

!!! warning "Declaration order is load-bearing here"

    Iceberg collects column bounds for the first
    `write.metadata.metrics.max-inferred-column-defaults` **leaf** columns in
    pre-order — 100 by default — and a `Book` is 140 leaves. `spread`,
    `micro_px` and `imbalance` are declared *before* `bid` and `ask` for that
    reason: behind them they landed at leaves 138–140, past the cutoff, so the
    columns the shape exists to make prunable would have shipped with no bounds
    and every filter on them would have read every file while looking like it
    worked. `test_every_column_a_reader_filters_on_is_inside_the_metrics_budget`
    pins it.

## Through Iceberg, Spark and Doris

The types here are chosen so the same table reads the same way in all three.

| declared | Arrow | Iceberg | Spark | Doris |
| --- | --- | --- | --- | --- |
| `uuid.UUID` | `fixed_size_binary[16]` | `fixed[16]` | `BinaryType` | `char(16)`, raw bytes |
| a `Ranged` code | `int32` | `int` | `IntegerType` | `int` |
| `int` (a `*unix`) | `int64` | `long` | `LongType` | `bigint` |
| `datetime.date` | `date32` | `date` | `DateType` | `date` |
| `float` | `float64` | `double` | `DoubleType` | `double` |
| `str` | `string` | `string` | `StringType` | `string` |
| `bool` | `bool` | `boolean` | `BooleanType` | `boolean` |
| `list[Level]` | `list<struct>` | `list<struct>` | `array<struct>` | `array<struct>` |
| `dict[str, str]` | `map<string, string>` | `map` | `map` | `map` |

Six things worth knowing before pointing an engine at one of these tables:

- **Create the table from here, not from Spark.** Iceberg's Spark type mapping
  has no inbound row that produces `fixed` or `uuid` — `CREATE TABLE` with a
  Spark `binary` column gives you Iceberg `binary`. Spark *writes into* an
  existing `fixed[16]` from a `BinaryType` column happily, and asserts the
  length while it does.
- **`fixed[16]` round-trips byte-exact through Spark**; Iceberg's own `uuid`
  becomes a 36-character string there, and writing bytes into one is
  [still an open issue](https://github.com/apache/iceberg/issues/10635).
- **Nothing inside a list or a map ever prunes.** Iceberg's Parquet metrics
  writer discards bounds for repeated fields outright. That is the whole
  argument for the flat derived columns above.
- **Doris pushes down top-level scalar columns only.** A predicate on a nested
  struct member reaches the scan as a row filter, not as file pruning, and a
  predicate on a whole struct, array or map column is dropped. `EXPLAIN
  VERBOSE` prints an `icebergPredicatePushdown=` block; anything missing from
  it was not pushed.
- **Doris cannot group or join on a struct or a map.** Which is why `symbol` is
  a flat column on `Event` as well as a member of `instrument`.
- **`identity` and `bucket[N]` are legal on `fixed[16]`; `truncate[W]` is
  not.** The partition here is `date` with an `identity` transform — a real
  date column rather than a `day` transform on the timestamp, because identity
  is the one form every engine reads alike and the transformed alternative
  needs Iceberg's Rust core on the writer for no gain a reader can see.

Doris surfaces `fixed[16]` as `char(16)` holding the raw bytes, which is
queryable and comparable but renders as mojibake in a client — select `hex()`
of it. From Doris 4.0.2 a catalog property maps it to `varbinary` instead,
which reads better but cannot be used in a group-by, a join key or a comparison
there; `char(16)` is the more useful of the two.

## Contracts

The five shapes that are tables are published under `schemas/rekep/`:

```text
schemas/rekep/
├── instrument.yaml
├── order.yaml
├── execution.yaml
├── bookside.yaml
└── book.yaml
```

`Event` and `MarketEvent` are not there, and neither are `Level` and
`LevelUpdate`: an abstract base is nothing two sides exchange, and a level
travels inside the side that holds it. Each file is pinned against its
declaration in `python/tests/test_schemas.py`, so a column added in Python and
not published fails the build. Regenerate them from `python/` with:

```bash
uv run python -c "import rekep.market as m; \
[getattr(m, n).FIELD.into_yaml(f'../schemas/rekep/{n.lower()}.yaml') \
 for n in ('Instrument', 'Order', 'Execution', 'BookSide', 'Book')]"
```

## Benchmarks

`benchmarks/bench_market.py` is the sweep behind every number here. It asserts
the vectorised identifiers *are* the scalar ones, and that a book actually
derived, before it times anything — a benchmark that measures the wrong answer
measures nothing. The method the whole site shares is on the
[Benchmarks](benchmarks.md) page.

```bash
cd python
uv run python benchmarks/bench_market.py            # 1,000,000 rows, best of 5
uv run python benchmarks/bench_market.py --quick    # 100,000 rows, best of 2
```

Every case is warmed once and reported as the best of `--repeat` runs. The
figures below were measured twice, on one machine; read the ratios, not the
milliseconds.

**Identifiers**, one million rows of `(shape, symbol, venue, client order id)`:

| case | measured |
| --- | --- |
| `h128_of`, one row at a time | ~3.4 µs/row |
| `h128_arrow`, whole column | ~530–545 ns/row, **6.3–6.5×** |
| — of which the join, no length prefix | ~37–43 ns/row |
| — of which the join, length prefixed | ~100–114 ns/row |

So injectivity costs about 65 ns a row, roughly 12% of building an identifier
column, and it is the difference between an identifier that cannot collide by
construction and one that merely usually does not.

**Books**, 100,000 rows:

| case | 1 level a side | 10 levels | 50 levels |
| --- | --- | --- | --- |
| `BookSide.summarise`: best, depth, total | ~115 ns/row | ~265 ns/row | ~495 ns/row |
| `Book.summarise`: both sides, then the prices | ~265 ns/row | ~590 ns/row | ~940–1050 ns/row |
| read the stored `micro_px` column | ~0.4 ns/row | ~0.4 ns/row | ~0.4 ns/row |
| read `bid.px`/`ask.px` from derived sides | ~2.9 ns/row | ~2.6–3.1 ns/row | ~2.9–4.0 ns/row |

The last two lines say which half is expensive: reaching into a nested struct
is a few nanoseconds, and walking the levels is hundreds. Deriving once at
write time is worth **700–2800×** against re-deriving per query — before
counting the files a flat column lets an engine skip and a nested one does not.
