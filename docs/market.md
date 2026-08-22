# Market

`rekep.market` is a set of declarations for what happens in a market — an
order, an execution, a side of a book, a book — written as a **history rather
than a state**. Nothing is ever updated in place: every version of every thing
is its own immutable row, keyed by a digest of its own content and
linked to the version before it.

```python
from rekep.market import Book, BookSide, Execution, Instrument, Order, State

Order.FIELD.into_arrow_schema()        # what the data is
Order.FIELD.into_iceberg_schema()      # the same thing, in Iceberg's terms
Order.FIELD.primary_keys()             # ['unix', 'hash']
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
    order.xhash        # the order, across every amendment of it
    order.hash         # this version of it
    order.version      # 0, 1, 2, ... within the lifecycle
    order.state        # where the lifecycle stands
    ```

    `xhash` is the thing and `hash` is the version. An order amended four
    times is four rows sharing one `xhash`, each with its own `hash`. That is
    what lets the store be append-only, and what lets a reader ask what was
    known at any moment instead of only what is true now.

    `hash` is a digest of what the event **says**, never of when it was
    recorded — so the same version arriving down two feeds is one row with one
    key, and re-reading yesterday's capture writes nothing.

=== "Five clocks"

    ```python
    event.unix         # when it happened          -- and the only one in the key
    event.cunix        # when it was created, upstream
    event.runix        # when it was written down here
    event.eunix        # when it stops being true  -- an expiry, or None
    event.sunix        # what it is a snapshot of  -- or None
    ```

    Separate because they answer different questions and disagree constantly:
    by microseconds on a good day, and by hours when something upstream has
    gone wrong. All five are whole nanoseconds since the epoch as `int64`, like
    [`Log.unix`](logs.md) and for the same reason — a timestamp
    width or zone that a downstream is picky about is a conversion per row.

    A snapshot's own `unix` is when the picture was taken, because that is what
    orders it against everything else in the stream; `sunix` is the instant it
    is a picture *of*, so staleness is `unix - sunix` rather than a join
    against whatever was snapshotted.

    `hunix` is `unix` floored to the hour and is **derived, never given** — it
    is denormalised for the partition, so `__post_init__` computes it and one
    authority stays one authority. `instrument_hash` is derived the same way,
    from `instrument.xhash`: a nested member nothing can partition on and a flat
    column everything does must not be free to disagree.

=== "Which kind of event"

    ```python
    Order.is_order()            # True
    Book.is_book()              # True
    Book.is_a(EventType.STATE)  # True -- a band answers for everything in it
    Book.is_snapshot()          # True
    order.etype                 # EventType.ORDER, taken from the class
    ```

    Each shape is its own table, so within one the `etype` column is constant
    and costs nothing — run-length and dictionary encoding both collapse it.
    It exists for the read that spans them: a union of orders, executions and
    books is one stream of `Event`s, and `etype` is the only thing that says
    which row is which without inspecting the columns that happen to be null.

    It is taken from the class rather than trusted from a caller, because a row
    whose type disagreed with the table holding it would be unreadable.

=== "The version before"

    ```python
    event.prev_hash    # the version this one replaced
    event.prev_state   # the state it moved out of
    event.prev_unix    # when that was
    ```

    Three columns, so "what changed" and "how long did it sit there" are read
    from one row rather than joined out of a window function over the whole
    lifecycle — which is a shuffle of the table for a question asked once a
    tick.

    `parent_hash` is the other direction: every event this one was built from,
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

**The schema says what the codes mean.** The column is a number and the enum
is ours, so a consumer that never imports this package would have nothing to
decode `410` with. Every enum column carries the member table under `enum:`
keys, beside the `fix:` ones:

```yaml
- name: state
  type: int32
  metadata:
    enum:name: State
    enum:type: int
    enum:values: '{"0":"UNKNOWN",...,"410":"FILLED",...}'
```

The value type is read off the *members*, not guessed from the base class:
`class K(str, Enum)` and `class K(IntEnum)` both subclass something, and only
the values say which of the two the column holds.

A value, once given, is never reused: `python/tests/market/test_enums.py`
pins the whole of `State` and `Side` against a literal table, so renumbering a
member fails the build rather than quietly rewriting what stored rows mean.
Everything from `Ranged.PRIVATE` (9000) up is left to venues and vendors.

**Storing them compactly** is an encoding question, not a type one:

```python
from rekep.market.fields import dictionary_arrow

dictionary_arrow(column, pyarrow.dictionary(pyarrow.int8(), pyarrow.int32()))
dictionary_arrow(encoded, pyarrow.int32())      # and back
```

Arrow's `dictionary` — **not** a `map`, which is a column of key/value pairs
per row — stores each distinct value once with an index per row, which is
exactly the shape of a column whose whole point is that it repeats. Three
questions, asked in this order because the first two are free and the third is
a pass over the data: same **value** type → encode as it stands; same **index**
type → take it as indices rather than encoding it again; neither → cast to the
value type first. The middle one has to be asked second and has to be asked at
all — a `dictionary<int32, int32>` of ranged codes has an index type and a
value type of the same width, and indices encoded as values point every row at
the wrong member. It is [4× smaller in memory](#benchmarks) at six distinct
states.

## Identifiers

Every identifier is a signed **`int64`** — the one column every engine below
Arrow reads the same way.

=== "Building one"

    ```python
    from rekep.market import Order

    Order.hash_of("XNAS", "AAPL", "cl-1")      # namespaced by the shape
    ```

    The class name goes in front of the parts, so an `Order` and a `Book` built
    from the same symbol and time cannot land on one identifier — a collision
    no hash width prevents, because the inputs really are equal.

=== "Building a column of them"

    ```python
    Order.hash_arrow("XNAS", batch.column("symbol"), batch.column("client_order_id"))
    ```

    The same identifiers, in kernels: one join over every column at once, then
    one digest per row read straight out of the joined buffer instead of out of
    Python strings. About [six times faster](#benchmarks) per row than building
    them one at a time. A scalar argument broadcasts.

=== "Why an int64"

    `fixed_size_binary[16]` is the better identifier on paper — 128 bits
    collide at a scale nothing reaches — and the worse one in practice,
    because half the ecosystem below Arrow reads it as something else. Doris
    surfaces it as `char(16)` of raw bytes that render as mojibake; Spark
    cannot *create* one, only write into one; Iceberg's own `uuid` reaches
    Spark as a string.

    An `int64` is the same column in every engine there is, and is a join key,
    a sort key and a bucket source in all of them.

    What that costs is collision margin, and it is smaller than it looks: the
    primary key is `(unix, hash)`, so two identifiers only collide **in the
    table** if they also fall on the same nanosecond. The birthday bound
    applies per instant rather than across the capture, and a nanosecond
    holding enough distinct events to matter is not a nanosecond.

Parts are hashed **behind their own byte lengths** — `4:AAPL:4:cl-1` — which is
what makes the encoding injective. A separator alone stops `("AB", "C")` and
`("A", "BC")` colliding, but not a part that contains the separator; and a raw
identifier used as a part contains any given byte about six times in a hundred.

And **a number is its own bytes**, not its text: an `int` is the eight of an
`int64`, a `float` the eight of a `float64`, a `bool` one. Text needs a
formatter, there are two here, and they disagree — Python writes `10.0`,
`1e-07` and `38983288990.155754` where Arrow writes `10`, `1e-7` and
`3.8983288990155754e+10`. Spelling a price one way in `hash_of` and another in
`hash_arrow` gave the same event two identifiers; reproducing Arrow's formatter
in Python would have been the same duplication that caused it. The bytes have
no formatter to disagree about, they are exact where a rendering is lossy, and
the vectorised path reinterprets the column's own buffer rather than rendering
anything.

The frame records what a part *is* and not what type it arrived as, so two
parts with the same bytes are one part: `0` and `0.0` are eight zero bytes
either way. A type tag would remove that and cost a kernel pass per part, for a
case a call site never has.

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

Around fifty columns carry the FIX field they came from under the `fix:` keys
`Field.fix` already reads, and every one of them is checked against the
published dictionary in `data/fix.zip` by `python/tests/market/test_fix.py` —
both the tag and the datatype — so a tag typed from memory fails the build
instead of mislabelling a column forever.

`Order` carries what was asked for (`kind`, `tif`, `stop_px`, `display_qty`)
and how far it got (`filled_qty`, `leaves_qty`, `avg_px`). `Execution` carries
`order_xhash`, a flat single-valued link to the order's lifecycle beside the
generic `parent_hash` list — because no engine below joins on an array without
exploding it first.

## Books

A `BookSide` carries **the state, the delta and the trace**: `alive` is every
live level, `updates` is the changes that produced this version, `executions`
is what traded against it.

A feed sends one or two of the three. Keeping only snapshots loses what moved;
keeping only increments makes every reader replay from the last snapshot before
it can answer anything; keeping neither trades means "was that level filled or
pulled?" needs a join against another table on a timestamp. Carrying all three
means a consumer reads state without replaying and reconstructs causation
without a second stream.

=== "Deriving the flat columns"

    ```python
    from rekep.market import Book, BookSide

    sides = BookSide.summarise_arrow(batch)   # px, qty, depth, total_qty
    books = Book.summarise_arrow(batch)       # each side, then px, spread, micro_px, imbalance
    ```

    Everything in kernels, no row looked at in Python. `Book.summarise_arrow`
    derives each side from its own levels *first* and then the prices across
    them, because the second needs the first — and through the same walk a
    `BookSide` uses on its own, so there is one set of rules about empty lists
    and null ones rather than two that drift.

    A row whose `alive` is null is an increment that was never resolved to a
    state, and is left exactly as it was found rather than derived into nulls.

=== "Building one out of events"

    ```python
    from rekep.market import Book, Execution, Order, Side, State

    book = Book(symbol="AAPL")
    book.append_event(Order(side=Side.BUY,  px=10.0, qty=100.0, state=State.NEW))
    book.append_event(Order(side=Side.SELL, px=10.2, qty=300.0, state=State.NEW))
    book.px, book.spread, book.micro_px     # 10.1, 0.2, 10.05
    ```

    `append_event` redirects to `append_order` or `append_execution` by what it
    is handed, and a `Book` routes each to the side it belongs on by
    `event.side.sign`. Every append is a **new version of the same `xhash`**:
    `prev_hash` is the version before, `parent_hash` gains the event that
    caused this one, and the derived columns follow.

    It is an **aggregated** book, not a per-order one: an order moves the level
    at its price by what it rests for — `display_qty` if the venue hides part
    of it, `leaves_qty` if it said how much is left, `qty` otherwise, and
    nothing at all once the order is terminal. A fill takes its quantity out,
    and only a report that `moves_shares` does: subtracting an acknowledgement's
    quantity is how a book empties by lunchtime.

    A market order is refused rather than rested — it has no price to sit at.

=== "Why they are columns at all"

    ```sql
    WHERE spread < 0                 -- crossed: skips files
    WHERE bid_alive[0].px > 100      -- reads every file, then throws rows away
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

**The two sides are flat**, not nested. Each contributes five scalars and its
three lists, prefixed `bid_`/`ask_`:

```text
bid_hash  bid_px  bid_qty  bid_depth  bid_total_qty  bid_alive  bid_updates  bid_executions
ask_hash  ask_px  ask_qty  ask_depth  ask_total_qty  ask_alive  ask_updates  ask_executions
```

`bid_hash` and `ask_hash` keep the provenance a nested side would carry —
exactly which version of each side this book was built from, which is also what
`parent_hash` holds — so a book is still reproducible without a second copy of
the fifteen-column event envelope in every row. `Book.into_side("bid")` lifts
one back out as the `BookSide` its columns describe, which is how routing an
appended event reuses the side's own rules rather than reimplementing them.

!!! warning "Nesting the sides cost the bounds this shape exists for"

    Iceberg collects column bounds for the first
    `write.metadata.metrics.max-inferred-column-defaults` **leaf** columns in
    pre-order — 100 by default. With a whole `BookSide` nested under `bid` and
    `ask` a `Book` was **140 leaves**, and `spread`, `micro_px` and `imbalance`
    landed at 138–140: past the cutoff, so the columns the shape exists to make
    prunable would have shipped with no bounds at all and every filter on them
    would have read every file while looking like it worked. Flattening the
    sides brings it to **80 leaves** with every scalar inside the budget.
    `test_every_column_a_reader_filters_on_is_inside_the_metrics_budget` pins
    it, and `test_the_leaf_walk_finds_the_nesting_it_is_supposed_to` stops that
    test passing by counting too few columns.

## Through Iceberg, Spark and Doris

The types here are chosen so the same table reads the same way in all three.

| declared | Arrow | Iceberg | Spark | Doris |
| --- | --- | --- | --- | --- |
| an identifier | `int64` | `long` | `LongType` | `bigint` |
| a `Ranged` code | `int32` | `int` | `IntegerType` | `int` |
| `int` (a `*unix`) | `int64` | `long` | `LongType` | `bigint` |
| `datetime.date` | `date32` | `date` | `DateType` | `date` |
| `float` | `float64` | `double` | `DoubleType` | `double` |
| `str` | `string` | `string` | `StringType` | `string` |
| `bool` | `bool` | `boolean` | `BooleanType` | `boolean` |
| `list[Level]` | `list<struct>` | `list<struct>` | `array<struct>` | `array<struct>` |
| `dict[str, str]` | `map<string, string>` | `map` | `map` | `map` |
| a shape of one member | its member's type, as an extension | the storage type | the storage type | the storage type |

**A shape with one member is that member.** A `struct` of one is a nesting
level carrying no information that costs a filter its pushdown on every engine
in the table, so a one-member `@field` class projects to an Arrow *extension
type* over the member's own storage instead. The class name rides in
`ARROW:extension:name`; anything that has never heard of the extension — every
column of the three engines above — sees the storage type and reads it
correctly.

Five things worth knowing before pointing an engine at one of these tables:

- **An identifier is a `long` everywhere**, so a table created here is one
  Spark and Doris can both create, join and sort on. That is the whole reason
  it is not `fixed[16]`: Iceberg's Spark type mapping has no inbound row
  producing `fixed` or `uuid`, so Spark can only write *into* one; Doris reads
  `fixed(N)` as `char(N)` of raw bytes; and Iceberg's `uuid` becomes a
  36-character string in Spark, where writing bytes into one is
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
- **`identity`, `bucket[N]` and `truncate[W]` are all legal on a `long`** —
  where `fixed[16]` allowed the first two and not the third. That is what makes
  the layout below possible at all.

None of the awkward Doris type mappings apply any more, which is the point of
the change: an `int64` is a `bigint` there, with no catalog property to set and
nothing to `hex()` before a human can read it.

## Folding a book

`Book.from_events` is the other end of the pipeline: one instrument's orders
and executions in time order, and the book after each instant that moved it.

```python
from rekep.market import Book, FixEvents

events = (one for line in lines for one in FixEvents.from_text(line, venue="XCME"))
for book in Book.from_events(events):
    book.bid_px, book.ask_px, book.spread, book.micro_px, book.imbalance
```

**One row per instant, never one per message.** Several events at the same
nanosecond are one state of the book, and writing three rows with the same
`unix` is writing the feed rather than the book. An instant that moved nothing
— an acknowledgement, a cancel of a level that was never there — yields
nothing at all.

**One instrument, sorted, and both are checked.** A stream carrying two
instruments folds into a book that is neither, silently and forever; an event
out of order asks the book to un-happen something. That is what
`instrument_hash`'s `bucket[16]` partition and the `unix` sort order are for —
read a partition and hand it straight here.

### Two streams, one pass

`Book.from_events` is one instrument's. A capture is not — it is every
instrument a venue publishes, interleaved, and what a reader wants out of it is
two tables: the books, and what was learned about the instruments while
building them. `BookIterator` is that pass.

```python
from rekep.market import BookIterator

folding = BookIterator(events=events, snapshot_every=HOUR)
for book in folding.books():          # one stream of books, every instrument
    ...
for known in folding.instruments():   # one stream of instrument versions
    ...
```

**One iterator, two streams, mutable state per instrument.** Internally each
instrument has its own resting orders and sorted price lists, kept and mutated
in place rather than rebuilt; a book is only *built* when the instant it
belongs to has closed — a new timestamp, or the end of the stream. So an
instrument that goes quiet costs nothing, and one that is being hammered costs
one build per instant rather than one per message.

The instrument stream is the other half of that state. A venue tells you about
an instrument in pieces — a symbol here, a currency there, an ISIN on the
security-definition message and never again — so what is known is accumulated
and a version is emitted whenever it grows. A capture where nothing new is
learned emits one row per instrument and no more.

**Identity moves, and the fold survives it.** An instrument identified by
symbol on one message and by ISIN on the next hashes two ways, which would open
two books for one instrument. `Instrument.identities()` names every hash a
version answers to and the iterator keeps the aliases, so the second message
finds the book the first one opened.

### Snapshots on the hour

A book is a *delta* against the version before it, which is what makes the
table small and what makes a reader of one hour depend on every hour before it.
`Event.make_snapshot(unix)` is the answer:

```python
taken = book.make_snapshot(unix)   # None when this shape is not snapshotted,
                                   # when `unix` is not later, or when the
                                   # event already snapshots something newer
```

A snapshot is the same state stamped at a later instant, carrying `sunix` —
what it is a snapshot *of* — so a reader can tell a restatement from something
that happened. `BookIterator` takes one per instrument per `snapshot_every`
nanoseconds, an hour by default, which is also the partition: **every hour of
the table can be read on its own.**

A snapshot carries no delta. It says what is, not what changed, so the
per-level insertions and removals are dropped on the way out — carrying them
would repeat one insertion in every hour of a quiet market, which is exactly
what it did before `forget_delta` existed.

### What is kept between events

The **live orders**, not the levels. A venue that restates an order has to
replace what that order was resting for, and a level cannot say which order
contributed what. `Resting` holds them per side, and it is the piece that makes
`BookSide.append_order`'s aggregated model — which moves a level by a delta and
keeps no idea which order contributed it — usable on a feed that publishes
orders rather than levels.

Sorted **by price, then by descending quantity**, which is what makes the best
bid and offer a read rather than a scan. Price first because that is what a
book is; size second because at one price the larger interest is the one a
taker meets first on most venues, and any stable second key beats an arbitrary
one.

```python
side = Resting(side=Side.BID)
side.apply(order)          # completed from the version this side already holds
side.best                  # the order at the touch
side.sorted_orders         # every live order, best first
side.into_levels()         # aggregated to prices, with the order count per level
```

A restatement finds what it continues by lifecycle *or* by the identifier the
venue gave it. That is not redundant: a lifecycle is hashed from the
instrument, the venue and the identifier, so a report that carries the
identifier and omits the venue — which venues do, because they know which one
they are — hashes to a different lifecycle than the order it continues.

### Which side a trade hit

Three readings, strongest first, because a feed gives different ones:

1. **The report's own side.** An execution's `side` is the side of the order it
   reports, and a filled buy order was resting on the bid — so its liquidity
   leaves the bid.
2. **The order it names.** A report with no side but an `order_xhash` that is
   live on one side names that side.
3. **Its price against the touch.** A market-data trade print carries neither,
   which is most prints: at or below the mid it took from the bid, above it
   from the ask. That is the tick rule, and it is the honest answer when the
   venue has not given a better one.

A print against liquidity the fold never saw takes nothing out, because there
is nothing to take it out of.

### The prices that only exist across the sides

`px` is the mid, `qty` is the size at the touch, and `spread`, `micro_px` and
`imbalance` are the three a reader would otherwise recompute. They are
**derived on every version and never carried**: `px` and `qty` are abstract
slots that a version inherits from the one before, which is right for an
order's limit and wrong for a mid — a book whose ask has just emptied has *no*
mid, and inheriting the last one makes a one-sided market look two-sided for as
long as it lasts.

## Completing a version

A venue restates only what changed. A report that says *"partially filled, 4
done"* and nothing else is a complete row only once the price, the side, the
instrument and everything else it did not repeat are carried forward from the
version it follows. `with_previous` is that.

```python
order = Order(unix=10, px=100.0, qty=10.0, order_id="ORD-1", ...).with_previous(None)
order.leaves_qty                # 10.0 -- a fresh order rests all of it

later = Order(unix=20, order_id="ORD-1", filled_qty=4.0).with_previous(order)
later.px, later.qty             # 100.0, 10.0 -- carried, never re-sent
later.leaves_qty                # 6.0  -- derived, not copied
later.version, later.prev_state # 1, State.NEW
```

Two halves, and they are separate methods because only one of them needs a
previous version:

- **`complete_from(previous)`** — what the row before implies about this one.
  Layered: each class overrides it and calls `super()`, and each fills only
  what this version left absent. A value the message actually sent always
  wins; this completes a row, it does not correct one.
- **`derive()`** — what *this* row implies about itself. A quantity that is the
  difference of two others, a notional that is the product of three. A first
  version has no previous and still derives.

What is not layered is the versioning, and it sits in `with_previous` so no
subclass can forget a piece of it: the counter moves on, the version before is
recorded as `prev_hash`/`prev_state`/`prev_unix` so a transition is on the row
rather than behind a self-join, and the content hash is re-derived **last** —
after every layer has filled, or it would identify a row that does not exist
yet.

### The rules that are arithmetic

| Rule | Why |
|---|---|
| `cunix` is **get-or-set** | A lifecycle is created once, and every later version of it was created then. Recomputing it makes "how old is this order" mean "how long since the last message about it". |
| `leaves_qty = qty - filled_qty` | A venue that sends `CumQty <14>` and not `LeavesQty <151>` has still said how much is working. Deriving it once stops every reader deriving it differently. |
| A terminal order rests **nothing** | `qty` and `leaves_qty` both go to zero. The order is done, cancelled or expired, and a book folding it has to take its liquidity out rather than leave it standing. What was asked for is on the version before, which `prev_hash` names. |
| `State.FILLED` filled what it asked for | A report that gives the state and not `CumQty <14>` has still said how much was done. |
| `prev_client_order_id` is the previous version's | FIX requires a new `ClOrdID <11>` per version and calls the old one `OrigClOrdID <41>`. |
| `filled_qty` accumulates, `leaves_qty` decreases | A fill that sends `LastQty <32>` and no running totals has still said what they now are. Only where shares actually moved — adding an acknowledgement's quantity is how a fills table starts overcounting. |
| `avg_px` is **re-weighted**, not copied | Copying it forward would leave every partial fill reporting the first one's price. |
| A notional needs all three of price, quantity and multiplier | One computed with a multiplier of "probably one" is wrong by a factor nobody notices until settlement. Only a cash instrument may assume it. |

### A version of it, or a different thing built from it

The abstract slots `px` and `qty` mean what the subclass says they mean — an
order's are what it asked for, a fill's are what traded, a book's are the mid
and the touch — so they carry only from a version of the **same shape**.
Carrying an execution's `LastQty` into an order's `OrderQty` made a partly
filled order claim it had asked for exactly what had just traded, and derive a
`leaves_qty` of zero from it. The named fields (`filled_qty`, `avg_px`, the
identifiers) mean the same thing everywhere and carry across shapes.

Whether `previous` is the version before or a different thing is read from the
identities, not the classes — because both answers happen between the same two
classes:

```python
fill = Execution(...).with_previous(order)
fill.version        # 0 -- version zero of its own life
fill.prev_hash      # None -- nothing came before it
fill.parent_hash    # [order.hash] -- but it was built from the order
```

`same_life_as` is asked **after** every layer has completed, and that is not
incidental: an order version carrying only its `OrderID <37>` does not know its
own instrument or venue until the previous version has given them to it, and
those are part of what its lifecycle is.

### An append that changed nothing

`append_order`, `append_execution` and `append_event` return the updated
version, or **None when nothing moved** — leaving the side or the book exactly
as it was: no version, no update, no new hash.

```python
side.append_order(fresh)        # the side, versioned
side.append_execution(acked)    # None: an acknowledgement moves no shares
side.append_order(terminal)     # None: it rests nothing, and never did
```

A caller that writes what it gets back therefore writes one row per real
change rather than one per message, which is the difference between a book
table and a copy of the feed.

## Reading a venue

`FixEvents` is the way in: a FIX message, or the pairs one was rendered as,
read as the orders and executions it carries.

=== "From a log line"

    ```python
    from rekep.market import FixEvents

    for event in FixEvents.from_text(line, venue="XCME", runix=recorded):
        ...   # Order, Execution, Order, ...
    ```

=== "From pairs"

    ```python
    FixEvents.from_pairs(
        [
            ("MsgType", "D"),
            ("Symbol", "BTC-USD"),
            ("ClOrdID", "CL-1"),
            ("Side", Side.BUY),
            ("OrderQty", 100.0),
            ("Price", 10.5),
            ("TransactTime", datetime.datetime(2026, 8, 21, 10, 0)),
            ("MyOwnField", "kept"),
        ]
    )
    ```

    The tag mapping defaults to `market_tags()` — every `fix_tag(...)` these
    shapes declare, plus the header and market-data tags the translation reads.
    Built from the declarations, so it cannot drift from them, and offline: no
    scrape, no network, no dictionary file.

### Which timestamp is the event's own

The decision the module exists to make, and FIX answers it directly.
`TransactTime <60>` is defined as *"timestamp when the business transaction
represented by the message occurred"*; `SendingTime <52>` is *"time of message
transmission"*. They are not the same instant, and reading them as
interchangeable is how a latency measurement comes out as zero and how two
venues' events interleave wrongly.

So `MarketEvent.unix` is the transaction and `Event.runix` is the recording,
and `unix` is taken from the first of these the message actually carries:

| # | Field | Why it is where it is |
|---|---|---|
| 1 | `TransactTime <60>` | When the business transaction occurred. The thing being asked for. |
| 2 | `MDEntryDate <272>` + `MDEntryTime <273>` | A market-data entry's own instant, split across two fields because that is how FIX carries it. Read **per entry**, so two entries of one refresh keep their own times. |
| 3 | `OrigTime <42>` | Time of message origination — for a relayed or republished message, nearer the transaction than the relay's own transmission. |
| 4 | `OrigSendingTime <122>` | On a `PossDupFlag <43>` resend, when the message first went out. Still transmission, but the original one. |
| 5 | `SendingTime <52>` | Transmission. Last, and only because a row with no time at all sorts nowhere. |

A message carrying none of them gets `unix = 0`, which says it does not know
rather than claiming the epoch. The order is `TRANSACTED`, and it is pinned by
a test: a reordering here silently changes which clock every downstream row is
stamped with.

### What a message becomes

| MsgType | Yields | Rule |
|---|---|---|
| `D`, `F`, `G` | one `Order` | A request is not an acknowledgement, so the state is `PENDING_NEW` / `PENDING_CANCEL` / `PENDING_REPLACE` rather than `NEW`. |
| `8` ExecutionReport | an `Order`, **then** an `Execution` when shares moved | FIX uses one message for "your order is now partially filled" and "here is the fill that did it". |
| `9` OrderCancelReject | one `Order` | With `CxlRejReason <102>` as the reason code. |
| `AE` TradeCaptureReport | one `Execution` | A trade with no order state to report. |
| `W`, `X` MarketData | one event **per entry** | Bid/Offer entries are `Order`s, a Trade entry is an `Execution`, and everything else it enumerates is a statistic about the market rather than an order in it. |
| anything else | nothing | A heartbeat, a logon. An empty iterator, not an error — a feed is mostly made of them. |

An ExecutionReport becoming two rows is the one worth dwelling on. The `Order`
carries `OrderQty <38>` and `Price <44>` — what was asked for — and the
`Execution` carries `LastQty <32>` and `LastPx <31>` — what moved. Storing only
one loses the other; storing them in one row makes `sum(qty)` mean two things.
The order is yielded first because the fill points at it.

An execution's own `state` is about the *fill*, not the order: a fill is
`FILLED` the instant it exists, whatever the order it belongs to is doing.
`ExecType <150>` and `OrdStatus <39>` share their lifecycle codes, so only the
four that are about a trade — `F`, `G`, `H` and the legacy partial/full fill —
need saying at all.

A message with no `MsgType <35>` is read from the fields it has: an
`MDEntryType <269>` means a refresh, an `ExecType <150>` or `ExecID <17>` means
a report, an order's own identifiers mean an order. A decoder that only worked
on complete headers would be no use on a log.

### Identity, by layer

Every event `FixEvents` produces is identified, because a row with no identity
cannot be deduplicated, joined or folded into a book. `identify()` fills what
the producer did not, and each layer says what it is made of:

```python
Order.life_parts()      # instrument, venue, OrderID | OrigClOrdID | ClOrdID
Execution.life_parts()  # instrument, venue, ExecID | TradeID
MarketEvent.version_parts()  # ... the lifecycle, version, unix, state, side, px, qty
```

`OrderID <37>` leads an order's lifecycle because the venue assigns it once and
keeps it across a cancel/replace, which is the definition of a lifecycle.
`ClOrdID <11>` does not survive one — the standard requires a new one per
version — so where only client identifiers exist, `OrigClOrdID <41>` is
preferred and a replacement lands on the same lifecycle as the version it
replaced.

A market-data entry with no `MDEntryID <278>` is a *level*, not an order, so
its price is what persists: that is what `MDUpdateAction <279>` addresses when
it says Change or Delete, and it is what makes a level findable across its own
updates.

### What is known about an instrument

An instrument arrives in pieces, and the pieces are worth keeping. A
security-definition message names the ISIN, the maturity, the strike and the
legs; a refresh a millisecond later names the symbol and nothing else.
`Instrument.enriched_with` merges the two, and `Reference` publishes the result
as its own versioned table beside the books.

```python
from rekep.market import Instrument, Leg, Reference

known.isin_code          # deduced when the id source says so, or from a CFI
known.alt_ids            # {'ISIN': 'US0378331005', 'RIC': 'AAPL.OQ', ...}
known.security_type      # FUT, OPT, MLEG, ...
known.legs               # [Leg(...), Leg(...)] on a multi-leg instrument
```

=== "Alternative identifiers"

    `NoSecurityAltID <454>` is a repeating group of
    `SecurityAltID <455>` / `SecurityAltIDSource <456>` pairs, and a venue puts
    the ISIN, the RIC, the SEDOL and its own internal number in it. They are
    kept under the *source's* name rather than merged into one column, because
    which registry issued an identifier is part of what the identifier means.

=== "Deducing the ISIN"

    In order, and each rule stops at the first that answers:

    1. `SecurityIDSource <22>` is `4` (ISIN) — so `SecurityID <48>` *is* one.
    2. A `NoSecurityAltID` entry whose source says ISIN.
    3. `SecurityID` that is shaped like one: twelve characters, two letters, a
       check digit that verifies.

    A twelve-character identifier that does not verify is not silently taken —
    an internal number that happens to be twelve characters long is not an
    ISIN, and writing one into `isin_code` is worse than leaving it empty.

=== "Legs"

    `NoLegs <555>` is a repeating group and each entry is a `Leg`: its symbol,
    side, ratio, and its own security identifiers, type, exchange, currency,
    maturity and strike. A spread, a butterfly and an option strategy are all
    one instrument with legs rather than several instruments, which is what the
    venue means by `MLEG`.

    `Leg` is declared **last** in `Instrument`, which is not a style choice:
    Iceberg counts bounds in declaration order, and a repeating group is a
    handful of leaves that can never carry a bound anyway — putting it in front
    of a filter column would spend the budget on something no reader filters on
    ([why](iceberg.md#a-dataset)).

Repeating groups are read by their count field and delimited by the tag FIX
says starts each entry, so a component nested inside one — a leg's own
`NoLegSecurityAltID` — belongs to the entry it appears in rather than to the
message. A group whose count disagrees with the entries present is read for the
entries that are there: a truncated log line is still worth what it carries.

### What has no column

Everything else lands in `metadata`, under the key it arrived as — the fields
no dictionary has, the ones a bridge renamed, the ones a later FIX version
added. What does *not* land there is what a column already holds, and the two
fields that frame the message rather than describe the event: `BodyLength <9>`
and `CheckSum <10>` are properties of the encoding, recomputed by anything that
re-emits the message. `BeginString <8>` stays, because which protocol a venue
speaks is a real fact about what arrived.

## Layout

A declaration says where its rows go, so nothing downstream has to be told
twice. `unix` is `Field.sort_key()`, `hunix` and `instrument_hash` are
`Field.partition_key(...)`, and `IcebergDataset` reads all three off the
schema when it creates the table:

```python
Order.FIELD.partition_keys()   # {'hunix': 'identity', 'instrument_hash': 'bucket[16]'}
Order.FIELD.sort_keys()        # {'unix': 'asc'}
```

**The hour, by identity.** `hunix` is `unix` floored to the hour, denormalised
so the partition is a plain column comparison. Identity is the one transform
every engine reads alike: a `day` or `hour` transform over a timestamp needs
Iceberg's Rust core on the writer, and buys a reader nothing it cannot get from
a column that already holds the value.

**The instrument, by bucket.** A market feed carries tens of thousands of
instruments. An `identity` partition on `instrument_hash` would be one
directory per instrument per hour — a hundred thousand partitions a day, each
holding one small file, which is the metadata explosion Iceberg's own docs warn
about. `bucket[16]` is sixteen files an hour, and a single-instrument read
still touches exactly one of them, because a bucket is a function of the value
and Iceberg pushes an `=` predicate through it. Sixteen is a deployment choice,
not a law: it is the width in the declaration and nowhere else.

That pairing is what makes `Book.from_events` possible. A book is built by
folding one instrument's events in time order, so the input has to be *one
instrument, sorted*. Bucketing on `instrument_hash` is what makes a partition
hold whole instruments rather than slices of many.

**Time, by sort order.** A sort order is not a partition: it does not decide
which file a row lands in, it decides where inside the file it lands. What that
buys is the column's min/max in the manifest — unsorted, every file's `unix`
range spans the whole hour and a range filter reads all of them; sorted, the
ranges are disjoint and it reads the few that overlap. Iceberg records the order
on the table and every engine that writes through it honours it, so nothing
sorts on read.

`Instrument.xhash` is derived rather than demanded, because a producer that only
has what the venue sent still has to emit rows that join. In order: a registered
identifier (`security_id` in the scheme `security_id_source` names) when both
are there, then the symbol *scoped to its exchange*, then `NIL`. Each branch
leads with a constant naming the scheme, so a symbol that reads like an ISIN
cannot collide with the ISIN, and `NIL` is a visible "unidentified" rather than
a hash of emptiness that would merge every unnamed instrument into one. A field
learnt later — a tick, a maturity, a label — is deliberately not part of the
key: an identity that moved when a row was enriched would break every join to
it.

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
| `hash_of`, one row at a time | ~5.8 µs/row |
| `hash_arrow`, whole column | ~730 ns/row, **8×** |
| — of which the join, no length prefix | ~71 ns/row |
| — of which the join, length prefixed | ~310 ns/row |

The parts here are a shape name, a symbol, a venue, a client order id, a price
and a sequence number — the last two on purpose, because the two builders
diverged on exactly the non-text ones and a guard fed only strings agreed with
itself.

So injectivity costs about 240 ns a row, and it is the difference between an
identifier that cannot collide by construction and one that merely usually
does not.

**Books**, 100,000 rows:

| case | 1 level a side | 10 levels | 50 levels |
| --- | --- | --- | --- |
| `BookSide.summarise`: best, depth, total | ~118 ns/row | ~268–280 ns/row | ~552–558 ns/row |
| `Book.summarise`: both sides, then the prices | ~245–272 ns/row | ~619–635 ns/row | ~1113–1136 ns/row |
| read the stored `micro_px` column | ~0.4–0.6 ns/row | ~0.4 ns/row | ~0.4 ns/row |
| recompute a mid from stored `bid_px`/`ask_px` | ~2.2–2.7 ns/row | ~2.1–2.2 ns/row | ~2.3–2.4 ns/row |

The last two lines say which half is expensive: reading a flat column, or two,
is a couple of nanoseconds; walking the levels is hundreds. Deriving once at
write time is worth **480–3090×** against re-deriving per query — before
counting the files a flat column lets an engine skip and a nested one does not.

**Reading a venue**, 50,000 messages of each shape. Parsing and translating
together, because that is what a task does — a line off a log becomes rows in a
table, and splitting the two would report a number no caller can have:

| message | events out | measured |
| --- | --- | --- |
| `NewOrderSingle <D>` | 1 | ~14.3–14.7k messages/s |
| `ExecutionReport <8>`, filled | 2 | ~7.3k messages/s, ~14.6–14.7k events/s |
| `MarketDataIncrementalRefresh <X>`, 5 entries | 5 | ~3.5k messages/s, ~17.4–17.6k events/s |

Per *event* the three agree within a fifth, which is the useful reading: the
cost is the event, not the message. Getting there took three measured changes —
`FixMessage.get` is a scan and then a regex scan, which cost **434 regex matches
per message** when the translation read forty fields off one; the tag mapping
was rebuilt per message; and the fold-vs-alternation choice for a key was
[raced rather than assumed](fix.md#benchmarks). Together, **2.1×** on a
`NewOrderSingle`.

**Folding a book**, 100,000 events of one instrument, a quarter of which
restate an order already resting:

| case | measured |
| --- | --- |
| `Book.from_events` | ~119 µs/event, ~6.3k books/s |

The fold keeps every live order, so its cost is the events and the depth rather
than the books. Three measured changes got it there, 2.2× together: a `Ranged`
member's band is computed once at construction (it was **1.1M evaluations per
four thousand events**, because `sign`, `moves_shares` and `is_a` are all
comparisons); `Resting` keeps running level totals as orders move instead of
re-aggregating every live order per snapshot; and `part_bytes` settles the four
types that are almost every part with one dict probe on the exact type.

The sweep warms Acero once before it starts, because the first grouped
aggregate in a process pays its own initialisation: unwarmed, it landed on
whichever depth ran first and made the *shallowest* book look 1.6× more
expensive than one ten times deeper.

**Ranged codes**, one million rows over six distinct states:

| case | measured |
| --- | --- |
| `dictionary_arrow`: values → encoded | ~4.6–4.7 ns/row |
| `dictionary_arrow`: indices → encoded | ~0.6 ns/row, **7.3–7.4×** |
| `dictionary_arrow`: encoded → values | ~1.0 ns/row |
| in memory | 4.00 MB → 1.00 MB, **4.0×** |

Which is why the index case is asked before the cast: it is the cheapest of the
three and, on a `dictionary<int32, int32>`, the one that is indistinguishable
from the value case by width alone.
