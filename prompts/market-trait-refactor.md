# Prompt: slim the `Market` trait

Two prompts, run in order: **A** in `Platob/yggdryl` (the Rust core owns the
trait), then **B** in `Platob/yggfin` once A ships as a release. Each one is
written to be pasted into a fresh agent session on its own.

---

## A. Yggdryl: split `MarketElement` into `Market` and `MarketOperation`

### Context

`rust/src/graph/element.rs` defines `MarketElement: Element` with 34 facts and
68 accessors. Every holder carries all of them (`MarketElementData`,
`MarketEventData` in `rust/src/graph/event.rs`, and through them `BookSide`,
`Book`, `Quote`, `Order`, `Execution`, `Trade`, `FixMsg`). A book pays for
facts that only an operation states: eight bid/ask lane fields, `tif`,
`tradable` and `marketoperationid`. On top of that,
five `Option<Code>` instrument slots and a heap `String` unit. The
`delegate_market_element!` macro in `graph/mod.rs` repeats the whole surface
for each wrapper.

Goal:
- A small `Market` trait that a book level or any simple struct can implement
  cheaply.
- A `MarketOperation` trait that adds the operation facts.
- `Book` implements `MarketEvent` (event plus slim market) and nothing more.
- Compact value types underneath all of it.

Follow `AGENTS.md`: Rust core first, smoke each step, then Python, then Node,
then docs. Delete what this replaces in the same change: no deprecated aliases
and no compatibility shims.

### Target traits: four layers

```text
Market                     the slim facts; a book level or any simple struct
├─ MarketEvent             = Event + Market (blanket)          -> Book, BookSide
└─ MarketOperation         = Market + operation facts          -> OrderEntry, QuoteEntry, ExecutionEntry
   └─ MarketOperationEvent = Event + MarketOperation (blanket) -> Order, Quote, Execution, Trade, FixMsg
```

A `Book` is a `MarketEvent` and never carries operation facts. Its
`executions` stay `Vec<Execution>`, so they are full operation events.

```rust
pub trait Market {
    fn get_execunix(&self) -> Option<i64>;
    fn get_miccode(&self) -> Option<&MicCode>;
    fn get_cficode(&self) -> Option<&CfiCode>;
    fn get_securityids(&self) -> &SecurityIds;     // derefs to &[SecurityId]
    fn get_side(&self) -> Side;                    // Copy, one byte
    fn get_currency(&self) -> &Currency;
    fn get_unit(&self) -> &Unit;
    fn get_price(&self) -> Decimal18;
    fn get_prevpx(&self) -> Option<Decimal18>;
    fn get_lastpx(&self) -> Option<Decimal18>;
    fn get_avgpx(&self) -> Option<Decimal18>;
    fn get_quantity(&self) -> Decimal18;
    fn get_prevqty(&self) -> Option<Decimal18>;
    fn get_lastqty(&self) -> Option<Decimal18>;
    fn get_leavesqty(&self) -> Option<Decimal18>;
    fn get_cumqty(&self) -> Option<Decimal18>;
    fn get_spotrate(&self) -> Option<Decimal18>;
    fn get_forwardpoints(&self) -> Option<Decimal18>;
    fn get_ticker(&self) -> Option<&str>;          // was `symbolticker`
    fn get_metadata(&self) -> &Metadata;
    // A matching `set_*` for each, plus
    // `securityids_mut(&mut self) -> &mut SecurityIds`.
}

pub trait MarketOperation: Market {
    fn get_marketoperationid(&self) -> Option<i32>;
    fn get_tif(&self) -> Option<&TimeInForce>;    // the crate's `TimeInForce`, not a String
    fn get_tradable(&self) -> Option<bool>;
    fn get_bid(&self) -> Option<&Lane>;            // Lane { price, spotrate, forwardpoints, currency, quantity, unit }
    fn get_ask(&self) -> Option<&Lane>;
    // A matching `set_*` for each.
}
```

- `Market` must be implementable by a plain struct with no uuid or digest,
  such as a book level. Keep `Element` (identity, digest, merge) out of its
  supertraits.
- Put the digest, merge and follow helpers on blanket impls:
  - `feed_market`, `merge_market` and `follow_market` on `Market + Element`.
    They cover the 20 slim facts only.
  - `feed_operation`, `merge_operation` and `follow_operation` on
    `MarketOperation + Element`. They continue the slim versions with the
    operation facts.
- `fill_market` splits the same way. The slim part fills the price and
  quantity from `lastpx`/`avgpx`/`lastqty` and adds the CUSIP embedded in an
  ISIN.
  `fill_operation` adds the lane rules and `fill_lanes`. `cumqty` and
  `leavesqty` stay off the fill ladder: the FIX dictionary already derives
  the ordered quantity from them, and that rule stays in one place.
- `execunix` moves from `Event` to `Market`. `Event` keeps the other clocks.
  An undated entry reports `None`.
- `metadata` is `Option<Box<BTreeMap<..>>>` internally, holding the free-form
  facts. A struct that states none pays one pointer, and the getter returns a
  shared static empty value when it is absent. It sits beside
  `Element::identifiers` and never holds an identifier.
- `Lane` is one struct shared by `bid` and `ask`. Store it as
  `Option<Box<Lane>>` or inline, whichever the size test favours.
- `cumqty` is a slim fact: a book level and a fill both state how much is
  done, and `leavesqty` is already slim beside it.
- `tif` uses the crate's existing `TimeInForce` from `rust/src/timeinforce.rs`,
  replacing `Option<String>`:
  - Holders store `Option<TimeInForce>`, so short codes stay inline with no
    allocation.
  - The `tif` column in `graph/market_column.rs` becomes the
    `yggdryl.timeinforce` extension type instead of `utf8`.
  - `merge_operation` folds it with `CodeValue::merge_with`, like the other
    codes.
  - The digest keeps feeding `as_str()` bytes, so identities do not move for
    `tif`.
  - Do not add a second time-in-force type or a string path beside it.
  - Its width is 8 bytes. Before settling, survey the `rust/benchmarks/fix`
    captures and test corpora for any stated `TimeInForce(59)` wider than
    that. A value wider than 8 must be a located error naming the field,
    never a truncation.
- `spotrate` and `forwardpoints` are the FX parts of a price, and they are
  slim facts because a book of FX forwards is quoted in them.
  - **FIX sources:** `LastSpotRate(194)` and `LastForwardPoints(195)` on
    executions and trades. `fix/native_derivations.rs` already lifts both as
    `Decimal18`. On quotes, `BidSpotRate(188)`/`OfferSpotRate(190)` and
    `BidForwardPoints(189)`/`OfferForwardPoints(191)` go on the `bid`/`ask`
    `Lane`. In market data (`mdinc`, `mdfull`), each entry's
    `MDEntrySpotRate(1026)` and `MDEntryForwardPoints(1027)` go on the level.
    `BidForwardPoints2`/`OfferForwardPoints2` (swap far legs) stay out of
    scope.
  - **Fill ladder:** the existing derivation in `fix/constants.rs`
    (`LastPx(31) = lastspotrate + lastforwardpoints`) stays the one rule. The
    slim `fill_market` does not add a second one, and never splits a price
    back into spot and points.
  - **Merge and follow:** the leading statement's value stands, like `lastpx`.
    Following does not carry them forward, because a spot rate is not a chain
    fact.
  - **Digest:** fed where stated, like `lastpx`. That moves identities only
    for rows that state them, which the rebuild already covers.
- `ticker` is a slim fact: a book is about one instrument, and a screen
  names it by its ticker.
- Rename `symbolticker` to `ticker` everywhere: trait, holders, digest label,
  Arrow column, both bindings and docs. The digest label changes, so identities
  move (see Contract).

### Holders

| Holder | Implements | Wrapped by |
| --- | --- | --- |
| `MarketData` (was `MarketElementData`, slimmed) | `Element + Market` | `BookSide` |
| `MarketEventData` (slimmed) | `MarketEvent` | `Book` |
| `MarketOperationData` | `Element + MarketOperation` | `OrderEntry`, `QuoteEntry`, `ExecutionEntry` |
| `MarketOperationEventData` | `MarketOperationEvent` | `Order`, `Quote`, `Execution`, `Trade` |

- `FixMsg` implements `MarketOperationEvent` directly.
- Each operation holder embeds the slim holder
  (`MarketOperationEventData { event: MarketEventData, operation: .. }`), so
  converting an operation into a book's view moves the slim part without
  cloning.
- `MarketEntryValue` converts through `MarketOperationData` and
  `MarketOperationValue` converts through `MarketOperationEventData`. Both keep
  their names and by-value moves.
- Replace `delegate_market_element!` with two delegates, one over the 20
  `Market` facts and one over the 5 `MarketOperation` facts. A wrapper that
  only needs `Market` generates only the first.

### `FixMsg` lifts the FX tags

Today `FixLifted` (`rust/src/fix/identity.rs`) holds 17 tags typed beside the
row (`LIFTED_TAGS`): the prices, the quantities and the order, quote and trade
IDs. The FX tags are only declared as native derivation inputs in
`fix/native_derivations.rs`, and read back out of the row.

Lift the six FX tags the same way:

| Tag | Lifted slot | Answers |
| --- | --- | --- |
| `LastSpotRate(194)` | `lastspotrate` | `Market::get_spotrate` |
| `LastForwardPoints(195)` | `lastforwardpoints` | `Market::get_forwardpoints` |
| `BidSpotRate(188)` | `bidspotrate` | `bid` lane `spotrate` |
| `BidForwardPoints(189)` | `bidforwardpoints` | `bid` lane `forwardpoints` |
| `OfferSpotRate(190)` | `offerspotrate` | `ask` lane `spotrate` |
| `OfferForwardPoints(191)` | `offerforwardpoints` | `ask` lane `forwardpoints` |

- Each slot is `Option<Decimal18>`, holding exactly what the message stated.
  Add the six tags to `LIFTED_TAGS`, give each a `FixLifted::fact` and
  `FixLifted::record` arm, and add a `FixLifted` accessor documented like
  `lastpx()`.
- Most messages are not FX, so keep the six slots in one
  `Option<Box<LiftedFx>>` inside `FixLifted`. It is allocated only when a
  message states one of them, so a non-FX message pays one pointer, not
  192 bytes. Pin both sizes in `rust/tests/allocations.rs`.
- A lifted tag re-emits under its own tag on the wire and lands as its own
  typed `decimal128(38, 18)` column on the `fixmsg` row, exactly as `lastpx`
  does. A line that said `194=` re-emits `194=`.
- The shipped derivation `LastPx(31) = lastspotrate + lastforwardpoints`
  (`fix/constants.rs`) reads the lifted slots through `get_by_tag`. Add a test
  that it still fires with the tags lifted and no `LastPx` stated. Do not
  change the derivation text or `SHIPPED_DERIVATIONS_SHA256` unless the text
  itself must change.
- `FixMsg`'s `Market` and `MarketOperation` getters answer from the lifted
  slots, and follow the `forced` rule the other derived facts follow. A
  write through a setter stops the derivation from answering over it.
- `MDEntrySpotRate(1026)` and `MDEntryForwardPoints(1027)` live inside the
  `MDEntries` repeating group, so they are not message-level lifts. The book
  walk reads them per entry onto each level.
- Python and Node expose the six on the `lifted` view beside `lastpx`.

### `identifiers`: read-only, derived from the definitions

Today `Element` has `get_identifiers() -> &BTreeMap<String, String>` and
`set_identifiers(..)`. The FIX layer writes the map once in
`fix/enrich.rs::enrich_restated` from the message type's `FIX:identifiers`
declaration (`FixMsgType::identifier_mapping`), and stores it as the
`identifiers` column (tag `65_020`). The graph holders copy it and union it on
merge and follow.

Target:

- `Element` exposes a getter only: `fn get_identifiers(&self) ->
  Cow<'_, Identifiers>`. Delete `set_identifiers` from the trait and from
  every implementor, delegate macro, binding and doc example.
- `Identifiers` is a sorted small vector of `(scheme, value)` pairs, both
  `SmolStr`, unique by scheme. It shares the sorted-union merge helper with
  `SecurityIds`. It replaces `BTreeMap<String, String>`.
- **`FixMsg` stores no identifiers.** Its `get_identifiers` answers
  `Cow::Owned` from `self.registry.get_msgtype(msgtype).identifier_values(self)`.
  The message type's own `FIX:identifiers` declaration is the only thing that
  decides which members count, and values are read off the message's own
  fields. Delete the `set_unsettled(IDENTIFIERS_TAG_NAME…)` write in
  `enrich_restated`.
- The `identifiers` column of the `fixmsg` row is written at the Arrow boundary
  from that accessor. There is no stored copy to drift from the fields it
  names.
- Graph holders (`MarketData`, `MarketEventData` and the operation holders)
  still keep an `Identifiers`, because an order learns `ClOrdID` from one
  message and `OrderID` from another. They fill it when built from a
  `FixMsg`. Merge and follow union it through a crate-private
  `identifiers_mut`, never a public setter. Python and Node expose a getter
  only.
- The definitions do the work, so fill them in. Only 64 of the 181
  `FIX:msgtype` components under `config/fix/components/` declare
  `FIX:identifiers`.
  - Declare it for every message type that reaches the market graph or names
    a request. That includes at least `marketdataincrementalrefresh` (X),
    `marketdatasnapshotfullrefresh` (W), `marketdatarequest` (V), `ioi`,
    `crossrequest`, `bidrequest`, `bidresponse` and
    `businessmessagereject`.
  - List each message's own direct scalar `…ID` members, in component order,
    the same way the existing 64 are written.
  - Session messages (`heartbeat`, `logon`, `logout`, …) declare none, on
    purpose.
  - Add a test that every message type mapped to a `marketoperationid`
    declares a non-empty `FIX:identifiers`, and that each declared member
    resolves to a direct scalar child. `identifier_positions` already refuses
    a bad one.
- `identifiers` is not a digest input today. Keep it that way, so this change
  moves no identity on its own.

### `SecType`: an open string key

- `SecType` is a validated string, `code_leaf!`-style over `SmolStr`: ASCII,
  folded to upper case, up to a fixed max width (for example 16). It is an
  open set. Any key a source states is accepted (`ISIN`, `RIC`, `BBGTICKER`,
  a venue's own `XETRA_WKN`), and there is no `Other` bucket.
- Known keys have one canonical spelling, and `SecType::read` maps the other
  spellings onto it:
  - the FIX `SecurityIDSource(22)` wire code (`1` → `CUSIP`, `2` → `SEDOL`,
    `3` → `QUIK`, `4` → `ISIN`, `5` → `RIC`, `8` → `EXCHSYMB`, `A` → `BBGSYMB`,
    `S` → `FIGI`, `T` → `LEI`, and every other code in the code set);
  - the spec's name for it;
  - the canonical key itself.

  Names fold the way `Side::from_spelling` folds, and wire codes do not fold.
  An unknown key is kept as written, upper-cased. One table holds the aliases,
  in `rust/src/`, beside the code set it mirrors.
- `SecType::validate_code(&str)` dispatches the known keys to the existing
  validators (`IsinCode`, `CusipCode`, `SedolCode`, `FIGICode`,
  `BloombergCode`). Any other key checks only ASCII text up to the code's max
  width.
- There is no `TICKER` key: `ticker` is its own field on `Market`, and one fact
  has one owner. `SecurityIds::insert` refuses `TICKER` with a located error
  pointing at `set_ticker`.
- Keys are data, not discriminants. Nothing hashes or stores a key's position,
  so adding a key later changes no identity.

### `SecurityId`: a key and a code in one allocation at most

- A newtype over one buffer: a 1-byte key length, then the key, then the code.
  For example `SmolStr` holding `[len][KEY][CODE]`.
- `ISIN` plus 12 characters, `CUSIP` plus 9, `SEDOL` plus 7, `FIGI` plus 12
  and `RIC` plus about 12 all fit `SmolStr`'s 23-byte inline limit, with zero
  heap allocations. A long key or code allocates once.
- Accessors:
  - `sectype() -> &str` borrows the key;
  - `code() -> &str` borrows the code;
  - `new(key: &str, code: &str) -> Result<Self>` reads the key through
    `SecType::read` and validates the code;
  - `Display` shows `ISIN:US0378331005`.
- `Ord` compares `(sectype, code)` as byte strings, so a sorted list groups by
  key.
- Pin the size with `size_of::<SecurityId>()` in `rust/tests/allocations.rs`,
  and assert zero allocations for an ISIN.

### `SecurityIds`: a sorted set with merge, add and remove

- Storage: `SmallVec<[SecurityId; 2]>` or `Box<[SecurityId]>`. Measure both in
  `rust/benchmarks/` and keep the faster one for book-sized workloads.
- Invariant: sorted by `(sectype, code)` with no duplicates, enforced by every
  constructor and mutator. Test it with a property check.
- `insert(id) -> bool`: binary search, then insert. A no-op when present.
- `remove(&SecurityId) -> bool` and `remove_key(&str) -> usize`.
- `get(key: &str) -> Option<&SecurityId>` (first under that key) and
  `iter_key(&str)`, both by binary search on the key after `SecType::read`, so
  `get("4")` and `get("isin")` find the ISIN.
- `merge(&mut self, other: &SecurityIds) -> bool`: a linear two-pointer union,
  O(n + m), with no re-sort. Returns whether anything was added. This replaces
  the per-code `CodeValue::merge_with` calls in `merge_market`.
- `Deref<Target = [SecurityId]>`, so the trait hands out `&[SecurityId]`.
- `SecurityIds` replaces the five code fields (`isincode`, `cusipcode`,
  `sedolcode`, `bloombergcode`, `figicode`).
- Fold the ISIN-to-CUSIP rule from `instrument::embedded_cusip` into the slim
  `fill_market` as an `insert`. `InstrumentCodes::enrich` reads and writes
  `SecurityIds`.
- Digest: feed the ids in sorted order as length-prefixed `key` then `code`,
  so the digest ignores insertion order and `AB`+`C` never collides with
  `A`+`BC`.

### `Side`: a `#[repr(u8)]` enum

- Replace the `code_leaf!(Side, SIDE_WIDTH)` `SmolStr` newtype with
  `#[repr(u8)] #[derive(Copy, ...)] enum Side { Unknown = 0, Buy, Sell,
  BuyMinus, SellPlus, SShort, SShortEx, Undisc, Cross, CrossSh, CrossShX,
  AsDef, Opposite, Subscr, Redeem, Lend, Borrow, SellUnd }`, with explicit
  discriminants.
- Keep `from_spelling` and `read`, fed by the same `SIDE_CODES` and
  `SIDE_NAMES` tables, now pointing at variants.
- `as_str()` returns the stored spelling (`"BUY"`, ...), `fix_code()` returns
  the `Side(54)` byte, and `is_bid()`/`is_ask()` become `matches!`.
- `merged` becomes `if self == Unknown { other } else { self }`.
- Arrow and Iceberg storage stay the `yggdryl.side` string extension, written
  from `as_str()` at the boundary. SQL readers keep `side = 'BUY'`. Changing
  the column to `uint8` is a separate, explicit decision; do not make it here.
- The digest keeps feeding the spelling bytes.
- Remove `Side` from the `family_value!` `Code` family if it no longer fits
  the `SmolStr` leaf shape. Keep it a `Scalar` variant.

### `Unit`: a validated string type

- `code_leaf!`-style newtype over `SmolStr`, so short units (`bbl`, `MWh`,
  `shares`) stay inline with no allocation. It is validated ASCII within a
  fixed max width (pick and document it, for example 32).
- `Unit::none()` is the empty unit and `is_none()` tests for it. Add a
  `yggdryl.unit` Arrow extension beside `yggdryl.currency`.
- Replaces `unit: String` and the unit inside `Lane`.

### Contract and storage

- The market row (`MarketEventData`, a book's row and each book side's level)
  is the 17 slim facts plus the event clocks:
  - one `securityids` column replaces the five code columns;
  - `ticker` replaces `symbolticker`;
  - `spotrate` and `forwardpoints` are new `decimal128(38, 18)` columns, and
    each lane struct gains them too;
  - the `bid*`/`ask*` lane columns, `tif`, `tradable` and
    `marketoperationid` leave it;
  - decimal columns stay `decimal128(38, 18)`.
- The operation row (orders, quotes, executions, trades, `fixmsg`) is the
  market row plus `marketoperationid`, `tif` (`yggdryl.timeinforce`), `tradable`,
  and `bid`/`ask` as two nullable `struct<price, currency, quantity, unit>`
  columns.
- Pick the `securityids` Arrow shape and state it in the schema docs.
  Default: `map<utf8, utf8>` keyed by `sectype`, with entries sorted. If
  decision 2 allows two codes under one key, use
  `list<struct<sectype: dictionary<int32, utf8>, code: utf8>>` instead.
- Identity changes: the digest inputs (`securityids`, `ticker`, the split between the slim and operation digests) change, so every market `curruuid`
  changes. Say so in the changelog and bump the minor version. Consumers
  rebuild from capture and never dual-write.
- Update `.api-inventory.txt`, both bindings (Python `graph`/`fix` getters,
  Node `fix.rs`) and the docs pages for the market graph. Python and Node
  expose:
  - `securityids` as a list of `(sectype, code)`;
  - `side` as its stored spelling;
  - `ticker` in place of `symbolticker`;
  - `bid`/`ask` as lane objects.

### Done means

- Smoke clean per `AGENTS.md`, with `rust/tests/graph*` covering book, quote,
  order and execution folding. Add a test that a `Book` built from orders
  keeps no operation facts, and that its executions keep theirs.
- A size and allocation test pins:
  - `size_of::<Side>() == 1`;
  - `SecurityId` zero-alloc for an ISIN;
  - `size_of` of all four holders, before and after. `MarketEventData` must
    shrink, and `Book` with it.

  Put the numbers in the PR body.
- A test parses a FIX FX forward execution stating `194=` and `195=` and no
  `31=`. It checks that:
  - both are lifted;
  - they round-trip on the wire;
  - the price is their sum;
  - both facts survive into the `Book`'s executions.
- A benchmark covers book building on the existing `rust/benchmarks/fix`
  capture, before and after.
- CI is green, and a release is tagged for yggfin to pin.

---

## B. Yggfin: adopt the slim `Market`

Run after A is released as Yggdryl `X.Y.Z`.

1. Bump `python/pyproject.toml` to the exact release and relock `python/uv.lock`.
2. Regenerate `schemas/rekep/marketevent.json` and `schemas/rekep/book.json`
   from the native fields. Never hand-edit them.
   - `book.json` and the `bid`/`ask` book levels take the slim market row:
     `securityids`, `execunix`, `cumqty`, `ticker`, `spotrate`,
     `forwardpoints`, no lanes, no `tif`/
     `tradable`/`marketoperationid`. The `executions` list keeps the full
     operation row.
   - `ticker` replaces `symbolticker` in both schemas.
   - Regenerate `schemas/rekep/fixmsg.json` too. It gains the six lifted FX
     columns (`lastspotrate`, `lastforwardpoints`, `bidspotrate`,
     `bidforwardpoints`, `offerspotrate`, `offerforwardpoints`), so
     `fix.refined` is recreated with the market tables.
   - `marketevent.json` (orders, quotes, executions) takes the operation row,
     with `bid`/`ask` lane structs in place of the eight lane columns.
3. Field ids renumber, so `fix.refined`, `market.books`, `market.orders`,
   `market.quotes` and `market.executions` are **recreated, not evolved**.
   `fix.refined` is rebuilt from `fix.raw` first, then the market tables are
   rebuilt from it. Record this beside the existing 0.1.10 identity migration note in
   `AGENTS.md` and `docs/`.
4. Adapt `python/src/rekep/market.py` (the `bid`/`ask` `deltas` flatten) and
   the `parse_books`, `parse_orders`, `parse_quotes` and `parse_executions`
   tasks to the new columns. Use Arrow kernels only, with no Python row loops.
5. dbt: in `stg_fix_messages.sql`, `orders_events.sql`, `orders_current.sql`
   and `executions_fills.sql`:
   - rename `symbolticker` to `ticker`;
   - read the ISIN out of `securityids` with one macro, for example
     `security_id(securityids, 'ISIN')`.

   `side`, `tif` and `cumqty` keep the same spelling in SQL.
6. `identifiers` keeps its column. It now reflects the message type's
   `FIX:identifiers` declaration, so market data rows gain `MDReqID` and the
   others newly declared. Check the dbt models and
   `docs/products/fixmsg.md` that read `identifiers[...]` keys.
7. Update `docs/contracts/types.md`, `docs/roadmap/order-book.md` and the
   pipeline task pages, then regenerate the samples.
8. `uv run pytest` must be green, including `test_schemas` and `test_docs`.
   Push the branch and read CI.

---

## Open decisions to confirm before running A

1. `metadata` sits beside `identifiers` and never holds an identifier. Is
   that right?
2. `SecurityIds` uniqueness: `(sectype, code)` (default, which allows two
   codes under one key) or one code per key? One per key makes the stored
   column a plain `map<utf8, utf8>`.
3. `Side` on the wire: keep the string extension (default) or move to `uint8`?
4. `Unit` max width.
5. `Lane` storage: boxed (small operations) or inline (lane-heavy quotes)?
   Decide it by the size test.
