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
    fn get_ticker(&self) -> Option<&str>;          // was `symbolticker`
    fn get_metadata(&self) -> &Metadata;
    // A matching `set_*` for each, plus
    // `securityids_mut(&mut self) -> &mut SecurityIds`.
}

pub trait MarketOperation: Market {
    fn get_marketoperationid(&self) -> Option<i32>;
    fn get_tif(&self) -> Option<&TimeInForce>;    // the crate's `TimeInForce`, not a String
    fn get_tradable(&self) -> Option<bool>;
    fn get_bid(&self) -> Option<&Lane>;            // Lane { price, currency, quantity, unit }
    fn get_ask(&self) -> Option<&Lane>;
    // A matching `set_*` for each.
}
```

- `Market` must be implementable by a plain struct with no uuid or digest,
  such as a book level. Keep `Element` (identity, digest, merge) out of its
  supertraits.
- Put the digest, merge and follow helpers on blanket impls:
  - `feed_market`, `merge_market` and `follow_market` on `Market + Element`.
    They cover the 18 slim facts only.
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
- Replace `delegate_market_element!` with two delegates, one over the 18
  `Market` facts and one over the 5 `MarketOperation` facts. A wrapper that
  only needs `Market` generates only the first.

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

### `SecType`: a `#[repr(u8)]` enum

- One variant per FIX `SecurityIDSource(22)` code: `1` CUSIP, `2` SEDOL,
  `3` QUIK, `4` ISIN, `5` RIC, `6` ISO currency, `7` ISO country, `8` exchange
  symbol, `9` CTA, `A` Bloomberg symbol, `B` Wertpapier, `C` Dutch, `D` Valoren,
  `E` Sicovam, `F` Belgian, `G` Common, `H` clearing house, `I` ISDA/FpML,
  `J` OPRA, `K` ISDA/FpML URL, `L` letter of credit, `M` marketplace,
  `N`/`P` Markit RED, `Q` CFTC, `R` ISDA commodity, `S` FIGI, `T` LEI,
  `U` synthetic, `V` FIM, `W` index name, `X` UMTF, `Y` DTI.
- Add `Other = 255`. There is no `Ticker` variant, because `ticker` is its own
  field on `Market` and one fact has one owner.
- Discriminants are a wire and digest contract: fix them explicitly, never by
  declaration order.
- `SecType::from_spelling` reads the wire code, the spec name and the stored
  name, folded the same way `Side::from_spelling` folds today.
- `SecType::validate(&str)` dispatches to the existing validators (`IsinCode`,
  `CusipCode`, `SedolCode`, `FIGICode`, `BloombergCode`). Every other type is
  ASCII text up to a fixed max width.

### `SecurityId`: one type-tagged value, at most one allocation

- A newtype over a single buffer: the `SecType` tag byte followed by the
  validated code bytes, for example `SmolStr` or `Box<str>` with a 1-byte
  prefix.
- ISIN, CUSIP, SEDOL, FIGI, QUICK and RIC must fit inline with zero heap
  allocations. Only an unusually long code may allocate, and then once.
- Accessors: `sectype() -> SecType` (reads byte 0), `as_str() -> &str` (the
  code), `new(SecType, &str) -> Result<Self>` (validated), and `Display` as
  `ISIN:US0378331005`.
- `Ord` compares `(sectype, code)`, so a sorted list groups by type.
- Pin the size with `size_of::<SecurityId>()` in `rust/tests/allocations.rs`,
  and assert zero allocations for an ISIN.

### `SecurityIds`: a sorted set with merge, add and remove

- Storage: `SmallVec<[SecurityId; 2]>` or `Box<[SecurityId]>`. Measure both in
  `rust/benchmarks/` and keep the faster one for book-sized workloads.
- Invariant: sorted by `(sectype, code)` with no duplicates, enforced by every
  constructor and mutator. Test it with a property check.
- `insert(id) -> bool`: binary search, then insert. A no-op when present.
- `remove(&SecurityId) -> bool` and `remove_type(SecType) -> usize`.
- `get(SecType) -> Option<&SecurityId>` (first of that type) and
  `iter_type(SecType)`, both by binary search on the tag.
- `merge(&mut self, other: &SecurityIds) -> bool`: a linear two-pointer union,
  O(n + m), with no re-sort. Returns whether anything was added. This replaces
  the per-code `CodeValue::merge_with` calls in `merge_market`.
- `Deref<Target = [SecurityId]>`, so the trait hands out `&[SecurityId]`.
- `SecurityIds` replaces the five code fields (`isincode`, `cusipcode`,
  `sedolcode`, `bloombergcode`, `figicode`).
- Fold the ISIN-to-CUSIP rule from `instrument::embedded_cusip` into the slim
  `fill_market` as an `insert`. `InstrumentCodes::enrich` reads and writes
  `SecurityIds`.
- Digest: feed the ids in sorted order as `tag byte + code`, so the digest
  ignores insertion order.

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
  - the `bid*`/`ask*` lane columns, `tif`, `tradable` and
    `marketoperationid` leave it;
  - decimal columns stay `decimal128(38, 18)`.
- The operation row (orders, quotes, executions, trades, `fixmsg`) is the
  market row plus `marketoperationid`, `tif` (`yggdryl.timeinforce`), `tradable`,
  and `bid`/`ask` as two nullable `struct<price, currency, quantity, unit>`
  columns.
- Pick the `securityids` Arrow shape and state it in the schema docs.
  Default: `list<struct<sectype: dictionary<uint8, utf8>, code: utf8>>`,
  sorted.
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
     `securityids`, `execunix`, `cumqty`, `ticker`, no lanes, no `tif`/
     `tradable`/`marketoperationid`. The `executions` list keeps the full
     operation row.
   - `ticker` replaces `symbolticker` in both schemas.
   - `marketevent.json` (orders, quotes, executions) takes the operation row,
     with `bid`/`ask` lane structs in
     place of the eight lane columns.
3. Field ids renumber, so `market.books`, `market.orders`, `market.quotes` and
   `market.executions` are **recreated and rebuilt from `fix.refined`**, not
   evolved. Record this beside the existing 0.1.10 identity migration note in
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
   listings of one type) or one id per `SecType`?
3. `Side` on the wire: keep the string extension (default) or move to `uint8`?
4. `Unit` max width.
5. `Lane` storage: boxed (small operations) or inline (lane-heavy quotes)?
   Decide it by the size test.
