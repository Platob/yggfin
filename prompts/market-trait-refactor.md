# Prompt: slim the `Market` trait

Two prompts, run in order: **A** in `Platob/yggdryl` (the Rust core owns the
trait), then **B** in `Platob/yggfin` once A ships as a release. Each one is
written to be pasted into a fresh agent session on its own.

---

## A. Yggdryl: split `MarketElement` into a slim `Market` trait

### Context

`rust/src/graph/element.rs` defines `MarketElement: Element` with 34 facts and
68 accessors. Every holder carries all of them (`MarketElementData`,
`MarketEventData` in `rust/src/graph/event.rs`, and through them `BookSide`,
`Book`, `Quote`, `Order`, `Execution`, `Trade`, `FixMsg`). A book level or a
simple quote pays for five `Option<Code>` instrument slots, eight bid/ask lane
fields, `tif`, `tradable`, `symbolticker`, `cumqty` and a heap `String` unit it
rarely uses. `delegate_market_element!` in `graph/mod.rs` repeats the whole
surface per wrapper.

Goal: one small `Market` trait that a book level, a quote or any simple struct
can implement cheaply, with compact value types underneath it.

Follow `AGENTS.md`: Rust core first, smoke each step, then Python, then Node,
then docs. Delete what this replaces in the same change: no deprecated aliases
and no compatibility shims.

### Target trait

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
    fn get_metadata(&self) -> &MarketMetadata;
    // A matching `set_*` for each, plus
    // `securityids_mut(&mut self) -> &mut SecurityIds`.
}
```

- Keep `Element` (identity, digest, merge) as the supertrait wherever an
  implementor needs identity. `Market` itself must be implementable by a plain
  struct with no uuid or digest, such as a book level. Put the digest, merge
  and follow helpers (`feed_market`, `merge_market`, `follow_market`,
  `fill_market`) on `Market + Element` blanket impls, not on `Market`.
- `execunix` moves from `Event` to `Market`. `Event` keeps the other clocks.
  An undated entry reports `None`.
- Replace `delegate_market_element!` with one delegate over the 16 facts. The
  macro should shrink by about half.

### Facts leaving the trait (decide, don't drop silently)

| Fact | Default destination |
| --- | --- |
| `isincode`, `cusipcode`, `sedolcode`, `bloombergcode`, `figicode` | `SecurityIds` entries |
| `symbolticker` | `SecurityId` of `SecType::Ticker` |
| `tif`, `tradable`, `marketoperationid` | `MarketMetadata` |
| `cumqty` | `MarketMetadata`. It is derivable as `quantity - leavesqty`, so keep it only where a message states it |
| `bidpx/bidcurrency/bidqty/bidunit`, `ask*` | Off the trait. `Quote` owns two optional lanes `{price, currency, quantity, unit}`. `Book` already has `bid`/`ask` `BookSide`s. `fill_lanes` moves to `Quote` |
| `Element::identifiers` map | Stays on `Element`. `MarketMetadata` does not duplicate it |

`MarketMetadata` should be `Option<Box<..>>` internally, so a struct that
states none of it pays one pointer. The getter returns a shared static empty
value when it is absent.

### `SecType`: a `#[repr(u8)]` enum

- One variant per FIX `SecurityIDSource(22)` code: `1` CUSIP, `2` SEDOL,
  `3` QUIK, `4` ISIN, `5` RIC, `6` ISO currency, `7` ISO country, `8` exchange
  symbol, `9` CTA, `A` Bloomberg symbol, `B` Wertpapier, `C` Dutch, `D` Valoren,
  `E` Sicovam, `F` Belgian, `G` Common, `H` clearing house, `I` ISDA/FpML,
  `J` OPRA, `K` ISDA/FpML URL, `L` letter of credit, `M` marketplace,
  `N`/`P` Markit RED, `Q` CFTC, `R` ISDA commodity, `S` FIGI, `T` LEI,
  `U` synthetic, `V` FIM, `W` index name, `X` UMTF, `Y` DTI.
- Add a `Ticker` variant for `Symbol(55)`, and `Other = 255`.
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
- Fold the ISIN-to-CUSIP rule from `instrument::embedded_cusip` into
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
- The digest keeps feeding the spelling bytes, so `curruuid` for side does not
  move. The `SecurityIds` change moves identities anyway (see Contract).
- Remove `Side` from the `family_value!` `Code` family if it no longer fits
  the `SmolStr` leaf shape. Keep it a `Scalar` variant.

### `Unit`: a validated string type

- `code_leaf!`-style newtype over `SmolStr`, so short units (`bbl`, `MWh`,
  `shares`) stay inline with no allocation. It is validated ASCII within a
  fixed max width (pick and document it, for example 32).
- `Unit::none()` is the empty unit and `is_none()` tests for it. Add a
  `yggdryl.unit` Arrow extension beside `yggdryl.currency`.
- Replaces `unit: String` and the lane `Option<String>` units.

### Contract and storage

- The market schema changes: five code columns and `symbolticker` become one
  `securityids` column, the lane columns leave the market row (they stay on the
  quote row only), and `tif`, `tradable`, `marketoperationid` and `cumqty` move
  under `metadata`.
- Pick the `securityids` Arrow shape and state it in the schema docs.
  Default: `list<struct<sectype: dictionary<uint8, utf8>, code: utf8>>`,
  sorted.
- Identity changes: the digest input changes, so every `curruuid` of a market
  row changes. Say so in the changelog and bump the minor version.
  Consumers rebuild from capture and never dual-write.
- Update `.api-inventory.txt`, both bindings (Python `graph`/`fix` getters,
  Node `fix.rs`) and the docs pages for the market graph. Python and Node
  expose `securityids` as a list of `(sectype, code)` and `side` as its stored
  spelling.

### Done means

- Smoke clean per `AGENTS.md`, with `rust/tests/graph*` covering book, quote,
  order and execution folding.
- A size and allocation test pins `size_of::<Side>() == 1`,
  `SecurityId` zero-alloc for ISIN, and `size_of` for the holders before and
  after. Put the numbers in the PR body.
- A benchmark covers book building on the existing `rust/benchmarks/fix`
  capture, before and after.
- CI is green, and a release is tagged for yggfin to pin.

---

## B. Yggfin: adopt the slim `Market`

Run after A is released as Yggdryl `X.Y.Z`.

1. Bump `python/pyproject.toml` to the exact release and relock `python/uv.lock`.
2. Regenerate `schemas/rekep/marketevent.json` and `schemas/rekep/book.json`
   from the native fields. Never hand-edit them. The fields are
   `securityids` in place of `isincode`/`cusipcode`/`sedolcode`/
   `bloombergcode`/`figicode`/`symbolticker`, lanes gone from non-quote rows,
   `metadata` holding `tif`/`tradable`/`marketoperationid`/`cumqty`, and
   `execunix` on every market row, including book levels.
3. Field ids renumber, so `market.books`, `market.orders`, `market.quotes` and
   `market.executions` are **recreated and rebuilt from `fix.refined`**, not
   evolved. Record this beside the existing 0.1.10 identity migration note in
   `AGENTS.md` and `docs/`.
4. Adapt `python/src/rekep/market.py` (the `bid`/`ask` `deltas` flatten) and
   the `parse_books`, `parse_orders`, `parse_quotes` and `parse_executions`
   tasks to the new columns. Use Arrow kernels only, with no Python row loops.
5. dbt: `stg_fix_messages.sql`, `orders_events.sql`, `orders_current.sql` and
   `executions_fills.sql` read `isincode`, `symbolticker`, `side` and `cumqty`
   today. Read ISIN and ticker out of `securityids` with one macro, for example
   `security_id(securityids, 'ISIN')`, and `cumqty` out of `metadata`.
   `side` is still the same string.
6. Update `docs/contracts/types.md`, `docs/roadmap/order-book.md` and the
   pipeline task pages, then regenerate the samples.
7. `uv run pytest` must be green, including `test_schemas` and `test_docs`.
   Push the branch and read CI.

---

## Open decisions to confirm before running A

1. Lanes: do they leave the trait for `Quote` only (default), or stay as
   `MarketMetadata`?
2. `SecurityIds` uniqueness: `(sectype, code)` (default, which allows two
   tickers on two venues) or one id per `SecType`?
3. `Side` on the wire: keep the string extension (default) or move to `uint8`?
4. `Unit` max width.
5. Does `symbolticker` become `SecType::Ticker` (default), or stay a plain
   field?
