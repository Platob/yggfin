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
    fn get_currency(&self) -> &Ccy;               // type renamed from `Currency`
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
    fn get_accountid(&self) -> Option<&str>;      // the account the operation is booked to
    fn get_userid(&self) -> Option<&str>;         // the user who acted on it
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
  shared static empty value when it is absent. It never holds an
  identifier: those are read off the message's own fields.
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
  - Normalize with `TimeInForce::from_spelling` to the FIX code, so
    `TIMEINFORCE=day` and `59=0` are one value (see "What `ulbridge.log`
    requires", item 4). That moves identities for rows that spelled a
    name, which the rebuild covers.
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
  `Market` facts and one over the 7 `MarketOperation` facts. A wrapper that
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

### `FixMsg` securityids: backed by `SecurityID` and `SecurityAltID`

`FixMsg` already stores instrument identifiers in FIX's own fields. Do not
add a second store. `Market::get_securityids` on `FixMsg` reads those fields,
and every write goes back into them with the correct code-set value.

**What exists today** (`rust/src/fix/msg.rs`, `rust/src/fix/latest.rs`):

- `FixMsg::identifier(sources, read)` reads the primary `SecurityID(48)` where
  `SecurityIDSource(22)` names one of `sources`, then each `secaltids`
  occurrence (`SecurityAltID(455)` / `SecurityAltIDSource(456)`). Each
  candidate is validated on its own, so a bad primary falls through to an
  alternate.
- `derive_market` fills `isincode` (`"4"`), `bloombergcode` (`"A"`) and
  `figicode` (`"S"`) through it. `FixMsg::fill_market` then clears CUSIP and
  SEDOL unless the row stated them (`ROW_STATED_CUSIP`/`ROW_STATED_SEDOL`).
- The setters (`set_isincode`, …) call
  `set_secaltid(code, value)` → `latest::sync_group_occurrence(self,
  "secaltids", 456, code, 455, value)`. That keeps one occurrence per
  source, rewrites `NoSecurityAltID(454)`, and spells both values through
  `typed_spelling` against the registry.
- Shipped derivations also touch these tags (`fix/constants.rs`):
  - `22`: guesses the source by casting `48` as ISIN, then CUSIP, then SEDOL;
  - `48`: taken from the `secaltids` ISIN;
  - `55`: `SecurityID` where the source is `8` or `A`.

**Defects to fix while replacing it:**

1. **A stale primary shadows the write.** A setter writes only `secaltids`.
   If `22` already names that source, `48` keeps the old code, and the reader
   checks the primary first, so the next settle answers the old value. The
   wire also emits both.
2. **Clearing leaves the primary.** `set_*(None)` removes the alternate
   occurrence, but the primary `48`/`22` still states the code.
3. **The counter may outlive its group.** Reading `sync_group_occurrence`,
   removing the last occurrence rewrites the counter from
   `occurrences.len()`, which is `454=0`, beside an empty group. Confirm this
   with a test first. FIX wants the counter and the group both absent.
4. **Codes are hard-coded twice.** `"4"`, `"1"`, `"2"`, `"A"` and `"S"`
   appear in the setters and in `derive_market`, separately from the
   registry's `securityidsourcecodeset`.

**Target:**

- **One key-to-code table**, in `rust/src/securityid.rs`. `SecType`'s alias
  table maps each
  canonical key to its `SecurityIDSource` value. Both 22 and 456 declare
  `FIX:codeset: securityidsourcecodeset`, whose 33 values are `1`–`9` and
  `A`–`Y`. Key choices:

  | Code | Key | Code | Key | Code | Key |
  | --- | --- | --- | --- | --- | --- |
  | `1` | `CUSIP` | `C` | `DUTCH` | `P` | `REDPAIR` |
  | `2` | `SEDOL` | `D` | `VALOR` | `Q` | `CFTC` |
  | `3` | `QUIK` | `E` | `SICOVAM` | `R` | `ISDACOMMODITY` |
  | `4` | `ISIN` | `F` | `BELGIAN` | `S` | `FIGI` |
  | `5` | `RIC` | `G` | `COMMON` | `T` | `LEI` |
  | `6` | `ISOCCY` | `H` | `CLEARINGHOUSE` | `U` | `SYNTHETIC` |
  | `7` | `ISOCTRY` | `I` | `FPMLSPEC` | `V` | `FIM` |
  | `8` | `EXCHSYMB` | `J` | `OPRA` | `W` | `INDEX` |
  | `9` | `CTA` | `K` | `FPMLURL` | `X` | `UMTF` |
  | `A` | `BLOOMBERG` | `L` | `LOC` | `Y` | `DTI` |
  | `B` | `WKN` | `M` | `MKTASSIGNED` | | |
  | | | `N` | `REDENTITY` | | |

  - The code set's own names (`ISINNumber`, `RICCode`,
    `FinancialInstrumentGlobalIdentifier`, …) are aliases that
    `SecType::read` accepts. `BBGSYMB` and `BloombergSymbol` read as `BLOOMBERG`.
  - A test loads the shipped registry and checks that the table and
    `securityidsourcecodeset` agree exactly. Every code must have one key,
    every key one code, and every code-set name must read to its key. A new
    code in a registry update then fails loudly instead of silently mapping
    to nothing.
  - `derive_market` and the setters call `SecType::fix_source` and
    `SecType::from_fix_source`. No literal source code is left in `fix/`.
- **Reading.** `FixMsg::get_securityids` is built once per settle from the
  stated FIX content: the primary `(22, 48)` plus every `secaltids`
  occurrence `(456, 455)`.
  - Each source code is read through the table, and each code validated by
    `SecType::validate_code`. An invalid code is skipped; it is not refused
    and not guessed.
  - An occurrence with a source outside the code set keeps its raw source
    text as its key, upper-cased, so nothing the message stated is dropped.
  - One code per key. The primary `48`/`22` fills first, then each
    `secaltids` occurrence in order, with `insert`. The same code twice is one
    entry. A later occurrence stating a different code under a filled key is
    dropped from the map, and an anomaly records it. It still stays in the
    FIX content and on the wire.
  - The CUSIP and SEDOL special case in `FixMsg::fill_market` goes away with
    their crated tags: a CUSIP the message stated is a `CUSIP` entry like any
    other. That changes
    `securityids` and the digest for CUSIP/SEDOL rows, which the rebuild
    covers.
- **Derived entries never reach the wire.** `securityid::embedded` and the
  per-ISIN `SecurityIdRegistry::enrich` fill are derived, not stated. They live
  in a derived overlay on the event, merged into what `get_securityids`
  answers. They are never written to `48`/`22`/`secaltids`, never
  re-emitted, and never part of the arrival record. This is the rule the
  `derive_market` docs already state for every derived fact.
- **Writing.** `insert`, `remove`, `remove_key` and `set_securityids` on a
  `FixMsg` go through one function,
  `latest::sync_security_id(msg, code, value: Option<&str>)`:
  - **Where the code lives:** if the primary `22` names `code`, write `48` in
    place. Otherwise, write the `secaltids` occurrence for `code` through the
    existing `sync_group_occurrence`. A value is never written in both
    places.
  - **Clearing:** `None` removes the primary `48` and `22` when they name
    `code`, and the matching alternate occurrence. When the group ends up
    empty, drop the group and `NoSecurityAltID(454)` together.
  - **New codes:** a first write for a code the message does not state goes
    to `secaltids`, never to the primary, so a message's primary identifier
    is never replaced by a setter.
  - **Spelling:** each value goes through `typed_spelling` against the
    registry, so a code-set-typed column holds its declared type.
  - **Keys without a code:** a key with no `SecurityIDSource` value (a
    venue's own key, `OMSINSTRUMENTID`) is written to `secaltids` with the
    key itself as the `456` text, outside the code set. Reading reads it
    back under the same key, so the round trip is exact. It never takes a
    private tag, and a key is never replaced by an invented code (no `M`
    for "marketplace").
  - **Flags:** every write sets `forced` and the matching row-stated bit, as
    the setters do today, and invalidates the cached view.
- **Derivations.** Keep derivations `22`, `48` and `55` as they are. They
  run before the view is built, so a `22` guessed from a bare `48` gives a
  keyed entry. Add a test for that case.
- **Tests:**
  - A message with `22=4|48=US0378331005`, then `set` ISIN
    `US5949181045`: `48` changes in place, no alternate occurrence is added,
    and the wire, the digest and `get_securityids` all agree.
  - The same message with ISIN removed: both `48` and `22` are gone.
  - `454=2` with `ISIN` and `FIGI`, then removing both: no group and no
    `454`.
  - Inserting `RIC` with no primary: `454=1|455=…|456=5`.
  - The embedded CUSIP of a US ISIN appears in `get_securityids` but never
    on the wire.
  - A `456` value outside the code set survives a round trip.
  - The code-set agreement test above.

### FIX parsing: unmapped instrument fields feed `securityids`

Venues and bridges send instrument identifiers under names no dictionary
maps. The shipped capture does this: `#CFICODE=ESVTFR|#ISINCODE=CH0012221716|
#LASTMKT=XSWX`. The builder already keeps them rather than dropping them
(`fix/build.rs`): an unknown tag stays nullable `utf8` under its decimal
spelling, an unknown or `#`-marked name stays one flat child under its own
name, and either records tag zero. `ISINCODE` and `BLOOMBERGCODE` resolve
because the crate declares its own `isincode` and `bloombergcode` columns.

**Crated code columns: delete CUSIP and SEDOL, keep ISIN, Bloomberg and
FIGI.**

| Constant | Tag | Fate |
| --- | --- | --- |
| `CUSIPCODE_TAG_NAME` | 65_057 | deleted |
| `SEDOLCODE_TAG_NAME` | 65_058 | deleted |
| `ISINCODE_TAG_NAME` | 65_055 | kept |
| `BLOOMBERGCODE_TAG_NAME` | 65_059 | kept |
| `FIGICODE_TAG_NAME` | 65_061 | kept |

- **Deleted (CUSIP, SEDOL).** Delete their `Crated::own` entries,
  `ROW_STATED_CUSIP`/`ROW_STATED_SEDOL`, their field entries in
  `config/fix/fields/000000650.json` and `components/fixmsg.json`, and the
  CUSIP/SEDOL branch of `FixMsg::fill_market`. The tags are retired, not
  reused. A CUSIP or SEDOL now lives only in `securityids`, from `48`/`22`,
  `secaltids`, an unmapped `CUSIPCODE`/`SEDOLCODE` (this rule), or
  `securityid::embedded`.
- **Kept (ISIN, Bloomberg, FIGI).** These are one scalar column each on the
  `fixmsg` row, and they are views of `securityids`, never a second owner:
  - **In:** a value arriving under the crated name (`ISINCODE`,
    `#ISINCODE` with no differing bare twin, `BLOOMBERGCODE`, `FIGICODE`)
    is a stated `securityids` entry. It keeps its `ROW_STATED_*` bit and
    ranks after `48`/`22` and `secaltids`.
  - **Out:** at the Arrow boundary the column is written from
    `securityids.get("ISIN" | "BLOOMBERG" | "FIGI")`, the one code under
    that key, derived ones included, as today's derived `isincode` is.
  - **Writes:** `latest::sync_security_id` removes a stated crated child
    for the key it writes, so the next boundary re-derives the column from
    `securityids`, and the message states that key in one place.
  - The crated tag itself never re-emits on the wire unless the line stated
    it, exactly as today.
- `fixmsg` also gains the `securityids` column, beside the three kept
  scalars.

**Rule.** After the builder has resolved every field, each **top-level,
unmapped** field (tag zero: an unknown name, a `#`-marked name, or an unknown
tag) whose name names an identifier scheme becomes a stated `securityids`
entry:

- **Name matching.** It lives in `securityid.rs`, as
  `SecType::from_field_name(&str) -> Option<SecType>`, with one table and no
  regular expression.
  - Strip a leading `#`, then fold the way names fold: ASCII
    case-insensitive, with `_`, `-` and spaces ignored.
  - Accept `[SECURITY]` + *alias* + `[CODE | ID | NUMBER]`, where *alias* is
    any `SecType` alias: `ISIN`, `CUSIP`, `SEDOL`, `FIGI`, `BLOOMBERG`,
    `BBG`, `RIC`, `WKN`, `VALOR`, `VALOREN`, `QUIK`, `LEI`, and the rest of
    the key table.
  - So `ISINCODE`, `isin_code`, `SecurityISIN`, `FIGI`, `BloombergCode`,
    `RICCode` and `ValorNumber` all match.
  - `SecurityID`, `SecurityAltID` and every other dictionary name never
    reach this rule, because they are mapped.
- **Not identifiers.** `TICKER`, `SYMBOL` and `SYMBOLTICKER` are not
  `securityids`; `ticker` owns them. Neither is anything with a
  `LEG`/`UNDERLYING`/`CONTRA`/`RELATED`/`BENCHMARK` prefix: those describe
  another instrument. A venue's own instrument ID is declared as a
  dictionary field (see "Instrument-ID fields"), not matched by this
  name rule.
- **Top level only.** A field inside a repeating group describes that
  occurrence's instrument, not the message's, and is never read.
- **Value.** The value is trimmed of padding and validated by the matched
  key (`SecType::validate_code`: check digits, width, Bloomberg's case and
  spaces kept).
  - An invalid value adds nothing to `securityids`. The field stays in the
    row as it arrived, and the refusal is recorded in the message's
    anomalies, as the builder records any value that will not type.
  - An empty value, or a null-like one (`null`, `none`, `[n/a]`), adds
    nothing and records nothing.
- **Precedence.** It is a stated source, not a derived one: the message said
  it. It ranks after the primary `SecurityID`/`SecurityIDSource` and after
  `secaltids`, and before anything derived, and it fills with `insert`. The
  same code stated in two places is one entry. A different code under a
  key a higher-ranked source already filled is dropped, and an anomaly
  records the dropped value, so a disagreement stays visible.
- **Writes.** `latest::sync_security_id(msg, code, value)` also removes an
  unmapped field that states the same key, so after a set or a clear the
  message states that key in exactly one place (`48`, or `secaltids`). The
  unmapped field is never rewritten in place, and a write never creates one.
- **Wire and digest.** The unmapped field re-emits exactly as it arrived,
  like every unknown field today. Its `securityids` entry is a digest input
  like any stated entry, and the rebuild covers the identity change.
- **Registry and embedded rules.** These entries are stated, so
  `SecurityIdRegistry` learns from them, and `securityid::embedded` derives
  from an ISIN that came this way.
- **Tests:**
  - The shipped capture line `…#CFICODE=ESVTFR|#ISINCODE=CH0012221716|
    #LASTMKT=XSWX…` gives `ISIN:CH0012221716` stated (through the kept
    crated `isincode`) and `VALOR:1222171` derived, with `cficode` `ESVTFR`
    and `miccode` `XSWX` as before.
  - `cusip_code=037833100` and `#SEDOLCODE=0263494` (names that are
    unmapped once their crated tags are gone) give `CUSIP:037833100` and
    `SEDOL:0263494`.
  - `#CUSIPCODE=037833101` (bad check digit) gives nothing, records an
    anomaly, and the field still re-emits.
  - `LegISIN=…` and a `#ISINCODE` inside a group give nothing.
  - `#TICKER=AAPL` gives nothing in `securityids`.
  - `22=4|48=US0378331005|#ISINCODE=US0378331005` gives one entry.
  - `set` ISIN on a message carrying only `ISINCODE` writes `secaltids`,
    removes the stated crated child, and the `isincode` column and the wire
    show the new value once.
  - `set` CUSIP on a message carrying only `CUSIPCODE` writes `secaltids`
    (`456=1`) and removes the unmapped `CUSIPCODE`.

### `cficode`: a detailed code, or null

A code with only a category and a group (`ESXXXX`, `EMXXXX`, `OPXXXX`: all
four attributes `X`) is not accepted as a CFI. The `cficode` field holds a
**detailed** code, or it is null.

- **Definition.** Add `CfiCode::is_detailed(&str)`: the code is well formed
  and classified (`CfiCode::is_classified`), and at least one attribute
  (positions 3–6) is not `X`. `X` inside a detailed code stays legal, as
  ISO 10962 uses it for "not applicable" (`FFICSX`).
- **Where it applies.** Everywhere a `cficode` is set or read in:
  - `Market::set_cficode` stores `None` for a non-detailed code. The setter
    is infallible, so it records nothing, and a debug assertion flags a
    caller passing one.
  - `FixMsg` derivation: the classification chain's result must pass
    `is_detailed`, else `cficode` is null.
  - Arrow reads: a stored `cficode` that is not detailed reads as null and
    records an anomaly, like any value that will not type.
  - Merge and follow: `CfiCode::merged` of two detailed codes is detailed,
    and a non-detailed operand never reaches them.
  - `SecurityIdRegistry`: it already ignores all-`X` codes when learning
    (`Association::classify`), and now fills only detailed ones.
- **The chain shrinks** (`fix/cfi.rs`, `FixMsg::classification`). A coarse
  step can only produce a category and group, and merging never lets it
  change a stated group, so on its own it can never make a detailed code.
  Delete the steps that only ever produced coarse codes:
  - `classification_of_security_type` (`SecurityType(167)`);
  - `category_of_product` (`Product(460)`);
  - `category_of_id_source` (`SecurityIDSource(22)`).

  What remains, in order:
  1. a stated `CFICode(461)`;
  2. an unmapped `DETAILEDCFICODE`, merged in to fill `X`s;
  3. `PutOrCall(201)`, refining a stated `OM` group.

  The result is kept only if it is detailed. `classification()` keeps its
  name and returns `None` where it used to return a coarse code. Rewrite
  its doctests: `167=CS`, `460=5` and `55=AAPL` alone now give `None`, and
  `461=ESXXXX|167=CS` gives `None`.
- **The raw fallback goes.** Delete the fallback in `fix/msg.rs` that
  takes raw `461` text when the chain answers `None` (`.or_else(|| word(
  CFICODE_TAG)…)`). It could only reintroduce a coarse code.
- **Row IDs.** `cficode` is a digest input, so rows whose code was coarse get
  a new identity. The rebuild covers that.
- **Tests:**
  - `461=ESXXXX` gives null;
  - `461=ESXXXX` with `DETAILEDCFICODE=ESVTFR` gives `ESVTFR`;
  - `461=ESVUFR` gives `ESVUFR`;
  - `461=OMAXXX|201=0` gives `OPAXXX`;
  - `167=OPT|201=0` alone gives null;
  - a stored `EMXXXX` in an Arrow `cficode` column reads null, with an
    anomaly.

### Instrument-ID fields land in `secaltids` and `securityids`

The bridge's instrument IDs are identifiers in their own right, whatever
they embed. `ulbridge.log` states them 21 times:
- `#OMSINSTRUMENTID=dbi;CH0012214059_XSWX_CHF`;
- `ULLINK.INSTRUMENTID=dbi;TW0002454006_XTAI_TWD`.

Keep each whole value as an alternate security ID, keyed by the field's
name.

- **Mechanism: the one `ExecBroker(76)` uses.** A dictionary field with a
  `FIX:replacements` plan that restates into a group. The probe verified
  on this capture that `fix/latest.rs` restates while carrying the arrival
  entries through untouched, so the wire stays byte-identical and the
  original value stays in `fixentries`. No new mechanism, and no
  special case in `derive_market`.
- **Declare two fields.** Put them in `config/fix/fields/000000650.json`,
  with tags taken in the crate's range after `pluginoriginator`. They are
  nullable `utf8`, and **not projected** as `fixmsg` columns, so their
  values stay in the residual exactly as `execbroker`'s does:

  | Field | `FIX:names` | Replacement plan |
  | --- | --- | --- |
  | `omsinstrumentid` | — | `select [{securityaltid: omsinstrumentid, securityaltidsource: 'OMSINSTRUMENTID'}] as secaltids where omsinstrumentid is not null` |
  | `ullinkinstrumentid` | `ullink.instrumentid` | `select [{securityaltid: ullinkinstrumentid, securityaltidsource: 'ULLINK.INSTRUMENTID'}] as secaltids where ullinkinstrumentid is not null` |

  A new bridge's instrument ID is one more such entry in the dictionary,
  never code.
- **The `456` value.** It is the field's own name, upper-cased
  (`OMSINSTRUMENTID`, `ULLINK.INSTRUMENTID`), a source outside
  `securityidsourcecodeset`. Check that `typed_spelling` keeps text outside
  a string field's code set rather than nulling it. If it does not, the
  replacement's all-or-nothing check blocks the whole rule, so fix that
  first, with a test.
- **The `securityids` view.** It already reads a `456` outside the code set
  as its raw text, upper-cased, so each occurrence becomes
  `OMSINSTRUMENTID:dbi;CH0012214059_XSWX_CHF` and
  `ULLINK.INSTRUMENTID:dbi;TW0002454006_XTAI_TWD`, ranked as a stated
  `secaltids` entry. Nothing extra is written for the view.
- **Widths.** Keys like `ULLINK.INSTRUMENTID` (19) need `SecType`'s key
  width at 32, not 16. The values (up to 25 here) fit the default 32-byte
  code width. A value over 32 blocks the rule, keeps the field in
  `fixentries`, and records an anomaly.
- **Merging.** The restatement merges into an existing `secaltids`
  occurrence with the same source and value, and never duplicates it. It
  never overwrites an occurrence whose value differs under the same
  source: what the message stated stands.
- **Tests:**
  - every `ulbridge.log` line carrying either field gets exactly one
    matching `secaltids` occurrence and `securityids` entry;
  - the wire output stays byte-identical;
  - `fixentries` still holds the original `omsinstrumentid` or
    `ullinkinstrumentid` entry;
  - a message stating the same `secaltids` occurrence already gets no
    duplicate.

### Instrument-key fields: `{ISIN}_{MIC}_{CCY}`

Bridges name the listing in one field: `#OMSINSTRUMENTID=dbi;CH0012214059_XSWX_CHF`
and `ULLINK.INSTRUMENTID=dbi;TW0002454006_XTAI_TWD` in `ulbridge.log`. Lift
from it naively.

- **Where to look.** The instrument IDs already grabbed into `securityids`:
  entries whose key ends in `INSTRUMENTID`, such as `OMSINSTRUMENTID` and
  `ULLINK.INSTRUMENTID`, from the section above. Nothing else is scanned.
- **Pattern.** Take the text after the last `;`, if any, and match
  `{ISIN}_{MIC}_{CCY}`: 12, 4 and 3 alphanumerics joined by `_`. The first
  value that matches wins.
- **Lift.** For ISIN and `currency`, set each from the match if it is
  **currently empty** after the other sources have run. The MIC part is not
  lifted separately. It is step 3 of the `miccode` ladder, ahead of
  `SecurityExchange(207)`. A fact
  that already has a value is left alone. There is no cross-check and no
  anomaly. A part that its type refuses (`IsinCode`, `MicCode::is_iso`,
  `Ccy`) is skipped, and the other parts still lift.
- The ISIN goes into `securityids`, so `securityid::embedded` and
  `SecurityIdRegistry` see it. Nothing is written back to the FIX fields, and
  the source field re-emits unchanged.
- **Tests:**
  - only `ULLINK.INSTRUMENTID=dbi;CH0012214059_XSWX_CHF` gives
    `ISIN:CH0012214059`, `miccode` `XSWX` and `currency` `CHF`;
  - with `15=EUR` stated, the currency stays `EUR`;
  - a stated ISIN is never replaced;
  - over `ulbridge.log`, count the messages where the key fills anything,
    and pin that count.

### What `ulbridge.log` requires

`rust/tests/fix/ulbridge.log` (the same file as yggfin's
`python/tests/data/ulbridge.log`) has 144 lines: 63 named `KEY=value|` bodies,
21 raw FIX lines and 2 FIXML lines. Its instruments are
- ABB (`CH0012221716`),
- Novartis (`CH0012005267`),
- Holcim (`CH0012214059`),
- `TW0001605004`,
- MediaTek (`TW0002454006`).

Every rule above must hold on it. A probe run of today's codec over it read
each message's `22`, `48`, `207`, `30`, `100`, `461`, `59` and `55` and its
market getters, and confirmed six things the rules must handle:

1. **Named bodies spell codes as names, in lower case.**
   `SECURITYIDSOURCE=isin` and `SECURITYALTIDSOURCE=isin|bloomberg|ric`,
   where raw FIX says `22=4` and `456=4`. Today `FixMsg::identifier(&["4"],
   …)` compares the literal `"4"`, so it misses every named-body ISIN, and
   only the crated `ISINCODE` column rescues it. The `FixMsg` securityids
   view reads each source through `SecType::read`, which accepts the code,
   the key and the name folded, so `isin`, `bloomberg` and `ric` resolve. A
   write always emits the code-set value (`4`, `A`, `5`). The same applies
   to `SIDE=buy` (already handled by `Side::from_spelling`) and
   `TIMEINFORCE=day` (see 4).
2. **Marked alternate-ID groups become real `secaltids`.** The MediaTek
   flow states `#NOSECURITYALTID=2`,
   `#NOSECURITYALTID[0]=SECURITYALTID=2454 TT Equity••SECURITYALTIDSOURCE=bloomberg••`
   and `#NOSECURITYALTID[1]=SECURITYALTID=2454.TW••SECURITYALTIDSOURCE=ric••`,
   with no bare twin.
   - `judge_hashed` (`fix/codec.rs`) answers `Bare` for a marked key with no
     bare twin, so the reader strips the `#`. The probe shows a real
     `secaltids` child, `207=XTAI` from `#SECURITYEXCHANGE`, and
     `48=TW0002454006` from `#SECURITYID`. No packed-value parsing is needed
     in the unmapped rule.
   - The gap is item 1: the sources are the names `bloomberg` and `ric`, so
     today nothing surfaces the RIC. Through `SecType::read` the view gives
     `RIC:2454.TW` and `BLOOMBERG:2454 TT Equity`.
   - A marked key *with* a differing bare twin stays a `#`-named flat child
     (a marked group with a bare twin is reindexed into it). Only a flat
     marked child whose name matches the unmapped rule feeds `securityids`.
   - The line with `22=isin|48=XX0000000001` fails the ISIN check digit and
     already gives no ISIN. Keep that: no entry, plus an anomaly.
3. **Venue codes are not MICs, and `MicCode::new` accepts them.** The flows
   state `EXDESTINATION=S` and `EXDESTINATION=TW`. `MicCode` is a
   `code_leaf!` over `ascii_text(4, …)`, which only refuses text *longer*
   than 4, so `S` and `TW` are valid `MicCode`s today. The `miccode` rule's
   per-source check must therefore be ISO 10383's shape: exactly 4 of
   `[A-Z0-9]`. Add `MicCode::is_iso(&str)` and use it for each of `207`, `30`
   and `100`. Do not tighten `MicCode::new` itself in this change, because
   stored columns may hold short codes.
   - Test: `SECURITYEXCHANGE` missing, `LASTMKT` missing and
     `EXDESTINATION=S` gives no MIC.
   - Test: line 123's `30=RJEA` (no `207`) gives `RJEA`.
4. **Time in force sometimes keeps its name.** Execution reports store
   `59=0` whether the line said `TIMEINFORCE=day` or `59=0`, but the
   MediaTek `cancelreject` keeps `59="day"`, and `TW0001605004` states
   `59=6`. So one lifecycle can hold `0` and `day` for the same order, and
   merge and digest see a conflict. Add `TimeInForce::from_spelling`,
   mirroring `Side::from_spelling`: it reads the `TimeInForce(59)` code, the
   code set's name (`Day`, `GoodTillCancel`, `ImmediateOrCancel`,
   `FillOrKill`, `GoodTillDate`, …) and the stored value, folded, and stores
   the code. Apply it wherever `tif` is set, whatever message type. A
   spelling it does not know is kept as stated, never refused. Pin the name
   table to the shipped `timeinforcecodeset` with a test.
5. **A detailed CFI rides beside a coarse one.** The Holcim and Novartis flows
   state `CFICODE=ESXXXX` with `#DETAILEDCFICODE=ESVTFR`, and the probe
   classifies them `ESXXXX`. Under the `cficode` rules above, they answer
   `ESVTFR` where `DETAILEDCFICODE` is stated and null where it is not. The
   MediaTek and `TW0001605004` flows (`ESXXXX` or nothing) answer null, and
   so does the `AE` trade capture's `461=OXXXXX`, which the probe classified
   `OMXXXX`.
6. **The security type arrives as a name** (`SECURITYTYPE=equity`). It no
   longer matters, because the `SecurityType` step is deleted.

**`ulbridge.log` tests**, run in `rust/tests/fix/ulbridge.rs` over the whole
file:

| Flow | Expect |
| --- | --- |
| ABB | `securityids` = `ISIN:CH0012221716` stated (from `22=4`/`48`, or `SECURITYIDSOURCE=isin`/`SECURITYID`, `secaltids`, `#ISINCODE`, deduplicated to one), and `VALOR:1222171` derived; `miccode` `XSWX`; `cficode` `ESVTFR`; `tif` `0` on both the named and the raw form |
| Novartis | `ISIN:CH0012005267`, `BLOOMBERG:NOVN SW`, derived `VALOR:1200526`; `cficode` `ESVTFR` where `DETAILEDCFICODE` is stated, else null (never `ESXXXX`) |
| Holcim | `ISIN:CH0012214059`, `BLOOMBERG:HOLN SW`, derived `VALOR:1221405`; `miccode` `XSWX`, never `S`; `cficode` `ESVTFR` |
| `TW0001605004` | `ISIN:TW0001605004`; `miccode` `RJEA`; `cficode` null |
| MediaTek | `ISIN:TW0002454006`, `BLOOMBERG:2454 TT Equity`, `RIC:2454.TW` (from the marked `secaltids` with named sources); `miccode` `XTAI` (from the instrument key, ahead of `207`), never `TW`; `tif` `0`, not `day`; `cficode` null (was `ESXXXX`) |

Also check:
- **The `ExecBroker(76)` redirect** (verified by a probe run of today's
  codec, keep it green):
  - all 40 messages stating it get a `parties` occurrence with
    `partyrole` `1` for the value (`2003103.001` on 36, `CITP` on the AE,
    `RJEA` on 3);
  - where the line already states that party (the 3 lines carrying both
    `SWXCCP` and `2003103.001` as executing firms), the redirect merges
    into the stated occurrence instead of adding a duplicate;
  - there is no `execbroker` column, and the original stays in `fixentries`
    as `{tag: 76, name: execbroker, value: …}` on every one of the 40 rows.
- no line produces a `securityids` entry the message did not state, apart
  from the derived `VALOR`s;
- `ticker` is the `SYMBOL` (`ABBN.S`, `NOVN`, `HOLN`, `1605`, `2454`),
  never `#SYMBOL`;
- every line still round-trips byte for byte on the wire.

### `miccode`: `LastMkt`, `ExDestination`, the instrument key, then `SecurityExchange`

Today `FixMsg` derives the MIC in `rust/src/fix/msg.rs` (the market-facts
block, near `// The market it is listed on, routed to, or last traded on.`):

```rust
let miccode = word(207).or_else(|| word(100)).or_else(|| word(30))
    .and_then(|held| MicCode::new(&held).ok());
```

Replace it with a four-step ladder. Each step is validated before the next is
tried, and the first valid MIC wins:

1. `LastMkt(30)`: where the last fill traded.
2. `ExDestination(100)`: where the order was routed.
3. The MIC part of the instrument-key pattern `{ISIN}_{MIC}_{CCY}`, the
   regex match described in "Instrument-key fields" below.
4. `SecurityExchange(207)`: where the instrument is listed, the last
   resolver.

```rust
let miccode = [30, 100].into_iter()
    .find_map(|tag| word(tag).filter(|held| MicCode::is_iso(held)))
    .or_else(|| instrument_key.and_then(|key| key.mic.clone()))
    .or_else(|| word(207).filter(|held| MicCode::is_iso(held)))
    .and_then(|held| MicCode::new(&held).ok());
```

- **Order.** A trade's actual venue comes first, then its route, then the
  listing the bridge's instrument key names, and only then the listing the
  message states. `SecurityExchange` moves from first today to last.
- **Validation.** Each candidate needs `MicCode::is_iso` (exactly 4 of
  `[A-Z0-9]`), not only `MicCode::new`, which accepts `S` and `TW` (see
  "What `ulbridge.log` requires", item 3). A broker code in
  `ExDestination` falls through instead of answering.
- A row that states `miccode` itself (`ROW_STATED_MIC`) still wins over the
  whole ladder. The ladder only answers where the row says nothing.
- **`INSTRUMENT[EXCHANGE]` is an alias of the crated `miccode`** (tag
  `65_060`). `ulbridge.log` states `#INSTRUMENT[EXCHANGE]=XSWX` on 9 lines,
  and today's parser keeps it as an unmapped flat child named
  `instrument[exchange]`.
  - Give `miccode` in `config/fix/fields/000000650.json`
    `"FIX:names": ["instrument[exchange]"]`. `is_word` accepts the
    brackets: any non-empty text needing no JSON escape.
  - Check first that the builder resolves the bracketed spelling as a flat
    name through the alias table, not as a group `INSTRUMENT` with
    occurrence `EXCHANGE`. `Key::parse` must leave a non-numeric bracket
    flat, as the probe shows it does today. Pin that with a test.
  - Being an alias of the stated column, a valid value **counts as a stated
    `miccode`**, so it wins over the whole ladder, like `MICCODE` itself.
    It follows the alias rules in the `OrderID` section: the canonical
    `MICCODE` wins when both arrive, and the alias then stays its own field.
  - The value must pass `MicCode::is_iso`. A value that does not (`S`,
    `TW`) sets no `ROW_STATED_MIC`, stays as its own field, and records an
    anomaly, and the ladder answers.
  - Tests:
    - `INSTRUMENT[EXCHANGE]=XSWX` with `30=XLON` gives `XSWX`;
    - `MICCODE=XLON|INSTRUMENT[EXCHANGE]=XSWX` gives `XLON`;
    - `INSTRUMENT[EXCHANGE]=S` with `30=XSWX` gives `XSWX` and an anomaly;
    - over `ulbridge.log`, the 9 lines keep `XSWX`, which already agrees
      with `30` and `207` there.
- Update the comment above the rule and the `crated.rs` module doc to the
  new order.
- **Tests:**
  - `30`, `100` and `207` all valid gives `30`'s;
  - no `30`, with `100` and `207` valid, gives `100`'s;
  - no `30`, `100=S`, a key `…_XSWX_CHF` and `207=XTAI` gives `XSWX` (the
    key beats `207`);
  - only `207` gives `207`'s;
  - `100=S` or `100=TW` alone gives no MIC;
  - a row-stated `miccode` wins over all four.
- **Row IDs.** `miccode` is a digest input, so rows whose MIC changes get a
  new identity: rows where `207` disagreed with `30`, `100` or the key. The
  rebuild covers that.

### Delete the `identifiers` map field

Today there are three copies of the same facts:
- `Element` has `get_identifiers() -> &BTreeMap<String, String>` and
  `set_identifiers(..)`.
- `fix/enrich.rs::enrich_restated` writes the map from the message type's
  `FIX:identifiers` declaration (`FixMsgType::identifier_mapping`), and stores
  it as the `identifiers` Map column (`IDENTIFIERS_TAG_NAME`, tag `65_020`,
  group `config/fix/groups/identifiers.json`).
- The graph holders copy the map, and `take_identifiers` unions it on merge
  and follow.

The values it holds are already on the message: `ClOrdID(11)`, `OrderID(37)`,
`ExecID(17)` and the other IDs are lifted typed in `FixLifted`, and the rest
are row fields. Delete the map.

- **Stored field.** Delete the `identifiers` Map column from the `fixmsg` row
  and from every market row. That means deleting:
  - `IDENTIFIERS_TAG_NAME` and its `Crated::event` entry in `fix/crated.rs`;
  - `EventColumn::Identifiers` in `graph/column.rs`;
  - `config/fix/groups/identifiers.json`;
  - the `set_unsettled(IDENTIFIERS_TAG_NAME…)` write in `enrich_restated`.

  Tag `65_020` is retired, not reused.
- **Trait.** Delete `get_identifiers`, `set_identifiers` and
  `take_identifiers` from `Element` and from every implementor, delegate
  macro, binding and doc example. The holders
  (`MarketData`, `MarketEventData` and both operation holders) lose the
  `BTreeMap` field. Merge and follow stop folding it.
- **Accessor only, on `FixMsg`.** `FixMsg::identifiers(&self) -> impl
  Iterator<Item = (&str, Cow<'_, str>)>` answers from
  `self.registry.get_msgtype(msgtype).identifier_values(self)`. The message
  type's own `FIX:identifiers` declaration decides which members count, and
  the values are read off the message's own fields. Nothing is stored or
  settled. Rename `identifier_mapping` or delete it with its only caller.
- **`msgsesseventid`.** It was the map's one non-FIX key
  (`MSGSESSEVENTID_IDENTIFIER` in `fix/msg.rs`), joined from the message type,
  capture session, capture context and `MsgSeqNum`. All four are already on
  the message.
  - Replace `sync_session_event_identifier` with a pure accessor,
    `FixMsg::msgsesseventid() -> Option<String>`, spelled exactly as today.
  - It stays out of the content digest, as it is today.
  - If a stored column is still needed for dedup, write it as its own
    nullable `utf8` column `msgsesseventid` at the Arrow boundary, from the
    accessor. Never a map again.
- **Cross code.** `crosscode` already comes from `identity::CROSS_TAGS` read
  off the lifted IDs, not the map, so it does not change. Confirm with a test.
- **Definitions do the work, so fill them in.** Only 64 of the 181
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
- **Digest.** The map was never a digest input, so deleting it moves no
  identity. The Arrow schema change still renumbers field ids (see Contract).

### `rust/src/securityid.rs`: one module for every security-ID interop

Everything that turns one security identifier into another, or into and out
of FIX, lives in one new module. Nothing else in `graph/` or `fix/` keeps its
own copy.

| Moves in | From |
| --- | --- |
| `SecType`, `SecurityId`, `SecurityIds` (below) | new |
| The key-to-`SecurityIDSource` table and its code-set agreement test | the `FixMsg` securityids section |
| `embedded_cusip` (a US or Canadian ISIN carries its CUSIP in positions 3–11) | `graph/instrument.rs` |
| The ISIN-keyed registry `InstrumentCodes`: its `Association` learning, budget (`MAX_REGISTRY_BYTES`, `ENTRY_CHARGE`, `MAX_INSTRUMENTS`) and `enrich` | `graph/instrument.rs` |
| The `internals` test wrapper (`lib.rs` re-exports it as `graph_instrument`) | `graph/instrument.rs` |

- **Delete `graph/instrument.rs`.** Rename the registry to
  `SecurityIdRegistry`, still `pub(crate)`, still one per ordered lifecycle,
  never per codec. `fix/enrich.rs` imports it from `crate::securityid`.
  Re-export the internals wrapper as `security_id`, drop `graph_instrument`,
  and move `rust/tests/graph/instrument.rs` to `rust/tests/securityid.rs`.
- **Key by ISIN, learn every key.** The registry is keyed by
  `SecurityIds::get("ISIN")`. Today it learns four fixed fields (`cusip`,
  `sedol`, `bloomberg`, `figi`). Instead it learns an `Association` per
  `SecurityId` key the event states, so a RIC, a WKN or a venue key is
  learned and filled the same way. The CFI association stays beside it.
  - The ambiguity rule is unchanged: two different codes under one key for
    one ISIN makes that key ambiguous, and it never fills again. There is no
    majority vote.
  - Re-derive `ENTRY_CHARGE` for a variable key set. Keep the
    compile-time asserts, and cap keys per instrument so the charge stays an
    upper bound. `MAX_BLOOMBERG_HEAP_ALLOWANCE` becomes the per-key heap
    allowance for the widest code, `SecType::max_code_width` of `BLOOMBERG`,
    charged once per learned key, because every key may hold a heap-sized
    code.
- **One place for embedded identifiers.** `embedded_cusip` becomes
  `securityid::embedded(isin: &IsinCode) -> impl Iterator<Item = SecurityId>`,
  with four rules. The slim `fill_market` and `MarketEventData` (the two
  callers in `graph/element.rs` and `graph/event.rs`) call it.

  Every rule first requires `IsinCode::is_canonical`, so the ISIN's own check
  digit closes. It then yields at most one code, and nothing when its guard
  fails. An ISIN matches at most one rule, because each rule is keyed by
  country prefix.

  | Key | Countries | ISIN shape | Code | Guard |
  | --- | --- | --- | --- | --- |
  | `CUSIP` | `US`, `CA` | `CC` + 9-char CUSIP + check | positions 2–10 | `CusipCode::new` (CUSIP check digit). Today's rule, byte for byte. |
  | `SEDOL` | `GB`, `IE`, `GG`, `JE`, `IM` | `CC00` + 7-char SEDOL + check | positions 4–10 | positions 2–3 are `00`, and `SedolCode::new` (SEDOL check digit) |
  | `WKN` | `DE` | `DE000` + 6-char WKN + check | positions 5–10 | positions 2–4 are `000`, and 6 of `[0-9A-HJ-NP-Z]` (WKN excludes `I` and `O`) |
  | `VALOR` | `CH`, `LI` | `CC` + 9-digit Valor, zero-padded, + check | positions 2–10, leading zeros stripped | all 9 are digits, and the stripped Valor is non-empty |

  - WKN and Valor have no check digit of their own, so the ISIN check digit
    plus the exact shape is the whole guard. Add each as a `SecType` code
    validator in `securityid.rs` (`WKN`: 6 of that alphabet; `VALOR`: 1–9
    digits, no leading zero). No new `code_leaf!` types.
  - The Valor is stored without leading zeros, as SIX publishes it
    (`CH0038863350` → `3886335`).
  - **Fill, never contradict.** A rule inserts only when the element states
    no code under that key, which is today's CUSIP behavior. A stated SEDOL
    that differs from the embedded one stays, and the embedded one is
    dropped.
  - Embedded codes are derived. They go to the overlay, never to a
    `FixMsg`'s FIX fields or the wire.
  - They are digest inputs, because `fill_market` runs before `finalize`, as
    the embedded CUSIP does today. GB, IE, GG, JE, IM, DE, CH and LI rows
    therefore get new identities, which the rebuild covers.
  - Tests, all real ISINs whose check digits close:

    | ISIN | Yields |
    | --- | --- |
    | `US0378331005` | `CUSIP:037833100` |
    | `GB0002634946` | `SEDOL:0263494` |
    | `IE00B4BNMY34` | `SEDOL:B4BNMY3` |
    | `JE00B4T3BW64` | `SEDOL:B4T3BW6` |
    | `DE0007164600` | `WKN:716460` |
    | `DE000BASF111` | `WKN:BASF11` |
    | `CH0038863350` | `VALOR:3886335` |
    | `LI0010737216` | `VALOR:1073721` |
    | `XS0203470157` | nothing |

    Negative cases:
    - a GB ISIN whose embedded SEDOL fails its check digit yields nothing;
    - a GB ISIN not starting `GB00` yields nothing;
    - a DE ISIN not starting `DE000`, or whose WKN part contains `I` or
      `O`, yields nothing;
    - any ISIN whose own check digit fails yields nothing;
    - `GB0002634946` on an element stating `SEDOL:B000009` keeps
      `B000009`, not the embedded `0263494`.
- **FIX interop lives here, not in `fix/`.** `SecType::fix_source(&self) ->
  Option<&'static str>` and `SecType::from_fix_source(&str) -> SecType` are
  the only code-to-key translation. `derive_market`, the setters and
  `latest::sync_security_id` in `fix/` call them and hold no literals.
  `fix/` keeps only what touches the message: reading `48`/`22`/`secaltids`
  and writing them back.
- **Derived stays derived.** What the registry and `embedded` fill goes to
  the event's derived overlay. On a `FixMsg` it never reaches `48`, `22`,
  `secaltids` or the wire.
- **Bindings.** Python and Node expose `SecurityId`, `SecurityIds` and the
  key table from one module each (`yggdryl.securityid` and its Node
  equivalent), redirecting into this file. The registry stays internal.

### `SecType`: an open string key

- `SecType` is a validated string, `code_leaf!`-style over `SmolStr`: ASCII,
  folded to upper case, up to a fixed max width of 32 (`ULLINK.INSTRUMENTID`
  is 19). It is an
  open set. Any key a source states is accepted (`ISIN`, `RIC`, `BBGTICKER`,
  a venue's own `XETRA_WKN`), and there is no `Other` bucket.
- Known keys have one canonical spelling, and `SecType::read` maps the other
  spellings onto it:
  - the FIX `SecurityIDSource(22)` wire code (`1` → `CUSIP`, `2` → `SEDOL`,
    `3` → `QUIK`, `4` → `ISIN`, `5` → `RIC`, `8` → `EXCHSYMB`, `A` → `BLOOMBERG`,
    `S` → `FIGI`, `T` → `LEI`, and every other code in the code set);
  - the spec's name for it;
  - the canonical key itself.

  Names fold the way `Side::from_spelling` folds, and wire codes do not fold.
  An unknown key is kept as written, upper-cased. One table holds the aliases,
  in `rust/src/securityid.rs`. It is the key-to-code table shown in the
  `FixMsg` securityids section, and a test pins it to the shipped
  `securityidsourcecodeset`.
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

- **Layout.** A newtype over one `SmolStr` buffer, with the key compacted so
  the code keeps as much of the 23-byte inline room as possible:
  - **A known key is one byte:** a tag byte `0x01..=0x7F` indexing the
    key-to-code table in `securityid.rs`, then the code. The index is internal
    to the buffer and never stored or digested (see Digest), so it may be
    renumbered freely.
  - **An unknown key is spelled out:** a `0x80 | len` byte, then the key,
    then the code.
- **Fits inline, zero allocations:** a 2-byte overhead leaves 21 bytes for
  the code, so `ISIN` (12), `CUSIP` (9), `SEDOL` (7), `FIGI` (12), `WKN` (6),
  `VALOR` (≤9), `RIC` (about 12) and short Bloomberg spellings all fit
  (`AAPL US Equity` is 14, `EURUSD Curncy` 13, `SPX Index` 9).
- **Bloomberg is the big code.** `BloombergCode` allows up to
  `BLOOMBERG_WIDTH` = 32 bytes (`T 4 1/2 02/15/36 Govt`, long option and
  bond spellings), so a 22-to-32-byte Bloomberg code is the expected heap
  case. It allocates exactly once, for the whole buffer. Design for it
  rather than treating it as an edge case:
  - **Width per key.** `SecType::max_code_width(&self)` is 32 for
    `BLOOMBERG`, the fixed width for the fixed-shape keys, and a documented
    default (32) for any other key. It is the single bound
    `SecurityId::new` checks. `BLOOMBERG_WIDTH` stays the constant it reads.
    The widest known code, and so the per-key allocation bound, is
    Bloomberg's.
  - **Preserve case and spaces.** A Bloomberg code keeps its case and its
    inner spaces exactly, as `BloombergCode::new` does today. It is never
    upper-cased, trimmed inside, or folded. Only the fixed-shape keys (ISIN,
    CUSIP, SEDOL, FIGI, WKN) fold to upper case, as their validators do now.
    `Ord`, `Eq` and `Hash` compare Bloomberg codes byte-exact, so
    `AAPL US Equity` and `AAPL US EQUITY` are two codes.
  - **Refuse the usual non-values.** Refuse the empty code, and the
    null-like spellings `null`, `none` and `[n/a]` (case-insensitive), which
    the registry rejects today in `InstrumentCodes::enrich`. That check moves
    into `SecType::validate_code` for `BLOOMBERG`, so every path refuses them,
    not only the registry.
  - **A FIGI in a Bloomberg field stays Bloomberg.** A Bloomberg field may
    hold a 12-byte FIGI. It stays under `BLOOMBERG` as stated, and is never
    moved or copied to `FIGI` by guessing. `FIGI` comes only from its own
    source (`S`).
- **Accessors:**
  - `sectype() -> &str` borrows the key: the table's static spelling for a
    known key, or the buffer's bytes for an unknown key;
  - `code() -> &str` borrows the code;
  - `new(key: &str, code: &str) -> Result<Self>` reads the key through
    `SecType::read` and validates the code against that key's width and
    shape;
  - `is_inline() -> bool`, for tests and benchmarks;
  - `Display` shows `ISIN:US0378331005`, or `BLOOMBERG:AAPL US Equity` with
    the space kept.
- **Order.** `Ord` compares `(sectype(), code())` as byte strings, never the
  tag byte, so ordering and the sorted `SecurityIds` invariant do not depend
  on table order.
- **Tests** in `rust/tests/allocations.rs`:
  - `size_of::<SecurityId>() == size_of::<SmolStr>()`;
  - zero allocations for ISIN, CUSIP, SEDOL, FIGI, WKN, Valor and
    `AAPL US Equity`;
  - exactly one allocation for a 32-byte Bloomberg code, and a 33-byte one
    refused;
  - clone of a heap Bloomberg `SecurityId` shares the buffer (`SmolStr` is
    reference-counted on the heap) and allocates nothing;
  - a round trip of `T 4 1/2 02/15/36 Govt` through `SecurityIds`, the Arrow
    column and a `FixMsg` `secaltids` write (`456=A`) is byte-identical;
  - `null`, `NONE` and `[N/A]` under `BLOOMBERG` are refused.

### `SecurityIds`: a sorted map from key to code

`SecurityIds` is a map with **one code per key**, kept sorted by key. The
key is the `SecType` (`ISIN`, `BLOOMBERG`, `RIC`, a venue key), and the value
is the code. Two codes under one key are two statements competing for one
fact, and one of them wins by the rules below. They never sit side by side.

- **Storage.** A sorted `SmallVec<[SecurityId; 2]>`, each `SecurityId`
  already holding its key and its code in one buffer, so the map adds no
  second allocation. Measure it against `Box<[SecurityId]>` in
  `rust/benchmarks/` and keep the faster one for book-sized workloads.
  Deduplicate on key, so there is no separate key vector or hash map.
- **Invariant.** Strictly sorted by key (byte order of `sectype()`), with
  unique keys, enforced by every constructor and mutator. Test it with a
  property check.
- **Lookup.** Binary search on the key after `SecType::read`, so `get("4")`,
  `get("isin")` and `get("ISIN")` find the same entry.
  - `get(key: &str) -> Option<&str>` returns the code;
  - `get_id(key) -> Option<&SecurityId>` returns the whole entry;
  - `contains_key(key)`.
- **Writes:**
  - `insert(id) -> bool` fills: it adds when the key is absent, and is a
    no-op returning `false` when present. Every lower-ranked source uses it:
    derived, embedded, registry, unmapped fields and the instrument key.
  - `set(id) -> Option<SecurityId>` replaces and returns the old entry. Only
    an explicit setter or a higher-ranked stated source uses it.
  - `remove(key) -> Option<SecurityId>`.
- **Merge.** `merge(&mut self, other: &SecurityIds) -> bool` is a linear
  two-pointer union by key, O(n + m), with no re-sort. On a shared key the
  leading statement's code stands, as every other market fact merges. It
  returns whether anything was added. This replaces the per-code
  `CodeValue::merge_with` calls in `merge_market`.
- **Iteration.** `iter() -> impl Iterator<Item = (&str, &str)>` yields
  `(key, code)` in key order. `Deref<Target = [SecurityId]>` stays for
  zero-copy slices.
- `SecurityIds` replaces the five code fields (`isincode`, `cusipcode`,
  `sedolcode`, `bloombergcode`, `figicode`).
- The slim `fill_market` calls `insert` with what `securityid::embedded`
  yields. On a `FixMsg`, that goes to the derived overlay, never to the FIX
  fields. `SecurityIdRegistry::enrich` reads and fills `SecurityIds`. Both
  live in `securityid.rs`.
- **Digest.** Feed the entries in key order as length-prefixed `key` then
  `code`, so the digest ignores insertion order, and `AB`+`C` never collides
  with `A`+`BC`.
- **Arrow.** `map<utf8, utf8>` with `keys_sorted = true`, a non-null key,
  and one entry per key. Readers use `securityids['ISIN']`. A stored map
  with a duplicate key, or unsorted keys, is refused on read with a located
  error, because the writer never produces one.

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

### `MARKETORDERID` and `OMSDEALERORDERID` are aliases of `OrderID(37)`

Bridges send the order's ID under their own names. `ulbridge.log` states
`#MARKETORDERID=L9EOEDIz-00` on 12 lines and `#OMSDEALERORDERID` on 9 of
them. On those 9 lines the two carry the same value, and `ORDERID` is also
stated, with a different value. Declare it with the dictionary's
existing alias mechanism, not a new one:

- In `config/fix/fields/000000000.json`, give `orderid` (tag `37`)
  `"FIX:names": ["marketorderid", "omsdealerorderid"]`, as 43 fields
  already declare alternate names (`tradetype` for `418`, `cardissno` for
  `491`). The names fold like every other name, so `MARKETORDERID`,
  `market_order_id`, `#OMSDEALERORDERID` and `OmsDealerOrderId` all resolve
  to `37`.
- **The alias is a fallback, never a rival.** In all 12 lines the message
  *also* states `ORDERID` with a different value (`00026877712XOEA0` beside
  `L9EOEDIz-00`). `OrderID` leads `crosscode`, so a naive alias would move
  every chain. The builder therefore:
  - uses an alias value for `37` only when no value arrives under the
    canonical name or tag `37` itself;
  - when several aliases arrive and the canonical does not, takes the first
    in `FIX:names` order (`marketorderid`, then `omsdealerorderid`), not in
    message order. The others stay as their own fields, whether or not they
    agree;
  - keeps the alias value as an unmapped child under its own name when both
    arrive. It re-emits unchanged, is not an anomaly, never overwrites `37`,
    and never becomes a second `37`.

  Make this the rule for every `FIX:names` alias, not a special case, and
  check first whether the builder already behaves this way. If it does not,
  the change covers the other 43 aliased fields too. Pin that each of them
  still resolves exactly as today when only one spelling arrives.
- **Everything reading `OrderID` sees the fallback.** That covers
  `FixLifted::orderid`, `crosscode` (`identity::CROSS_TAGS`), the
  definition-driven `FixMsg::identifiers()`, and the wire, where a message
  that stated only `MARKETORDERID` re-emits it as written, since the arrival
  spelling is preserved. They see it because it resolved to `37`, not
  because anything special-cases it.
- **Tests:**
  - `MARKETORDERID=X` alone gives `orderid` `X` and `crosscode` `X`;
  - `OMSDEALERORDERID=Y` alone gives `orderid` `Y`;
  - `OMSDEALERORDERID=Y|MARKETORDERID=X` (no `ORDERID`) gives `orderid` `X`,
    with an `omsdealerorderid` child `Y`;
  - `ORDERID=A|MARKETORDERID=B|OMSDEALERORDERID=B` gives `orderid` `A`,
    `crosscode` `A`, and both alias children `B`;
  - over `ulbridge.log`, every `orderid` and `crosscode` is unchanged from
    today, and the 12 `#MARKETORDERID` and 9 `#OMSDEALERORDERID` values
    survive as their own fields.
- **Row IDs.** They move only for messages that stated an alias without
  `ORDERID`, because their `orderid` and `crosscode` now fill. None
  in `ulbridge.log` do. Other captures are covered by the rebuild.

### `MarketOperation::accountid` and `userid`

Two operation facts: the **account** an order, quote or execution is booked
to, and the **user** who acted on it. A book never carries them; a
`Book`'s executions do. Both are `Option<SmolStr>` on the operation holders,
with a getter and a setter each on `MarketOperation`.

**Fill rules**, derived in `FixMsg::derive_market` under the `forced` rule.
Each is a ladder: the first non-empty source wins, and a value stated on the
row itself (a `ROW_STATED_*` bit, as for the other facts) wins over the
ladder. The sources come from what `ulbridge.log` actually carries:

| Step | `accountid` source | Log example |
| --- | --- | --- |
| 1 | `Account(1)` | `ACCOUNT=PBRK6_EDA` (24 named bodies), `1=PBRK6_EDA`, `client`, `ACCT1` |
| 2 | `Parties` occurrence with `PartyRole` `24` (CustomerAccount) | `customeraccount` → `PBRK6_EDA` |
| 3 | the bridge's dealer account, field `omsdealeraccount` | `#OMSDEALERACCOUNT=PBRK6` |

| Step | `userid` source | Log example |
| --- | --- | --- |
| 1 | the bridge's OMS user, field `omsuserid` | `#OMSUSERID=trader1` |
| 2 | `Parties` occurrence with `PartyRole` `36` (EnteringTrader) | `89680`, `TRADER2`, `0101` |
| 3 | `Parties` occurrence with `PartyRole` `12` (ExecutingTrader) | `trader1`, `89680` |
| 4 | `SenderSubID(50)` | `50=89680` |
| 5 | `OnBehalfOfSubID(116)` | `116=trader3` |

- **Not sources, on purpose.**
  - `TECH.ACCOUNT` (`HIGH_TOUCH`) and `TECH.CLIENTID` (`OMSX1`) are routing
    tags, not the booked account or a person.
  - `USERDISPLAYNAME` is a display label (`trader1)` appears with a stray
    parenthesis), not an ID.
  - Party role `3` (ClientID) is the client firm, not a user.
  - `ULLINK.CLIENTID` equals `#OMSUSERID` on all 7 lines that carry it,
    but its name says client. Leave it out unless you say otherwise.
  - The custom tags `9435` and `9513` (`trader1`) are one feed's
    user-defined tags, and a tag cannot be a `FIX:names` alias. Map them
    only if that feed's tag list confirms them.
- **The bridge fields are declared in the dictionary**, like the
  instrument-ID fields:
  - `omsdealeraccount` and `omsuserid`, nullable `utf8`, in
    `config/fix/fields/000000650.json`, with tags in the crate's range;
  - not projected as `fixmsg` columns, so they stay in `fixentries`;
  - the ladder reads them by name, and no string literal of a bridge's
    spelling appears in `derive_market`.
- **Party roles are read as codes** (`24`, `36`, `12`). The parser already
  normalizes `PARTYROLE=customeraccount` / `enteringtrader` /
  `executingtrader` to them; the probe shows `partyrole: 36` and `12` on
  this capture. A party occurrence whose role stayed a name or empty (the
  probe shows `orderoriginatorsystem` → empty) is not a source. Where
  several occurrences carry the role, the first in group order wins.
- **Merge, follow and digest.** They behave like `tif`:
  - **Merge:** the leading statement's value stands, else the other's.
  - **Follow:** carried forward where an event states none, because an
    execution belongs to its order's account and user.
  - **Digest:** fed where stated. That moves identities only for rows that
    carry them, which the rebuild covers.
- **Tests:**
  - `1=A|#OMSDEALERACCOUNT=B` gives `A`;
  - a party role `24` alone gives its `PartyID`;
  - `#OMSUSERID=u|parties role 36=v` gives `u`;
  - only `50=s` gives `s`;
  - an execution with neither inherits its order's values by following;
  - over `ulbridge.log`, the Holcim and Novartis order-out flows give
    `accountid` `PBRK6_EDA` and `userid` `trader1`. Pin, per flow, the value
    and the source step that answered, and report any flow where steps
    disagree (for example, entering trader `TRADER2` against OMS user
    `trader1` on the same message).

### Crated field `pluginoriginator`

`msgpluginid` (crated `65_009`) is the plugin that **logged** a line, and
the row header's `[…]` capture fills it. One message is logged at every hop:
- `ULBridge`;
- the enrichment plugins (`TECH_AddFields_OMS_X1`,
  `MIFID_BuySideROE_Add_Fields`, …);
- `ULFilter`;
- the outbound session.

So `msgpluginid` says where a statement was written, not where the message
came from. Add a crated field naming the plugin the message **entered the
bridge through**.

- **Declaration.**
  - `PLUGINORIGINATOR_TAG_NAME: (i32, &str) = (<next>, "pluginoriginator")`
    in `fix/crated.rs`: nullable `utf8`, with a `Crated::event` entry
    documented beside `msgpluginid`, and a field entry in
    `config/fix/fields/000000650.json` and `components/fixmsg.json`.
  - `<next>` is the next free tag after the highest crated tag declared
    anywhere. The `*_TAG_NAME` constants top out at `65_064`
    (`refrecdunix`), so check the others (`msgseqnum`, `msgdirection`, …)
    before choosing.
  - Retired tags (`65_020`, `65_057`, `65_058`) are never reused.
- **Where it is read.** In `fix/ulbridge.rs`, from what the bridge states
  about a message, first match wins:
  1. `Message received: … from (<X> as <alias>) forwarded to (…)` gives `X`
     (`OMS_X1_OrderOut`, `Autex_FIX42_BuySide`);
  2. `Execution report from <X> type …` gives `X`;
  3. a `Receiving :` line gives that line's own `msgpluginid`
     (`OMS_X1_TradeCapture`, `OMS_X1_FIXML_In`,
     `Virtu_TritonBlack_TradeCapture`), because the plugin receiving a frame
     is where it entered;
  4. otherwise null at parse. An enrichment, `RouteMessage`,
     `PushMessage`, post-enrichment or `Sending` line does not know where
     the message came from, and says nothing.

  The patterns sit beside `ULBRIDGE_ROWHEADER`, as named constants a
  different bridge layout replaces, not as a second header.
- **Lifecycle.** The walk folds every hop of one event into one row. Its
  `pluginoriginator` is the value of the statement with the **earliest**
  `recdunix` that states one, folded the way `creaunix` folds to its
  earliest. A later hop never overwrites it. Duplicate statements agreeing
  on it keep it, and two different originators for one event keep the
  earliest, with an anomaly.
- **Not identity.** Like `msgpluginid` and the other capture facts, it is
  provenance. It is not a digest input, not part of `msgsesseventid`, and
  moves no `curruuid`.
- **Bindings and docs.** A `FixMsg::pluginoriginator()` getter in Rust,
  Python and Node. The yggfin `fixmsg.json` gains the column, and the
  AGENTS.md sentence listing the bridge's native fields names it beside
  `msgpluginid`.
- **Tests** over `ulbridge.log`:
  - the execution report received `from (OMS_X1_OrderOut as OD9EOEDJ400)`
    has `pluginoriginator` `OMS_X1_OrderOut` on `fix.raw`, and on its
    refined row after folding its enrichment, filter and sending hops;
  - the `OMS_X1_TradeCapture` `Receiving :` flow gives
    `OMS_X1_TradeCapture`;
  - the MediaTek flow gives `Autex_FIX42_BuySide`;
  - a `PushMessage` or `After Enrichment` line on its own gives null;
  - pin the count of refined rows with a null originator, and report it.

### Rename the `Currency` type to `Ccy`

Rename the currency **type** everywhere it is spelled. Field and column
**names** stay FIX's (`currency`, `bidcurrency`, `askcurrency`,
`settlcurrency`, `Currency(15)`), so no SQL changes. Prefer deletion: no alias,
no deprecated re-export, no second spelling accepted.

| Before | After |
| --- | --- |
| `rust/src/currency.rs`, `pub struct Currency` | `rust/src/ccy.rs`, `pub struct Ccy` |
| `Currency::new`, `Currency::none` (`XXX`), `as_str` | `Ccy::new`, `Ccy::none`, `as_str` |
| `CURRENCY_WIDTH`, `CURRENCY_EXTENSION_NAME` | `CCY_WIDTH`, `CCY_EXTENSION_NAME` |
| Arrow extension `yggdryl.currency` | `yggdryl.ccy` |
| `DataType::Currency`, `DataType::currency()`, `Scalar::Currency`, `CurrencyType`, the `Code` family variant | `DataType::Ccy`, `DataType::ccy()`, `Scalar::Ccy`, `CcyType`, `Code::Ccy` |
| dtype spelling `"currency"` (`serde.rs`, `datatype_id.rs`) | `"ccy"` |
| `NativeKind::Currency` (`fix/native_derivations.rs`) | `NativeKind::Ccy` |
| `{"type": "currency"}` in the 51 files under `config/fix/` | `{"type": "ccy"}` |
| Python `yggdryl.Currency`, Node `Currency` | `yggdryl.Ccy`, `Ccy` |

- It covers about 169 lines in `rust/src`, and 38 `.rs` files across
  `rust/src`, `python/src` and `node/src`. Do it as one mechanical rename in its own commit, before the
  market changes, so review can tell it apart from behavior. Then check that
  no `Currency` (the type) or `yggdryl.currency` spelling is left:
  `rg -w 'Currency|CurrencyType|yggdryl\.currency'` should find only FIX
  field names and prose about currencies.
- `.api-inventory.txt`, `.api-bindings.txt`, the rustdoc and binding doc
  examples, and the docs pages move with it.
- Row IDs do not move: the digest feeds `as_str()` bytes, not the type
  name. Iceberg columns do not change either, because the narrowing strips
  semantic extensions to storage.
- Arrow data written with `yggdryl.currency` metadata (IPC, Parquet key-value
  metadata, field JSON) is not read as a `Ccy` after this. That is
  acceptable because every affected product is rebuilt. Test that reading
  one fails with a located error naming the retired extension, rather than
  silently falling back to `utf8`.

### `Unit`: a validated string type

- `code_leaf!`-style newtype over `SmolStr`, so short units (`bbl`, `MWh`,
  `shares`) stay inline with no allocation. It is validated ASCII within a
  fixed max width (pick and document it, for example 32).
- `Unit::none()` is the empty unit and `is_none()` tests for it. Add a
  `yggdryl.unit` Arrow extension beside `yggdryl.ccy`.
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
  `accountid` and `userid` (nullable `utf8`),
  and `bid`/`ask` as two nullable `struct<price, currency, quantity, unit>`
  columns.
- Pick the `securityids` Arrow shape and state it in the schema docs.
  It is `map<utf8, utf8>` keyed by `sectype`, `keys_sorted = true`, with
  one code per key (see `SecurityIds`).
- Identity changes: the digest inputs (`securityids`, `ticker`, the split between the slim and operation digests) change, so every market `curruuid`
  changes. Say so in the changelog and bump the minor version. Consumers
  rebuild from capture and never dual-write.
- Update `.api-inventory.txt`, both bindings (Python `graph`/`fix` getters,
  Node `fix.rs`) and the docs pages for the market graph. Python and Node
  expose:
  - `securityids` as a sorted mapping of key to code (a `dict` in Python
    and an object in Node, key order preserved);
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
   - `fixmsg.json` loses `cusipcode` and `sedolcode`, keeps `isincode`,
     `bloombergcode` and `figicode` (now views of `securityids`), and gains
     `securityids`. Update `docs/pipeline/tasks/parse-fix-raw.md` (its
     "normalized code columns" line) to drop CUSIP and SEDOL. dbt models
     reading `isincode` on `fix.refined` keep working. Anything reading
     `cusipcode` or `sedolcode` reads `securityids['CUSIP']` or
     `securityids['SEDOL']`. The sample captures' `#ISINCODE` must still land
     an ISIN.
   - Regenerate `schemas/rekep/fixmsg.json` too. It gains the six lifted FX
     columns (`lastspotrate`, `lastforwardpoints`, `bidspotrate`,
     `bidforwardpoints`, `offerspotrate`, `offerforwardpoints`), so
     `fix.refined` is recreated with the market tables.
   - `marketevent.json` (orders, quotes, executions) takes the operation row,
     with `bid`/`ask` lane structs in place of the eight lane columns, and
     gains `accountid` and `userid`.
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
6. `miccode` now comes from `LastMkt(30)`, then `ExDestination(100)`, then
   the instrument key's MIC, then `SecurityExchange(207)`. No yggfin page
   lists its sources today. When
   the samples are regenerated, check that captures stating only `LASTMKT`
   still land a MIC.
7. `Currency` is now `Ccy` in yggdryl:
   - `python/tests/fields/test_storage.py` asserts `yggdryl.ccy`, not
     `yggdryl.currency`;
   - the bundled registry under `python/src/rekep/_data/fix` is refreshed
     from the release, with `{"type": "ccy"}`;
   - any `yggdryl.Currency` in `python/src`, `tasks/` or the docs becomes
     `yggdryl.Ccy`.

   Column names stay `currency`.
8. The `identifiers` column is gone from `fix.refined` and every market
   table.
   - Replace every `identifiers["msgsesseventid"]` with the new
     `msgsesseventid` column. That covers `docs/products/fixmsg.md`,
     `docs/fix/quality.md`, `docs/pipeline/tasks/parse-fix-raw.md` and
     `docs/pipeline/tasks/parse-fix-refined.md`, plus any task or dbt model
     that reads it.
   - Read business IDs from their own typed columns (`clordid`, `orderid`,
     `execid`, …), never from a map.
   - Remove "identifiers" from the column lists in `parse-orders.md`,
     `parse-quotes.md`, `parse-books.md` and `docs/roadmap/orders.md`.
9. Update `docs/contracts/types.md`, `docs/roadmap/order-book.md` and the
   pipeline task pages, then regenerate the samples.
10. `uv run pytest` must be green, including `test_schemas` and `test_docs`.
   Push the branch and read CI.

---

## Open decisions to confirm before running A

1. `msgsesseventid`: keep it as its own stored `utf8` column on `fixmsg`
   (default, because yggfin dedup and docs use it), or accessor only?
   And `metadata` never holds an identifier: is that right?
2. `Side` on the wire: keep the string extension (default) or move to `uint8`?
3. `Unit` max width.
4. `Lane` storage: boxed (small operations) or inline (lane-heavy quotes)?
   Decide it by the size test.
