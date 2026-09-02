# W3·A2 — GATED: remove `comp` from the `Entry` struct (179 B/row, one real blocker)

**Gate 1:** W2·A (item A) must have landed and been measured.
**Gate 2:** someone must decide the schema evolution is worth **~49 B/row beyond
what A already recovered**. **"We stopped after A" is a legitimate outcome and
must be written in the report. Half-landing this is not.**
**Owns:** `rekep/entries.py`, ~40 read sites, ~50 test functions,
`schemas/rekep/*.yaml`, `docs/`, `data/fix/fields/000030.json`,
`fix/registry.zip`.

Read `BRIEF.md` §A2 **in full** before writing anything, plus
`00-STANDING-RULES.md`.

## The measurement, and the honest cost

`comp` is `string`, nullable, field index 3 of the entry struct (`entries.py:54`).
On these feeds it is **100% null: 3,089,398 of 3,089,398 entries, zero distinct
non-null values**, and it still costs **179.2 B/row — 9.3% of `entries`, 4.7% of
the whole row**, because an all-null Arrow string array still pays its offsets
buffer. Deleting it takes `entries` from 1922.6 to 1743.5 B/row.

**But item A's `dictionary(int8, string)` already recovers ~73% of those bytes at
near-zero risk.** Full removal buys **~49 B/row more** (~2.5% of `entries`) and
costs a schema evolution, two regenerated contract YAMLs, and ~50 test functions.
**The incremental win is small and the blast radius is not.**

## Do this check FIRST — it is the whole decision

```python
from rekep.entries import Entry, _key_parts

losses = 0
for entry in entries:                        # Entry instances from a parsed batch
    if entry.comp is None:
        continue
    tag, key, comp = _key_parts(entry.spelling)
    if comp != entry.comp or key != entry.key:
        losses += 1
        if losses < 10:
            print(f"{entry.spelling!r}: comp {entry.comp!r} -> {comp!r}, key {entry.key!r} -> {key!r}")
print(f"{losses} entries whose comp cannot be recovered from their spelling")
```

**Zero is the only acceptable result, and you will not get zero until the
referential prefix carries an index.** Every non-zero row is data the removal
would destroy.

> **Run it across FIX, XML *and* referential fixtures.** On a FIX capture `comp` is
> 100% null and this check passes **vacuously** — that is exactly the trap.

## The premise most people start from is wrong

"Downstream FIX parsing can rebuild component context from the field registry plus
key parsing." **Audited; it does not hold:**

- `fix/registry.py` (`component()`, `components()`, `group_count_tags()`,
  `repeating_groups()`) declares *which* fields sit in which component and which
  tags open a group. It holds **no occurrence index**. `[0]` vs `[1]` is
  per-message runtime data with no registry representation.
- The registry is never *asked* about a component path today: `fix/access.py:196-201`
  and `_KEY_TAIL` (`:496-501`) resolve on the terminal name with lead and index
  **stripped**.
- The writers **move** the prefix out of `key` rather than copying it
  (`structure_arrow` `entries.py:275-308`, `_key_parts` `:317-322`):

| input spelling | stored `key` | stored `comp` |
|---|---|---|
| `NoPartyIDs[0].PartyID` | `PartyID` | `NoPartyIDs[0]` |
| `Instrument.NoSecurityAltID[0].SecurityAltID` | `SecurityAltID` | `Instrument.NoSecurityAltID[0]` |
| `Strategies[0].NoLegs[0].600` | `600` | `Strategies[0].NoLegs[0]` |
| `Instrument.Symbol` | `Instrument.Symbol` | *null* |

**Dropping the column as-is destroys information that exists nowhere else**: the
group name, its occurrence index, and the full ancestor chain — the occurrence
identity `market/event.py:1121` groups on, the `event[i].action[j].order[k]` tree
`fix/oms.py:19-32` reconstructs, the `TickRules[i]` ladder order
`market/instrument.py:1211` rebuilds, and the scoped-vs-root disambiguation at
`fix/components.py:343-347`. **Do not delete the column and leave the writers
alone.**

## The version that works: keep the whole spelling in `key`, derive `comp` on read

Store `NoPartyIDs[0].PartyID`, not `PartyID`, and derive the split at the read
boundary. **Fully derivable by regex, no registry involved**, because
`_GROUPED_KEY` (`entries.py:18`) is a pure syntactic split:

```
(?s)^(?:(?P<comp>.*\[[0-9]+\])\.(?P<key>[^.]+)|(?P<plain>.*))$
```

Tag derivation survives (`_key_parts` rpartitions *before* `_terminal_tag`:
`Strategies[0].NoLegs[0].600` → tag 600). The read view is already comp-agnostic:
`_view()` (`:164`) concatenates the halves before matching, so `name`, `index`,
`lead`, `entry_lead`, `folded` keep working. `fix/components.py:230`
(`_INDEXED_COMPONENT`) is the second-stage splitter and is unchanged.

**The byte accounting shifts rather than vanishing: `key` gets longer.** Under item
A's `dictionary(int16, string)` that is nearly free — the distinct spellings are
the dictionary, and 896 distinct keys becoming a few thousand still fits `int16`.
**Measure `key`'s bytes/row after, not just `comp`'s. If `key` grows by more than
the 179 B you removed, A2 is a loss — report that.**

## The blocker: `comp="Referential"` has no index

`_REFERENTIAL_COMP = "Referential"` (`text/entries.py:88`) is written at **seven
sites** (`:435,437,441,459,467,470,556`) with **no `[N]`**. `ENTRY_LEAD`
(`entries.py:27`) is `\[[0-9]+\]$`, so:

```
Entry(key="InstrumentKey", value="X", comp="Referential").spelling == "Referential.InstrumentKey"
_key_parts("Referential.InstrumentKey") == (0, "Referential.InstrumentKey", None)   # comp lost
```

A merged `Referential.InstrumentKey` is **indistinguishable from a genuine dotted
proprietary key** such as `TECH.CLIENTID`. Not cosmetic:
`fix/transcribe.py:781-783` documents in-line that `TECH.CLIENTID` must not
resolve as `CLIENTID`, so collapsing the forms **silently changes registry
resolution for every referential entry.**

Resolve this **explicitly, before touching anything else**. In order of preference:

1. **Give the referential prefix an index** — write `Referential[0]` at those seven
   sites. One-line change each, syntactically indistinguishable from any other
   indexed lead, and `_GROUPED_KEY` recovers it. Costs a stored-form change for
   referential rows → needs its own before/after comparison of the referential
   test fixtures.
2. Keep a marker the split can recognise that a proprietary key cannot produce.
3. Decide referential rows may lose the prefix — **only with sign-off from whoever
   consumes them**, and only after checking `text/fixmsg.py:939,1315,1329` and
   `market/transacted.py:520-528`, which gate behaviour on `comp` being non-null.

> **Do not pick (3) by default because it is the least code.**

## What breaks loudly (fine) and what breaks silently (dangerous)

Positional 4-element `from_arrays` calls fail the moment the type is 3 wide —
`text/entries.py:301-309`, `text/fixmsg_arrow.py:160-168`,
`fix/transcribe.py:1176-1179`, `text/entries.py:913-915` (implicitly 4-wide).

These adapt on their own because they are name- or arity-driven:
`fix/transcribe.py:2137-2143` (`zip(ENTRY_PARTS, parts, strict=True)` — `strict=True`
couples `structure_arrow`'s arity to `ENTRY_PARTS`' length, so **they must change
together, and it will catch a half-done change for you**),
`fix/components.py:995-1002`, `fix/oms.py:571-575`. `ENTRY_PARTS` (`entries.py:314`)
derives from `ENTRIES.value_type`, so it shrinks to a 3-tuple by itself.

**Two failures are silent. Check these by hand:**
- `market/instrument.py:1137` — `{"key","value","comp"}.issubset(source.type.value_type.names)`
  is a structural duck-type probe. Remove `comp` and it returns `False` for every
  entries column, and the function **silently yields nothing**.
- `text/fixmsg.py:3352-3354` — `entry.get("comp")` on a `Mapping` returns `None`
  instead of raising. Same shape in the test helper at
  `tests/text/test_messages.py:1188`, which feeds `_pairs` and `_keys` and
  therefore any test using them.

## Full scope, so you can decide before starting

- **~40 read sites**: `fix/transcribe.py`, `fix/components.py`, `fix/oms.py`,
  `fix/message.py`, `text/fixmsg.py`, `text/fixmsg_arrow.py`, `market/event.py`,
  `market/fix_arrow.py`, `market/instrument.py`, `market/transacted.py`.
- **~50 test functions.** Six pin the struct shape directly and **must be updated
  first** — they tell you whether the change is coherent:
  `tests/fix/test_transcribe.py:924`, `tests/text/test_fixmsg.py:378-379`,
  `tests/text/test_message.py:100`, `tests/test_cli.py:207`,
  `tests/test_schemas.py:51`, `tests/test_docs.py:443`.
- **Two committed contract YAMLs regenerated in the same change**:
  `schemas/rekep/message.yaml:302-305`, `schemas/rekep/fixmsg.yaml:300-303` **and
  `:329-332`** — comp appears twice in fixmsg. Generated via
  `rekep fields dump --pyclass rekep.text.message:Message`; `tests/test_schemas.py:51`
  enforces agreement.
- **Executable docs**: `docs/fix/index.md:95` and `docs/products/message.md:71`
  print `entry.comp`, and `tests/test_docs.py:443` runs every python fence under
  `docs/` and diffs stdout against the following fence. Also update the prose at
  `docs/products/message.md:80-84` and the member contract table at
  `docs/fix/fixmsg.md:129-136`.
- **Committed registry data**: `data/fix/fields/000030.json:326` and the same
  member inside `fix/registry.zip → fields/000030.json` — the `Unmap`
  pseudo-field, tag 30021 (`fix/rekep.py:220`, `:248`).
- **Iceberg field ids renumber.** No `field_id` is pinned in `schemas/` (`grep -c
  field_id` is 0 for all six files, despite `docs/contracts/index.md:40` claiming
  ids are stored); they are assigned at runtime by fresh numbering
  (`iceberg/fields.py:37-63`). Removing member 3 renumbers **every field id after
  it** in any table carrying `ENTRIES`. There is no migration code and every
  contract is version 1 with no migration path (`docs/contracts/index.md:97-107`).
  **Do it in a scratch catalog, never against a shared warehouse.**

## Deliverable

Either a complete A2 — blocker resolved, lossless check at zero, all scope items
updated, `key` bytes/row measured — **or** a report saying you stopped at item A's
dictionary and why. Nothing in between.
