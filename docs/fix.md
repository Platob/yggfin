# FIX

A trading log carries FIX messages, and a FIX message is `tag=value` pairs
joined by SOH — which every log prints as something visible instead:

```text
8=FIX.4.2|9=2058|35=8|54=1|10=045
8=FIX.4.4^A9=100^A35=D^A10=001
```

`rekep.fix` parses those lines — one at a time or whole columns in Arrow
kernels — and carries a dictionary of every FIX version's fields, scraped once
from the [OnixS FIX Dictionary](https://www.onixs.biz/fix-dictionary.html) and
cached under `~/.config/fix/` so everything after works offline.

## Parsing lines

=== "One message"

    ```python
    from rekep.fix import FixMessage

    parsed = FixMessage.from_text("8=FIX.4.2|9=2058|35=8|54=1|10=045")
    parsed.begin_string          # 'FIX.4.2'
    parsed.msg_type              # '8'
    parsed.get(54)               # '1'
    parsed.pairs                 # every field, in wire order
    ```

    The separator is read off the message itself — whatever follows the
    BeginString value *is* it — so SOH, `|`, `^A`, `^` and `;` all parse
    without being named. Log noise around the message is shed: parsing starts
    at `8=FIX` when one is there, a token that is not `tag=value` with a
    numeric tag is skipped, and the CheckSum `<10>` ends the message.

=== "Whole columns"

    ```python
    import pyarrow
    from rekep.fix import parse_arrow_array

    maps = parse_arrow_array(table.column("message"))
    maps.type                    # map<string, string>: tags repeat, order kept
    ```

    The same rules, vectorised: one split, one regex classification and one
    cumulative sum per batch — no Python per row. A map rather than a struct,
    because a repeating group *is* tags repeating, and an Arrow map keeps
    duplicate keys in order. Measured (`benchmarks/bench_fix.py`, twice):
    ~300k wire rows/s at twelve fields per row, ~5× the scalar parser, and
    ~140–260k rendered rows/s depending on group density.

    ```python
    from rekep.fix import FixRegistry, tag_arrow_array

    tagged = tag_arrow_array(maps)                     # map<int32, string>
    tagged = tag_arrow_array(maps, names=FixRegistry().tags())
    ```

    `tag_arrow_array` turns the text keys into the integer tags FIX defines —
    what a join or a filter wants. All-numeric keys (tag-mode parsing) are one
    cast kernel, ~140M keys/s; rendered keys resolve through `names` by their
    member (`NoPartyIDs[0].PartyID` → `PartyID` → 448), each distinct
    spelling once. A key that resolves nowhere is refused by name;
    `drop_unknown=True` drops those entries instead, which is how a rendered
    log's `took=5ms` noise falls out.

=== "Repeating groups"

    ```python
    parsed.group(268)                          # entries, split on the delimiter tag
    parsed.group(268, members=[269, 270, 271]) # exact ends, from a dictionary
    ```

    The standard's own rules: the NumInGroup field precedes its entries, every
    entry starts with the delimiter tag — the first tag after the count — and
    tags never repeat within one entry. Without `members` the last entry's end
    is taken from that no-repetition rule; with them it is exact.

=== "Rendered names"

    ```python
    parsed = FixMessage.from_text(
        "Side=1 | NoPartyIDs[0]=PartyID=BRK | NoPartyIDs[1]=PartyID=CLI"
    )
    parsed.pairs                     # [('Side','1'), ('NoPartyIDs[0].PartyID','BRK'), ...]
    parsed.indexed_group("NoPartyIDs")   # entries folded back, ordered by index
    parsed.values("PartyID")             # ['BRK', 'CLI'] — reaches through the indexes
    ```

    Logs also print messages *decoded*: `Name=Value` pairs, and repeating
    groups entry by entry as `Group[i]=Member=Value` (or the canonical
    `Group[i].Member=Value`, which round-trips). One regex grammar reads all
    the spellings; a line with a BeginString keeps the wire rule (digits
    only), a line without one is taken as rendered pairs, and `named=True` /
    `named=False` forces either. Name matching is case-insensitive
    throughout.

## The dictionary

```python
from rekep.fix import FixRegistry

registry = FixRegistry()             # cache_dir defaults to ~/.config/fix
registry.versions                    # ('5.0.SP2', '5.0.SP1', ..., 'FIXT1.1')
registry.load("4.4")                 # scrape one version into the cache, once
```

Every field comes back as the same generic `Field` a `@field` class projects
to: the Arrow type follows the FIX datatype, the page's description is the
column comment, and the FIX identity rides the `fix:` metadata prefix.

```python
side = registry.field("Side")        # newest version that has it
side.arrow_type                      # string  (char is a string, not one char)
side.description                     # 'Side of order.'
side.fix["tag"]                      # '54'
side.fix["type"]                     # 'char'
side.fix["values"]                   # '{"1":"Buy","2":"Sell",...}'

registry.lookup("Side")              # every version's definition, newest first
registry.lookup(54, "4.2")           # by tag, one version
registry.search("reject")            # name, tag or description, ranked
registry.search("Sied")              # nothing matches -> Levenshtein fallback
```

!!! note "The first scrape is the expensive one"

    `fields(version)` fetches one page per field, concurrently, and lands the
    result in `~/.config/fix/{version}.json`. Every later call — on this
    machine or any machine the directory is copied to — answers from the file.
    `refresh=True` scrapes over a stale cache.

## Reading values

The projection is deliberately forgiving where the wire is:

- **`char` is a string**, not one character — a value that outgrew its type is
  still a value, and a fixed width would truncate it silently.
- **A Boolean reads the spellings real feeds print**: `Y`/`N`, `true`/`false`,
  `yes`/`no`, `oui`/`non`, `1`/`0`, `on`/`off`, either case. Anything else is
  null, never a guess.

```python
from rekep.fix import arrow_type_of, cast_arrow_bool

arrow_type_of("char")        # string
arrow_type_of("Price")       # double
cast_arrow_bool(pyarrow.array(["Y", "non", "TRUE", "maybe"]))
# [true, false, true, null]
```
