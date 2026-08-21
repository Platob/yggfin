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
cached under `~/.config/fix/` so everything after works offline. That scrape is
[committed here](#the-dump-in-this-repository) as well, as `data/fix.zip`, so
a machine that was never online has the dictionary too.

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
    duplicate keys in order. It runs several times faster than the scalar
    parser ([measured](#benchmarks)).

    ```python
    from rekep.fix import FixRegistry, tag_arrow_array

    tagged = tag_arrow_array(maps)                     # map<int32, string>
    tagged = tag_arrow_array(maps, names=FixRegistry().tags())
    ```

    `tag_arrow_array` turns the text keys into the integer tags FIX defines —
    what a join or a filter wants. All-numeric keys (tag-mode parsing) are one
    cast kernel ([measured](#benchmarks)); rendered keys resolve through
    `names` by their member (`NoPartyIDs[0].PartyID` → `PartyID` → 448),
    each distinct spelling once. A key that resolves nowhere is refused by
    name; `drop_unknown=True` drops those entries instead, which is how a
    rendered log's `took=5ms` noise falls out.

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

    A whole-dictionary scrape is around seven thousand pages, and the site
    paces it: `429 Too Many Requests` arrives partway through. A refused page
    is waited out and asked for again — `Retry-After` when the site sends one,
    a doubling pause when it does not — and a page still refused after
    `retries` attempts *fails the version*. It is not treated as a page that
    does not exist, because that writes a field with no type and no comment
    into the cache and answers every later call from it.

### The dump in this repository

`data/fix.zip` is that scrape, committed: one JSON document per version —
exactly what the registry writes into `~/.config/fix/` — packed into one
archive. Point a registry at it and the whole dictionary answers on a machine
that has never been online.

```python
registry = FixRegistry(cache_dir="data/fix.zip")
registry.tags()                      # every name to its tag, nothing fetched
```

What each document says, and how to refresh one, is in
[`data/README.md`](https://github.com/Platob/rekep/blob/main/data/README.md);
`python/tests/test_data.py` is what keeps a throttled scrape from shipping as
one.

### A directory, or a zip of it

`cache_dir` names either, and **the extension is what decides** — the same
inference `Field.from_("quote.yaml")` makes. A path ending in `.zip` is an
archive of the same JSON documents; anything else is a directory of them.

=== "A directory"

    ```python
    registry = FixRegistry(cache_dir="~/.config/fix")   # the default
    registry.load("4.4")                                # writes 4.4.json
    ```

    One file per version, so a diff shows a field that changed and a single
    version can be refreshed on its own.

=== "An archive"

    ```python
    registry = FixRegistry(cache_dir="data/fix.zip")
    registry.load("4.4")     # writes the member 4.4.json, replacing it
    registry.archived        # True -- read off the extension, nothing else
    ```

    One file to publish, copy or attach, six times smaller. Scrapes land in
    it the same way: a member is written whole, and never twice.

Both are the same store to everything above them — the scraping, the version
rules, the ordering, the search — so a dictionary can be moved between them
without anything downstream noticing:

```python
FixRegistry(cache_dir="~/.config/fix").into_zip("fix.zip")   # publish it
FixRegistry(cache_dir="fix.zip").fields("4.4")               # read it back
```

`into_zip` stamps every member at the start of zip time, so building the same
dictionary twice gives the same bytes — which is what makes an archive worth
committing: "nothing changed" looks like nothing changed.

!!! note "What an archive costs, and what it saves"

    A zip made of the *folder* (`zip -r fix.zip fix/`) reads too: members are
    found by their file name, whatever directory they sit in, and a member
    written into such an archive joins its neighbours rather than landing at
    the root. A torn archive is treated as a cold cache — scraped over, not
    mourned — exactly as a torn file is.

    Reading through deflate costs 12–17% of a question, and the archive is
    6.1× smaller than the directory ([measured](#benchmarks)).

## Building from pairs

The other way in. `from_text` reads a line; `from_pairs` takes what a bridge, a
decoder or a test already has and normalises it into the same message — keys
resolved to tag numbers, values spelled the way the wire spells them, order and
repetition kept because that is what a message *is*.

```python
from rekep.fix import FixMessage
from rekep.market import market_tags

FixMessage.from_pairs(
    [
        ("MsgType", "D"),          # a name, any casing
        ("side", 1),               # ... and any separator convention
        ("Price", 100.5),
        ("NoPartyIDs[0].PartyID", "ACME"),
        ("VenueOwnField", "kept"), # nothing resolves it, so it stays as it is
        ("Text", None),            # dropped: `58=` is malformed, not empty
    ],
    market_tags(),
).into_text("|")
# '35=D|54=1|44=100.5|NoPartyIDs[0].448=ACME|VenueOwnField=kept'
```

Three rules make that work:

- **A key is a tag, a name, or a decorated name.** Digits are already a tag. A
  name resolves through the mapping you pass, after a fold that lowercases it
  and drops the separators a renderer's convention adds — so one entry answers
  for `MsgType`, `msgtype`, `msg_type`, `msg-type` and `Msg Type`. A component
  path (`Instrument.Symbol`) or an entry index (`PartyID[1]`) says *where* the
  field sits, not what it is, so the name resolves without it and the
  decoration is kept on the stored key — exactly what `from_text` stores, so
  both ways in agree.
- **An unresolved key is kept as it arrived.** Every venue sends fields no
  dictionary has, and dropping them loses data the map, the round trip and
  `get` all handle. With no mapping at all every name stays a name, and
  `get("Side")` still finds it through the rendered-spelling fallback.
- **A value is rendered as the wire spells it.** A boolean is `Y`/`N`, not
  `True`. A float is positional — FIX `Price` is digits with an optional point,
  and `1e-07` is a number Python prints and no venue parses. A datetime is
  `YYYYMMDD-HH:MM:SS.ssssss`. A value that knows its own FIX spelling is asked
  for it, which is how a [banded code](market.md#codes) renders as the
  character it was read from.

!!! note "The regex is for the shape, the fold is for the name"

    Resolving a key looks like a job for one case-insensitive regex over every
    known name. It is not, and the benchmark is why: an alternation still has
    to be probed for the *tag* afterwards, so it is a scan added in front of
    the lookup it cannot replace — and its cost scales with the dictionary
    while a hash probe's does not. Measured on mixed keys: **4.2M keys/s at
    nine names, 89k at fifteen hundred**, against **3.4–3.8M/s flat** for the
    probe-then-fold that ships. Nine names is exactly the size at which "just
    use a regex" looks right.

    So the regex does the part that really is regex work — splitting a
    decorated key into the name and the decoration — and the fold does the
    part that really is a lookup. The lowercase spelling is probed *first*,
    because a name with no separator in it folds to its own lowercase and
    never has to pay for the `sub`.

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

## Benchmarks

`benchmarks/bench_fix.py` is the sweep behind every number below. It builds
synthetic columns whose shape matches the tests' fixtures — wire messages with
and without log noise and repeating groups, rendered `Name=Value` and
`Group[i]=Member=Value` lines — and asserts the vectorised answer *is* the
scalar one before it times anything, because a benchmark that measures the
wrong answer measures nothing. The method the whole site shares is on the
[Benchmarks](benchmarks.md) page.

```bash
cd python
uv run python benchmarks/bench_fix.py            # full sweep: 100,000 rows, best of 5
uv run python benchmarks/bench_fix.py --quick    # 10,000 rows, best of 3
```

`--rows` and `--repeat` override either. Every case is warmed once and
reported as the best of `--repeat` runs. The figures below were measured
twice.

| case | measured |
| --- | --- |
| wire messages, twelve fields per row | ~300k rows/s, ~5× the scalar parser |
| rendered messages | ~140–260k rows/s, depending on group density |
| all-numeric keys, `tag_arrow_array` | ~140M keys/s |

**The registry's two stores.** `benchmarks/bench_fix_registry.py` is the other
sweep: the same published dictionary as a directory and as an archive, every
answer asserted equal before anything is timed.

```bash
cd python
uv run python benchmarks/bench_fix_registry.py
```

| question, from cold | directory | zip |
| --- | --- | --- |
| `field("Side")`, every version | ~71 ms | ~80 ms |
| `field(54, "4.4")`, one version | ~10.4 ms | ~11.9 ms |
| `tags()`, every version | ~78 ms | ~89 ms |
| `search("reject")` | ~81 ms | ~88 ms |
| `fields("4.4")`, one version | ~9.8 ms | ~11.4 ms |

So deflate costs 12–17% of a question, and never changes an answer.

**What the archive saves.** 2.86 MB of JSON becomes 0.47 MB, 6.1× smaller, in
~62 ms. The deflate level is zlib's own: level 9 is 2% smaller for twice the
time, level 1 is 26% bigger, and level 0 — stored rather than deflated, the
case in the sweep expected to be bad — is the whole 2.86 MB back.

**Where the time actually goes.** Both stores spend most of a question
building `Field` objects: `fields("4.4")` is 953 of them, and a `lookup`
across versions builds all 6,479 to return one. That is why the two stores
sit within a fifth of each other however the bytes are packed — and why the
one number that moves either of them is how many fields a question has to
build.
