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
[committed here](#the-dump-in-this-repository) as well, under `data/fix/`, so a
machine that was never online has the dictionary too.

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

`data/fix/` is that scrape, committed: one JSON per version, exactly what the
registry writes into `~/.config/fix/`. Point a registry at it and the whole
dictionary answers on a machine that has never been online.

```python
registry = FixRegistry(cache_dir="data/fix")
registry.tags()                      # every name to its tag, nothing fetched
```

What each file says, and how to refresh one, is in
[`data/README.md`](https://github.com/Platob/yggfin/blob/main/data/README.md);
`python/tests/test_data.py` is what keeps a throttled scrape from shipping as
one.

### The indexed registry

Reading a dictionary out of JSON means parsing every version and building a
`Field` for every field in it — 6,479 of them — to answer a question about
one. `SqliteFixRegistry` keeps the same fields in one indexed file and asks
for the rows the question is about:

```python
from rekep.fix import SqliteFixRegistry

registry = SqliteFixRegistry(cache_dir="data/fix")   # fix.db beside the dump
registry.field("Side")               # one indexed query, one Field built
registry.tags()                      # one GROUP BY, no objects at all
registry.load()                      # index every version now, ~150 ms
```

It is the same class of thing as `FixRegistry` — same scrape, same versions,
same `Field`s, same case-insensitivity, same offline rules — and every answer
is [pinned against the JSON registry's](https://github.com/Platob/yggfin/blob/main/python/tests/fix/test_sqlite.py),
because the four search ranks and the newest-version-first walk are spelled
twice: in Python there and in SQL here.

Where it is pointed decides where the fields come from, in this order: the
index, then a JSON dump beside it (imported once, no network), then the site.
So a fresh checkout answers offline, and a fresh `~/.config/fix` scrapes as
`FixRegistry` would.

!!! note "One file, and a `Field` only for what came back"

    The index is `fix.db` inside `cache_dir` unless `database` says
    otherwise, and it is a cache, not a publication: `data/fix/*.json` stays
    the diffable dump, `data/fix/fix.db` is gitignored and rebuilt in about a
    tenth of a second. Connections are per thread — Python's `sqlite3` keeps
    a statement cache on the connection, and two threads reading through one
    is API misuse — and `close()` closes every one it handed out.

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

**The registry.** `benchmarks/bench_fix_registry.py` is the other sweep: both
registries over the published dump, every answer asserted equal before
anything is timed.

```bash
cd python
uv run python benchmarks/bench_fix_registry.py
```

| question, from cold | JSON | indexed |
| --- | --- | --- |
| `lookup("Side")`, every version | ~75 ms | ~0.45 ms |
| `field(54, "4.4")`, one version | ~10.6 ms | ~0.31 ms |
| `tags()`, every version | ~79 ms | ~4.4 ms |
| `search("reject")` | ~82 ms | ~3.9 ms |
| `fields("4.4")`, a whole version | ~10 ms | ~9.4 ms |

Resident objects after `tags()`: 6.4 MB against 0.09 MB. The file is 2.78 MB
against the dump's 2.86 MB, built in ~150 ms.

**Where the index buys nothing.** Handing back a whole version is `Field`
construction, and both registries pay it in full — 9.4 ms against 10 ms. Every
other row is a question the JSON registry answers by building thousands of
fields in order to look at a handful.

**What was measured and left out.** A trigram FTS5 index answers
`search("reject")` in 0.04 ms against the LIKE scan's 2.3 ms — and returns
*nothing* for a two-letter query, because a trigram tokenizer cannot match
terms shorter than three characters, while nearly tripling the file
(7.6 MB). A window function for `tags()` is 8.3 ms against the folded
`min()`'s 2.2 ms. Parsing the Arrow type per row rather than per distinct
spelling costs 7.3 ms a version against 0.1 ms. All three are cases in the
sweep, because the reason a thing was not done is worth keeping.

**Fields per row.** The work is per token, not per row, so a rows/s at one
message shape says nothing about another: the wire fixture is twelve fields
and a CheckSum per line, and a wider message pays for every field it adds.
That is why the sweep prints a fields/s column beside rows/s — it is the
column to compare across cases.

**Group density.** Repeating groups are extra tokens, and in a rendered line
they are the *expensive* tokens: the inner `member=` regex only runs on
`Group[i]=Member=Value`, so a column with no group entries skips it entirely
while one with them sends a third of its tokens through it. The sweep runs
both, and that bracket is the ~140–260k spread.

**Wire against rendered.** A wire line cuts at the first `=` with a numeric
tag on the left; a rendered line has to read a name, an optional index and an
optional member out of the same grammar. Rendered parsing therefore sits
below wire on the same rows. The sweep also runs wire lines separated by SOH,
by `|`, with log noise wrapped around the message, and with repeating groups
— those cases are measured by the script and no number for them is quoted
here.

**Numeric against rendered keys.** `tag_arrow_array` over all-numeric keys is
one cast kernel over the map's key array, which is why it is quoted in keys/s
and not in rows/s. Rendered keys instead resolve through `names` once per
distinct spelling, dictionary-encoded, so their cost follows the number of
distinct names in the column rather than its length. The script measures the
`int64` key type and the rendered resolution as their own cases; neither is
quoted as a number on this page.

**The tag/value cut.** The script also races the cut itself — one
`split_pattern` plus `list_element` against one `extract_regex`, trimming and
greedy — on ten tokens per row of the sweep. That race is what
`parse_arrow_array`'s choice was decided by, and it is measured, not quoted:
the loser looked entirely plausible.
