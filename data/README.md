# Data

Every file under this directory is **a dictionary this repository publishes**:
data that is the same for everyone, scraped or derived once and kept here so
nothing downstream has to fetch it. `schemas/` beside it says what data *is*;
this says what the protocols it names already agreed on.

```text
data/
└── fix/        the OnixS FIX dictionary, one file per FIX version
    ├── versions.json
    ├── 5.0.SP2.json
    ├── ...
    └── FIXT1.1.json
```

## The FIX dictionary

`data/fix/` is exactly what `FixRegistry` writes into `~/.config/fix/`, so it
*is* a warm cache: point a registry at it and every version answers without a
network call, on a machine that was never online.

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix")
registry.field("Side").fix["values"]     # '{"1":"Buy","2":"Sell",...}'
registry.tags()                          # every name to its tag, for tag_arrow_array
```

One file per version, plus `versions.json` listing them newest first. A version
file is plain JSON — inspectable, diffable, copyable:

| key | meaning |
| --- | --- |
| `version` | the FIX version, spelled as the dictionary spells it (`5.0.SP2`) |
| `url` | where it was scraped from |
| `fields` | every field of that version, in tag order, each one a `Field` document |

A field is the same document `schemas/` publishes — `name`, `type` (the Arrow
type, not the FIX one), `nullable`, `description` (the page's own prose, which
becomes the column comment) — with the FIX identity under the `fix:` metadata
prefix: `fix:tag`, `fix:type` (`char`, `Price`, `UTCTimestamp`), `fix:values`
(the enumeration, as JSON), `fix:used_in` (the messages that carry it) and
`fix:note` (`no longer used`, where the dictionary says so).

```json
{
 "name": "Side",
 "type": "string",
 "nullable": true,
 "description": "Side of order.",
 "metadata": {
  "fix:tag": "54",
  "fix:type": "char",
  "fix:version": "4.4",
  "fix:values": "{\"1\":\"Buy\",\"2\":\"Sell\", ...}",
  "fix:used_in": "[\"IOI\",\"Execution Report\", ...]"
 }
}
```

## Refreshing it

The dump is the registry's own, so refreshing it is one scrape — thousands of
pages, and the site throttles partway through, which the registry waits out:

```bash
cd python
uv run python -c "from rekep.fix import FixRegistry; print(FixRegistry(cache_dir='../data/fix').load(refresh=True))"
```

Name versions (`load('4.4', refresh=True)`) to refresh only those. Without
`refresh` the call is a check: it reports the field count of every version and
scrapes only what is missing.

`python/tests/test_data.py` is what keeps a bad scrape from shipping. It reads
every file back through `Field`, and asserts the parts a page carries are
there — because the first scrape of this directory came back a fifth empty,
every throttled page a field with no type and no description, and every file
still parsed.
