# Data

Every file under this directory is **a dictionary this repository publishes**:
data that is the same for everyone, scraped or derived once and kept here so
nothing downstream has to fetch it. `schemas/` beside it says what data *is*;
this says what the protocols it names already agreed on.

```text
data/
└── fix.zip     the OnixS FIX dictionary: one JSON document per FIX version
```

## The FIX dictionary

`data/fix.zip` is exactly what `FixRegistry` writes into `~/.config/fix/`,
packed into one archive — so it *is* a warm cache: point a registry at it and
every version answers without a network call, on a machine that was never
online.

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix.zip")
registry.field("Side").fix["values"]     # '{"1":"Buy","2":"Sell",...}'
registry.tags()                          # every name to its tag, for tag_arrow_array
```

A directory of the same documents works the same way, and **the extension is
what decides which is which**: `cache_dir="data/fix"` is a directory,
`cache_dir="data/fix.zip"` is an archive. Unpacking one is `unzip fix.zip -d
fix/`, and packing one is `FixRegistry(cache_dir="fix").into_zip("fix.zip")`
— the archive is 6.1× smaller than the directory (2.86 MB of JSON in 0.47 MB)
and one file to copy, and the directory is what a line-by-line diff wants.

One member per version, plus `versions.json` listing them newest first. Each
is plain JSON — inspectable, diffable once unpacked, copyable:

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

The archive is the registry's own store, so refreshing it is one scrape —
thousands of pages, and the site throttles partway through, which the registry
waits out:

```bash
cd python
uv run python -c "from rekep.fix import FixRegistry; print(FixRegistry(cache_dir='../data/fix.zip').load(refresh=True))"
```

Name versions (`load('4.4', refresh=True)`) to refresh only those; each lands
as one member, replacing the one that was there. Without `refresh` the call is
a check: it reports the field count of every version and scrapes only what is
missing. Rebuilding the archive from a directory is
`FixRegistry(cache_dir='../data/fix').into_zip('../data/fix.zip')`, and it is
byte-for-byte reproducible — every member is stamped at the start of zip time,
so a rebuild that changes nothing changes no bytes.

`python/tests/test_data.py` is what keeps a bad scrape from shipping. It reads
every member back through `Field`, asserts the parts a page carries are there
— because the first scrape of this dictionary came back a fifth empty, every
throttled page a field with no type and no description, and every file still
parsed — and rebuilds the archive to check it is what publishing it produces.
