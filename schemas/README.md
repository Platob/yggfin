# Schema contracts

Every file under this directory is **one Arrow schema, written down**: a
`Field` document that `rekep` reads back as the exact type it names, nested
kinds included. It is the shape two sides of an exchange agree on before
either of them has data — a producer casts onto it on the way out, a consumer
casts onto it on the way in, and neither has to import the other's code.

```python
from rekep import Field

quote = Field.from_yaml("schemas/trading/quote.yaml")   # or from_json, by extension
quote.into_arrow_schema()                               # what the data must be
quote.cast_arrow(batch)                                 # what makes real data fit
quote.primary_keys()                                    # ['symbol']
```

`from_yaml` and `from_json` take a path, a URI or an open file, so a contract
published to an object store is read the same way:
`Field.from_yaml("s3://contracts/trading/quote.yaml")`.

## Layout

```text
schemas/
├── rekep/      shapes this package itself produces
│   ├── log.yaml
│   ├── instrument.yaml
│   ├── order.yaml
│   ├── execution.yaml
│   ├── bookside.yaml
│   └── book.yaml
└── trading/    an example exchange: one YAML contract, one JSON
    ├── quote.yaml
    └── venue.json
```

The five market files are `rekep.market`'s tables, published. `Event` and
`MarketEvent` are not here because an abstract base is nothing two sides
exchange, and `Level` and `LevelUpdate` are not because a level travels inside
the side that holds it. They also carry the `fix:` keys naming the wire field
each column came from, so a consumer that has never imported this package can
still tell that `tif` is FIX `TimeInForce <59>`.

One directory per namespace, one file per shape, named after it in lower case.
YAML or JSON — the extension picks the reader, and the two spell the same
document.

## What a contract says

| key | meaning |
| --- | --- |
| `name` | the shape's own name; it travels in the Arrow schema's metadata |
| `type` | the Arrow type, as `str(type)` writes it (`int64`, `struct`, `list`, `map`, `large_list`, `fixed_size_list`, `list_view`, `large_list_view`, `decimal128(38, 9)`, `timestamp[us, tz=UTC]`, `fixed_size_binary[16]`) |
| `nullable` | **absent means NOT NULL.** Nullability is declared, never guessed |
| `description` | the column comment, everywhere it can be carried |
| `metadata` | free-form `str -> str`; `namespace`, a `version`, an `owner`, and the protocol keys below |
| `fields` | a struct's members, in order |
| `item` | a list's element, whatever the list flavour |
| `key` / `value` | a map's two halves |
| `list_size` | how wide a `fixed_size_list` is — part of its type, so it is stated |
| `keys_sorted` | whether a map's keys are sorted — also part of its type |

Protocol keys are prefixed with the protocol that owns them, so one
namespace's keys can never collide with another's:

- `iceberg:primary_key: 'true'` — this column identifies a row.
- `iceberg:partition_key: identity` — this column partitions the table; any
  Iceberg transform (`identity`, `day`, `bucket[16]`, …) is a value here.

**Quote the booleans.** Metadata values are strings, so YAML's bare `true`
arrives as `"True"` and no longer matches what a dump writes. Write
`'true'`.

## Adding one

Write the file, or dump a declaration you already have:

```bash
cd python
uv run python -c "from rekep import Log; Log.FIELD.into_yaml('../schemas/rekep/log.yaml')"
```

`schemas/rekep/log.yaml` is that dump, and `python/tests/test_schemas.py`
pins it against `Log.FIELD`, so a column added in Python and not published
here fails the build. Every other file in this directory is checked for the
one property a contract has to have: what it says is what it loads back as.

## Changing one

A contract is an agreement, so it changes the way an agreement does — by
adding, never by moving:

- **Add a column as nullable.** Rows written before it exists have nothing to
  put in it. `Field.merge_with` does exactly this, at every level.
- **Never retype or drop.** That is a migration, and it belongs in a new
  version of the contract (`quote.v2.yaml`), announced to the consumers, not
  in an edit that changes what the old name means.
- **Never re-describe a column into something else.** A description is the
  column comment a consumer reads; correcting the wording is fine, redefining
  the meaning is a new column.

The whole of the reasoning, and the process around it, is on the
documentation site: **[Design rules](../docs/design.md)** and
**[Schema contracts](../docs/contracts.md)**.
