# Schema contracts

A contract is one Arrow schema, written down: a `Field` document that says what
the data is before either side of an exchange has any. It lives in the
repository under `schemas/`, in YAML or JSON, and it loads back as exactly the
Arrow type it names — nested kinds, keys, partitions and column comments
included.

It exists for the case where the two sides do not share code. A producer casts
onto the contract on the way out, a consumer casts onto it on the way in, and
neither imports the other.

```python
from rekep import Field

quote = Field.from_yaml("schemas/trading/quote.yaml")
quote.into_arrow_schema()      # what the data must be
quote.cast_arrow(batch)        # what makes real data fit it
```

## The directory

```text
schemas/
├── README.md
├── rekep/                shapes this package itself produces
│   └── log.yaml
└── trading/              an example exchange: one YAML contract, one JSON
    ├── quote.yaml
    └── venue.json
```

One directory per namespace, one file per shape, named after it in lower case.
The extension picks the reader, and the two formats spell the same document —
YAML where humans maintain it, JSON where a tool writes it.

## Reading one

=== "From the repository"

    ```python
    from rekep import Field

    quote = Field.from_yaml("schemas/trading/quote.yaml")
    venue = Field.from_json("schemas/trading/venue.json")
    quote = Field.from_("schemas/trading/quote.yaml")   # or let the extension decide
    ```

=== "From a store"

    ```python
    # a contract published to an object store is read the same way: a path, a
    # URI or an open file, through the same pyarrow.fs handles as the data
    quote = Field.from_yaml("s3://contracts/trading/quote.yaml")
    ```

=== "What comes back"

    ```python
    quote.name                    # 'Quote'
    quote.names                   # ['symbol', 'day', 'received_at', ...]
    quote.primary_keys()          # ['symbol']
    quote.partition_keys()        # {'day': 'identity'}
    quote.field("venue").field("mic").description
    quote.into_arrow_schema()     # the Arrow schema the contract names
    ```

=== "As a target shape"

    ```python
    quote.cast_arrow(batch)                     # a batch, table or reader
    quote.cast_arrow(batch, merge_schema=True)  # keep columns the contract does not name

    quotes = IcebergDataset(
        name="trading.quotes",
        catalog="local",
        properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///wh"},
        struct=quote,                           # the table is created from the contract
    )
    quotes.write_arrow(reader, merge_by=True)
    ```

## What a contract says

| key | meaning |
| --- | --- |
| `name` | the shape's own name; it travels in the Arrow schema's metadata |
| `type` | the Arrow type, spelled as `str(type)` writes it |
| `nullable` | **absent means NOT NULL** — nullability is declared, never guessed |
| `description` | the column comment, everywhere the format can carry one |
| `metadata` | free-form `str -> str`: `namespace`, a `version`, an `owner`, and the protocol keys below |
| `fields` | a struct's members, in order |
| `item` | a list's element, whatever the flavour of list |
| `key` / `value` | a map's two halves |
| `list_size` | how wide a `fixed_size_list` is |
| `keys_sorted` | whether a map's keys are sorted |

Protocol keys carry the prefix of the protocol that owns them, so one
namespace's keys can never collide with another's:

```yaml
metadata:
  iceberg:primary_key: 'true'          # this column identifies a row
  iceberg:partition_key: identity      # or day, or bucket[16] -- any transform
  iceberg:field_id: '3'                # the id the table knows this column by
```

`iceberg:field_id` is the one that only exists once a table does. Iceberg
identifies a column by id and never by name, so a contract published *from* a
table says which id each column has — and handing that contract back builds the
same ids rather than a fresh numbering, which is what makes a rename a rename
instead of a new column. A declaration written in Python carries none, and
Iceberg numbers it on the first write.

```python
venue = Field.from_json("schemas/trading/venue.json")
venue.field("mic").field_id                     # 1
venue.field("sessions").item.field("label").field_id   # 6, at any depth
venue.into_iceberg_schema()                     # the same ids, not new ones
```

!!! warning "Quote the booleans"

    Metadata values are strings. YAML's bare `true` arrives as `"True"`, which
    no longer matches what a dump writes, so a schema compared byte for byte
    stops matching. Write `'true'`.

## Nested types

The nesting is the part a flat `struct<...>` string would bury, so a contract
nests instead: a struct is a `fields:` list, a list an `item:`, a map a
`key:`/`value:` pair. Each of them is a field in its own right, with its own
nullability and its own description.

=== "Struct"

    ```yaml
    - name: venue
      type: struct
      description: Where the quote was seen.
      fields:
      - name: mic
        type: string
        description: ISO 10383 market identifier.
      - name: country
        type: string
        nullable: true
    ```

=== "List of structs"

    ```yaml
    - name: legs
      type: list
      nullable: true
      item:
        type: struct
        nullable: true
        fields:
        - name: side
          type: string
        - name: size
          type: int32
    ```

=== "Map"

    ```yaml
    - name: tags
      type: map
      nullable: true
      key:
        type: string
      value:
        type: string
        nullable: true
    ```

    A map keeps duplicate keys, in order, which is why anything free-form —
    venue annotations, a FIX repeating group — is a map here and not a struct.

=== "The other list flavours"

    ```yaml
    - name: history
      type: large_list        # list, large_list, list_view, large_list_view
      item:
        type: int64

    - name: top_of_book
      type: fixed_size_list
      list_size: 2            # the width is part of the type, so it is stated
      item:
        type: float64
    ```

    A `large_list` is a different Arrow type from a `list`, not a bigger one:
    its offsets are 64 bit. A contract that spelled both `list` would narrow
    one of them on the way back in — silently, because both cast.

=== "Scalars worth naming"

    ```yaml
    - name: amount
      type: decimal128(38, 9)          # exact: a price is never a float here
    - name: received_at
      type: timestamp[us, tz=UTC]      # the zone is part of the type
    - name: checksum
      type: fixed_size_binary[16]      # 16 bytes, exactly
    - name: day
      type: date32[day]
    ```

Everything else is what Arrow calls it: `int8` … `int64`, `uint*`, `float`,
`double`, `bool`, `string`, `large_string`, `binary`, `time64[us]`,
`duration[ns]`, and the rest of the aliases.

## Writing one

=== "By hand"

    Write the file. A contract is an agreement, and an agreement is authored —
    it is not a dump of whatever a producer happens to hold today.

    ```yaml
    name: Quote
    type: struct
    description: One quote for one instrument on one venue.
    metadata:
      namespace: trading
      version: '1'
      owner: market-data
    fields:
    - name: symbol
      type: string
      description: Instrument identifier, as the venue spells it.
      metadata:
        iceberg:primary_key: 'true'
    ```

=== "From a declaration"

    Where the shape already exists in Python, publish it rather than retyping
    it:

    ```bash
    cd python
    uv run python -c "from rekep import Log; Log.FIELD.into_yaml('../schemas/rekep/log.yaml')"
    ```

    ```python
    Quote.FIELD.into_json("schemas/trading/quote.json")   # the same, as JSON
    ```

=== "From a store's own schema"

    A table that already exists is a shape too, and `Field.from_arrow_schema`
    keeps whatever identity the schema carries:

    ```python
    Field.from_arrow_schema(quotes.into_arrow_schema()).into_yaml("schemas/trading/quote.yaml")
    ```

!!! note "A missing type is refused by name"

    `Field.from_dict({"name": "venue"})` raises rather than guessing — a
    contract says what the data *is*, so every field names a type, and the
    error names the field that forgot.

## Using one at a boundary

The whole point is that neither side needs the other's code.

=== "The producer"

    ```python
    contract = Field.from_yaml("schemas/trading/quote.yaml")

    for batch in source:
        sink.write(contract.cast_arrow(batch))   # cast before it leaves
    ```

    Casting on the way out is not politeness: a producer that ships whatever it
    happens to hold makes a parser out of every consumer, and the first one to
    get it wrong does so silently.

=== "The consumer"

    ```python
    contract = Field.from_yaml("schemas/trading/quote.yaml")

    rows = contract.cast_arrow(reader, merge_schema=True)
    ```

    `merge_schema=True` keeps the columns the data has and the contract does
    not — a producer that has moved ahead does not break a consumer that has
    not.

=== "The store"

    ```python
    quotes = IcebergDataset(
        name="trading.quotes",
        catalog="local",
        properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///wh"},
        struct=Field.from_yaml("schemas/trading/quote.yaml"),
    )
    quotes.write_arrow(reader, merge_by=True)   # created from the contract, keys included
    ```

    The table is created from the contract — schema, column comments,
    identifier fields and partition spec — so what is stored and what was
    agreed cannot drift apart.

## Changing one

A contract changes the way an agreement does: by adding.

=== "Adding a column"

    ```python
    wider = Field.from_yaml("schemas/trading/quote.yaml").merge_with(
        pyarrow.schema([("desk", pyarrow.string())])
    )
    wider.into_yaml("schemas/trading/quote.yaml")   # publish it
    quotes.add_fields(wider)                        # ['desk'] -- and the table follows
    ```

    New columns arrive **nullable**, at every level: rows already written have
    nothing to put in them. That is true of a member gained by a struct, a
    list's item or a map's value too — evolution can add those, and they are
    columns as much as a top-level one is.

=== "What is not a change"

    - **Retyping a column** — a consumer built against the old type reads
      garbage or raises. New version.
    - **Dropping a column** — a consumer that selects it stops working. New
      version.
    - **Redefining what a column means** while keeping its name and type — the
      worst of the three, because nothing anywhere raises. New column.

    A new version is a new file (`quote.v2.yaml`) and an announcement, not an
    edit that changes what the old name meant.

=== "The version in the file"

    ```yaml
    metadata:
      namespace: trading
      version: '1'
      owner: market-data
    ```

    It rides in the metadata, so it reaches the Arrow schema, and from there
    whatever the data is written into. A consumer that pins the version it was
    built against can say so out loud.

## From the command line

Publishing a declaration and checking a document builds are the two things CI
and a pre-commit hook need without writing Python, so they are a command.

=== "Dump"

    ```bash
    rekep fields dump --pyclass rekep.text.log:Log --target schemas/rekep/log.yaml
    rekep fields dump --pyclass rekep.text.log:Log --format json      # to stdout
    rekep fields dump --pyclass trading.quotes:Quote --target schemas/trading/quote.yaml
    ```

    `--pyclass` is `module:Attribute` (or `module.Attribute`, which a docstring
    is more likely to write) and it takes whatever names a shape: a `@field`
    class, a plain dataclass, or a `Field` a module holds. `--format` is
    `yaml`, `json` or `toml`, inferred from `--target`'s extension when it says
    one and winning over it when both are given. With no `--target` the
    document goes to stdout — and *only* the document does, so it pipes.

=== "Load"

    ```bash
    rekep fields load --target schemas/rekep/log.yaml
    ```

    ```text
    Log: 10 columns, builds
      url: string
      unix: int64  [primary key]
      unix_hour: date32[day]  [partition identity]
      ...
      primary keys: ['unix', 'hash']
      partition keys: {'unix_hour': 'identity'}
    ```

    Parsing is not the check — **building** is. A document can be valid YAML
    and still name a type Arrow does not have, a `fixed_size_list` with no
    width, or a map with a nullable key, and each of those is a contract two
    systems would read differently. `load` builds the Arrow schema and prints
    what it found, exit code 1 and one line on stderr when it cannot.

=== "In a check"

    ```bash
    # a contract that no longer matches the code fails here rather than at a consumer
    rekep fields dump --pyclass rekep.text.log:Log --target /tmp/log.yaml
    diff -u schemas/rekep/log.yaml /tmp/log.yaml

    # and every published contract still builds
    for contract in schemas/*/*.yaml schemas/*/*.json; do
        rekep fields load --target "$contract" > /dev/null || exit 1
    done
    ```

Both commands are thin on purpose: `dump` is `field_of(...)` and an `into_*`
method, `load` is `Field.from_file(...)` and `into_arrow_schema()`. The command
line can never do something the library cannot, or do it differently.

```python
Field.from_file("schemas/trading/quote.yaml")   # what `load` reads with
Field.from_file("s3://contracts/quote.yaml")    # a path, a URI, or a filesystem
```

## How they are checked

A contract nobody verifies is a comment. `python/tests/test_schemas.py` runs in
CI over every file in the directory and pins two things:

- **What a file says is what it loads back as** — parse, dump, parse, and the
  three have to agree. Otherwise a producer and a consumer can read the same
  document as different types, which is the one failure a contract exists to
  rule out.
- **A published declaration still matches its code** — `schemas/rekep/log.yaml`
  is `Log.FIELD` dumped, and a column added in Python and not published here
  fails the build rather than surprising a consumer.

```bash
cd python
uv run pytest tests/test_schemas.py
```

The command line runs the same two checks without a test runner — see
[From the command line](#from-the-command-line).

The example contract is also the format reference, and the test asserts it
exercises every nested kind — a struct, a list of structs, a map, a
`fixed_size_list` with its width, a `large_list`, a decimal, a zoned timestamp
and a fixed-width binary — so the page you are reading cannot describe a
spelling the code does not read.

## Benchmarks

A contract has no runtime of its own: loading one is reading a small document,
and what it *costs* is the cast onto it, which is measured in
[Types](types.md#benchmarks) — 431k–542k rows/s per column-pass for a full
reshape, and nothing at all for a batch that already has the right shape, which
is returned as it is.
