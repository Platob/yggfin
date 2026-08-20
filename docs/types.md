# Types

Everything in this package is described by one class: `Field`. It holds the
three things Arrow itself holds — a **name**, an **Arrow type** and
**metadata** — and it holds them *before* there is any data, which is what
makes it a declaration as well as a description.

```python
from rekep import Field

Field(name="size", arrow_type=pyarrow.int32(), nullable=False, metadata={"unit": "lots"})
```

## The `@field` decorator

`@field` turns a class into a field: a dataclass whose members are its Arrow
struct, reachable as `FIELD`.

=== "Declare"

    ```python
    import datetime
    from typing import Annotated

    from rekep import Convertible, Field, field


    @field
    class Venue(Convertible):
        """A trading venue."""

        mic: Annotated[str, Field.primary_key()]
        """ISO 10383 market identifier."""

        day: Annotated[datetime.date, Field.partition_key()]
        """Trading day."""

        size: Annotated[int, Field(arrow_type=pyarrow.int32(), metadata={"unit": "lots"})]
        """Lots on the book."""

        timeout: float | None = None
        """Seconds before giving up on a quote."""
    ```

=== "Read it back"

    ```python
    Venue.FIELD.name                        # 'Venue'
    Venue.FIELD.names                       # ['mic', 'day', 'size', 'timeout']
    Venue.FIELD.field("mic").description    # 'ISO 10383 market identifier.'
    Venue.FIELD.field("size").metadata      # {'unit': 'lots', 'description': 'Lots on the book.'}
    Venue.FIELD.primary_keys()              # ['mic']
    Venue.FIELD.partition_keys()            # {'day': 'identity'}
    ```

=== "Rules"

    - **Nullability is declared, never guessed.** `str` is NOT NULL,
      `str | None` is nullable, and item nullability survives:
      `list[str | None]`.
    - **A doc is the string literal under the member**, one line. It lands as
      the column comment everywhere — Arrow metadata, Iceberg `doc`, a parquet
      footer. Rationale goes in a `#` comment above the member.
    - `__`-prefixed annotations are working state, never columns.
    - The projection is built once per class, lazily, and a subclass builds its
      own.

!!! note "Inherit `Convertible` alongside"

    `@field` gives the class its schema; `Convertible` gives the *instance*
    `into_json`/`from_yaml`/… . They are separate on purpose — a shape does not
    have to be a document, and a document does not have to be Arrow.

## What `Annotated` can say

Inference gets the common cases right. `Annotated` is for what it cannot know.

=== "Full form"

    ```python
    size: Annotated[int, Field(arrow_type=pyarrow.int32(), metadata={"unit": "lots"})]
    loose: Annotated[str, Field(nullable=True)]
    ```

=== "Shorthands"

    ```python
    size: Annotated[int, pyarrow.int32()]          # a bare type is the type
    size: Annotated[int, {"unit": "lots"}]         # a mapping is metadata
    size: Annotated[int, "Lots on the book."]      # a string is the description
    ```

=== "Keys and partitions"

    ```python
    unix: Annotated[int, Field.primary_key()]
    day: Annotated[datetime.date, Field.partition_key()]          # identity
    symbol: Annotated[str, Field.partition_key("bucket[16]")]     # any transform
    ```

    A nullable primary key is refused, at the declaration and at the setter:
    an identifier that may be missing identifies nothing.

    Every transform but `identity` is computed by Iceberg's Rust core when a
    write partitions on it. The `iceberg` extra pulls it in, so a `day` or a
    `bucket[16]` partition works out of the box; a bare `pyiceberg` install
    raises `NotInstalledError` at the first such write.

## Kinds: the type picks the class

`Field(...)` returns the subclass its Arrow type calls for, so what is *inside*
a field is reachable as what it is.

=== "Struct"

    ```python
    book = Field(name="book", arrow_type=pyarrow.struct([("bid", pyarrow.float64())]))
    type(book)                 # <class 'rekep.fields.field.StructField'>
    book.fields                # (Field(name='bid', ...),)
    book.field("bid")
    book.into_arrow_schema()   # a struct is also a schema
    ```

=== "List"

    ```python
    legs = Field(name="legs", arrow_type=pyarrow.list_(pyarrow.int64()))
    legs.item                  # the element, as a field of its own

    # every flavour has its own class
    Field(name="x", arrow_type=pyarrow.large_list(pyarrow.int64()))       # LargeListField
    Field(name="x", arrow_type=pyarrow.list_view(pyarrow.int64()))        # ListViewField
    Field(name="x", arrow_type=pyarrow.large_list_view(pyarrow.int64()))  # LargeListViewField
    Field(name="x", arrow_type=pyarrow.list_(pyarrow.int64(), 3))         # FixedSizeListField
    ```

=== "Map"

    ```python
    tags = Field(name="tags", arrow_type=pyarrow.map_(pyarrow.string(), pyarrow.int64()))
    tags.key                   # the key half of one entry
    tags.value                 # the value half
    ```

=== "A member is a view"

    ```python
    Venue.FIELD.field("size").description = "Lots."   # rebuilds the struct it is in
    Venue.FIELD.field("mic").is_primary_key = True    # and so does this
    quote.field("book").field("bid").is_partition_key = "day"   # at any depth
    ```

## Casting data onto a field

A field is a *target shape*. `cast_arrow` picks the method by what you hand it;
each one is also there by name.

=== "Anything"

    ```python
    Quote.FIELD.cast_arrow(array)      # -> cast_arrow_array
    Quote.FIELD.cast_arrow(batch)      # -> cast_arrow_batch
    Quote.FIELD.cast_arrow(table)      # -> cast_arrow_table
    Quote.FIELD.cast_arrow(batches)    # -> cast_arrow_reader (an iterator or a reader)
    ```

=== "What it fixes"

    ```python
    # an int64 where the target wants int32          -> cast (unsafe by default)
    # columns in another order                       -> reordered
    # a column the source never produced             -> filled with nulls, if nullable
    # a column the target does not declare           -> dropped
    # a struct column whose members grew             -> handled where they are declared
    Quote.FIELD.cast_arrow_batch(batch)
    ```

    A missing **NOT NULL** column is refused by its path — `'Quote.day' is
    missing and not nullable` — because filling it builds data that only fails
    later, at the write.

=== "Keeping extra columns"

    ```python
    Quote.FIELD.cast_arrow_batch(batch, merge_schema=True)
    ```

    The columns the data has and the field does not are appended after the
    declared ones; the shared ones stay the field's, so data is cast onto the
    declaration and never the other way round.

=== "Safety"

    ```python
    Quote.FIELD.cast_arrow_batch(batch, safe=True)    # Arrow's checking back on
    ```

    Unsafe is the default *deliberately*: casting to a declared type is a
    statement that the declaration is the authority, so a narrowing is the
    intent rather than an accident.

### Between the nested kinds

The cast recurses, and the recursion is what lets one nested shape become
another. Every conversion below is Arrow kernels only — no Python loop ever
sees a row.

=== "Map ⇄ struct"

    ```python
    # a map becomes a struct: each member is looked up as a key (Arrow's map_lookup)
    Field(name="v", arrow_type=pyarrow.struct([("mic", pyarrow.string())])).cast_arrow(maps)

    # a struct becomes a map: the member names are the keys
    Field(name="v", arrow_type=pyarrow.map_(pyarrow.string(), pyarrow.string())).cast_arrow(structs)
    ```

=== "Struct ⇄ list"

    ```python
    # a struct becomes a list of its members, in declaration order
    Field(name="v", arrow_type=pyarrow.list_(pyarrow.int64())).cast_arrow(structs)

    # a list becomes a struct by position: element 0 is the first member
    Field(name="v", arrow_type=pyarrow.struct([("low", pyarrow.int64())])).cast_arrow(lists)
    ```

=== "Map ⇄ list"

    ```python
    entry = pyarrow.struct([("key", pyarrow.string()), ("value", pyarrow.int64())])
    Field(name="v", arrow_type=pyarrow.list_(entry)).cast_arrow(maps)        # entries
    Field(name="v", arrow_type=pyarrow.map_(pyarrow.string(), pyarrow.int64())).cast_arrow(entries)
    ```

=== "List flavours"

    ```python
    # list, large_list, list_view, large_list_view and fixed_size_list all
    # convert to each other, item conversion included
    Field(name="v", arrow_type=pyarrow.large_list(pyarrow.int32())).cast_arrow(lists)
    ```

!!! tip "Why not just `Array.cast`?"

    Arrow refuses most of these outright (struct → map, list → struct, anything
    → a list view), and `RecordBatch.cast` cannot even reorder columns. Where
    Arrow *can* do the work it is used — the layout change of a list flavour is
    one Arrow call — and where it cannot, the walk is done in kernels.
    `benchmarks/bench_cast.py` measures both.

## Merging two declarations

=== "Merge"

    ```python
    wider = Quote.FIELD.merge_with(other)              # a field, schema, or @field class
    wider = Quote.FIELD.merge_with_arrow_field(field)  # one already in hand
    ```

=== "The rule"

    - **This** field wins wherever both say something: its type, its
      nullability, its metadata.
    - Whatever the other has and it does not is **added, forced nullable** —
      rows already stored predate the column and have nothing to put in it.
    - At every level, so a struct member, a list item and a map value grow the
      same way.

## Conversions, both ways

=== "Arrow"

    ```python
    Quote.FIELD.into_arrow_schema()          # members flat, identity in metadata
    Quote.FIELD.into_arrow_field()
    Quote.FIELD.into_arrow_type()

    Field.from_arrow_schema(schema)          # -> StructField, identity taken back
    Field.from_arrow_field(field)
    Field.from_arrow_type(pyarrow.int32(), "size")
    ```

=== "Iceberg"

    ```python
    Quote.FIELD.into_iceberg_schema()         # ids, docs, identifier fields
    Quote.FIELD.into_iceberg_partition_spec()
    Quote.FIELD.field("size").into_iceberg_field(field_id=7)

    StructField.from_iceberg_schema(schema, "Quote", spec)
    ```

=== "Python"

    ```python
    Quote.FIELD.into_dataclass()             # a @field class, losslessly
    Field.from_dataclass(Quote)
    ```

=== "Documents"

    ```python
    Quote.FIELD.into_json("quote.json")      # nested: fields / item / key / value
    Quote.FIELD.into_yaml()
    Field.from_json("quote.json")
    Field.from_dict(dumped)
    ```

!!! info "A schema knows where it came from"

    `into_arrow_schema()` puts the field's name and metadata in the schema's own
    metadata, so a schema that travels still says which class produced it — and
    `from_arrow_schema` reads that identity back, which is what makes the round
    trip an equality rather than a resemblance.
