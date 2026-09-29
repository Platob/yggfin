# Design rules

## One public vocabulary

Applications import resource, field, text, FIX and table behavior from
`rekep`. There are no parallel field classes, path layers, text readers,
codecs, registries or FIX row models: `rekep.Field`, `rekep.IOBase`,
`rekep.FixRegistry` and `rekep.FixCodec` are the native types themselves.

```python
from typing import Annotated

from rekep import Field, scalar
from rekep.fields import primary_key


@scalar
class Row:
    id: Annotated[str, primary_key()]
    value: float | None = None


assert isinstance(Row.into_field(), Field)
```

## Who owns what

| part | owns |
| --- | --- |
| resources, `rekep.IOBase` | URI binding, local and object-store traversal, decompression, bounded reads |
| text, `rekep.text` | physical-line framing, the row header's captures, the line's event columns |
| fields, `rekep.Field` | schema metadata, casts, digests, partitions, Arrow conversion |
| FIX, `rekep.FixRegistry` and `rekep.FixCodec` | the dictionary, code sets, parsing, the lifecycle walk, the fixed row |
| market, `rekep.market` | admission, continuation, book state, expirations, execution leaves |
| Arrow | columnar delta selection and list flattening |
| Iceberg, `rekep.iceberg` | table conversion, identifiers, snapshots, scan planning, atomic window commits |
| tasks, `rekep.pipeline` | one function per table: its window, its source scan, its write mode, what it answers |
| storages, `rekep.Storages` | one catalog per layer, and the tables named across them |

## Arrow is the transport

Tasks hand one `pyarrow.RecordBatchReader` from the read to the write. Tables
are used only where an operation is explicitly memory-sized, and no Python row
loop sits between parsing and storage. [Why Arrow](arrow.md) says more.

## Metadata is executable

Primary keys, partitions, sort orders, derived values, digests, FIX tags, code
sets and descriptions live on `Field`. Producers and consumers call
`Field.apply_arrow_*`, so cast, derivation and digest run in their declared
order, and a column may be absent only where its field is nullable.

## Every table is an event table

Every table opens with the same event columns -- `currunix`, the identities,
the content codes, `seqnum`, `srcuuids`, `state` -- is keyed on `curruuid`,
laid out by the hour of `currunix` and sorted by `currunix, seqnum, curruuid`
within a partition. A line is the event the read settled over it; a FIX row
is the message it parsed; a book is the fold's answer at an instant. The
[Tables](../tables/index.md) pages list the columns.

## Text before interpretation

Bronze `log_messages` keeps the line as text: the header's captures typed,
and the `body` past the header as the read decoded it. Bronze `fix_messages`
interprets every line and stores lifted columns plus the residual
`fixentries`. Silver `fix_messages` restates those rows with their chains
walked. A parser change is replayed from `log_messages`; a lifecycle change
replays the bronze FIX rows without reading the capture again.

## Replays are ordinary runs

The first three tasks merge their rows into their table on `curruuid`
within its hour -- a row it lacks is inserted, one it holds with other values
replaced, one it holds as it is left alone -- and the book and event tasks
replace exactly their window. So a task run again over a window writes
nothing, or replaces the rows it landed before with the same rows, and a
table holds each row once however often a window runs. A scheduler retries a
task by running it again.

## Documentation names contracts

The table contracts under `schemas/` and the column pages under
[Tables](../tables/index.md) are generated from the fields the tasks declare,
and the [Data samples](../samples/index.md) from a real run: nothing a page
states about a table is written by hand.
