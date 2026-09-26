# Tables

The graph writes seven tables, two in bronze and five in silver. Each page
below is generated from the field the task that writes the table declares it
with, and lists every column with its Iceberg type and meaning; each links
the rows the task lands over the shipped capture.

| table | written by | one row per | columns |
| --- | --- | --- | ---: |
| [`bronze.record_keeping.log_messages`](bronze/log_messages.md) | `parse_log_messages` | captured line | 23 |
| [`bronze.record_keeping.fix_messages`](bronze/fix_messages.md) | `parse_fix_messages_raw` | FIX message parsed out of a line | 132 |
| [`silver.record_keeping.fix_messages`](silver/fix_messages.md) | `parse_fix_messages_refined` | FIX event, walked | 132 |
| [`silver.record_keeping.books`](silver/books.md) | `parse_books` | book the fold answered, per symbol and instant | 59 |
| [`silver.record_keeping.orders`](silver/orders.md) | `parse_orders` | order delta of a book | 49 |
| [`silver.record_keeping.quotes`](silver/quotes.md) | `parse_quotes` | quote delta of a book | 49 |
| [`silver.record_keeping.executions`](silver/executions.md) | `parse_executions` | execution leaf of a book | 49 |

Every table is an event table laid out the same way: it opens with the event
columns -- `currunix`, the clocks, `curruuid`, `crossuuid`, `crosscode`, the
content codes, `prevuuid`, `seqnum`, `srcuuids` and `state` -- is keyed on
`curruuid` alone, partitioned by `hour(currunix)` and sorted by `currunix,
seqnum, curruuid` within a partition. `state` is an `int32` lifecycle code,
the same on every table: [States](states.md) lists all 60, and `rekep.State`
reads one back.

```python
from rekep import State

assert State(8003) is State.FILLED
assert (int(State.FILLED), State.FILLED.rank, str(State.FILLED)) == (8003, 80, "FILLED")
assert State.FILLED.is_done() and not State.PARTIALLY_FILLED.is_done()
assert State.from_fix_status(39, "2") is State.FILLED
assert State.from_fix_msgtype("D") is State.PENDING_NEW
assert State.PENDING_NEW < State.NEW < State.FILLED < State.CANCELED < State.REJECTED
```

Four fields make the seven shapes, each the field of the data it describes
narrowed to what Iceberg v2 stores -- nanoseconds to microseconds, identities
to sixteen bytes, unsigned codes to the signed bits of the same value,
semantic extensions to their storage -- with the key, partition and sort
order declared on it:

| field | tables |
| --- | --- |
| `rekep.text.log_message_field()`, the text read's own row | bronze `log_messages` |
| `rekep.fix.fix_message_field(codec)`, the FIX dictionary's fixed row | bronze and silver `fix_messages` |
| `rekep.market.book_field()`, the book fold's row | silver `books` |
| `rekep.market.market_event_field()`, the book's execution child | silver `orders`, `quotes`, `executions` |

[Field contract](fields.md) says what a field carries and how it is applied.

## Contracts

`schemas/` publishes every table twice, as files a consumer reads without
installing rekep:

| file | holds |
| --- | --- |
| `schemas/<layer>/<namespace>/<table>.json` | the Iceberg contract: `schema` with its `identifier-field-ids`, `partition-spec` and `sort-order`, as PyIceberg's own model JSON |
| `schemas/<layer>/schema.yml` | the layer as a dbt source, version 2: every table, every top-level column with its Iceberg type, description and tests |

A contract loads into PyIceberg's own models, which is what a table created
from it records:

```python
import json
from pathlib import Path

from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder

document = json.loads(
    Path("schemas/silver/record_keeping/fix_messages.json").read_text(encoding="utf-8")
)
schema = Schema.model_validate(document["schema"])
spec = PartitionSpec.model_validate(document["partition-spec"])
order = SortOrder.model_validate(document["sort-order"])

assert schema.identifier_field_names() == {"curruuid"}
assert [str(field.transform) for field in spec.fields] == ["hour"]
assert schema.find_column_name(spec.fields[0].source_id) == "currunix"
assert [schema.find_column_name(field.source_id) for field in order.fields] == [
    "currunix",
    "seqnum",
    "curruuid",
]
```

`catalog.create_table(identifier, schema=schema, partition_spec=spec,
sort_order=order)` then creates the table any PyIceberg catalog holds, and
`rekep.iceberg.iceberg_contract_field(text, name)` reads the same document
back as a `Field`.

## dbt sources

Each `schema.yml` declares one dbt source per layer and namespace, named as
the layer -- `bronze`, `silver`, `gold` -- with `database` the layer's
catalog and `schema` the namespace, so a model selects a table as:

```sql
select currunix, crosscode, state
from {{ source('silver', 'fix_messages') }}
where state = 8003
```

and `{{ source('bronze', 'log_messages') }}` reaches the lines. A column
carries its Iceberg type as `data_type`, its description, and the tests its
field implies: `not_null` on a required column, `unique` on the key, and
`accepted_values` with every [state code](states.md) on `state`. A table's
`meta` names the task that writes it and its key, partition and sort order.
The gold source declares no table: it is where a consumer's own models land.

## Regenerate

The contracts, the table pages and [States](states.md) are generated:

```bash
uv run --project python python tools/schemas_dump.py
```

Run it from the repository root whenever a field changes, and review the
diff: `python/tests/test_schemas.py` fails on any drift between a file and
the fields, and validates every contract through PyIceberg's models and every
`schema.yml` as a dbt version 2 source file.
