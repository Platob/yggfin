# Table contracts

Every table the `rekep` tasks write, published as files a consumer reads
without installing `rekep`: the Iceberg contract of each table, and each
layer as a dbt source. They are generated from the fields the tasks declare
the tables with, never edited by hand.

```text
schemas/
  bronze/
    schema.yml                      dbt sources, version 2: the bronze layer
    record_keeping/
      log_messages.json             Iceberg contract of bronze.record_keeping.log_messages
      fix_messages.json             Iceberg contract of bronze.record_keeping.fix_messages
  silver/
    schema.yml                      dbt sources, version 2: the silver layer
    record_keeping/
      fix_messages.json
      books.json
      orders.json
      quotes.json
      executions.json
  gold/
    schema.yml                      dbt sources, version 2: no table yet
```

A table is named `<layer>.<namespace>.<table>`, and its contract is
`schemas/<layer>/<namespace>/<table>.json`: the layer is the Iceberg catalog
that holds it, and `<namespace>.<table>` its name in that catalog.

| table | written by | columns |
| --- | --- | ---: |
| `bronze.record_keeping.log_messages` | `parse_log_messages` | 23 |
| `bronze.record_keeping.fix_messages` | `parse_fix_messages_raw` | 133 |
| `silver.record_keeping.fix_messages` | `parse_fix_messages_refined` | 133 |
| `silver.record_keeping.books` | `parse_books` | 53 |
| `silver.record_keeping.orders` | `parse_orders` | 48 |
| `silver.record_keeping.quotes` | `parse_quotes` | 48 |
| `silver.record_keeping.executions` | `parse_executions` | 48 |

Every table is keyed on `curruuid`, partitioned by `hour(currunix)` and sorted
by `currunix, seqnum, curruuid`. The column meanings are on the
[Tables](https://platob.github.io/yggfin/tables/) pages.

## The Iceberg contract

`<table>.json` is the three things an Iceberg table records about its shape,
each as PyIceberg's own model serializes it:

| key | PyIceberg model | holds |
| --- | --- | --- |
| `schema` | `pyiceberg.schema.Schema` | every column with its field id, type, `required` and `doc`, and `identifier-field-ids`, the key |
| `partition-spec` | `pyiceberg.partitioning.PartitionSpec` | `hour(currunix)` |
| `sort-order` | `pyiceberg.table.sorting.SortOrder` | `currunix, seqnum, curruuid`, ascending |

It holds no data location, table UUID, snapshot or catalog state. Load it
into those models, from the repository root:

```python
import json
from pathlib import Path

from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder

document = json.loads(
    Path("schemas/bronze/record_keeping/log_messages.json").read_text(encoding="utf-8")
)
schema = Schema.model_validate(document["schema"])
spec = PartitionSpec.model_validate(document["partition-spec"])
order = SortOrder.model_validate(document["sort-order"])

assert schema.identifier_field_names() == {"curruuid"}
assert schema.find_column_name(spec.fields[0].source_id) == "currunix"
assert len(order.fields) == 3
```

`catalog.create_table("record_keeping.log_messages", schema=schema,
partition_spec=spec, sort_order=order)` then creates the table in any
PyIceberg catalog, as a task's first write would. From `rekep`,
`rekep.iceberg.iceberg_contract_field(text, name)` reads a contract back as
the `Field` it states.

## The dbt sources

`<layer>/schema.yml` is a dbt properties file, `version: 2`, declaring one
source per layer and namespace, named as the layer:

| key | holds |
| --- | --- |
| `sources[].name` | the layer: `bronze`, `silver`, `gold` |
| `sources[].database` | the layer's catalog |
| `sources[].schema` | the namespace, `record_keeping` |
| `tables[].name` | the table, as a model names it |
| `tables[].meta` | the task that writes it, and its `primary_key`, `partitioned_by` and `sorted_by` |
| `columns[].data_type` | the column's Iceberg type |
| `columns[].data_tests` | `not_null` on a required column, `unique` on the key, and `accepted_values` with every state code on `state`, every side code on `side` and every market data kind on `marketdatakind` |

Copy a layer's file under a dbt project's model paths, or point
`model-paths` at it, and a model selects a table by the source name and the
table name:

```sql
select currunix, crosscode, state
from {{ source('silver', 'fix_messages') }}
where state = 8003  -- FILLED
```

`{{ source('bronze', 'log_messages') }}` reaches the captured lines, and
`dbt test --select source:silver` runs the tests the file declares. The
`state` codes are those of a lifecycle-sorted enum, `rekep.State`, listed on
the [States](https://platob.github.io/yggfin/tables/states/) page. The gold
source declares no table: it is where a consumer's own models land.

## Regenerate

From the repository root, whenever a task's field changes:

```bash
uv run --project python python tools/schemas_dump.py
```

It rewrites every file here, the table pages under `docs/tables/`, and
`docs/tables/states.md`. Review the diff before committing it: a changed
contract is a changed table, and a table written under another key or other
field ids is rebuilt from the capture rather than evolved.
`python/tests/test_schemas.py` fails on any drift between these files and the
fields, validates every contract through PyIceberg's models, and every
`schema.yml` as a dbt version 2 source file.
