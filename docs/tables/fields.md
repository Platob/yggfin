# Field contract

`rekep.Field` is the single schema node at the Python, Arrow, FIX, Parquet
and Iceberg boundaries. A struct root owns ordered children; each child owns
its datatype, nullability, metadata and protocol views, and the table
declarations -- key, partition, sort order -- are metadata on it.

```python
from rekep import Field
from rekep.text import log_message_field

field = log_message_field()
assert isinstance(field, Field)
assert field.name == "log_messages"
assert field["body"].nullable is False
assert field["crosscode"].nullable is True
assert field["curruuid"].iceberg["primary_key"] == "true"
```

## Declaration metadata

| helper | effect |
| --- | --- |
| `primary_key()` | marks a non-null Iceberg identity column |
| `partition_key("hour")` | declares an Iceberg transform |
| `derived_from("at")` | names the source of a computed field |
| `digest_key(["body"])` | declares a digest holder and its exact inputs |
| `sort_key("desc")` | declares physical sort intent |

```python
import datetime
from typing import Annotated

from rekep import scalar
from rekep.fields import derived_from, partition_key, primary_key


@scalar
class Event:
    id: Annotated[str, primary_key()]
    at: datetime.datetime
    hour: Annotated[
        datetime.datetime,
        partition_key("hour"),
        derived_from("at"),
    ]


assert [member.name for member in Event.into_field()] == ["id", "at", "hour"]
```

## Arrow boundary

`apply_arrow_batch`, `apply_arrow_reader` and `apply_arrow_table` cast,
derive, digest, reorder and validate a producer's columns into the field, in
that order. A column may be absent, or null, only where the field declares it
nullable.

```python
import pyarrow

from rekep.text import log_message_field

field = log_message_field()
source = pyarrow.RecordBatchReader.from_batches(field.into_arrow_schema(), [])
applied = field.apply_arrow_reader(source)
assert applied.schema.equals(field.into_arrow_schema(), check_metadata=True)
```

`safe=True` refuses lossy casts. The storage boundary uses `safe=False` only
after its field has declared the microsecond timestamps Iceberg v2 stores.

## Portable forms

There are two, each for a different reader.

`Field.into_json(indent=2)` is the **runtime declaration**: deterministic,
metadata-bearing, and restored in full by `Field.from_json`, because it keeps
what only Arrow metadata states -- a digest's sources, a derived column's
sources, a FIX tag.

`rekep.iceberg.iceberg_contract(field)` is the **published contract**: the
`schema`, `partition-spec` and `sort-order` PyIceberg itself serializes, and
what the files under [`schemas/`](index.md#contracts) hold.
`iceberg_contract_field` reads one back.

```python
from rekep.iceberg import iceberg_contract, iceberg_contract_field, primary_keys
from rekep.text import log_message_field

document = iceberg_contract(log_message_field())
restored = iceberg_contract_field(document, "log_messages")
assert iceberg_contract(restored) == document
assert primary_keys(restored) == ["curruuid"]
```

Arrow metadata remains authoritative for both. The contract derives from it
and derives less: it states what Iceberg stores, and nothing Iceberg has no
place for.
