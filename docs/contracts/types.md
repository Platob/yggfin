# Field contract

`rekep.Field` is the single schema node used at Python, Arrow, FIX, Parquet,
and Iceberg boundaries. A struct root owns ordered children; each child owns
its datatype, nullability, metadata, and protocol views.

```python
from rekep import Field, Message

field = Message.field()
assert isinstance(field, Field)
assert field.name == "Message"
assert field["url"].nullable is False
assert field["bodyhash"].digest.sources == ["body"]
```

## Declaration metadata

| helper | effect |
| --- | --- |
| `primary_key()` | marks a non-null Iceberg identity column |
| `partition_key("hour")` | declares an Iceberg transform |
| `derived_from("timestamp")` | names the source of a computed field |
| `digest_key(["body"])` | declares a digest holder and exact inputs |
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
```

## Arrow boundary

Use `apply_arrow_batch`, `apply_arrow_reader`, or `apply_arrow_table` when a
producer has columns but the contract must cast, derive, digest, reorder, and
validate them.

```python
import pyarrow

from rekep import Message

source = pyarrow.RecordBatchReader.from_batches(Message.field().into_arrow_schema(), [])
applied = Message.field().apply_arrow_reader(source, nullability="strict")
assert applied.schema.equals(Message.field().into_arrow_schema(), check_metadata=True)
```

`safe=True` refuses lossy casts. The FIX-to-Iceberg boundary explicitly uses
`safe=False` only after its field has declared microsecond timestamp storage.

## Portable form

`Field.into_json(indent=2)` is deterministic and `Field.from_json` restores the
same metadata-bearing tree. Arrow IPC metadata remains authoritative; the JSON
form exists for review, deployment checks, and cross-process contracts.
