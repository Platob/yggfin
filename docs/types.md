# Types

`@scalar` turns a dataclass into one lazily cached Arrow declaration.

```python
import datetime
from typing import Annotated

import pyarrow
from rekep import Convertible, Field, scalar


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument code."""

    day: Annotated[datetime.date, Field.partition_key()]
    """Trading day."""

    size: Annotated[int, Field(arrow_type=pyarrow.int32())]
    """Quantity in lots."""

    note: str | None = None
    """Source note; null when absent."""
```

```python
field = Quote.into_field()
field.names
field.primary_keys()
field.partition_keys()
field.cast_arrow(batch)
```

The annotation owns nullability. `str` is non-null, `str | None` is nullable,
and `list[str | None]` keeps nullable items. `Field(...)` may declare an exact
Arrow type, metadata, key, partition transform, or sort direction.

Containers keep their physical kind: struct, map, list, large list, list view,
large list view, or fixed-size list. Recursive casts preserve that shape while
casting members, reordering columns, filling nullable omissions, and dropping
undeclared extras. `merge_schema=True` retains extras after declared fields.

```python
field.into_yaml("quote.yaml")
same = Field.from_yaml("quote.yaml")
same.into_dataclass()
```

The one-line member literal becomes the column description. State units,
source, derivation, or null meaning; omit anything already obvious from the
name and type.

Hot row shapes use `@scalar(slots=True)`. `Event`, `MarketEvent`, `Log`,
`Instrument`, `Leg`, `Order`, `Execution`, `Book`, and `Level` therefore have
no per-instance `__dict__`; transient private slots remain excluded from Arrow
and document conversion. Shallow instance storage fell from 3,376 to 904 bytes
for `Log`, from 1,632 to 360 bytes for `Instrument`, and from 1,632 to 424 bytes
for `Order` and `Execution` on the measured Python 3.12 runtime.

## Optional dependencies

YAML, TOML, Iceberg, and Polars are imported only when used. Install the
matching extra rather than making every Arrow-only consumer carry them.

## Benchmark

`python/benchmarks/bench_cast.py --quick` compares recursive casts with Arrow's
reference operations after verifying equal values.
