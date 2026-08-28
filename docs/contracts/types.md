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

    size: Annotated[int, Field(dtype=pyarrow.int32())]
    """Quantity in lots."""

    note: str | None = None
    """Source note; null when absent."""
```

```python
field = Quote.into_field()
batch = field.into_arrow_schema().empty_table()

print(field.primary_keys(), field.partition_keys())
print(field.cast_arrow(batch).num_columns, len(field.names))
```

```text
['symbol'] {'day': 'identity'}
4 4
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

Hot row shapes use `@scalar(slots=True)`. `Event`, `MarketEvent`, `FixMsg`,
`Instrument`, `Leg`, `Order`, `Execution`, `Book`, and `Level` therefore have
no per-instance `__dict__`; transient private slots stay out of Arrow and
document conversion.

On the measured Python 3.12 runtime, shallow instance storage fell from 3,376
to 904 bytes for `FixMsg`, from 1,632 to 360 bytes for `Instrument`, and from
1,632 to 424 bytes for `Order` and `Execution`.

## Reading an instant

`rekep.times.unix_of` and `datetime_of` are the one reading of "an instant",
whatever spelled it: a `datetime`, a `date`, an integer already in the
nanoseconds an instant column such as `unix` holds, an ISO or FIX string, a
wrapped value a pyarrow or numpy scalar hands back, and the named instants `now`, `utcnow`,
`today`, `yesterday`, `tomorrow` and `epoch`.

```python
from rekep import unix_of

print(unix_of("2026-08-14"), unix_of("2026-08-14", upper=True))
```

```text
1786665600000000000 1786752000000000000
```

A naive instant is read as UTC. `upper=True` treats a value naming a whole
day as the exclusive end of it, so `end: 2026-08-14` means all of the 14th.
What names no instant is `None`, never a guess -- a day-first `03/04/2026` is
refused rather than silently moved a month. Every task notebook takes its
window through this, so one spelling means one instant in every job.

### The shapes a stamp is written in

`rekep.times.SHAPES` declares the three spellings that carry a date, a clock
and a fraction, and every one of them takes a fraction of one to nine digits
or none at all:

| shape | example | widths sliced |
| --- | --- | --- |
| `ISO` | `2026-08-14 00:05:01.147_250` | 19, 23, 26, 27, 29 |
| `FIX` | `20260824-10:00:01.123` | 17, 21, 24, 27 |
| `COMPACT` | `20260824100001123` | 14, 17, 20, 23 |

One declaration, because the accepted spellings are one behavior even where
the execution is two: this module reads a configuration value with
`Stamp.read`, once per job, while `rekep.text.text_file` reads a column of
log-line stamps in Arrow kernels, once per line.

The fast path cannot use `strptime`, which cannot read a compact stamp's
fraction at all, having no separator to anchor `%f` to, so both read the
components off the same declared offsets. `HEADER_PATTERN` is built from the
same shapes, so a stamp a window can name is a stamp a header may open with.

Three widths are shared by two shapes: 17 is a FIX stamp and a compact one
with milliseconds, 23 an ISO stamp with milliseconds and a compact one with
nanoseconds, 27 an ISO stamp with a split fraction and a FIX one with
nanoseconds.

A width therefore never decides which shape a stamp is; the separators do,
and a batch mixing two shapes of one width is grouped rather than sliced as
either. A fraction finer than a microsecond is truncated, which is what the
microsecond column stores.

## Optional dependencies

YAML, Iceberg, and Polars are imported only when used. Install the
matching extra rather than making every Arrow-only consumer carry them.

## Benchmark

`python/benchmarks/bench_cast.py --quick` compares recursive casts with Arrow's
reference operations after verifying equal values.
