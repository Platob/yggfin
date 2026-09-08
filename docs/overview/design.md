# Design rules

## One public vocabulary

Applications import resource, field, text, and FIX behavior from `rekep`.
There are no parallel field classes, path layers, codecs, registries, or FIX
row models.

```python
from typing import Annotated

from rekep import scalar
from rekep.fields import primary_key


@scalar
class Row:
    id: Annotated[str, primary_key()]
    value: float | None = None
```

## Arrow is the transport

Primary APIs consume and return `pyarrow.RecordBatchReader`. Tables are used
only where an operation is explicitly memory-sized. Shape conversion remains
columnar; Python row loops do not sit between parsing and storage.

## Metadata is executable

Primary keys, partitions, derived values, digests, FIX tags, code sets, and
descriptions live on `Field`. Producers and consumers call `Field.apply_arrow_*`
so cast, derivation, digest, and nullability rules execute in their declared
order.

## Raw before interpreted

`logs.messages` preserves exact text bodies. `fix.messages` interprets every
body while retaining `nofixentries` and `nounmappedfixentries`. A parser update
can therefore be replayed from Iceberg without rereading the original files.

## Replays are ordinary runs

Both current products merge on `(url, rownum)`. Reprocessing the same capture
reads the rows, writes zero new rows, and does not create an empty snapshot.

## Documentation names contracts

Schema JSON derives from the runtime field. Product pages describe its
meaning and link to the reviewed snapshot. Examples use the same public calls
as tasks, tests, and operators.
