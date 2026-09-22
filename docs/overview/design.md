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

## Text before interpretation

`logs.messages` keeps the line as text: the header's captures typed, and the
`body` past the header as the read decoded it. `fix.raw` interprets every
line and stores lifted columns plus residual `fixentries`, under the
`nofixentries` that counts them. Unknown and unrepresentable pairs remain
residual; successfully lifted values are not duplicated. `fix.refined`
restates those canonical rows with their chains walked. A parser update is
replayed from `logs.messages`; a lifecycle change replays the `fix.raw` rows
without parsing the capture again.

## Replays are ordinary runs

All three ingestion tables replace on their sole `curruuid` key. On
`logs.messages` it identifies one line; on the FIX tables it identifies one
settled event, because a bridge may log that event again at every hop it
passes.
A run parses one window, `[start, end)` -- the last day up to now when a task
is given neither bound -- and reprocessing the same window reads the same rows
and lands them over the ones it landed before: the table holds each key once,
and the run reports what it carried. The walk re-settles the identity of a
message it dates, so a `fix.refined` key is not always its `fix.raw` twin's:
`fix.refined` is written from `fix.raw` and never in place.

## Documentation names contracts

Schema JSON derives from the runtime field. Product pages describe its
meaning and link to the reviewed snapshot. Examples use the same public calls
as tasks, tests, and operators.
