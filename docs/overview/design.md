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

`logs.messages` preserves exact line bytes. `fix.bronze` interprets every
line while retaining the arrival record in `fixentries`, under the
`nofixentries` that counts it, where a pair no dictionary explains is an entry
of tag 0 under its own key; `fix.silver` restates those rows with their chains
walked. A parser update can therefore be replayed from Iceberg without
rereading the original files, and a change to the walk from the parsed rows
without parsing again.

## Replays are ordinary runs

`logs.messages` replaces on `bodyhash`; `fix.bronze` and `fix.silver` replace
on `curruuid`, because a bridge logs one message again at every hop it passes.
A run parses one window, `[start, end)` -- the last day up to now when a task
is given neither bound -- and reprocessing the same window reads the same rows
and lands them over the ones it landed before: the table holds each key once,
and the run reports what it carried. The walk re-settles the identity of a
message it dates, so a silver key is not always its bronze twin's: `fix.silver`
is written from `fix.bronze` and never in place.

## Documentation names contracts

Schema JSON derives from the runtime field. Product pages describe its
meaning and link to the reviewed snapshot. Examples use the same public calls
as tasks, tests, and operators.
