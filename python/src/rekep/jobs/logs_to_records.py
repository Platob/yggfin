"""LogsToRecords: structure `Log.message` into `ParsedMessage` records.

Regex, not `pyarrow.compute`: extracting an unknown number of `key=value`
pairs per row is inherently row-shaped work -- a split, a match, a dict
append -- exactly the "per-row work" AGENTS.md carves out for Python; only
the bulk column construction happens once per batch, in Arrow.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Iterator

import pyarrow

from rekep.job import Job
from rekep.models import ParsedMessage
from rekep.records import record

#: One `key=value` segment of a `|`-delimited message, key optionally
#: `#`-prefixed (`#8=FIX.4.4`, `9=112`). Values may be empty (`150=`); a key
#: may not itself contain `=` or `|`.
SEGMENT_PATTERN = re.compile(r"^#?(?P<key>[^=|]+)=(?P<value>[^|]*)$")

#: FIX's BeginString tag: its value, when present, becomes `protocol`.
PROTOCOL_TAG = "8"


def parse_fields(message: str) -> dict[str, str]:
    """`message` as `{key: value}`, `|`-split, `#`-stripped, non-matches dropped.

    Not FIX-specific: any pipe-separated run of `key=value` segments decodes
    the same way -- FIX messages (`8=FIX.4.4|9=112|35=D|`) are the common
    case here, not a special one. A trailing `|` (or an empty message)
    leaves nothing to match, so `{}` is the answer for those, not an error.
    """
    fields: dict[str, str] = {}
    for segment in message.split("|"):
        if not segment:
            continue
        match = SEGMENT_PATTERN.match(segment)
        if match:
            fields[match["key"]] = match["value"]
    return fields


@record
class LogsToRecords(Job):
    """Structure `Log.message` into `ParsedMessage`: `key=value` pairs, `|`-split.

    One `ParsedMessage` per `Log` row -- `fields` comes back empty rather
    than the row dropped when a message is not pipe/key-value shaped, so row
    count survives the transform and every input line stays traceable.
    """

    consumes: list[str] = dataclasses.field(default_factory=lambda: ["rekep.models.Log"])
    """Defaults to `Log` -- this job structures what `FilesToLogs` produces."""

    produces: list[str] = dataclasses.field(default_factory=lambda: ["rekep.models.ParsedMessage"])
    """Defaults to `ParsedMessage` -- the transform's only possible output."""

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        schema = ParsedMessage.into_arrow_schema()
        for batch in batches:
            yield _parse_batch(batch, schema)


def _parse_batch(batch: pyarrow.RecordBatch, schema: pyarrow.Schema) -> pyarrow.RecordBatch:
    """One `Log` batch to one `ParsedMessage` batch: same identity, new shape.

    `url`/`unix`/`date`/`hash64` pass through the same Arrow arrays
    unchanged -- `hash64` is already this row's stable identity, carried
    over as `ParsedMessage`'s primary key rather than recomputed. Only
    `message` is walked, row by row, into `protocol` and `fields`.
    """
    passthrough = {name: batch.column(name) for name in ("url", "unix", "date", "hash64")}
    protocols: list[str | None] = []
    fields: list[dict[str, str]] = []
    for message in batch.column("message").to_pylist():
        parsed = parse_fields(message) if message else {}
        fields.append(parsed)
        protocols.append(parsed.get(PROTOCOL_TAG))
    return pyarrow.RecordBatch.from_arrays(
        [
            passthrough["url"],
            passthrough["unix"],
            passthrough["date"],
            passthrough["hash64"],
            pyarrow.array(protocols, type=pyarrow.string()),
            pyarrow.array(fields, type=pyarrow.map_(pyarrow.string(), pyarrow.string())),
        ],
        schema=schema,
    )
