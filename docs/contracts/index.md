# Portable schema

[`schemas/rekep/message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json)
is the checked portable contract for `logs.messages`.
[`schemas/rekep/fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json)
is the `FixMsg` snapshot this checkout's dictionary produces, used for review
and mock Iceberg writes.

```python
from pathlib import Path

from yggdryl import Field

document = Path("schemas/rekep/message.json").read_text(encoding="utf-8")
field = Field.from_json(document)
assert f"{field.into_json(indent=2)}\n" == document
print(field.into_arrow_schema())
```

The file is native Yggdryl `Field` JSON. It carries the Arrow types,
nullability, field metadata, and Iceberg key markers needed to reproduce the
table shape. Native partition and digest declarations, when present, use the
same validated metadata and round-trip through `Field.from_json` and
`Field.into_json`. The raw Message contract declares neither generated
protocol. Rekep has no parallel schema class or document implementation.

Regenerate it from the declaration:

```bash
rekep fields dump --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
rekep fields load --target schemas/rekep/message.json
```

Schema changes update `Message` and this generated document together.

The FIX snapshot has 101 source-first columns: the twelve raw `Message` columns
(two of them renamed, see [Parse FIX](../pipeline/tasks/parse-fix.md#the-two-renamed-columns)),
eighty-seven columns named by their canonical tag -- the eighty session and
order tags Yggdryl's fixed schema carries, typed as
[`config/fix`](https://github.com/Platob/yggfin/tree/main/config/fix) declares
them, and the seven Yggdryl derives on its own branch (`30001` through
`30007`) -- and the closing `entries` and `unmapped` pair lists. The schema is
a fixed projection rather than a column per definition: a dictionary of six
thousand fields would otherwise be a table of six thousand columns, and every
pair the projection does not name is still in `entries`. Every timestamp
in it is `timestamp[us, UTC]`; `(url, rownum)` remains the primary key and
`timepartition` retains its hourly Iceberg marker. It is a derived fixture, not
schema authority: `parse_fix` obtains the live schema from the selected Yggdryl
registry before streaming rows, so a dictionary that types a tag differently --
or declares a group where a scalar was -- is a differently typed table without a
code change.

Generate it without parsing a message:

```python
import pathlib

import pyarrow
from yggdryl import Field, IOBase
from yggdryl.fix import FixRegistry, parse_arrow_reader

from rekep.text import Message

CARRIED = {"branch": "logbranch", "msgdirection": "direction"}

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.with_crate_fields()
capture = Message.field().into_arrow_schema()
capture = pyarrow.schema(
    [member.with_name(CARRIED.get(member.name, member.name)) for member in capture]
)
empty = pyarrow.RecordBatchReader.from_batches(capture, [])
parsed = parse_arrow_reader(empty, registry, "body")


def microseconds(dtype):
    """One Arrow type with every nanosecond clock in it narrowed.

    The same walk `parse_fix` runs, and for the same reason: Iceberg v2 has
    no nanosecond timestamp, and a clock nested in a repeating group is a
    clock.
    """
    if pyarrow.types.is_timestamp(dtype) and dtype.unit == "ns":
        return pyarrow.timestamp("us", tz=dtype.tz)
    if pyarrow.types.is_list(dtype):
        item = dtype.field(0)
        return pyarrow.list_(item.with_type(microseconds(item.type)))
    if pyarrow.types.is_large_list(dtype):
        item = dtype.field(0)
        return pyarrow.large_list(item.with_type(microseconds(item.type)))
    if pyarrow.types.is_struct(dtype):
        return pyarrow.struct([member.with_type(microseconds(member.type)) for member in dtype])
    return dtype


members = [member.with_type(microseconds(member.type)) for member in parsed.schema]
fixed = Field.from_arrow_schema(pyarrow.schema(members), name="FixMessage")
parsed.close()
pathlib.Path("schemas/rekep/fix-message.json").write_text(
    f"{fixed.into_json(indent=2)}\n", encoding="utf-8"
)
```

That is the same shape `parse_fix` builds at runtime, which is why the two
agree without either reading the other. The integration test loads the JSON
with `Field.from_json`, constructs one schema-shaped mock batch, streams it
through the regular Iceberg writer, and reads back all 101 columns. This
isolates schema compatibility from parser behavior.
