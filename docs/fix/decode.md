# Decode

Use `FixCodec` for one line and `parse_arrow_reader` for a stream. Both use the
same registry, field resolution, frame detection, typing, stamps, and folded
column names.

## One line

```python
from yggdryl import IOBase
from yggdryl.fix import FixCodec, FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.with_ulbridge_fields()
codec = FixCodec(registry, branch="ulbridge")

message = codec.transform_line(
    b"Sending : 8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
)

assert message.field.name == "D"
assert message.by_tag(55).as_py() == "AAPL"
assert message.by_name("Side").as_py() == "1"
assert len(message.digest()) == 16
assert message.anomalies() == []
```

The method chooses the syntax from the line. Pin it when the caller already
knows the source:

| method | input |
| --- | --- |
| `transform_line` | detect FIX, ULLINK, bridge configuration, or FIXML |
| `transform_fix_line` | numeric FIX bytes; optional separator byte |
| `transform_ullink_line` | ULLINK/bridge key-value bytes |
| `transform_ulconfig_line` | ULBridge configuration bytes |
| `transform_fixml_line` | FIXML bytes |
| `transform_pairs` | already-split key/value pairs |

`branch`, `version`, and `null_values` belong to the codec. Per-line parameters
can also come from source columns in the Arrow path.

## Arrow stream

```python
import pyarrow
from yggdryl import IOBase
from yggdryl.fix import FixRegistry, parse_arrow_reader

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.with_ulbridge_fields()

schema = pyarrow.schema(
    [
        pyarrow.field("url", pyarrow.string(), nullable=False),
        pyarrow.field("rownum", pyarrow.int64(), nullable=False),
        pyarrow.field("body", pyarrow.binary(), nullable=False),
    ]
)
batch = pyarrow.RecordBatch.from_pylist(
    [
        {
            "url": "file:///capture.log",
            "rownum": 1,
            "body": b"8=FIX.4.4|35=D|55=AAPL|10=000|",
        }
    ],
    schema=schema,
)
source = pyarrow.RecordBatchReader.from_batches(schema, [batch])
parsed = parse_arrow_reader(source, registry, "body", branch="ulbridge")

assert parsed.schema.names[:3] == ["url", "rownum", "body"]
assert parsed.schema.names[-2:] == ["nofixentries", "nounmappedfixentries"]
assert parsed.read_all().column("symbol").to_pylist() == ["AAPL"]
```

The output schema is available before the first input batch. Source columns
lead it unless a fixed column claims the same folded name. A matching source
column fills that fixed field instead of being renamed or duplicated.

## Row behavior

One input row produces one output row. Content cannot make the stream fail:
an unrecognized line has an empty arrival record and nullable typed fields,
while `beginstring`, `msghash`, `timestamp`, and `unixpartition` receive their
documented fallbacks. I/O and invalid root/options remain errors.

A value that cannot be cast leaves its typed field null and remains verbatim in
`FixMsg.entries()` and the Arrow `nofixentries` column. Repeating groups are
lists of structs under their folded counter-field name.

## Inspect one result

`FixMsg` resolves by id, tag, folded name, or path. Its support surface includes
`entries()`, `anomalies()`, `digest()`, `market_timestamp()`, and `to_bytes()`.
Use it for one-message diagnosis; use `parse_arrow_reader` for datasets.

## Try it in the browser

<div data-fix="decode">Loading the dictionary…</div>

The browser displays frame location, syntax, resolved fields, raw pairs, and
unmapped pairs. Nothing pasted into it leaves the tab.
