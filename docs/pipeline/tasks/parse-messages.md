# Parse messages

`parse_messages` streams text captures into `logs.messages`. It splits the
wire shape but leaves FIX names, components and typed values to `parse_fix`.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter source=python/tests/data/app_messages_sample.txt \
  --output parse_messages.executed.ipynb
```

Deploy the catalog first: [deploy from scratch](../operations/deploy.md).

## Output

One retained payload becomes one [`Message`](../../products/message.md):

```yaml
protocol: FIX
msgtype: D
eventtype: ORDER
message: "8=FIX.4.4|35=D|11=ORD-1|10=001|"
entries:
  - {tag: 11, key: "11", value: ORD-1}
  - {tag: 10, key: "10", value: "001"}
unix: 1787306400123000000       # recording clock
sourceurl: file:///capture.log
sourcerownum: 1
```

Repeated tags remain repeated list items in wire order. `vhash` identifies the
payload bytes, `hash` adds `unix`, and `xhash` initially equals `vhash`.

## Filters and bounds

Payload and MsgType filters run before entry splitting. `technical_plugins`
runs after header parsing and before persistence:

```yaml
include_regexes: []
exclude_regexes: []
include_msgtypes: []
exclude_msgtypes: ["0", "1"]
technical_plugins: [jolokia]

batch_row_size: 65536
batch_byte_size: 67108864
max_row_byte_size: 67108864
duration_ns: null
```

Empty lists keep every value. A truncated or oversized record is retained with
its dropped-byte count in `reason`; a bound that prevents even a header from
being read raises instead of reporting an empty capture.

Compressed local and remote files stream one at a time. Set `spill: true` to
stage only the compressed bytes in a temporary local file. A live `TextFile`
reader owns its handle, so close it before opening another reader on the same
object.

`logs.messages` is retained so field rules can be replayed without reopening
the source captures. Rebuild it only when source filtering, header parsing,
protocol syntax rules or MsgType event metadata changes.
