# Parse messages

`parse_messages` streams text captures into `logs.messages`. It splits the
wire shape but leaves FIX names, components and typed values to the three
`parse_fix_*` tasks.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter source=python/tests/data/app_messages_sample.txt
```

The command runs `tasks/parse_messages/parse_messages.py`; the adjacent
`parse_messages.yml` declares its parameters and their defaults.

Calendar-partitioned paths expand before the files are opened:

```yaml
source: s3://example-bucket/capture/{year}/{month}/{day}
start: 2026-08-30
end: 2026-08-31
```

`year`, `month`, and `day` are zero-padded. A templated source requires both
bounds; a date-only `end` includes that whole day.

Deploy the catalog first: [deploy from scratch](../operations/deploy.md).

## Output

One retained payload becomes one [`Message`](../../products/message.md):

```yaml
protocol: FIX4.4
msgtype: D
eventtype: ORDER
plugin: ""
body: !!binary OD1GSVguNC40fDM1PUR8MTE9T1JELTF8MTA9MDAxfA==
entries:
  - {tag: 11, key: "11", value: ORD-1}
  - {tag: 10, key: "10", value: "001"}
unix: 1787306400123000000       # recording clock
code: ""                       # FIX has not selected a lifecycle field yet
altids: {}
sourceurl: file:///capture.log
sourcerownum: 1
```

Repeated tags remain repeated list items in wire order. `vhash` identifies the
payload bytes and `hash` adds `unix`. The raw stage has no lifecycle code, so
its `xhash` is zero; the selected `parse_fix_*` task fills `code`, every code
in `altids`, and `xhash = XXH3-128(UTF-8(code))`.

`protocol` already includes the version resolved by the selected FIX
dictionary. Evidence-free UL rows use that dictionary's newest application
version, so stored messages and later FIX transcription agree.

## Filters and bounds

Payload and MsgType filters run before entry splitting. `technical_plugins`
uses the parsed header to reject operational rows before timestamp, payload or
entry parsing:

```yaml
include_regexes: []
exclude_regexes: []
include_msgtypes: []
exclude_msgtypes: ["0", "1"]
technical_plugins: [jolokia]

plugin_keys:
  XmlApi: {clientid: ClOrdID, type: MsgType}
null_values: ["", "null", "<null>", "n/a", "none"]

batch_row_size: 65536
batch_byte_size: 67108864
max_row_byte_size: 67108864
duration_ns: null
commit_batch_num: 8
commit_row_size: null # Optional earlier row cap.
```

`plugin_keys` renames only rows recorded by that plugin, before fields such as
`MsgType` lift. Null matching is case-insensitive. Empty filter lists keep
every row. A truncated or oversized record is retained with its dropped-byte
count in `reason`; a bound that prevents even a header from being read raises.

Compressed local and remote files stream one at a time. Set `spill: true` to
stage only the compressed bytes in a temporary local file. A live `TextFile`
reader owns its handle, so close it before opening another reader on the same
object.

`logs.messages` is retained so field rules can be replayed without reopening
the source captures. Rebuild it when source filtering, key normalization,
header parsing, protocol rules or MsgType metadata changes.
