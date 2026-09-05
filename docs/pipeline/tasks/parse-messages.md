# Parse messages

`parse_messages` streams text records into `logs.messages`. yggdryl owns the
filesystem and text-media read; yggfin owns the Iceberg table and commit.
Nothing in this task interprets the record body.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter source=python/tests/data/app_messages_sample.txt
```

The command runs `tasks/parse_messages/parse_messages.py`; the adjacent YAML
document supplies the source, text options, and Iceberg write settings.

Calendar-partitioned paths expand before objects are opened:

```yaml
source: s3://example-bucket/capture/{year}/{month}/{day}
start: 2026-08-30
end: 2026-08-31
```

`year`, `month`, and `day` are zero-padded. A templated source requires both
bounds; a date-only `end` includes that whole day.

Deploy the catalog first: [deploy from scratch](../operations/deploy.md).

## Text media

The row-header expression matches and removes only the log prefix. It names
the header columns; `body` remains yggdryl's exact binary remainder.

```python
import pyarrow.fs
from yggdryl import IOBase, TextOptions

options = TextOptions()
options.with_rownum = 1
options.autotype = False
options.rowheader = (
    r"^(?<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}_\d{3}) "
    r"\[(?<threadname>[^]]*)\] \[(?<plugin>[^]]*)\] "
    r"(?:\((?<level>[A-Za-z]{1,12})\) )?"
)

source = IOBase.from_fs(
    pyarrow.fs.S3FileSystem(region="eu-west-1"),
    "example-bucket/capture/app.log.gz",
).into_text(options)
reader = source.read_arrow_reader()
```

`IOBase.from_fs` accepts a local, S3, subtree, or caller-defined Arrow
filesystem. yggdryl infers content encoding from the object and returns a
`RecordBatchReader`; yggfin renames `url` and `rownum` to `sourceurl` and
`sourcerownum`, casts the raw contract, and appends those batches to Iceberg.

## Output

One retained source row becomes one raw [`Message`](../../products/message.md):

```yaml
sourceurl: file:///capture.log
sourcerownum: 1
timestamp: "2026-08-21 12:00:00.123_000"
threadname: fix-reader
plugin: VenueBridge
level: INFO
body: !!binary OD1GSVguNC40fDM1PUR8MTE9T1JELTF8MTA9MDAxfA==
```

The table contains no `protocol`, `direction`, `msgtype`, `eventtype`, session
fields, `entries`, event lifecycle, or payload-derived identity. Source URL and
row number are its key. Header captures remain their source spellings.

The three `parse_fix_*` runs read `body` and own UTF-8 repair, protocol and
direction classification, MsgType filtering, entry tokenization, dictionary
resolution, diagnostics, clocks, and event identity. Changing those rules
reruns parse_fix without reopening the original text objects.

## Bounds

`batch_row_size` controls the Arrow batches yggdryl yields. `limit` bounds the
task result, while `commit_batch_num` and `commit_row_size` control yggfin's
Iceberg commit cadence. These are independent: a text batch is not an Iceberg
commit.

```yaml
commit_batch_num: 8
commit_row_size: null
```

Current yggdryl text media emits one record per physical line. The proposed
logical-record framing and per-record byte diagnostic are tracked in
[`docs/prompts/yggdryl-text-records.md`](../../prompts/yggdryl-text-records.md).
