# Parse messages

`parse_messages` recursively reads physical text records and merges raw
`Message` rows into `logs.messages`.

```bash
rekep task run tasks/parse_messages/parse_messages.yml
```

```yaml
parameters:
  filesystem: file:data/capture
  # filesystem: s3://example-bucket/capture?region=eu-west-1
```

The URI is passed unchanged to `IOBase.from_uri`. Yggdryl selects supported
text leaves, opens each once, derives gzip or zstd decoding from the filename
or declared media type, and emits row-bounded Arrow batches. No remote object
is staged locally.

The task configures `TextOptions` with:

- physical `rownum` output;
- the fixed named header expression from `rekep.times.MESSAGE_HEADER`;
- type inference disabled, preserving source spellings and body bytes.

Yggdryl names source columns `url` and `rownum`; the task renames them to
`sourceurl` and `sourcerownum`, strictly casts the batch to `Message.field()`,
and sends the iterator directly to Iceberg. It never accumulates the source or
an output table in memory. Transport read-ahead is 1 MiB and batches default to
65,536 rows; one individual record remains bounded only when
`TextOptions.max_record_byte_size` is set. Its current truncation policy would
change `body`, so this task leaves it unset until Yggdryl exposes the
error-on-overflow mode specified in the streaming prompt.

The table merge key is `(sourceurl, sourcerownum)`. On replay, existing keys
are skipped before a commit; a fully repeated source creates no data file or
snapshot.

## Compressed input

Single-member gzip and zstd inputs stream correctly, and concatenated zstd
frames are already read in order. Concatenated gzip members currently need the
Yggdryl change described in
[`yggdryl-text-streaming.md`](../../prompts/yggdryl-text-streaming.md); staging
remote objects locally is not a replacement for fixing the decoder.

## Throughput

The focused benchmark generates 200,000 log rows and reports the fastest of
three warmed runs. On the reference Windows development host:

| source | native rows/s | Message rows/s | first batch |
| --- | ---: | ---: | ---: |
| `file:` plain | 85,660 | 96,848 | 684 ms |
| `file:` gzip | 99,359 | 97,813 | 653 ms |

Run the same focused measurement:

```bash
cd python
uv run python benchmarks/bench_message.py --rows 200000 --repeat 3
```

`native rows/s` measures Yggdryl text emission. `Message rows/s` includes the
rename and strict schema cast used by the task. Timing is a diagnostic, not a
portable performance guarantee.

The sequential text path already has encoded transport read-ahead. Yggdryl's
positional `buffered()` cache is not used by record reads, and a local-staging
diagnostic was slower, so remote objects remain direct streams.
