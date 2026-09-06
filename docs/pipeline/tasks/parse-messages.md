# Parse messages

`parse_messages` recursively reads physical text records and merges raw
`Message` rows into `logs.messages`.

It deliberately leaves `body` uninterpreted. The downstream
[`parse_fix`](parse-fix.md) task owns the native protocol pass.

```bash
rekep task run tasks/parse_messages/parse_messages.json
```

```json
{
  "parameters": {
    "filesystem": "file:data/capture",
    "direction": "sent"
  }
}
```

For AWS S3, use
`s3://example-bucket/capture?region=eu-west-1` as `filesystem`.

`direction` names what a line carrying no verb in front of its payload took:
`sent`, `recv`, or `unknown`. Classification needs no dictionary at all -- the
scan reads the frame's own shape -- so this task takes no `registry`.

The URI is passed unchanged to `IOBase.from_uri`. Yggdryl selects supported
text leaves, opens each once, derives gzip or zstd decoding from the filename
or declared media type, and emits row-bounded Arrow batches. No remote object
is staged locally.

The task configures `TextOptions` with:

- physical `rownum` output;
- the fixed named header expression from `rekep.times.MESSAGE_HEADER`;
- general type inference disabled, leaving header captures as text and the body
  as bytes.

`Message.apply_arrow_batch` uses Arrow kernels to normalize the captured
timestamp without Python row loops, classifies the body in one native scan, then
native `Field.apply_arrow_batch` derives `timepartition` and fills the `msghash`
digest holder. An offset-free header means UTC; fractional digits are
padded or truncated to microseconds. A missing header stays null and an invalid
captured instant fails the batch. Both timestamp columns are nullable
`timestamp[us, UTC]`; Iceberg applies its `hour` transform to `timepartition`.

Yggdryl names source columns `url` and `rownum`; the task applies this Message
boundary to each batch, captures the second header bracket as `branch`, and
exposes the batches as one schema-bearing
`RecordBatchReader` to Iceberg. Yggdryl compiles the native writer apply once;
neither boundary accumulates the source or an output table in memory. Transport
read-ahead is 1 MiB and batches default to 65,536 rows; one individual record
remains bounded only when
`TextOptions.max_record_byte_size` is set. Its current truncation policy would
change `body`, so this task leaves it unset until Yggdryl exposes the
error-on-overflow mode specified in the streaming prompt.

## Classification and digest

Four columns are added beside the capture, and none of them interprets the
body:

- `mimetype`, the media type the frame's own shape proves;
- `msgtype`, the raw `MsgType` the frame spells, or `unknown`;
- `msgdirection`, `SENT` or `RECV` from the verbs beside the frame;
- `msghash`, the XXH3-128 digest of the exact body bytes, filled by Yggdryl.

The first three are one native `classify_arrow_array` pass over the payload
column, so no row crosses into Python. Downstream,
[`parse_fix`](parse-fix.md) reads this classification rather than recomputing
it. [Quality](../../fix/quality.md) states what each value means.

The table merge key is `(url, rownum)`. On replay, existing keys
are skipped before a commit; a fully repeated source creates no data file or
snapshot.

## Compressed input

Single-member gzip and zstd inputs stream correctly, and concatenated zstd
frames are already read in order. Concatenated gzip members currently need the
Yggdryl change described in
[`yggdryl-text-streaming.md`](../../prompts/yggdryl-text-streaming.md); staging
remote objects locally is not a replacement for fixing the decoder.

## Throughput

The focused benchmark generates realistic log rows, verifies its output, and
times both native Yggdryl emission and the complete Message boundary:

```bash
cd python
uv run python benchmarks/bench_message.py
```

The measured parser and complete Message-to-Iceberg results, including build
provenance, live in the single
[benchmark record](../../storage/benchmarks.md).

The sequential text path already has encoded transport read-ahead. Yggdryl's
positional `buffered()` cache is not used by record reads, and a local-staging
diagnostic was slower, so remote objects remain direct streams.
