# rekep

`rekep` streams physical text records through Arrow into Iceberg. Yggdryl owns
resource binding, filesystem traversal, decompression, text framing, and the
native `Field`; yggfin keeps the PyIceberg read/write boundary.

```bash
pip install "rekep[iceberg]"
```

```json
{
  "name": "parse_messages",
  "application": "parse_messages.py",
  "parameters": {
    "filesystem": "file:data/capture",
    "catalog": {
      "name": "rekep",
      "properties": {
        "type": "sql",
        "uri": "sqlite:///data/catalog.db",
        "warehouse": "data/warehouse"
      }
    }
  }
}
```

For AWS S3, set `filesystem` to
`s3://example-bucket/capture?region=eu-west-1`. An S3-compatible store can add
`endpoint_override`, `scheme`, and `force_path_style` URI query parameters.

```bash
rekep task run tasks/parse_messages/parse_messages.json
```

The task recursively reads every supported text leaf beneath `filesystem`,
including gzip and zstd objects, and appends raw rows to `logs.messages`.
Replaying the same source skips its `(url, rownum)` keys.

```text
IOBase / TextOptions -> Message batches -> logs.messages
```

The legacy Rekep FIX registry, parser, market models, tasks, contracts, tests,
and benchmarks have been removed. A later change can rebuild that layer
directly on `yggdryl.fix`; it must not restore the deleted compatibility stack.

Development:

```bash
cd python
uv sync --all-extras --dev
uv run pytest
uv run ruff check .
```

Long Iceberg checks are explicit:

```bash
uv run pytest -m integration
```

See the [documentation](https://platob.github.io/yggfin/) or the local
[pipeline guide](docs/pipeline/index.md).
