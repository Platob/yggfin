# rekep

`rekep` streams ULBridge text records through Arrow into Iceberg. Yggdryl
owns resource binding, traversal, decompression, text framing, field
application, FIX registries, and FIX parsing. rekep owns the raw `Message`
contract and the PyArrow/PyIceberg seam.

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
rekep task run tasks/parse_fix/parse_fix.json
```

The first task recursively reads supported text leaves, including gzip and
zstd objects, and writes one raw row per physical line. The second sends every
stored body through Yggdryl's FIX codec. Both stages retain `(url, rownum)`, so
replaying a source writes no duplicate rows.

```text
IOBase + Message.text_options -> logs.messages  (12 columns)
logs.messages + FixCodec      -> fix.messages   (108 columns)
```

`logs.messages.bodyhash` identifies the exact captured bytes.
`fix.messages.msghash` identifies what the codec parsed after excluding the
session envelope. The generated [Message](schemas/rekep/message.json) and
[FixMessage](schemas/rekep/fix-message.json) contracts are review snapshots;
the runtime registry remains authoritative for FIX types.

Development:

```bash
cd python
uv sync --all-extras --dev
uv run pytest
uv run pytest -m integration
uv run ruff check .
```

See the [documentation](https://platob.github.io/yggfin/) or the local
[pipeline guide](docs/pipeline/index.md).
