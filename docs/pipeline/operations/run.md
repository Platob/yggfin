# Run ingestion

Point the task at a text file or directory:

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
`s3://example-bucket/capture?region=eu-west-1`. For an S3-compatible endpoint,
add `endpoint_override`, `scheme`, and `force_path_style` query parameters.

Then run exactly that document:

```bash
rekep task run tasks/parse_messages/parse_messages.json
```

The command prints one compact JSON result to stdout and progress to stderr.
The default source path is illustrative and is not created by the package.

Replaying an unchanged source is safe. Rows already stored under `(url, rownum)`
are counted as skipped and do not create another
Iceberg snapshot.

Override one source without editing the document:

```bash
rekep task run tasks/parse_messages/parse_messages.json \
  --parameter filesystem='"file:data/capture-2026-09-06.log.gz"'
```
