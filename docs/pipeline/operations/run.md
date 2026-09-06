# Run ingestion

Point the task at a text file or directory:

```yaml
# tasks/parse_messages/parse_messages.yml
parameters:
  filesystem: file:data/capture
  # filesystem: s3://example-bucket/capture?region=eu-west-1
  catalog:
    name: rekep
    properties:
      type: sql
      uri: sqlite:///data/catalog.db
      warehouse: data/warehouse
```

Then run exactly that document:

```bash
rekep task run tasks/parse_messages/parse_messages.yml
```

The command prints one compact JSON result to stdout and progress to stderr.
The default source path is illustrative and is not created by the package.

Replaying an unchanged source is safe. Rows already stored under
`(sourceurl, sourcerownum)` are counted as skipped and do not create another
Iceberg snapshot.

Override one source without editing the document:

```bash
rekep task run tasks/parse_messages/parse_messages.yml \
  --parameter filesystem='"file:data/capture-2026-09-06.log.gz"'
```
