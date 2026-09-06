# Deploy the table

The write path creates `logs.messages` when it is missing. Deploy it ahead of
the job when catalog creation belongs to a separate operator:

```bash
rekep iceberg deploy tasks/parse_messages/parse_messages.json
```

Preview without changing the catalog:

```bash
rekep iceberg deploy tasks/parse_messages/parse_messages.json --dry-run
```

The task document is the catalog authority. CLI `--catalog`, `--property`,
`--table-property`, and `--branch` flags override it for one deployment.

For S3, keep the capture source URI and Iceberg warehouse configuration
separate. The capture URI belongs to Yggdryl; PyIceberg's standard `s3.*`
catalog properties configure the warehouse. Glue needs the package extra:

```bash
pip install "rekep[glue]"
```

```json
{
  "name": "parse_messages",
  "application": "parse_messages.py",
  "parameters": {
    "filesystem": "s3://capture-bucket/logs?region=eu-west-1",
    "catalog": {
      "name": "production",
      "properties": {
        "type": "glue",
        "warehouse": "s3://warehouse-bucket/rekep",
        "glue.region": "eu-west-1",
        "s3.region": "eu-west-1",
        "glue.id": "123456789012"
      }
    }
  }
}
```

Omit `glue.id` unless the catalog belongs to another AWS account. Configure
KMS encryption on the bucket; per-request `s3.sse.*` properties are not
supported by this boundary.
