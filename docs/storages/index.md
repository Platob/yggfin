# Three catalogs

A run lands its tables in three Iceberg catalogs, one per layer: `bronze`,
`silver` and `gold`. `rekep.Storages` holds the three, and names a table
across them as `<layer>.<namespace>.<table>`: the layer is the catalog that
holds it, and `<namespace>.<table>` is its name inside that catalog.

| table | catalog | name in the catalog |
| --- | --- | --- |
| `bronze.record_keeping.log_messages` | `bronze` | `record_keeping.log_messages` |
| `bronze.record_keeping.fix_messages` | `bronze` | `record_keeping.fix_messages` |
| `silver.record_keeping.fix_messages` | `silver` | `record_keeping.fix_messages` |
| `silver.record_keeping.books`, `.orders`, `.quotes`, `.executions` | `silver` | `record_keeping.<table>` |

Bronze and silver both hold a `record_keeping.fix_messages`, so the layers
never share one catalog: each is a SQLite database of its own, a Glue Data
Catalog of its own, or an S3 table bucket of its own.

`Storages.from_dict` takes one mapping per layer, each the
`{"name": ..., "properties": {...}}` that `IcebergCatalog.from_dict` reads:
PyIceberg's own catalog and FileIO properties, kept exactly as written. All
three layers are required and nothing else is read, so a missing or unknown
layer is refused before any catalog is opened. The catalogs are the caller's:
a task opens and closes the datasets it reads and writes through them, and
never closes a catalog. Close them with `close()`, or use `Storages` as a
context manager.

## Local SQLite

Three SQLite databases, each with a warehouse folder beside it. From the
repository root, under a scratch folder:

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.fix import fix_message_field
from rekep.storages import split_identifier

root = Path(tempfile.mkdtemp())
mapping = {
    layer: {
        "name": layer,
        "properties": {
            "type": "sql",
            "uri": f"sqlite:///{root / layer}.db",
            "warehouse": str(root / layer),
        },
    }
    for layer in ("bronze", "silver", "gold")
}
table = "bronze.record_keeping.fix_messages"
assert split_identifier(table) == ("bronze", "record_keeping.fix_messages")
with Storages.from_dict(mapping) as storages:
    assert list(storages.tables()) == []
    raw = storages.dataset(table, field=fix_message_field())
    try:
        assert raw.identifier == "record_keeping.fix_messages"
        assert not raw.exists
    finally:
        raw.close()

try:
    Storages.from_dict({"bronze": mapping["bronze"]})
except ValueError as refusal:
    assert "missing silver, gold" in str(refusal)
```

A scheme-less warehouse path is rewritten into an absolute `file://` URL
against the working directory, and a relative SQLite `uri` resolves against
it too: build the catalogs from one directory, or give both absolute values.
The same mapping as JSON, for a file a deployment keeps beside its code:

```json
{
  "bronze": {
    "name": "bronze",
    "properties": {
      "type": "sql",
      "uri": "sqlite:////srv/rekep/bronze.db",
      "warehouse": "/srv/rekep/bronze"
    }
  },
  "silver": {
    "name": "silver",
    "properties": {
      "type": "sql",
      "uri": "sqlite:////srv/rekep/silver.db",
      "warehouse": "/srv/rekep/silver"
    }
  },
  "gold": {
    "name": "gold",
    "properties": {
      "type": "sql",
      "uri": "sqlite:////srv/rekep/gold.db",
      "warehouse": "/srv/rekep/gold"
    }
  }
}
```

`Storages.from_dict(json.load(handle))` reads it. One SQLite file serves one
writer at a time; a shared SQL service -- `uri` naming PostgreSQL, say -- is
the same mapping with another `uri`.

## AWS Glue

A Glue Data Catalog names a namespace as a Glue database, and one Data
Catalog holds one `record_keeping` database, so each layer is a Data Catalog
of its own, named by `glue.id`, the catalog ID every Glue call is made
against. Install `rekep[glue]`, because PyIceberg reaches Glue through boto3.

```json
{
  "bronze": {
    "name": "bronze",
    "properties": {
      "type": "glue",
      "glue.id": "111111111111",
      "glue.region": "eu-west-1",
      "warehouse": "s3://market-bronze/rekep",
      "s3.region": "eu-west-1"
    }
  },
  "silver": {
    "name": "silver",
    "properties": {
      "type": "glue",
      "glue.id": "222222222222",
      "glue.region": "eu-west-1",
      "warehouse": "s3://market-silver/rekep",
      "s3.region": "eu-west-1"
    }
  },
  "gold": {
    "name": "gold",
    "properties": {
      "type": "glue",
      "glue.id": "333333333333",
      "glue.region": "eu-west-1",
      "warehouse": "s3://market-gold/rekep",
      "s3.region": "eu-west-1"
    }
  }
}
```

The worker needs the Glue database and table actions on each catalog and
S3 list, read, write and delete on each warehouse prefix; the capture bucket
needs list and read. Authenticate with a role, a web identity, a profile or
the standard AWS environment, never with keys in the mapping.
[Catalog types](catalogs.md#aws-glue-and-s3) has the details.

## AWS S3 Tables

A table bucket is an Iceberg REST catalog AWS hosts and maintains, so each
layer is a table bucket, and its ARN is the whole configuration. Install
`rekep[s3tables]`, because PyIceberg signs those REST calls through boto3.

```json
{
  "bronze": {
    "name": "bronze",
    "properties": {
      "type": "s3tables",
      "warehouse": "arn:aws:s3tables:eu-west-1:123456789012:bucket/market-bronze"
    }
  },
  "silver": {
    "name": "silver",
    "properties": {
      "type": "s3tables",
      "warehouse": "arn:aws:s3tables:eu-west-1:123456789012:bucket/market-silver"
    }
  },
  "gold": {
    "name": "gold",
    "properties": {
      "type": "s3tables",
      "warehouse": "arn:aws:s3tables:eu-west-1:123456789012:bucket/market-gold"
    }
  }
}
```

`record_keeping` is already a valid table-bucket namespace: one level,
lowercase letters, digits and underscores. A bucket integrated with the AWS
analytics services is reached through Glue instead, as
`123456789012:s3tablescatalog/market-bronze` with `rest.signing-region`; both
doors, the endpoints they resolve to and what the service owns are on
[Catalog types](catalogs.md#aws-s3-tables).

The layers need not share a type: a local bronze and silver beside a gold
table bucket is three mappings like any other.

## Create the tables ahead of a run

A task creates its target on its first write, so a run against empty catalogs
lands every table it needs. `rekep.deploy.deploy(storages)` creates them ahead
of time instead, for catalogs the runner may not create tables in -- a Glue
catalog or a table bucket an account owner deploys once.

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.deploy import TABLES, deploy

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
with storages:
    declared = [table.table for table in TABLES]
    assert set(deploy(storages, dry_run=True).values()) == {"missing"}

    zstd = {"write.parquet.compression-codec": "zstd"}
    raw = "bronze.record_keeping.fix_messages"
    assert deploy(storages, tables=[raw], table_properties=zstd) == {raw: "created"}
    done = deploy(storages)
    assert list(done) == declared
    assert done[raw] == "present"
    assert set(deploy(storages).values()) == {"present"}

    held = storages.catalog("bronze").load_table("record_keeping.fix_messages")
    assert held.properties["write.parquet.compression-codec"] == "zstd"
    assert sorted(storages.tables()) == sorted(declared)
```

`deploy` answers `created`, `present` or, under `dry_run`, `missing` per
table, in production order. It is idempotent in one direction only: a table
already there is reported `present` and left exactly as it is, properties
included. `tables` narrows it to some of `TABLES`, `table_properties` apply to
the tables it creates, `branch` names the ref, and `codec` types the two FIX
tables the way the run parsing into them does -- the process registry's when
None ([FIX registry](../fix/index.md)). Gold holds no table rekep writes, so
nothing is deployed there.

## A table's shape changed

`merge_schema=True`, which every task after `parse_log_messages` writes with,
adds a column a newer dictionary declares. It never retires or renames a
column, and never translates an identity. A table written under an older
contract -- another key, other field ids, another identity derivation -- is
rebuilt rather than evolved: drop it and every table after it in the graph
(`storages.catalog(layer).drop_table(name, purge=True)`), deploy the current
declarations, and replay the tasks in order from the capture. Replay the
capture from where it was first read: a line its header could not date takes
its object's modification time, and a copy of the object made later states
another identity for it.
