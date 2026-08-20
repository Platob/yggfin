# Datasets

A `Dataset` is OpenLineage's resource for one namespace-qualified data
product: schema, physical location across platforms, and the writers that
move data into it — each write internally lineage-tracked.

## Declaring

`record` names the schema helper — any `Record` subclass, whose Arrow
projection *is* the dataset's schema facet:

```python
from rekep.dataset import Dataset
from rekep.models import Log

dataset = Dataset(record="rekep.models.Log", name="logs", namespace="trading")
dataset.schema_facet()   # OpenLineage SchemaDatasetFacet: the record's fields
dataset.uri()            # "dataset://trading/logs" -- globally unique
```

## Location: shared, direct, protocol-specific

Location is layered the same way `Arrow` field metadata already is — shared
keys and protocol-prefixed overrides:

```python
dataset = Dataset(
    record="rekep.models.Log",
    direct="s3://lake/log",                              # every protocol's fallback
    properties={"format": "parquet"},                     # shared by every protocol
    protocols={"iceberg": {"location": "s3://lake/iceberg/log"}},  # iceberg's own
)
dataset.location("iceberg")               # "s3://lake/iceberg/log"
dataset.location("doris")                 # "s3://lake/log" -- falls back to direct
dataset.protocol_properties("iceberg")    # properties merged with iceberg's own
```

## Deploying: autonomous, no `tables/` side file

`stacks/iceberg/` and `stacks/doris/` declare only `catalogs/` and
`namespaces/` — a table needs no matching side file of its own. A `Dataset`
under `stacks/datasets/` carries everything a table declaration used to
(record, name, namespace, protocol properties) and converges itself:

```python
from rekep.dataset import Dataset
from rekep.iceberg import Iceberg

stack = Iceberg.load("stacks/iceberg")      # catalogs + namespaces only
for dataset in Dataset.load_all("stacks/datasets"):
    dataset.deploy_iceberg(stack)           # or dataset.deploy("iceberg", stack)
```

`into_iceberg_table()`/`into_doris_table()` build the ad hoc `IcebergTable`/
`DorisTable` `deploy_iceberg`/`deploy_doris` hand to `Iceberg.deploy_one`/
`Doris.deploy_one` — the same catalog-check, namespace-`get_or_create`,
table-`create_or_update` sequence `rekep service records deploy` uses for a
bare record, just resolved from the dataset's own fields instead of stack
defaults. The CLI shape is `rekep service dataset deploy --target iceberg`
(see the [CLI guide](cli.md)).

## Writing: generic dispatch, protocol-specific hooks

`write_arrow_reader` dispatches by `format` to a private
`_{format}_write_arrow_reader`, which opens a `Run`, calls the matching
*public* `{format}_write_arrow_reader` hook, and closes the run on the way
out — `START` before, `COMPLETE`/`FAIL` after:

```python
dataset.write_arrow_reader(reader, format="iceberg", table=live_table)
dataset.write_arrow_reader(reader, format="file")   # uses direct/protocols["file"]["location"]
```

- **`iceberg_write_arrow_reader`** is the Iceberg hook: public because it is
  the customisation point a deployment overrides (resolve a table from a
  catalog, choose how the append happens), abstract in spirit because the
  default only knows how to append to a `table=` it is handed.
- **`file_write_arrow_reader`** is the same shape for any `pyarrow.fs`
  filesystem: a URI maps to a `(filesystem, path)` pair through
  `rekep.filesystems.resolve`, cached per URL, and the reader streams
  straight into `pyarrow.dataset.write_dataset` — never materialised.

Both stream one batch at a time and return the row count written; neither
one is called directly by `write_arrow_reader` — the private
`_{format}_write_arrow_reader` between them is where the lineage tracking
lives, so a new protocol only has to implement the public hook.

## Lineage: internal, not emitted

Every tracked write appends `RunEvent`s to the dataset instance itself:

```python
dataset.write_arrow_reader(reader, format="iceberg", table=live_table)
dataset.events()   # [RunEvent(START, ...), RunEvent(COMPLETE, outputStatistics={"rowCount": ...})]
```

Nothing is sent anywhere — this is internal bookkeeping in exactly the shape
an OpenLineage client would need if one is ever wired in. `Job.run_tracked()`
(and `@arrow_task`) wrap a whole `extract -> transform -> load` the same way.
