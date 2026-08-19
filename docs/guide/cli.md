# CLI

The command line is organised by service: `rekep service <service> <command>`.
A new capability is a new service class, not a flag on an old command.

## DDL

Emit `CREATE TABLE` from a record declaration:

```bash
rekep service ddl dump \
    --namespace rekep.models.Log \
    --table-name log_records \
    --property "write.format.default=parquet" \
    --out stacks/ddl/iceberg
```

writes `stacks/ddl/iceberg/log_records.sql`:

```sql
CREATE TABLE IF NOT EXISTS log_records (
    url STRING NOT NULL COMMENT 'Path of the log the line came from, ...',
    unix BIGINT NOT NULL COMMENT 'Timestamp as whole nanoseconds since the epoch, naive UTC.',
    ...
)
USING iceberg
COMMENT 'One parsed line of a trading log.'
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);
```

Column comments come from the field docstrings; `NOT NULL` from the type
hints; default partition columns from `Arrow(iceberg={"partition": "true"})`
field metadata, overridable with `--partition-by`.

## Jinja everywhere a deploy value goes

Any string option may be a template, rendered with the other arguments,
`--var` pairs, and the process environment:

```bash
DATA_BUCKET=s3://lake rekep service ddl dump \
    --namespace rekep.models.Log \
    --location "{{ env.DATA_BUCKET }}/logs" \
    --var zone=eu \
    --property "comment={{ zone }}:{{ namespace }}" \
    --out -
```

Undefined variables fail loudly — a half-rendered config is worse than a loud
one. Templating needs the `jinja` extra; untemplated values never touch it.

## Deploy one record

The stacks converge whole folders; `records deploy` converges a single record
class into one or more of them:

```bash
rekep service records deploy --pyclass rekep.models.Log --target iceberg
rekep service records deploy --pyclass rekep.models.Log     --target iceberg --target doris --dry-run
```

The deployment's own `tables/` entry wins when one declares that record;
otherwise the stack defaults apply. Iceberg converges live (catalog checked,
namespace and table `get_or_create`/`create_or_update`); Doris emits its
ordered statements. `--dry-run` and `--verbose` behave as everywhere else.
