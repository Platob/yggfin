# Deploy from scratch

Three things have to exist before the first task runs: the **package**, a **FIX
registry**, and a **catalog with a warehouse**. Everything else the pipeline
creates for itself.

!!! warning "A wheel by itself is not a deployment"

    DAG parsing reads the YAML under `tasks/`, workers execute the adjacent
    notebooks, and the FIX registry is a separate artifact. Keep package, DAG,
    YAML, notebooks, schemas and registry on one revision — see
    [Deploy and operate with Airflow](airflow.md).

## 1. Install

```bash
pip install "rekep[iceberg]"              # persisted tables
pip install "rekep[iceberg,polars,yaml]"  # what the notebooks import
pip install "rekep[all]"                  # plus Glue, for a catalog on AWS
```

From a private index, use `--extra-index-url`. `--index-url` *replaces* PyPI,
so the dependencies stop resolving:

```bash
uv pip install --extra-index-url https://artifacts.example.net/api/pypi/pypi/simple rekep
```

## 2. The FIX registry

The wheel carries a **reduced** registry — the 180 standard fields the shipped
contracts promote and 36 frozen rekep fields. The full store is 6,110:

```python
from rekep.fix import FixRegistry

print(len(FixRegistry.from_builtin().field_records()))
```

```text
216
```

!!! danger "A missing registry path is silent"

    `fix_dictionary` pointing at a path that does not exist yields an **empty
    registry**, not an error. The pipeline then runs and transcribes nothing.

    ```python
    from rekep.fix import FixRegistry

    absent = FixRegistry(cache_dir="/no/such/place")
    print(len(absent.versions), len(absent.field_records()))
    ```

    ```text
    0 0
    ```

Check it on every worker before the first run:

```bash
rekep fix registry check --store data/fix
```

A store is a directory or a zip — `data/fix.zip` works as a `fix_dictionary`
value directly, which is what makes shipping one to workers a single file:

```yaml
# tasks/parse_fix/parse_fix.yml
parameters:
  fix_dictionary: data/fix.zip
```

The release workflow publishes the full store beside the wheel, so a
deployment fetches it rather than scraping:

```bash
curl --fail -H "Authorization: Bearer $ARTIFACTORY_TOKEN" \
  -o data/fix.zip "$REKEP_FIX_REGISTRY_URL"
```

## 3. Catalog and warehouse

### Local

The only step that is not automatic: **the SQLite catalog's parent directory
must already exist**. Without it the first connection raises
`unable to open database file`.

```bash
mkdir -p data/warehouse
```

```yaml
catalog:
  catalog_name: rekep
  properties:
    type: sql
    uri: sqlite:///data/catalog.db
    warehouse: file://data/warehouse
```

Namespaces and tables are created on first commit; nothing else to bootstrap.

### Creating the tables ahead of the jobs

Where the account that owns the catalog is not the account the jobs run under,
create them separately. One command takes the catalog, its properties and the
branch straight off a task document, so a deployment cannot land somewhere the
pipeline will not read:

```bash
rekep iceberg deploy tasks/parse_fix/parse_fix.yml
```

```text
logs.messages -> created
fix.market -> created
fix.misc -> created
fix.unknown -> created
market.instruments -> created
market.books -> created
market.orders -> created
market.executions -> created
```

`--catalog`, `--property NAME=VALUE`, `--table-property NAME=VALUE` and
`--branch` override the document; `--table` restricts the run to one table and
`--dry-run` reports what is missing without creating it. Running it again
reports every table `present` and changes nothing — including properties,
which [Iceberg maintenance](airflow.md#run-iceberg-maintenance) owns retrofitting.

`rekep.deploy.TABLES` lists each table and its Arrow shape in the order a run
fills them.

```python
from rekep.deploy import TABLES

print([shape.table for shape in TABLES])
```

```text
['logs.messages', 'fix.market', 'fix.misc', 'fix.unknown', 'market.instruments', 'market.books', 'market.orders', 'market.executions']
```

### AWS S3

Every location a run touches takes an `s3://` URL: the raw capture, the FIX
dictionary, the Iceberg warehouse, and the per-table data and metadata paths.
Only the catalog needs anything extra — pyiceberg reaches Glue through boto3,
which the `iceberg` extra does not pull:

```bash
pip install "rekep[glue,iceberg,polars,yaml]"   # or: rekep[all]
```

**1. Prove who you are, and make the buckets.** Nothing in the pipeline
creates one.

```bash
aws sts get-caller-identity
aws s3api create-bucket --bucket rekep-warehouse \
  --region eu-west-1 --create-bucket-configuration LocationConstraint=eu-west-1
aws s3api put-bucket-encryption --bucket rekep-warehouse \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":
    {"SSEAlgorithm":"aws:kms","KMSMasterKeyID":"arn:aws:kms:eu-west-1:111122223333:key/…"}}]}'
aws s3api get-bucket-location --bucket rekep-warehouse
```

`KMSMasterKeyID` selects the custom key for every object the pipeline writes.
Encryption is the bucket's own default; `s3.sse.type: kms` and `s3.sse.key`
are refused rather than ignored — see
[Encryption at rest](../../storage/iceberg.md#encryption-at-rest).

**2. Put the dictionary and the capture where the workers can read them.**

```bash
aws s3 cp data/fix.zip s3://rekep-warehouse/fix/fix.zip
aws s3 sync ./capture s3://rekep-capture/2026-08-30/
```

**3. Point the task documents at them.** The same three keys in every YAML
under `tasks/`:

```yaml
# tasks/parse_messages/parse_messages.yml
source: s3://rekep-capture/2026-08-30
fix_dictionary: s3://rekep-warehouse/fix/fix.zip
catalog:
  catalog_name: rekep-production
  properties:
    type: glue
    warehouse: s3://rekep-warehouse/rekep
    glue.region: eu-west-1   # Catalog region.
    s3.region: eu-west-1     # Warehouse region.
    # The bucket rule above owns KMS; per-request s3.sse.* is unsupported.
```

A registry on a bucket is fetched **once** and kept under
`~/.config/fix-remote`, reused across processes by identity and size — a zip
is read by seeking, and seeking over an object store reads it whole every
lookup. A dictionary given as a *directory* is served in place.

The capture `source` is resolved on its own rather than through
`catalog.properties`, so an endpoint or region for that bucket comes from the
URL or from the environment:

```bash
export AWS_PROFILE=rekep AWS_REGION=eu-west-1
# or, for any S3-compatible store, the portable spelling:
export S3_ENDPOINT_URL=https://minio.example:9000
export S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=... S3_REGION=us-east-1
```

Mind what a netloc means before you name a bucket: `s3://store/bucket/key` is
a store addressed path-style, and a bucket whose last label looks like a
registered name is read as one —
[what an `s3://` netloc names](../../storage/iceberg.md#what-an-s3-netloc-names)
has the rule and the escape.

**4. Check it end to end before scheduling anything.** One command proves the
credentials, the region and the registry all resolve:

```bash
uv run --project python rekep fix registry check --store s3://rekep-warehouse/fix/fix.zip
```

**5. Deploy the tables, then run.**

```bash
uv run --project python rekep iceberg deploy tasks/parse_fix/parse_fix.yml --dry-run
uv run --project python rekep iceberg deploy tasks/parse_fix/parse_fix.yml
```

The six task commands in [End-to-end run](run.md) are unchanged — only the
values in the YAML moved.

**6. Verify the warehouse filled.**

```bash
aws s3 ls --recursive --summarize s3://rekep-warehouse/rekep/
aws glue get-tables --database-name market --query 'TableList[].Name'
```

A run of the shipped fixture against an S3 endpoint — capture, dictionary and
warehouse all on the bucket — lands 51 objects across the seven tables: 11
Parquet files, 22 Avro manifests and 18 metadata documents.

Credentials come from the standard AWS chain, or from `s3.role-arn` — which
reaches the data filesystem only, never the Glue client, so the Glue call
itself is signed by the chain. `s3.profile-name` and `s3.signer.*` are read by
nothing; see
[Settings a location carries](../../storage/iceberg.md#settings-a-location-carries).

### MinIO and other S3-compatible stores

Name the endpoint, and give the store a real region if it signs with one.

```bash
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb local/rekep-warehouse
```

```yaml
catalog:
  catalog_name: rekep
  properties:
    type: sql
    uri: sqlite:///data/catalog.db
    warehouse: s3://rekep-warehouse/rekep
    s3.endpoint: http://minio:9000
    s3.region: us-east-1
    s3.access-key-id: ${MINIO_ROOT_USER}
    s3.secret-access-key: ${MINIO_ROOT_PASSWORD}
```

An overridden endpoint is addressed path-style, which is what MinIO and Ceph
want. [Filesystems](../../storage/iceberg.md#filesystems) has the full set of
settings a location may carry, and the one-liner that prints what any of them
becomes.

Verify the store answers before scheduling anything:

```bash
rekep fields load --target schemas/rekep/message.yaml | tail -2
```

## 4. Run

[End-to-end run](run.md) has the seven commands in dependency order.
[Airflow](airflow.md) has the scheduled deployment.

## Build and publish

```bash
cd python
uv build --no-sources
```

```text
Successfully built dist/rekep-0.1.0.tar.gz
Successfully built dist/rekep-0.1.0-py3-none-any.whl
```

A pure-Python universal wheel, plus the reduced registry as its one non-Python
payload (`rekep/fix/registry.zip`).

`.github/workflows/release.yml` publishes both artifacts on a published
release, or on demand:

```bash
uv publish --no-attestations dist/rekep-*.whl dist/rekep-*.tar.gz
```

It reads `UV_PUBLISH_URL`, `UV_PUBLISH_CHECK_URL`, `UV_PUBLISH_USERNAME` and
`UV_PUBLISH_PASSWORD` from the `artifactory` GitHub environment, then uploads
the **full** registry zip to `REKEP_FIX_REGISTRY_URL` with its SHA-256.

### Serving the wheel from S3

A Python index is resolved over HTTP, so an S3 bucket serves one only behind
static website hosting or CloudFront. The layout is PEP 503 — one directory
per project, one link per file:

```bash
mkdir -p simple/rekep && cp python/dist/rekep-* simple/rekep/
python - <<'PY'
import pathlib
files = sorted(pathlib.Path("simple/rekep").glob("rekep-*"))
links = "\n".join(f'<a href="{f.name}">{f.name}</a><br>' for f in files)
pathlib.Path("simple/rekep/index.html").write_text(f"<!DOCTYPE html><html><body>\n{links}\n</body></html>\n")
pathlib.Path("simple/index.html").write_text('<!DOCTYPE html><html><body><a href="rekep/">rekep</a></body></html>\n')
PY
aws s3 sync simple/ s3://rekep-packages/simple/ --delete
```

```bash
uv pip install --extra-index-url https://packages.example.net/simple rekep
```

Artifactory can front an S3 bucket itself, in which case `uv publish` above is
the whole story and the sync is Artifactory's.
