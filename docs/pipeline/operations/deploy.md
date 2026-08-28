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
pip install "rekep[iceberg]"            # persisted tables
pip install "rekep[iceberg,polars,yaml]"  # what the notebooks import
```

From a private index, use `--extra-index-url`. `--index-url` *replaces* PyPI,
so the dependencies stop resolving:

```bash
uv pip install --extra-index-url https://artifacts.example.net/api/pypi/pypi/simple rekep
```

## 2. The FIX registry

The wheel carries a **reduced** registry — the 180 fields the shipped
contracts promote. The full store is 6,074:

```python
from rekep.fix import FixRegistry

print(len(FixRegistry.from_builtin().field_records()))
```

```text
180
```

!!! danger "A missing registry path is silent"

    `fix_dictionary` pointing at a path that does not exist yields an **empty
    registry**, not an error. The pipeline then runs and transcribes nothing.

    ```python
    from rekep.fix import FixRegistry

    absent = FixRegistry(cache_dir="/no/such/place", offline=True)
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
catalog: rekep
catalog_properties:
  type: sql
  uri: sqlite:///data/catalog.db
  warehouse: file://data/warehouse
```

Namespaces and tables are created on first commit; nothing else to bootstrap.

### AWS S3

Create the bucket out of band — nothing in the pipeline creates one.

```bash
aws s3api create-bucket --bucket rekep-warehouse \
  --region eu-west-1 --create-bucket-configuration LocationConstraint=eu-west-1
aws s3api put-bucket-encryption --bucket rekep-warehouse \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

```yaml
catalog: rekep-production
catalog_properties:
  type: glue
  warehouse: s3://rekep-warehouse/rekep
  glue.region: eu-west-1
  s3.region: eu-west-1
```

Credentials come from the standard AWS chain, or from `s3.role-arn`. Encryption
is the bucket's own default; `s3.sse.*` is refused rather than ignored — see
[Encryption at rest](../../storage/iceberg.md#encryption-at-rest).

### MinIO and other S3-compatible stores

Name the endpoint, and give the store a real region if it signs with one.

```bash
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb local/rekep-warehouse
```

```yaml
catalog: rekep
catalog_properties:
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

[End-to-end run](run.md) has the six commands in dependency order.
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
