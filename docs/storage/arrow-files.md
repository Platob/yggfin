# Arrow paths and FileIO

`ArrowPath` keeps one URL, its Arrow filesystem, and the filesystem-relative
path together.

```python
import datetime

from rekep import ArrowPath

root = ArrowPath("data/capture/{year}/{month}/{day}").resolve(".")
day = root.at_time(datetime.date(2026, 8, 31))

for source in day.glob("*.log*"):
    with source.open("rb", seekable=False, compression="detect") as stream:
        print(source.name, stream.read(64))
```

`ls(recursive=True)`, `glob`, and `rglob` are lazy iterators. Calendar tokens
are expanded only in the URL path; `{year}`, `{month}`, and `{day}` are
zero-padded.

## Read, write, and delete

```python
from rekep import ArrowPath

target = ArrowPath("data/output/result.json").resolve(".")
target.write_bytes(b'{"ok":true}')  # creates a missing local parent lazily
assert target.read_bytes() == b'{"ok":true}'
assert target.delete()

missing = target.parent / "missing.json"
assert missing.read_bytes() is None
assert not missing.delete()
assert list(missing.ls()) == []
```

Use `strict=True` where absence is an error. Operations attempt the backend
request first: a write creates its parent only after the backend reports it
missing, and a safe read or delete does not pay for a preceding metadata call.
`open` accepts only binary `rb`, `wb`, and `xb` modes.

## Iceberg FileIO

`ArrowFileIO` is PyIceberg's FileIO and uses the same `ArrowPath` operations.
An unbound instance is a file factory; `at` returns a bound owner.

```python
from rekep.arrow_file_io import ArrowFileIO

files = ArrowFileIO(
    {
        "s3.region": "eu-west-1",
        "s3.connect-timeout": "10.0",
        "rekep.io.cache-bytes": str(64 * 1024 * 1024),
    }
)

capture = files.at("s3://bucket/2026/08/31/app.log.gz")
with capture.open(seekable=False, compression="gzip") as stream:
    head = stream.read(64)

with capture.spill(temporary=True) as local:
    with local.open() as stream:
        replay = stream.read(64)
```

A temporary spill is uniquely owned and deleted on close. A persistent spill
uses a deterministic, size-validated local name. Copies are chunked; compressed
files stay compressed while moving.

The process-wide immutable-content cache covers Iceberg manifests, manifest
lists, and UUID-named metadata JSON. Set its budget inside the catalog:

```yaml
catalog:
  name: rekep
  properties:
    rekep.io.cache-bytes: "0"  # Disable this process's cache reads and writes.
```

One file larger than one eighth of the budget is not cached. Mutable
Hadoop-style names such as `v3.metadata.json` are never cached.

## S3 environment

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
export AWS_REGION=eu-west-1
```

At the first `rekep` import, missing `S3_ACCESS_KEY_ID`,
`S3_SECRET_ACCESS_KEY`, `S3_SESSION_TOKEN`, and `S3_REGION` values are filled
from their standard AWS counterparts. `AWS_DEFAULT_REGION` is the region
fallback; `AWS_ENDPOINT_URL_S3` and then `AWS_ENDPOINT_URL` fill
`S3_ENDPOINT_URL`.

An explicit catalog property wins over a location URL, which wins over the
environment. A later change to `AWS_*` does not replace the captured `S3_*`
values. Without explicit credentials, Arrow uses the normal AWS profile and
workload-role chain.
