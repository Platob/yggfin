# Iceberg

`IcebergDataset` exposes a table as an Arrow stream. Pyiceberg owns planning,
schema ids, snapshots, and commits; rekep supplies recursive casting,
filesystem normalization, commit grouping, and maintenance policy.

```python
from rekep import FixMsg
from rekep.iceberg import IcebergDataset

logs = IcebergDataset(
    field=FixMsg.into_field("fix.market"),
    catalog="local",
    properties={
        "type": "sql",
        "uri": "sqlite:///catalog.db",
        "warehouse": "file://warehouse",
    },
    branch="root",
)
```

The field name is the full table identifier; `logs.namespace` is `"fix"`.

## Read

```python
reader = logs.read_arrow_reader(
    row_filter="msgtype = 'D'",
    columns=["unix", "hash", "msgseqnum", "msgtype"],
    order_by=["unix", "msgseqnum", "hash"],
    snapshot_id=None,
    branch="root",
)
print(reader.schema.names)


def rows():
    """One fresh reader per call: a stream is consumed by whoever reads it."""
    return logs.read_arrow_reader(columns=["unix", "hash", "msgseqnum", "msgtype"])
```

```text
['unix', 'hash', 'msgseqnum', 'msgtype']
```

Every `order_by` column must be projected — sorting on one the reader will not
hand back is refused rather than dropped, so `MsgSeqNum` is in `columns` here.

Filters and projections reach scan planning. Ordered reads sort planned
partition paths deterministically and stream one partition at a time. A file
whose recorded sort order starts with the requested keys joins the direct
merge; any other file is externally sorted through bounded local IPC runs and
a multi-pass merge. This preserves event order without materializing the
table. Unordered reads remain the store's native stream.

A table that was never written reads as no rows under the shape asked for,
rather than raising: on the first interval of a fresh catalog every stage
reads an upstream its own upstream has not created yet. With neither a schema
nor a declared shape there is nothing to answer with, and the read raises.

## Write

```python
# A reader is consumed by the write it is handed to, so each takes its own.
logs.overwrite_arrow_reader(rows(), merge_by=True, commit_row_size=250_000)
logs.append_arrow_reader(rows(), merge_by=True, commit_row_size=250_000)
```

`overwrite_*` replaces the rows whose keys match and inserts the rest. A false
`merge_by` on a table partitioned by `identity`, `day`, `hour`, `month`,
`year`, `bucket`, or `truncate` makes the streamed rows the complete
replacement for their partitions; it remains invalid on an unpartitioned
table.

`append_*` inserts, skipping stored keys when `merge_by` names them and
inserting everything when it does not.

Appending to a missing table creates it. `merge_by=True` skips keys already
stored; write/upsert replaces them. Input batches accumulate to the requested
commit size. Schema additions are nullable and additive.

Keyed writes scope primary and explicit merge keys by the table's transformed
partition identity. Primary-key values are therefore unique within a partition;
the same value in another partition is another row. The raw partition source is
not row equality: two timestamps in the same `day(timestamp)` partition still
name one key.

Each bounded input chunk is split by transformed partition, and merge or
insert plans, reads, and commits one touched partition at a time. Deletes use
the matched stored row's source values, so their exact predicate cannot reach
the same key in another partition.

The default physical sort order is exactly the declared sort keys. Partition
transforms decide file placement independently and add no implicit sort
column.

A complete partition replacement applies the table's transforms with Arrow,
stages bounded local Parquet files, then copies them through the table's
configured filesystem to final Iceberg data locations.

One transaction drops only data files in the exact transformed partitions and
registers each new file from its footer; order-preserving transforms use
PyIceberg `add_files`, while bucket partitions carry the already-computed hash
into the same snapshot. Parquet bytes are never collected into an Arrow table
for the commit.

Local stages are removed after each upload, and cleanup attempts every
uploaded path even if one removal fails. A failure whose catalog
acknowledgement is ambiguous leaves the files for orphan maintenance rather
than risk deleting committed data.

The source must keep each partition contiguous, and a recurrence after another
partition is rejected. Writes are incremental: complete groups committed
before a later source or ordering error remain committed.

Replacements accumulate toward `commit_row_size`; the default is 1,000,000
rows and the value must be positive so every transaction remains bounded. The
same setting caps each staged Parquet file, so an individual partition may
exceed it without having to fit in memory.

Renaming, retyping or narrowing a column is not an additive Iceberg
evolution and no merge migrates one: recreate or rewrite the table, on every
retained branch, before appending. Dataset writes do not guess missing lineage
or keep dropped columns alive.

Every data verb accepts `branch`; reads also accept `snapshot_id`. `root`,
`main`, and `master` are aliases for Iceberg's physical `main` ref, so task
configuration does not depend on an organization's default-branch spelling.
A producer that does not provide Iceberg ids is numbered at creation, while a
contract that already carries ids keeps them.

### Arrow widths on read

A read hands back `string` and `binary`, not `large_string` and `large_binary`:

```python
from rekep import FixMsg
from rekep.iceberg import IcebergDataset

logs = IcebergDataset(
    field=FixMsg.into_field("fix.market"),
    catalog="local",
    properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file://warehouse"},
)
print(logs.read_arrow_reader().schema.field("msgtype").type)
```

```text
string
```

PyIceberg's configuration page documents `pyarrow.use-large-types-on-read`,
defaulting to true. **0.11.1 reads it nowhere** — no constant, no `Config()`
lookup, and setting `PYICEBERG_PYARROW__USE_LARGE_TYPES_ON_READ` moves
nothing. `schema_to_pyarrow` returns `pa.large_string()` outright, so a table
written from `string` columns read back wider than it was written and every
join against a value this package built itself had two widths to reconcile.

So the width is decided here instead, at the seam where the scan's shape is
built. That also keeps the reading stable across a PyIceberg that changes its
mind, since the dependency names no upper bound. Measured over 400,000 rows,
interleaved in one process, the cast does not show above host noise.

Nothing downstream depends on it either way: the transcription brings a batch
onto the raw declaration before its kernels see it, so a wide batch and a
narrow one meet the same code.

### Table properties

The tasks declare three, and every write task declares the same three:

```yaml
table_properties:
  history.expire.max-snapshot-age-ms: "604800000"   # a week of time travel
  write.metadata.previous-versions-max: "20"
  write.metadata.delete-after-commit.enabled: "true"
```

The last two bound the metadata directory. A commit writes a new
`*.metadata.json` and keeps every earlier one, so a pipeline committing every
`commit_row_size` rows grows that directory for as long as it runs. Over 40
appends to one table:

| declared | `*.metadata.json` | `*.avro` |
| --- | ---: | ---: |
| neither | 41 | 80 |
| both | 21 | 80 |

`optimize` retrofits the same two values onto a table that lacks them, so this
is not a second mechanism -- it is the same one, applied from the commit that
creates the table rather than from the first maintenance pass to reach it. A
table that already carries them costs `optimize` no commit to set.

Four more are worth knowing about and are deliberately **not** declared:

| property | why not |
| --- | --- |
| `commit.manifest-merge.enabled` | measured *worse* at write time -- 84 manifests against 80 -- because a merge writes new ones and the originals stay until expiry. `optimize` sets it for the pass that also compacts and expires, which is where it pays. |
| `write.parquet.compression-codec` | PyIceberg already defaults to `zstd`. |
| `downcast-ns-timestamp-to-us-on-write` | every temporal column in these contracts is already `timestamp[us, tz=UTC]`, so nothing is downcast. |
| `pyarrow.use-large-types-on-read` | 0.11.1 reads it nowhere, so neither value changes anything. The width is decided in this package instead -- see [Arrow widths on read](#arrow-widths-on-read). |

`write.object-storage.enabled` inserts a hashed prefix before each data file
(`data/0010/0011/1010/10110100/…`), which spreads writes across S3 prefixes.
The tables partition hourly and commit far below S3's per-prefix request
ceiling, so it is off; turning it on changes every future data path and does
not move existing files.

## Filesystems

Local and object-store locations resolve through `pyarrow.fs`. Credentials,
endpoint, bucket, and path are parsed once by `Url`; explicit catalog
properties win. Hadoop-style `s3a://` and legacy `s3n://` locations use the
same Arrow S3 filesystem as `s3://`.

`ArrowPath` keeps that parsed URL and filesystem together:

```python
from rekep import ArrowPath

capture = ArrowPath("data/capture/app.log").resolve(".")
print(capture.name, capture.parent, capture.exists())
with capture.open("rb") as source:
    head = source.read(64)

for entry in capture.parent.ls():
    print(entry)

optional = capture.parent / "optional.log"
assert optional.read_bytes() is None
assert not optional.delete()

# Required inputs keep a missing-path error explicit.
required = optional.read_bytes(strict=True)
```

Joining with `/`, globbing, byte reads and writes, and input/output streams all
reuse the same filesystem. Listings and default byte reads/deletes treat a
missing path as empty; `strict=True` is for required data. A write tries the
target first and creates its parent only when that backend requires one. A
bound `ArrowFileIO` holds this one path instead of parallel URL, filesystem,
and opened-file state.

Maintenance lists through the table's own FileIO rather than resolving the
location again, because a location this package canonicalized has had its
endpoint and credentials taken out of it -- resolved afresh, a sweep of a MinIO
warehouse would look for the bucket on AWS. Recorded locations are then reduced
against the table's data and metadata directories; one that no spelling reduces
is matched by base name instead, which is weaker in the safe direction -- every
name Iceberg mints carries a UUID, so a false match leaves a file behind rather
than deleting a live one.

An object key keeps the escapes the location spells. Iceberg writes a partition
value as `v=a%2Fb` so the value's own slash does not become a directory, and
that escape is the key: decoded, it would name an object no manifest recorded.

### What an `s3://` netloc names

`s3://bucket/key` is a bucket. `s3://store/bucket/key` is an S3-compatible
store addressed path-style. A netloc is the store when it carries a port, when
it is an IP address, when it is one of Amazon's own hostnames, or when its last
label is a public suffix -- a registered name somebody pointed at a machine.
Nothing else is: one label, a numeric last label, and the private-use suffixes
(`.internal`, `.local`, `.lan`, `.corp`, ...) stay bucket names, which a bucket
with dots in it needs.

```python
from rekep.urls import Url

for text in (
    "s3://bucket/logs/a.txt",                    # a bucket
    "s3://minio:9000/bucket/logs/a.txt",         # a store, by its port
    "s3://s3.eu.cloud.ovh.net/bucket/logs/a.txt",  # a store, by its name
    "s3://bucket.s3.eu-west-1.amazonaws.com/logs/a.txt",  # AWS, virtual-hosted
    "s3://my.logs.2026/logs/a.txt",              # a bucket, dots and all
):
    url = Url.from_string(text)
    print(f"{url.bucket:<14} {url.key:<12} {url.endpoint}")
```

```text
bucket         logs/a.txt   None
bucket         logs/a.txt   minio:9000
bucket         logs/a.txt   s3.eu.cloud.ovh.net
bucket         logs/a.txt   s3.eu-west-1.amazonaws.com
my.logs.2026   logs/a.txt   None
```

Two spellings the shape cannot decide say so with `?endpoint_override=`, which
is a decision and beats a shape:

- a bucket really named for a domain -- the S3 static-website pattern,
  `s3://www.example.com/index.html?endpoint_override=s3.amazonaws.com`;
- a bucket addressed virtual-hosted style on a store that is not Amazon's,
  `s3://bucket/key?endpoint_override=s3.example.net&force_virtual_addressing=true`,
  because only AWS publishes which of its leading labels is a bucket -- and an
  overridden endpoint is addressed path-style unless the location says
  otherwise.

Everything derived from the location follows that reading: the Arrow
`S3FileSystem`, the `s3.endpoint`, `s3.access-key-id`, `s3.secret-access-key`
and `s3.region` a catalog is configured with, the FileIO cache identity, and
the spill identity.

A location this package writes into table metadata is canonical -- scheme,
bucket and key, with the endpoint and credentials moved onto the catalog. One
that is *not* -- written by another tool, or before those settings moved --
still reaches the store it names: the FileIO builds a filesystem from the
location, and the catalog fills what the location leaves unsaid.

### Settings a location carries

| query key | Arrow argument | catalog property |
| --- | --- | --- |
| `region` | `region` | `s3.region` |
| `scheme` | `scheme` | part of `s3.endpoint` |
| `endpoint_override` | `endpoint_override` | `s3.endpoint` |
| `force_virtual_addressing` | `force_virtual_addressing` | `s3.force-virtual-addressing` |
| `anonymous` | `anonymous` | `s3.anonymous` |
| `allow_bucket_creation` | `allow_bucket_creation` | -- |

Beyond what a location spells, these PyIceberg catalog properties reach Arrow
through this FileIO. A production Glue catalog:

```yaml
catalog: rekep-production
catalog_properties:
  type: glue
  warehouse: s3://example-bucket/rekep/warehouse
  glue.region: eu-west-1
  s3.region: eu-west-1
  s3.connect-timeout: "10.0"      # seconds
  s3.request-timeout: "60.0"      # seconds
  s3.role-arn: arn:aws:iam::123456789012:role/rekep-writer
  s3.role-session-name: rekep
# A custom KMS key belongs in the bucket default. Do not add
# `s3.sse.type: kms` or `s3.sse.key` here: Arrow cannot send their headers.
```

Verify what any of them become:

```python
from rekep.arrow_file_io import ArrowFileIO

filesystem = ArrowFileIO({"s3.region": "eu-west-1", "s3.connect-timeout": "12.5"})
print(filesystem.fs_by_scheme("s3", "bucket").__reduce__()[1][0]["connect_timeout"])
```

```text
12.5
```

`s3.connect-timeout`, `s3.request-timeout`, `s3.role-arn`,
`s3.role-session-name`, `s3.proxy-uri`, `s3.retry-strategy-impl` and
`s3.resolve-region` all arrive. `s3.profile-name` and the `s3.signer.*` names
do **not**: PyIceberg's Arrow FileIO never reads them, so a catalog that names
a profile is signed by whatever the credential chain found instead. Name the
credentials, a role, or the environment.

Two catalog properties are this package's own: `rekep.io.cache-bytes` sizes the
immutable-content cache below, and `rekep.io.delegate-file-io` names a FileIO
to wrap when `py-io-impl` is not this one, so a failed commit still owns every
output it created. Iceberg's `s3.sse.*` are refused rather than ignored -- see
[Encryption at rest](#encryption-at-rest).

```python
from pprint import pprint

from rekep.urls import Url

warehouse = "s3://key:secret@minio.example.net/rekep/warehouse?force_virtual_addressing=true"
pprint(dict(Url.from_string(warehouse).into_properties()), sort_dicts=False, width=74)
```

```text
{'s3.endpoint': 'https://minio.example.net',
 's3.access-key-id': 'key',
 's3.secret-access-key': 'secret',
 's3.region': 'us-east-1',
 's3.force-virtual-addressing': 'true'}
```

A secret is read from the userinfo and percent-decoded, so one containing
`:`, `/` or `@` has to arrive percent-encoded -- `wJalrXUtnFEMI%2FK7MDENG` --
and an unencoded `/` is refused with the location's secret taken out of the
message.

The region is resolved in order: `?region=`, then a region label inside
`?endpoint_override=`'s hostname, then one in the netloc. So
`s3://bucket.s3.eu-west-1.amazonaws.com/key` yields `s3.region: eu-west-1` and
no `s3.endpoint` -- overriding AWS with AWS only forces path-style addressing,
while the region is the half of that hostname that has to travel.

A flag is on only where it is spelled `true`, so a typo reads as off, on the
catalog path as well as Arrow's. The two that decide whether a store answers at
all are the addressing style -- Arrow addresses an overridden endpoint
path-style, which is what MinIO and Ceph want and what a store serving only
`bucket.endpoint` refuses -- and `anonymous`, so a public bucket is read as
nobody instead of as whatever the credential chain found.

`endpoint_override` may carry its own transport (`http://minio:9000`) or not
(`minio:9000`); either way it reaches Arrow as a connect string and the catalog
as one URL. Without a transport, a hostname with no dot -- a container or a
laptop -- is read as plaintext and anything else as TLS; `scheme` says it
outright.

A location naming an endpoint and no region is signed for `us-east-1`, which
is what Arrow and every S3-compatible store default to. Name `region` wherever
the store signs with a real one -- Wasabi and Backblaze do. The default is not
a convenience: PyIceberg with no region asks *AWS* which region hosts a bucket
of that name, which blocks on the first touch of each bucket, discloses the
name, and answers for a stranger's bucket when a real AWS bucket happens to
share it -- signing every request to your store for that bucket's region.

PyIceberg is configured with the package-level
`rekep.arrow_file_io.ArrowFileIO` implementation.

### Encryption at rest

Turn it on at the **bucket**. A bucket whose default encryption is SSE-S3 or
SSE-KMS encrypts every object this package writes -- Parquet data, manifests,
manifest lists and metadata JSON alike -- and decrypts every one it reads, with
no configuration here and no change to a read or a write.

```bash
aws s3api put-bucket-encryption --bucket rekep-warehouse \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":
    {"SSEAlgorithm":"aws:kms","KMSMasterKeyID":"arn:aws:kms:eu-west-1:111122223333:key/…"}}]}'
```

`KMSMasterKeyID` is the custom-key setting. Do not translate it into
`s3.sse.type: kms` plus `s3.sse.key` in `catalog_properties`: those Iceberg
names ask for per-request headers that this FileIO cannot send.

Per-*request* encryption is not available. `pyarrow.fs.S3FileSystem` has no
parameter for it and drops an `x-amz-server-side-encryption` handed to
`default_metadata`; PyIceberg reads none of Iceberg's `s3.sse.*` names in
either of its FileIOs. So SSE-C -- a customer-provided key, which must travel
on every GET as well as every PUT -- cannot be used at all, and a bucket policy
that *denies* a PUT without the header refuses every write.

A catalog that names `s3.sse.type`, `s3.sse.key` or `s3.sse.md5` is refused
rather than ignored: the setting says those objects must be encrypted, and a
layer that quietly drops it writes them in the clear and reports success.
`s3.sse.type: none` is the one value honoured, because it asks for nothing.

Where per-request encryption is a requirement, `rekep.io.delegate-file-io`
names a FileIO to use in place of this one; it is wrapped for output ownership
and everything else on this page still applies.

### Immutable content cache

`ArrowFileIO` serves the files Iceberg promises never to rewrite -- `.avro`
manifests and manifest lists, and a `metadata.json` whose name carries a UUID --
out of memory, so a plan that reads the same manifest twice pays one GET. A
Hadoop-style `v3.metadata.json` is never cached: two racing writers can both
produce that name with different bytes.

```yaml
catalog_properties:
  rekep.io.cache-bytes: "0"        # opt this process out
```

The budget is process-wide, not per catalog, because the files are shared too:
setting `rekep.io.cache-bytes` on one catalog resizes the cache for every
catalog in the process. It defaults to 64 MiB, and one file larger than an
eighth of the budget is never stored. Entries are keyed by the store serving
them -- scheme, endpoint, access key, region, bucket and key -- so two stores
carrying one path are never confused, and deleting a file evicts it under that
same key.

Partition staging uses that same instance to copy local Parquet files to the
final path produced by Iceberg's location provider. Local warehouses and
`s3`/`s3a` warehouses therefore share one bounded copy path and the catalog's
endpoint, region, and credential configuration.

`ArrowFileIO.spill(local=None, temporary=False)` returns a local `ArrowFileIO`;
an already-local bound input returns itself. Persistent spills
use a deterministic name, are pulled again when the remote byte size changes,
and never serve stale bytes after the remote disappears. Temporary spills are
uniquely owned and deleted on `close`, after their open stream is closed.

`TextFile` holds that owner directly, and uses it only where a caller opts in
with `spill=True`: a compressed remote log is then copied as raw compressed
bytes in 4 MiB chunks, decoded incrementally by Arrow, and purged when the
reader finishes, so its disk use is the compressed object size. `spill=False`,
the default, leaves Arrow decoding the object-store stream directly and writes
nothing. Either way memory stays bounded by the copy/read chunk and one parsed
record batch; the expanded capture is never materialized as a file or Arrow
table.

A log is written by appending, which an object store cannot do: `append_arrow_*`
works on a local path and is refused on S3 and GCS, which have only a
whole-object put. `overwrite_arrow_*` is refused everywhere -- a log is a
sequence of lines with no key to replace one by. Write locally and upload, or
write to a dataset that owns its own files.

Exhausting the reader closes that owner. A partial consumer closes the
surrounding `TextFile` context to release the decoder and temporary spill.

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
export AWS_REGION=eu-west-1
```

At the first `rekep` import, these standard AWS values are copied into empty
or absent `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_SESSION_TOKEN`, and
`S3_REGION` variables. `AWS_DEFAULT_REGION` is the region fallback.
`AWS_ENDPOINT_URL_S3` and then `AWS_ENDPOINT_URL` similarly fill
`S3_ENDPOINT_URL`.

The resulting `S3_*` values become Iceberg defaults and configure direct Arrow
access. An explicit `S3_*` value wins. The AWS-to-S3 copy runs once, so later
changes to `AWS_*` do not overwrite the process's `S3_*` values.

An explicit catalog property wins over a value in a location URL, which wins
over the environment. Arrow still uses the standard AWS profile and
workload-role credential chain when no environment credentials were captured.

## Maintenance

```python
import datetime

logs.compact(branch="root")
logs.cleanup(retain=24, older_than=datetime.timedelta(days=7))
logs.optimize(
    branch="root",
    retain=24,
    older_than=datetime.timedelta(days=7),
    orphan_age=datetime.timedelta(days=3),
)
```

After a successful outermost append, insert, merge, or overwrite, the writer
expires old snapshots once. `snapshot_expiry` accepts a `timedelta` relative to
now, a `datetime`, or a parseable instant string. When omitted, the dataset
reads `history.expire.max-snapshot-age-ms` and then Iceberg's default.

Writers do not compact or sweep files. Expiration removes history from the
current metadata JSON -- it permanently removes time travel and rollback to an
expired state, and PyIceberg 0.11 deletes no physical files during it.

`cleanup` follows that commit with rekep's reachability sweep. Only files
unreferenced by every retained snapshot, branch, tag, statistics entry and
current metadata version are eligible: obsolete Parquet, manifest-list Avro,
manifest Avro and untracked metadata JSON. A live file whose recorded spelling
the sweep cannot reduce against the table's directories is kept, never deleted.
`orphan_age` is a grace period for files an in-flight writer produced but never
committed.

`optimize` compacts, then cleans, and returns counts for each. It also
retrofits absent manifest-merge and metadata-retention properties; an explicit
table property still wins.

The shipped schedule and policy live in the
[Airflow deployment](../pipeline/operations/airflow.md#run-iceberg-maintenance)
guide. Run it when ordinary writers are quiet: commit conflicts fail safely and
retry, but avoiding overlap prevents wasted rewrites.

## Testing and benchmark

Long transaction suites are marked `integration`; normal pull-request tests
cover planning and pure helpers. `python/benchmarks/bench_iceberg.py --quick`
measures focused commits, scans, merges, and maintenance—not a full pipeline.
