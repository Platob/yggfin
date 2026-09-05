# Upstream prompt: Arrow filesystem parity

Implement a complete Arrow-compatible filesystem and bound-location contract in
yggdryl. Start from current yggdryl main and replace the seven-method foreign
filesystem approximation shipped as `0.1.1`; do not add another adapter beside
it.

This belongs in yggdryl because it owns filesystems, byte streams, `IOBase`, and
text media. Do not add a PyIceberg dependency or PyIceberg-shaped classes.
Downstream projects keep their Iceberg catalogs, table reads, commits, caches,
and `FileIO` adapters and build those over this public storage contract.

## One bound location

Keep one public handle for a resource: `IOBase`, backed by the existing Rust
`holder::fs::{Path, File, Folder}` implementation. A filesystem-backed handle
must retain three separate facts for its whole lifetime and for every parent,
child, listing, and glob result:

- the exact filesystem instance/equality domain;
- the opaque path passed to that filesystem;
- the caller's optional URI spelling and a credential-free diagnostic form.

Never derive the backend path by reparsing a bound path as a URL. In particular,
`%2F`, `%25`, `+`, repeated slashes, and non-ASCII text in a filesystem path are
literal object-name characters. They are not percent-decoded or normalized.
Only a dedicated URI constructor may split a URI into filesystem configuration
and path, and it must preserve escapes in the resulting object key.

Filesystem identity cannot be `type_name + path`. Two `SubTreeFileSystem`, S3
endpoint, credential scope, or custom handler instances can expose the same path
and different bytes. Provide an opaque, hashable bound-location identity and a
`same_location` operation based on filesystem equality plus the exact raw path.
Do not include credentials, session tokens, or secrets in that identity's text,
`str`, `repr`, errors, or logs. Keep the exact input URI available only through
an explicitly named property; expose a masked form for diagnostics.

`IOBase.from_fs(filesystem, path, ...)` always treats `path` as a path in the
injected filesystem, even when it contains `://`. Add a separate URI boundary
that resolves `file`, `s3`, `s3a`, and `s3n` locations once. It must correctly
separate endpoint, bucket, and key for at least these shapes:

```text
s3://bucket/key
s3a://bucket/key
s3://key:secret@bucket/key
s3://key:secret@minio:9000/bucket/key
s3://bucket/key?endpoint_override=minio%3A9000&scheme=http&region=eu-west-1
s3://bucket.s3.eu-west-1.amazonaws.com/key
```

An endpoint with a port is not a bucket. An endpoint override, transport,
region, anonymous mode, and virtual/path addressing must reach the configured
Arrow S3 filesystem. URI user information configures the filesystem and is not
part of the object path. Preserve a key such as `v=a%2Fb` exactly as that key;
this is required for escaped Iceberg partition paths even though Iceberg itself
is outside this change.

## Rust API

Make `holder::fs` a public, object-safe Arrow filesystem seam rather than a
whole-value storage approximation. Export its core declarations from the
documented public module. The final API must cover the Arrow operations below;
names may follow established Rust style, but every operation and option must be
represented without a lossy default:

```rust
pub trait FileSystem: Send + Sync {
    fn type_name(&self) -> &str;
    fn equals(&self, other: &dyn FileSystem) -> bool;
    fn normalize_path(&self, path: &str) -> Result<String>;
    fn file_info(&self, path: &str) -> Result<FileInfo>;
    fn list(&self, selector: &FileSelector) -> FileInfos;

    fn create_dir(&self, path: &str, recursive: bool) -> Result<()>;
    fn delete_dir(&self, path: &str) -> Result<()>;
    fn delete_dir_contents(&self, path: &str, missing_dir_ok: bool) -> Result<()>;
    fn delete_root_dir_contents(&self) -> Result<()>;
    fn delete_file(&self, path: &str) -> Result<()>;
    fn copy_file(&self, source: &str, target: &str) -> Result<()>;
    fn move_file(&self, source: &str, target: &str) -> Result<()>;

    fn open_input_file(&self, path: &str) -> Result<Box<dyn RandomAccessReader>>;
    fn open_input_stream(&self, path: &str) -> Result<Box<dyn ByteReader>>;
    fn open_output_stream(
        &self,
        path: &str,
        metadata: Option<&OutputMetadata>,
    ) -> Result<Box<dyn ByteWriter>>;
    fn open_append_stream(
        &self,
        path: &str,
        metadata: Option<&OutputMetadata>,
    ) -> Result<Box<dyn ByteWriter>>;
}
```

Define `RandomAccessReader`, `ByteReader`, and `ByteWriter` with the standard
read/read-at/seek/tell/write/flush/close capabilities their names imply. Do not
implement `open_*` by repeatedly calling `read_range`, or output by staging the
whole object in a `Vec`. If a backend genuinely lacks append, atomic exclusive
create, root deletion, or another optional capability, return a typed
`Unsupported` error; do not silently emulate different semantics.

`FileSelector` carries `base_dir`, `recursive`, and `allow_not_found`.
`FileInfo` carries `path`, `kind`, optional `size`, and optional UTC modification
time with nanosecond precision. File, directory, and not-found are distinct;
zero bytes or a missing mtime must not stand in for a missing object. Preserve
mtime when adapting a foreign filesystem.

Extend `MemoryFileSystem` and `LocalFileSystem` as reference implementations of
the complete trait. Thin foreign adapters and all `IOBase` byte and media code
must use the same trait; there must not be a second copy/move/delete/list or
stream implementation in a language binding.

## Python API

Accept every `pyarrow.fs.FileSystem`, including `LocalFileSystem`,
`MockFileSystem`, `SubTreeFileSystem`, and `PyFileSystem`. Make the following
surface public and fully typed in the package stubs:

```text
IOBase.from_fs(
    filesystem: pyarrow.fs.FileSystem,
    path: str | os.PathLike[str],
    *,
    uri: str | os.PathLike[str] | None = None,
) -> IOBase
IOBase.from_uri(uri: str | os.PathLike[str], *, options: Mapping[str, object] | None = None) -> IOBase

handle.filesystem -> pyarrow.fs.FileSystem | None
handle.path -> str | None
handle.uri -> str | None          # exact caller spelling
handle.masked_uri -> str | None   # safe for repr/logging
handle.info() -> pyarrow.fs.FileInfo
handle.same_location(other: IOBase) -> bool

handle.open_input_file() -> pyarrow.NativeFile
handle.open_input_stream(compression="detect", buffer_size=None) -> pyarrow.NativeFile
handle.open_output_stream(
    compression="detect", buffer_size=None, metadata=None
) -> pyarrow.NativeFile
handle.open_append_stream(
    compression="detect", buffer_size=None, metadata=None
) -> pyarrow.NativeFile
handle.copy_into(target: IOBase) -> int
handle.move_into(target: IOBase) -> IOBase
```

Also expose the bound directory and file lifecycle operations corresponding to
the Rust trait. Keep the existing convenient `IOBase` names where they already
express the operation, but make their behavior unambiguous. Removing an empty
directory removes the directory itself; recursive removal removes descendants
and the selected root; clearing a directory leaves the root; deleting a file
never reports success after receiving a directory. Root-content deletion must
be an explicit method and must not be reachable accidentally from a broad or
empty path.

For a PyArrow-backed handle, return the original filesystem object and delegate
to its methods. Use `FileSystem.equals` for equality when available. Forward
`compression`, `buffer_size`, and output `metadata` exactly. Same-filesystem
copy and move call `copy_file` and `move` once. Cross-filesystem copy streams in
bounded chunks and publishes only a complete target; a missing or failed source
must neither truncate nor create the target. Never turn a failed copy into an
empty file.

Make `IOCursor` a real Python binary file object: context management, idempotent
`close`, `closed`, `readable`, `writable`, `seekable`, `readinto`, `read`,
`seek`, `tell`, `write`, and `flush` must satisfy `io.RawIOBase`/`BufferedIOBase`
expectations. A native yggdryl filesystem may therefore be exposed to PyArrow
through `pyarrow.PythonFile` without materializing its contents.

Do not catch a general `OSError` or arbitrary Python exception and answer
`unknown`, `False`, or empty. Preserve `FileNotFoundError`, `PermissionError`,
`FileExistsError`, `NotADirectoryError`, `IsADirectoryError`, directory-not-empty,
and unsupported-operation distinctions across the Rust/Python boundary.
`get_file_info` may represent absence as Arrow `NotFound`; strict opens and
mutations must raise the typed error. Existing high-level methods that
intentionally treat absence as empty may do so only by handling typed
not-found, never by suppressing permission or transport failures.

## JavaScript API

Expand the existing camel-case `FileSystemHandler` and generated TypeScript
declarations to the same contract. Arrow JS has no filesystem implementation,
so publish the protocols rather than adding a backend-specific dependency:

```ts
interface FileSystemHandler {
  readonly typeName: string
  equals(other: FileSystemHandler): boolean
  normalizePath(path: string): string
  fileInfo(path: string): ArrowFileInfo
  list(selector: FileSelector): Iterable<ArrowFileInfo>
  createDir(path: string, recursive: boolean): void
  deleteDir(path: string): void
  deleteDirContents(path: string, missingDirOk: boolean): void
  deleteRootDirContents(): void
  deleteFile(path: string): void
  copyFile(source: string, target: string): void
  move(source: string, target: string): void
  openInputFile(path: string): RandomAccessReader
  openInputStream(path: string): ByteReader
  openOutputStream(path: string, metadata?: OutputMetadata): ByteWriter
  openAppendStream(path: string, metadata?: OutputMetadata): ByteWriter
}
```

Use `bigint` for sizes, offsets, and nanosecond mtimes. Define the reader and
writer protocols and typed error codes in TypeScript, and expose the same bound
facts and operations as `IOBase.fromFs`, `IOBase.fromUri`, `filesystem`, `path`,
`uri`, `maskedUri`, `info`, `sameLocation`, the four `open*` methods,
`copyInto`, and `moveInto`. Streams must have explicit close/dispose behavior.
Keep the adapter synchronous if that is the binding's established contract;
do not disguise a collected whole object as a stream.

## Operation semantics

Match Arrow's observable behavior at the filesystem seam:

- `normalize_path` is filesystem-owned and never applied implicitly to an
  injected opaque path.
- `file_info` returns not-found only for absence; all other failures propagate.
- a selector with `allow_not_found=false` fails for a missing base directory;
  with `true` it lists empty.
- listings are deterministic in ascending path order. Native recursive walks
  remain bounded by one directory and foreign eager listings are sorted once.
  Iteration is fused after the first error and never suppresses that error.
- glob and recursive glob retain the bound filesystem, raw path, and URI facts,
  honor `include_private`, use the longest fixed prefix, and produce the same
  deterministic order as listing.
- same-filesystem copy and move use backend-native operations. Cross-filesystem
  transfer uses one input and one output stream with bounded memory.
- file creation and mutation preserve the backend's Arrow semantics; they do
  not invent parent directories or overwrite/exclusive guarantees the backend
  did not provide.
- `delete_file`, `delete_dir`, `delete_dir_contents`, and
  `delete_root_dir_contents` are distinct. A non-recursive removal of a
  non-empty directory reports directory-not-empty.
- input-file handles are random access; input streams are sequential; output
  and append streams forward writes as they arrive. Closing flushes exactly
  once, and a close/write failure remains visible.

Do not probe metadata before every operation. Let the attempted open, copy,
move, or delete decide and translate its typed result. This avoids races and
removes the current per-chunk `get_file_info`/`open_input_file` churn.

## Required tests

Add shared conformance tests and run them against Rust memory and local
implementations plus this Python matrix:

```text
pyarrow.fs.LocalFileSystem
pyarrow.fs._MockFileSystem
pyarrow.fs.SubTreeFileSystem
pyarrow.fs.PyFileSystem(FileSystemHandler implementation)
S3-shaped URI resolution without a network dependency
```

Add the equivalent map/local/custom-handler coverage in JavaScript. At minimum,
pin these regressions:

- `from_fs(mock, "bucket/v=a%2Fb.bin", uri="s3://bucket/v=a%2Fb.bin")`
  writes and reads the literal `%2F` key;
- URI resolution preserves that key and correctly resolves every S3 form above;
- two subtree/custom filesystems serving the same raw path do not share
  identity, while equal filesystems and the same path do;
- credentials never appear in `repr`, thrown messages, or snapshots;
- reverse-ordered handler listings emerge in ascending path order;
- permission and transport failures are not reported as absence;
- file info preserves exact size and optional mtime through Rust, Python, and
  JavaScript;
- output metadata reaches the foreign output stream;
- empty-directory removal removes its root, recursive removal removes its root,
  and clear/delete-contents keeps its root;
- file deletion refuses a directory and non-recursive directory deletion
  refuses a non-empty directory;
- missing-source copy leaves an existing target unchanged and does not create a
  missing target; a mid-stream failure leaves no published partial result;
- native same-filesystem copy and move make exactly one backend call and perform
  no client-side read/write;
- random and sequential input, output, append, context exit, repeated close,
  and close-after-error obey their stream contracts;
- parents, joins, listings, and globs retain the identical filesystem and raw
  path semantics on all five Python filesystem shapes.

Do not limit tests to returned bytes. Assert backend call counts, arguments,
created/deleted names, stream closure, metadata, and final stored objects.

## Performance gates

Turn the architectural constraints into hard, instrumented gates:

- streaming ten bytes in three-byte batches performs at most one metadata
  lookup and exactly one input open, not one stat/open pair per chunk;
- sequential read/write and cross-filesystem copy keep peak retained payload
  memory proportional to the configured chunk (no whole-object buffer);
- same-filesystem copy/move performs one native operation and zero byte-stream
  operations;
- native local/memory recursive listing materializes at most one directory at a
  time before yielding;
- a bounded benchmark over at least 64 MiB compares yggdryl with direct
  PyArrow/local reads, writes, and copies at the same chunk size. Report median
  throughput and fail the benchmark gate if the wrapper is more than 25% slower
  after warm-up.

Keep the benchmark focused and deterministic. The call-count and bounded-memory
checks belong in normal tests; do not rely on timing alone to prove streaming.

## Documentation and release

Update Rust docs, Python stubs/API docs, TypeScript declarations, and one
example-first filesystem guide. Show the safe injected-filesystem form first,
then URI resolution, literal escaped keys, stream lifetime, error semantics,
metadata, copy/move, directory deletion, and S3 endpoint configuration. State
that yggdryl supplies storage and text media while downstream libraries own
Iceberg commits and catalogs.

Delete the old claims that whole-value replacement is the contract every Arrow
filesystem supports, and delete compatibility code that stages foreign writes
in memory or infers absence by probing after an exception. Keep one
implementation per behavior.

Bump every published Rust, Python, and Node package to the same version greater
than `0.1.1` (use `0.2.0` if the expanded handler trait is breaking), update
lockfiles and changelog/release notes, and run the repository's complete Rust,
Python, JavaScript, formatting, lint, type-check, documentation, and benchmark
gates. The work is complete only when every public operation above is backed by
the real filesystem capability, all matrix tests pass, no binding contains a
second storage implementation, and no manifest adds PyIceberg.
