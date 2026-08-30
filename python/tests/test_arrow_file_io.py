"""`ArrowFileIO`: location parsing on both hosts, spilling, and the content cache.

The Windows branch is data (`_WINDOWS`), so a POSIX runner can pin what a
Windows one would do and the other way round -- the whole point of the class
is behaviour CI's two legs do not share. The cache tests count real opens
below it, because "served from memory" is the whole claim.
"""

import gzip
import threading
from pathlib import Path

import pyarrow.fs
import pytest
from pyiceberg.io.pyarrow import PyArrowFile

import rekep.arrow_file_io as arrow_file_io
from rekep.arrow_file_io import (
    CONTENT_CACHE,
    DEFAULT_CACHE_BYTES,
    ArrowFileIO,
    CachedInputFile,
    ContentCache,
    canonical_location,
    inferred_properties,
    track_outputs,
)
from rekep.urls import S3


@pytest.fixture
def windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(arrow_file_io, "_WINDOWS", True)


@pytest.fixture
def posix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(arrow_file_io, "_WINDOWS", False)


def test_a_file_uri_with_a_drive_sheds_the_leading_slash(windows: None) -> None:
    assert ArrowFileIO.parse_location("file:///C:/warehouse/t") == ("file", "", "C:/warehouse/t")


def test_a_bare_drive_path_is_local_not_a_scheme(windows: None) -> None:
    assert ArrowFileIO.parse_location("C:/warehouse/t") == ("file", "", "C:/warehouse/t")
    # Both separators name one location, so both parse to one spelling of it --
    # which is what lets a swept path be compared against a recorded one.
    assert ArrowFileIO.parse_location("C:\\warehouse\\t") == ("file", "", "C:/warehouse/t")


def test_everything_without_a_drive_is_the_parents_answer(windows: None) -> None:
    assert ArrowFileIO.parse_location("file:///data/t") == ("file", "", "/data/t")
    assert ArrowFileIO.parse_location("file:/data/t") == ("file", "", "/data/t")
    assert ArrowFileIO.parse_location("s3://bucket/t") == ("s3", "bucket", "bucket/t")
    assert ArrowFileIO.parse_location("s3a://bucket/t") == ("s3a", "bucket", "bucket/t")


@pytest.mark.parametrize("scheme", sorted(S3))
def test_s3_locations_store_only_the_bucket_and_key(scheme: str) -> None:
    location = f"{scheme}://key:secret@bucket/table?endpoint_override=minio%3A9000&scheme=http"
    assert canonical_location(location) == f"{scheme}://bucket/table"


@pytest.mark.parametrize("scheme", sorted(S3))
def test_an_escaped_partition_value_names_the_object_iceberg_wrote(scheme: str) -> None:
    """Iceberg escapes a partition value into the path, and the escape is the key.

    `quote_plus(value, safe="")` writes `v=a%2Fb` so the value's own slash does
    not become a directory. Decoding it names `v=a/b/...` -- an object the
    manifest never recorded, which a read misses and the orphan sweep deletes
    the live file for.
    """
    location = f"{scheme}://bucket/wh/db/t/data/v=a%2Fb/x.parquet"
    key = "wh/db/t/data/v=a%2Fb/x.parquet"

    assert ArrowFileIO.parse_location(location) == (scheme, "bucket", f"bucket/{key}")
    assert canonical_location(location) == f"{scheme}://bucket/{key}"
    assert canonical_location(f"{scheme}://k:s@minio:9000/bucket/{key}") == (
        f"{scheme}://bucket/{key}"
    )


def test_a_posix_directory_named_like_a_drive_keeps_meaning_what_it_says(posix: None) -> None:
    assert ArrowFileIO.parse_location("file:///C:/warehouse/t") == ("file", "", "/C:/warehouse/t")


# -- an S3 endpoint, which is not a bucket ----------------------------------


def test_an_endpoint_keeps_the_bucket_below_it(posix: None) -> None:
    """The parent reads `minio` as the bucket and drops the port, silently."""
    assert ArrowFileIO.parse_location("s3://key:secret@minio:9000/wh/t") == (
        "s3",
        "wh",
        "wh/t",
    )


def test_a_location_that_names_no_endpoint_is_the_parents_answer(posix: None) -> None:
    """`s3://bucket/key` means one thing everywhere, so nothing is corrected."""
    assert ArrowFileIO.parse_location("s3://bucket/t") == ("s3", "bucket", "bucket/t")
    assert ArrowFileIO.parse_location("s3://key:secret@bucket/t") == ("s3", "bucket", "bucket/t")


def test_an_endpoint_hostname_keeps_the_bucket_below_it_too(posix: None) -> None:
    """The endpoint most warehouses actually name carries no port at all.

    `s3.eu-west-1.amazonaws.com` answers on 443, so a port-only reading takes
    the whole hostname for the bucket -- and every location under the
    warehouse is then addressed in a bucket nobody created.
    """
    assert ArrowFileIO.parse_location("s3://s3.eu-west-1.amazonaws.com/wh/t") == (
        "s3",
        "wh",
        "wh/t",
    )
    assert ArrowFileIO.parse_location("s3://wh.s3.eu-west-1.amazonaws.com/t") == (
        "s3",
        "wh",
        "wh/t",
    )


def test_a_warehouse_url_on_a_hosted_store_configures_it_from_the_hostname() -> None:
    """A MinIO behind a certificate says where it is without saying a port."""
    assert inferred_properties({"warehouse": "s3://key:secret@minio.corp.com/wh"}) == {
        "warehouse": "s3://wh/",
        "s3.endpoint": "https://minio.corp.com",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "secret",
        "s3.region": "us-east-1",
    }


@pytest.mark.parametrize("scheme", sorted(S3))
def test_a_warehouse_url_configures_the_filesystem_it_names(scheme: str) -> None:
    """Said once as a location, rather than again as three settings."""
    location = f"{scheme}://key:sec:ret@minio:9000/wh"
    assert inferred_properties({"warehouse": location}) == {
        "warehouse": f"{scheme}://wh/",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "sec:ret",
        # A store reached by an endpoint is signed for the region every
        # compatible one defaults to, rather than the one AWS answers with
        # for a bucket that is not on it.
        "s3.region": "us-east-1",
    }


def test_s3_query_settings_leave_the_location_before_iceberg_appends_to_it() -> None:
    """A table path belongs before `?`; leaving it after made every file one key."""
    inferred = inferred_properties(
        {
            "warehouse": (
                "s3://key:sec%3Aret%2Fword%40x@bucket/wh"
                "?endpoint_override=127.0.0.1%3A19000&scheme=http&region=eu-west-1"
            ),
            "vendor.option": "kept",
        }
    )
    assert inferred == {
        "warehouse": "s3://bucket/wh",
        "s3.endpoint": "http://127.0.0.1:19000",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "sec:ret/word@x",
        "s3.region": "eu-west-1",
        "vendor.option": "kept",
    }


def test_what_the_caller_set_wins_over_what_the_location_says() -> None:
    """An explicit property is a decision; a URL is a default."""
    inferred = inferred_properties(
        {
            "warehouse": "s3://key:secret@minio:9000/wh",
            "s3.access-key-id": "other",
            "s3.endpoint": "https://chosen.example.com",
        }
    )
    assert inferred["s3.access-key-id"] == "other"
    assert inferred["s3.endpoint"] == "https://chosen.example.com"


def test_s3_process_defaults_are_below_locations_and_catalog_properties(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://environment:9000")
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "environment-key")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "environment-secret")
    monkeypatch.setenv("S3_SESSION_TOKEN", "environment-token")
    monkeypatch.setenv("S3_REGION", "us-east-1")
    inferred = inferred_properties(
        {
            "warehouse": "s3://url-key:url-secret@minio:9000/wh?region=eu-west-1",
            "s3.secret-access-key": "catalog-secret",
        }
    )
    assert inferred["s3.endpoint"] == "http://minio:9000"
    assert inferred["s3.access-key-id"] == "url-key"
    assert inferred["s3.secret-access-key"] == "catalog-secret"
    assert "s3.session-token" not in inferred
    assert inferred["s3.region"] == "eu-west-1"


def test_an_aws_location_suppresses_a_compatible_store_process_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_REGION", "us-east-1")
    location = "s3://logs.s3.eu-west-1.amazonaws.com/wh"
    assert inferred_properties({"warehouse": location}) == {
        "warehouse": "s3://logs/wh",
        "s3.region": "eu-west-1",
    }


def test_s3_process_defaults_exist_before_a_table_location_is_known(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_REGION", "eu-west-1")
    assert inferred_properties({"vendor.option": "kept"}) == {
        "s3.endpoint": "http://minio:9000",
        "s3.region": "eu-west-1",
        "vendor.option": "kept",
    }


def test_a_location_that_says_nothing_adds_nothing() -> None:
    for properties in (
        {"warehouse": "s3://bucket/wh"},
        {"warehouse": "/tmp/wh", "vendor.option": "kept"},
        {"vendor.option": "kept"},
        {},
    ):
        assert inferred_properties(properties) == properties


def test_the_filesystem_a_catalog_builds_reaches_that_endpoint() -> None:
    """Which is the point of inferring them: pyiceberg reads `s3.*`, not URLs."""
    io = ArrowFileIO({"warehouse": "s3://key:secret@minio:9000/wh"})
    filesystem = io.fs_by_scheme("s3", "wh")
    # pyiceberg passes `s3.endpoint` on with its scheme, which pyarrow reads;
    # settings are write-only there, so pickling is where they can be read back.
    settings = filesystem.__reduce__()[1][0]
    assert settings["endpoint_override"] == "http://minio:9000"
    assert settings["access_key"] == "key"


def test_a_location_that_names_its_own_store_reaches_that_store() -> None:
    """`parse_location` hands PyIceberg the bucket as the netloc, so an endpoint
    or credentials written into the location would be discarded and the file
    opened against a default AWS filesystem. A table another tool wrote records
    exactly such a location."""
    io = ArrowFileIO({})
    described = io.new_input("s3://key:secret@minio:9000/wh/db/t/metadata/x.avro")
    settings = described._inner._filesystem.__reduce__()[1][0]

    assert settings["endpoint_override"] == "http://minio:9000"
    assert settings["access_key"] == "key"
    # One filesystem per store, not one per file.
    again = io.new_input("s3://key:secret@minio:9000/wh/db/t/metadata/y.avro")
    assert again._inner._filesystem is described._inner._filesystem


def test_the_catalog_fills_what_such_a_location_leaves_unsaid() -> None:
    """It names the store; the credentials stay where a deployment put them."""
    io = ArrowFileIO({"s3.access-key-id": "ck", "s3.secret-access-key": "cs"})
    settings = io.new_input("s3://s3.example.net/wh/t.parquet")._filesystem.__reduce__()[1][0]

    assert settings["endpoint_override"] == "https://s3.example.net"
    assert settings["access_key"] == "ck"


def test_a_plain_bucket_location_still_takes_the_catalog_s_own_filesystem() -> None:
    """Nothing in it describes a store, so there is nothing to build from."""
    io = ArrowFileIO({"s3.endpoint": "http://minio:9000"})
    settings = io.new_input("s3://bucket/wh/t.parquet")._filesystem.__reduce__()[1][0]

    assert settings["endpoint_override"] == "http://minio:9000"


def test_an_encryption_this_cannot_send_is_refused_rather_than_ignored() -> None:
    """A catalog carrying `s3.sse.type` says its objects must be encrypted.

    Neither `pyarrow.fs.S3FileSystem` nor pyiceberg reads any of these names,
    so honouring the setting is impossible and ignoring it writes plaintext and
    reports success -- the one failure a reader of the table can never see.
    """
    for asked in (
        {"s3.sse.type": "kms", "s3.sse.key": "arn:aws:kms:eu-west-1:1:key/abc"},
        {"s3.sse.type": "s3"},
        {"s3.sse.type": "custom", "s3.sse.key": "base64key", "s3.sse.md5": "digest"},
        {"s3.sse.key": "base64key"},
    ):
        with pytest.raises(ValueError, match="server-side encryption"):
            inferred_properties({"warehouse": "s3://bucket/wh", **asked})
        with pytest.raises(ValueError, match="server-side encryption"):
            ArrowFileIO(asked)


def test_the_one_encryption_setting_that_asks_for_nothing_is_honoured() -> None:
    """`none` is satisfied by doing nothing, which is what this does."""
    assert inferred_properties({"s3.sse.type": "none"}) == {"s3.sse.type": "none"}
    assert ArrowFileIO({"s3.sse.type": "none"}).properties["s3.sse.type"] == "none"


# -- staged uploads ---------------------------------------------------------


def test_output_tracking_spans_file_io_instances_and_stops_with_its_context(
    tmp_path: Path,
) -> None:
    from pyiceberg.utils.concurrent import ExecutorFactory

    first = ArrowFileIO()
    second = ArrowFileIO()
    paths = [str(tmp_path / "first.avro"), str(tmp_path / "second.metadata.json")]
    workers = ExecutorFactory.get_or_create()._max_workers  # noqa: SLF001

    with track_outputs() as outputs:
        first.new_output(paths[0])
        ExecutorFactory.get_or_create().submit(second.new_output, paths[1]).result()

    first.new_output(str(tmp_path / "later.avro"))
    assert outputs == set(paths)
    assert ExecutorFactory.get_or_create()._max_workers == workers  # noqa: SLF001


def test_output_tracking_settles_delayed_worker_writes(tmp_path: Path) -> None:
    from pyiceberg.utils.concurrent import ExecutorFactory

    release = threading.Event()
    paths = [str(tmp_path / f"worker-{index}.parquet") for index in range(12)]

    def delayed(path: str) -> None:
        release.wait()
        ArrowFileIO().new_output(path)

    with track_outputs() as outputs:
        executor = ExecutorFactory.get_or_create()
        for path in paths:
            executor.submit(delayed, path)
        timer = threading.Timer(0.05, release.set)
        timer.start()
        outputs.settle()
        timer.join()

    assert outputs == set(paths)


def test_output_settlement_keeps_waiting_after_an_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from concurrent.futures import Future

    future: Future[None] = Future()
    result = future.result
    calls = 0

    def interrupted_once(timeout: float | None = None) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyboardInterrupt
        result(timeout)

    monkeypatch.setattr(future, "result", interrupted_once)
    with track_outputs() as outputs:
        outputs.watch(future)
        timer = threading.Timer(0.05, future.set_result, args=(None,))
        timer.start()
        outputs.settle()
        timer.join()

    assert calls == 2


@pytest.mark.parametrize("scheme", sorted(S3))
def test_a_local_stage_copies_to_the_configured_s3_path(tmp_path: Path, scheme: str) -> None:
    store = pyarrow.fs._MockFileSystem()
    store.create_dir("bucket/table/data", recursive=True)
    source = tmp_path / "stage.parquet"
    source.write_bytes(b"parquet bytes")
    requested: list[tuple[str, str]] = []
    io = ArrowFileIO()

    def filesystem(found_scheme: str, netloc: str) -> pyarrow.fs.FileSystem:
        requested.append((found_scheme, netloc))
        return store

    io.fs_by_scheme = filesystem
    target = f"{scheme}://bucket/table/data/part.parquet"

    assert io.copy_from_local(source, target) == target
    with store.open_input_file("bucket/table/data/part.parquet") as copied:
        assert copied.read() == b"parquet bytes"
    assert requested == [(scheme, "bucket")]


def test_a_failed_local_stage_copy_removes_the_partial_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = pyarrow.fs._MockFileSystem()
    store.create_dir("bucket/data", recursive=True)
    source = tmp_path / "stage.parquet"
    source.write_bytes(b"complete")
    io = ArrowFileIO()
    io.fs_by_scheme = lambda *_: store
    original = pyarrow.fs.copy_files

    def broken(*args: object, **kwargs: object) -> None:
        with store.open_output_stream("bucket/data/part.parquet") as output:
            output.write(b"half")
        raise OSError("upload stopped")

    monkeypatch.setattr(pyarrow.fs, "copy_files", broken)
    with pytest.raises(OSError, match="upload stopped"):
        io.copy_from_local(source, "s3://bucket/data/part.parquet")
    assert store.get_file_info("bucket/data/part.parquet").type == pyarrow.fs.FileType.NotFound
    monkeypatch.setattr(pyarrow.fs, "copy_files", original)


# -- remote spills ----------------------------------------------------------


def test_spilling_a_local_input_returns_the_same_input(tmp_path: Path) -> None:
    path = tmp_path / "capture.txt.gz"
    path.write_bytes(gzip.compress(b"local"))
    source = ArrowFileIO().new_input(path.as_uri())
    bound = ArrowFileIO(opened=source)
    cache = tmp_path / "unused"

    assert bound.spill(local=cache) is bound
    assert not cache.exists()


def test_spilling_an_input_on_a_local_subtree_returns_it_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "mounted"
    root.mkdir()
    path = root / "capture.txt.gz"
    path.write_bytes(gzip.compress(b"local"))
    filesystem = pyarrow.fs.SubTreeFileSystem(root.as_posix(), pyarrow.fs.LocalFileSystem())
    source = PyArrowFile(
        location="mounted:capture.txt.gz",
        path="capture.txt.gz",
        fs=filesystem,
    )
    bound = ArrowFileIO(opened=source)
    cache = tmp_path / "unused"

    assert bound.spill(local=cache) is bound
    assert not cache.exists()


def test_a_remote_spill_is_deterministic_reused_and_refreshed_by_size(tmp_path: Path) -> None:
    store = pyarrow.fs._MockFileSystem()
    store.create_dir("bucket/logs", recursive=True)
    remote_path = "bucket/logs/capture.txt.gz"
    location = f"s3://{remote_path}"
    first_payload = gzip.compress(b"first")
    with store.open_output_stream(remote_path, compression=None) as stream:
        stream.write(first_payload)
    io = ArrowFileIO()
    io.fs_by_scheme = lambda *_: store
    source = io.at(location)

    first = source.spill(tmp_path)
    assert first is not None
    assert isinstance(first, ArrowFileIO)
    assert isinstance(first.opened, PyArrowFile)
    assert isinstance(first.filesystem, pyarrow.fs.LocalFileSystem)
    target = Path(first.location)
    assert target.parent == tmp_path
    assert target.suffix == ".gz"
    assert target.read_bytes() == first_payload

    same_size = b"x" * len(first_payload)
    target.write_bytes(same_size)
    reused = source.spill(tmp_path)
    assert reused is not None
    assert reused.location == first.location
    assert target.read_bytes() == same_size, "equal remote size costs no second GET"

    second_payload = gzip.compress(b"a different and longer capture")
    assert len(second_payload) != len(first_payload)
    with store.open_output_stream(remote_path, compression=None) as stream:
        stream.write(second_payload)
    refreshed = source.spill(tmp_path)
    assert refreshed is not None
    assert refreshed.location == first.location
    assert target.read_bytes() == second_payload

    store.delete_file(remote_path)
    assert source.spill(tmp_path) is None
    assert target.read_bytes() == second_payload, "a missing remote never serves the stale spill"


def test_default_spills_are_deterministic_and_distinct_by_remote_path() -> None:
    store = pyarrow.fs._MockFileSystem()
    for path in ("bucket/one/capture.zst", "bucket/two/capture.zst"):
        store.create_dir(path.rpartition("/")[0], recursive=True)
        with store.open_output_stream(path, compression=None) as stream:
            stream.write(path.encode())
    io = ArrowFileIO()
    io.fs_by_scheme = lambda *_: store

    first = io.at("s3://bucket/one/capture.zst").spill()
    again = io.at("s3a://bucket/one/capture.zst").spill()
    other = io.at("s3://bucket/two/capture.zst").spill()

    assert first is not None and again is not None and other is not None
    assert first.location == again.location, "S3 aliases name one deterministic spill"
    assert first.location != other.location
    assert Path(first.location).suffix == ".zst"

    second_store = pyarrow.fs._MockFileSystem()
    second_store.create_dir("bucket/one", recursive=True)
    with second_store.open_output_stream("bucket/one/capture.zst", compression=None) as stream:
        stream.write(b"another store")
    second_io = ArrowFileIO({"s3.endpoint": "http://other-store:9000"})
    second_io.fs_by_scheme = lambda *_: second_store
    elsewhere = second_io.at("s3://bucket/one/capture.zst").spill()
    assert elsewhere is not None
    assert elsewhere.location != first.location, "equal paths on two stores cannot share bytes"

    rotated = ArrowFileIO(
        {
            "s3.access-key-id": "rotated",
            "s3.secret-access-key": "new-secret",
        }
    )
    rotated.fs_by_scheme = lambda *_: store
    after_rotation = rotated.at("s3://bucket/one/capture.zst").spill()
    assert after_rotation is not None
    assert after_rotation.location == first.location, "credentials do not identify stored bytes"


def test_a_temporary_spill_is_owned_until_close_and_never_shared(tmp_path: Path) -> None:
    store = pyarrow.fs._MockFileSystem()
    store.create_dir("bucket")
    with store.open_output_stream("bucket/capture.gz", compression=None) as stream:
        stream.write(gzip.compress(b"capture"))
    io = ArrowFileIO()
    io.fs_by_scheme = lambda *_: store
    source = io.at("s3://bucket/capture.gz")

    first = source.spill(tmp_path, temporary=True)
    second = source.spill(tmp_path, temporary=True)

    assert first is not None and second is not None
    assert first.temporary and second.temporary
    assert first.location != second.location
    assert Path(first.location).exists() and Path(second.location).exists()
    first.close()
    first.close()
    assert not Path(first.location).exists()
    assert Path(second.location).exists(), "one reader cannot purge another reader's spill"
    second.close()
    assert not Path(second.location).exists()


# -- the immutable-content cache --------------------------------------------


@pytest.fixture(autouse=True)
def pristine_cache() -> None:
    """The cache is process-wide on purpose; tests must not share through it."""
    CONTENT_CACHE.clear()
    CONTENT_CACHE.resize(DEFAULT_CACHE_BYTES)
    yield
    CONTENT_CACHE.clear()
    CONTENT_CACHE.resize(DEFAULT_CACHE_BYTES)


@pytest.fixture
def opens(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Opens the store actually serves, counted below the cache."""
    counted = {"opens": 0}
    original = PyArrowFile.open

    def watched(self: PyArrowFile, *args: object, **kwargs: object) -> object:
        counted["opens"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PyArrowFile, "open", watched)
    return counted


def test_only_what_iceberg_never_rewrites_is_cacheable() -> None:
    version = "00001-2f1a4c6e-8b3d-4a5f-9c7e-1d2b3a4c5d6e.metadata.json"
    assert arrow_file_io._immutable("wh/metadata/snap-1-abc.avro"), "a manifest list"
    assert arrow_file_io._immutable("wh/metadata/abc-m0.avro"), "a manifest"
    assert arrow_file_io._immutable(f"wh/metadata/{version}"), "a metadata version Iceberg minted"
    assert arrow_file_io._immutable(f"C:\\wh\\metadata\\{version}"), "however the path is spelled"
    assert not arrow_file_io._immutable("wh/metadata/version-hint.text"), "the one mutable file"
    assert not arrow_file_io._immutable("wh/metadata/v3.metadata.json"), (
        "no UUID, so two racing writers can both produce it with different bytes"
    )
    assert not arrow_file_io._immutable("wh/data/day=1/abc.parquet"), (
        "data is read once, not repeatedly"
    )


def test_the_cache_holds_bounded_bytes_and_forgets_the_coldest_first() -> None:
    cache = ContentCache(limit=40)
    for i in range(10):
        cache.put(f"e{i}", b"abcd")  # 40 bytes: exactly at the limit
    assert cache.get("e0") == b"abcd", "e0 is now the warmest"
    cache.put("new", b"abcd")  # 44 > 40: e1, now the coldest, goes
    assert cache.get("e1") is None
    assert cache.get("e0") == b"abcd"
    assert cache.get("new") == b"abcd"
    report = cache.stats()
    assert report["entries"] == 10
    assert report["bytes"] == 40


def test_a_file_past_the_per_file_cap_is_never_held() -> None:
    cache = ContentCache(limit=80)
    cache.put("big", b"x" * 11)  # over limit // 8, would evict everything else
    assert cache.get("big") is None


def test_the_same_s3_path_on_two_endpoints_never_shares_cached_bytes() -> None:
    location = "s3://same-bucket/metadata/shared.avro"
    stores = [pyarrow.fs._MockFileSystem(), pyarrow.fs._MockFileSystem()]
    payloads = [b"first store", b"second store"]
    readers = []
    for index, (store, payload) in enumerate(zip(stores, payloads, strict=True)):
        store.create_dir("same-bucket/metadata", recursive=True)
        with store.open_output_stream("same-bucket/metadata/shared.avro") as output:
            output.write(payload)
        io = ArrowFileIO({"s3.endpoint": f"http://minio-{index}:9000"})
        io.fs_by_scheme = lambda *_args, store=store: store
        readers.append(io)

    with readers[0].new_input(location).open() as first:
        assert first.read() == payloads[0]
    with readers[1].new_input(location).open() as second:
        assert second.read() == payloads[1]


def test_an_oversized_immutable_input_streams_without_an_eager_copy(tmp_path: Path) -> None:
    location = (tmp_path / "large.avro").as_posix()
    (tmp_path / "large.avro").write_bytes(b"x" * 200)
    io = ArrowFileIO({"rekep.io.cache-bytes": "1024"})

    with io.new_input(location).open() as stream:
        assert not isinstance(stream, pyarrow.BufferReader)
        assert stream.read(1) == b"x"
    assert CONTENT_CACHE.peek(location) is None


def test_a_written_manifest_reads_back_without_the_store(tmp_path: Path) -> None:
    """Write-through: the file a commit just wrote is the file the next scan plans."""
    io = ArrowFileIO()
    location = (tmp_path / "m.avro").as_posix()
    with io.new_output(location).create() as out:
        out.write(b"avro bytes")
    (tmp_path / "m.avro").unlink()  # the store forgets it; the cache must not
    source = io.new_input(location)
    assert isinstance(source, CachedInputFile)
    assert source.exists()
    assert len(source) == len(b"avro bytes")
    with source.open() as stream:
        assert stream.read() == b"avro bytes"


def test_a_read_fills_the_cache_for_every_later_reader(
    tmp_path: Path, opens: dict[str, int]
) -> None:
    location = (tmp_path / "m.avro").as_posix()
    (tmp_path / "m.avro").write_bytes(b"cold")
    io = ArrowFileIO()
    for _ in range(3):
        with io.new_input(location).open() as stream:
            assert stream.read() == b"cold"
    assert opens["opens"] == 1, "one fetch, then memory"


def test_a_data_file_is_never_cached(tmp_path: Path, opens: dict[str, int]) -> None:
    location = (tmp_path / "d.parquet").as_posix()
    (tmp_path / "d.parquet").write_bytes(b"rows")
    io = ArrowFileIO()
    for _ in range(2):
        with io.new_input(location).open() as stream:
            stream.read()
    assert opens["opens"] == 2, "data is the store's to stream, every time"


def test_a_write_past_the_cap_stops_copying_itself(tmp_path: Path) -> None:
    """`put` refuses anything over an eighth of the budget, so accumulating the
    rest of it buys a second copy of a file that is dropped on arrival."""
    io = ArrowFileIO({"rekep.io.cache-bytes": "1024"})
    location = (tmp_path / "big.avro").as_posix()
    out = io.new_output(location)
    stream = out.create()
    stream.write(b"x" * 100)
    assert stream._buffer is not None, "still worth keeping at 100 bytes of a 128 cap"
    stream.write(b"x" * 100)
    assert stream._buffer is None, "and dropped the moment it cannot be"
    stream.close()
    assert CONTENT_CACHE.peek(location) is None
    assert (tmp_path / "big.avro").read_bytes() == b"x" * 200, "the store still got all of it"
    ArrowFileIO({"rekep.io.cache-bytes": str(DEFAULT_CACHE_BYTES)})


def test_an_abandoned_write_never_reaches_the_cache(tmp_path: Path) -> None:
    io = ArrowFileIO()
    location = (tmp_path / "broken.avro").as_posix()
    with pytest.raises(RuntimeError, match="mid-write"):
        with io.new_output(location).create() as out:
            out.write(b"half")
            raise RuntimeError("died mid-write")
    assert CONTENT_CACHE.peek(location) is None, "half a file must not read as a whole one"


def test_a_deleted_file_is_forgotten_with_the_store(tmp_path: Path) -> None:
    io = ArrowFileIO()
    location = (tmp_path / "old.avro").as_posix()
    with io.new_output(location).create() as out:
        out.write(b"expired")
    io.delete(location)
    assert CONTENT_CACHE.peek(location) is None
    with pytest.raises(FileNotFoundError):
        io.new_input(location).open()


def test_a_catalog_can_opt_out(tmp_path: Path, opens: dict[str, int]) -> None:
    location = (tmp_path / "m.avro").as_posix()
    (tmp_path / "m.avro").write_bytes(b"cold")
    plain = ArrowFileIO({"rekep.io.cache-bytes": "0"})
    assert isinstance(plain.new_input(location), PyArrowFile), "no wrapper at all"
    for _ in range(2):
        with plain.new_input(location).open() as stream:
            stream.read()
    assert opens["opens"] == 2
    assert CONTENT_CACHE.peek(location) is None


def test_a_catalog_can_resize_the_shared_budget() -> None:
    ArrowFileIO({"rekep.io.cache-bytes": "4096"})
    assert CONTENT_CACHE.limit == 4096


@pytest.mark.parametrize("scheme", sorted(S3))
def test_every_spelling_of_one_store_shares_one_cache_entry(scheme: str) -> None:
    """Cache identity is the store and the object, so `s3a` and `s3` are one."""
    io = ArrowFileIO({"s3.endpoint": "http://minio:9000", "s3.region": "eu-west-1"})
    identity = io.content_identity(f"{scheme}://bucket/wh/x.avro")

    assert identity == io.content_identity("s3://bucket/wh/x.avro")
    assert identity.split("\0")[:3] == ["s3", "http://minio:9000", "bucket"]
    assert io.content_identity(f"{scheme}://other/wh/x.avro") != identity, "not across buckets"
    assert io.content_identity(f"{scheme}://k:s@elsewhere:9000/bucket/wh/x.avro") != identity, (
        "and never across stores"
    )


@pytest.mark.parametrize("scheme", sorted(S3))
def test_every_spelling_reaches_the_same_iceberg_configuration(scheme: str) -> None:
    """One warehouse, written three ways, configures one store and keeps its spelling."""
    configured = inferred_properties({"warehouse": f"{scheme}://key:secret@minio:9000/wh"})
    assert configured == {
        "warehouse": f"{scheme}://wh/",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "secret",
        "s3.region": "us-east-1",
    }
    assert {name: value for name, value in configured.items() if name != "warehouse"} == {
        name: value
        for name, value in inferred_properties(
            {"warehouse": "s3://key:secret@minio:9000/wh"}
        ).items()
        if name != "warehouse"
    }
