"""`ArrowFileIO`: location parsing on both hosts, and the content cache.

The Windows branch is data (`_WINDOWS`), so a POSIX runner can pin what a
Windows one would do and the other way round -- the whole point of the class
is behaviour CI's two legs do not share. The cache tests count real opens
below it, because "served from memory" is the whole claim.
"""

from pathlib import Path

import pytest
from pyiceberg.io.pyarrow import PyArrowFile

from rekep.iceberg import fileio
from rekep.iceberg.fileio import (
    CONTENT_CACHE,
    DEFAULT_CACHE_BYTES,
    ArrowFileIO,
    CachedInputFile,
    ContentCache,
    inferred_properties,
)


@pytest.fixture
def windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fileio, "_WINDOWS", True)


@pytest.fixture
def posix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fileio, "_WINDOWS", False)


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
        "warehouse": "s3://key:secret@minio.corp.com/wh",
        "s3.endpoint": "https://minio.corp.com",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "secret",
    }


def test_a_warehouse_url_configures_the_filesystem_it_names() -> None:
    """Said once as a location, rather than again as three settings."""
    assert inferred_properties({"warehouse": "s3://key:sec:ret@minio:9000/wh"}) == {
        "warehouse": "s3://key:sec:ret@minio:9000/wh",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "sec:ret",
    }


def test_what_the_caller_set_wins_over_what_the_location_says() -> None:
    """An explicit property is a decision; a URL is a default."""
    inferred = inferred_properties(
        {"warehouse": "s3://key:secret@minio:9000/wh", "s3.access-key-id": "other"}
    )
    assert inferred["s3.access-key-id"] == "other"
    assert inferred["s3.endpoint"] == "http://minio:9000"


def test_a_location_that_says_nothing_adds_nothing() -> None:
    for properties in ({"warehouse": "s3://bucket/wh"}, {"warehouse": "/tmp/wh"}, {}):
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
    assert fileio._immutable("wh/metadata/snap-1-abc.avro"), "a manifest list"
    assert fileio._immutable("wh/metadata/abc-m0.avro"), "a manifest"
    assert fileio._immutable(f"wh/metadata/{version}"), "a metadata version Iceberg minted"
    assert fileio._immutable(f"C:\\wh\\metadata\\{version}"), "however the path is spelled"
    assert not fileio._immutable("wh/metadata/version-hint.text"), "the one mutable file"
    assert not fileio._immutable("wh/metadata/v3.metadata.json"), (
        "no UUID, so two racing writers can both produce it with different bytes"
    )
    assert not fileio._immutable("wh/data/day=1/abc.parquet"), "data is read once, not repeatedly"


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
