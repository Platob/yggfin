"""One parser for every location, and the three things it gets right."""

import os
from pathlib import Path

import pyarrow.fs
import pytest

from rekep import Url
from rekep.filesystems import resolve
from rekep.urls import properties_of

# -- parsing ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("s3://bucket/key/deep.txt", ("s3", None, None, "bucket", None, "key/deep.txt")),
        ("s3://key:secret@bucket/k", ("s3", "key", "secret", "bucket", None, "k")),
        (
            "s3://key:secret@minio:9000/logs/a.txt",
            ("s3", "key", "secret", "minio", 9000, "logs/a.txt"),
        ),
        ("file:///var/log/app.txt", ("file", None, None, "", None, "/var/log/app.txt")),
        ("file:/var/log/app.txt", ("file", None, None, "", None, "/var/log/app.txt")),
        ("/var/log/app.txt", ("file", None, None, "", None, "/var/log/app.txt")),
        ("logs/app.txt", ("file", None, None, "", None, "logs/app.txt")),
        ("gs://bucket/key", ("gs", None, None, "bucket", None, "key")),
    ],
)
def test_the_parts_a_location_is_made_of(text: str, expected: tuple) -> None:
    url = Url.from_string(text)
    assert (url.scheme, url.user, url.password, url.host, url.port, url.path) == expected


def test_a_secret_may_contain_a_colon() -> None:
    """Userinfo splits on the *first* one: everything after it is the secret."""
    url = Url.from_string("s3://AKIA:sec:ret@bucket/key")
    assert (url.user, url.password) == ("AKIA", "sec:ret")


def test_every_part_is_percent_decoded() -> None:
    """A secret with a `/` or an `@` has to arrive encoded to survive the URL."""
    url = Url.from_string("s3://AKIA:pa%2Fss%3Aword%40x@bucket/a%20name.txt")
    assert url.password == "pa/ss:word@x"
    assert url.path == "a%20name.txt".replace("%20", " ")


def test_a_user_with_no_secret_is_not_given_one() -> None:
    url = Url.from_string("abfss://container@account.dfs.core.windows.net/path")
    assert (url.user, url.password) == ("container", None)
    assert url.into_string() == "abfss://container@account.dfs.core.windows.net/path"


def test_a_windows_drive_is_a_path_and_not_a_scheme() -> None:
    assert Url.from_string("C:/warehouse/x").path == "C:/warehouse/x"
    assert Url.from_string("file:///C:/warehouse/x").path == "C:/warehouse/x"
    assert Url.from_string("C:/warehouse/x").scheme == "file"


def test_both_windows_separators_name_one_location() -> None:
    """Which is what lets a swept path be compared against a recorded one."""
    assert Url.from_string("C:\\warehouse\\x") == Url.from_string("C:/warehouse/x")


def test_a_scheme_needs_more_than_one_letter() -> None:
    """`file:/x` is a URI a store writes; `C:/x` is a drive, and both parse."""
    assert Url.from_string("file:/x").path == "/x"
    assert Url.from_string("c:/x").path == "c:/x"


def test_the_query_is_kept() -> None:
    url = Url.from_string("s3://k:s@minio:9000/b?scheme=http&region=eu-west-1")
    assert url.query == {"scheme": "http", "region": "eu-west-1"}


# -- endpoints and buckets ---------------------------------------------------


def test_a_port_means_an_endpoint_and_the_bucket_is_below_it() -> None:
    """What pyarrow reads as a bucket called `minio`, with the port dropped."""
    url = Url.from_string("s3://key:secret@minio:9000/logs/2026/app.txt")
    assert url.endpoint == "minio:9000"
    assert url.bucket == "logs"
    assert url.key == "2026/app.txt"
    assert url.store_path == "logs/2026/app.txt"


def test_without_a_port_the_host_is_the_bucket() -> None:
    """An `s3://bucket/key` URL means the same thing everywhere, so it keeps it."""
    url = Url.from_string("s3://logs/2026/app.txt")
    assert url.endpoint is None
    assert url.bucket == "logs"
    assert url.key == "2026/app.txt"
    assert url.store_path == "logs/2026/app.txt"


def test_a_bucket_keeps_the_capitals_it_was_named_with() -> None:
    """A host is lowercased by every URL parser, and a bucket is a host here."""
    url = Url.from_string("s3://MyBucket/key")
    assert url.bucket == "MyBucket"
    assert url.into_string() == "s3://MyBucket/key"


def test_an_ipv6_endpoint_keeps_its_brackets_wherever_it_is_spelled() -> None:
    """They are what told the host from the port, so they have to come back."""
    url = Url.from_string("s3://k:s@[::1]:9000/b")
    assert (url.host, url.port) == ("::1", 9000)
    assert url.endpoint == "[::1]:9000"
    assert Url.from_string(url.into_string()) == url


def test_a_host_with_dots_is_still_a_bucket() -> None:
    """Bucket names may carry dots, so the shape of a hostname decides nothing."""
    assert Url.from_string("s3://logs.example.com/key").endpoint is None
    assert Url.from_string("s3://logs.example.com/key").bucket == "logs.example.com"


def test_an_explicit_endpoint_override_leaves_the_bucket_where_it_was() -> None:
    """It is a setting beside the location, not a claim about the netloc."""
    url = Url.from_string("s3://bucket/key?endpoint_override=minio:9000")
    assert url.endpoint == "minio:9000"
    assert url.hosts_a_store is False
    assert (url.bucket, url.key, url.store_path) == ("bucket", "key", "bucket/key")


# -- spelling it back --------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "s3://bucket/key",
        "s3://key:secret@bucket/key",
        "s3://key:sec%3Aret@minio:9000/logs/a.txt",
        "s3://k:s@minio:9000/b?region=eu-west-1",
        "file:///var/log/app.txt",
        "gs://bucket/key",
    ],
)
def test_a_location_round_trips_through_its_own_spelling(text: str) -> None:
    url = Url.from_string(text)
    assert Url.from_string(url.into_string()) == url


def test_a_relative_path_becomes_absolute_where_it_has_to_be_a_uri() -> None:
    """There is no relative `file://` URI, so it is resolved once, here."""
    url = Url.from_string("logs/app.txt")
    assert url.path == "logs/app.txt"
    assert url.into_string() == f"file://{os.path.abspath('logs/app.txt')}"


def test_the_secret_is_masked_wherever_something_prints() -> None:
    """A `repr` is what a log and a traceback carry, so it must not carry this."""
    url = Url.from_string("s3://key:secret@minio:9000/logs")
    assert "secret" not in repr(url)
    assert "secret" not in str(url)
    assert "***" in url.masked
    assert "secret" in url.into_string(), "and the one method that writes it out does"


# -- walking -----------------------------------------------------------------


def test_join_walks_in_place() -> None:
    url = Url.from_string("s3://key:secret@minio:9000/logs")
    assert url.join("2026-08-14", "app.txt") is url
    assert url.path == "logs/2026-08-14/app.txt"
    assert url.bucket == "logs"


def test_join_collapses_the_separators_a_configured_root_brings() -> None:
    root = Url.from_string("file:///var/log/")
    assert root.copy().join("app.txt").path == "/var/log/app.txt"
    assert root.copy().join("/app.txt").path == "/var/log/app.txt"
    assert root.copy().join("a/", "/b").path == root.copy().join("a", "b").path


def test_parent_walks_up_and_stops_at_the_root() -> None:
    url = Url.from_string("s3://bucket/a/b/c")
    assert url.parent().path == "a/b"
    assert url.parent().path == "a"
    assert url.parent().path == ""
    assert url.parent().path == ""


def test_an_absolute_path_stays_absolute_all_the_way_up() -> None:
    """`/var` up is `/`; the empty path is the working directory, elsewhere."""
    url = Url.from_string("/var/log")
    assert url.parent().path == "/var"
    assert url.parent().path == "/"
    assert url.parent().path == "/"


def test_a_copy_is_where_a_walk_branches() -> None:
    root = Url.from_string("file:///var/log")
    child = root.copy().join("app.txt")
    assert root.path == "/var/log"
    assert child.path == "/var/log/app.txt"
    assert child.query is not root.query


# -- the filesystem it names -------------------------------------------------


def test_a_local_location_builds_the_local_filesystem(tmp_path: Path) -> None:
    filesystem, path = Url.from_string(str(tmp_path / "x.txt")).into_filesystem()
    assert isinstance(filesystem, pyarrow.fs.LocalFileSystem)
    assert path == str(tmp_path / "x.txt")


def test_a_relative_local_location_is_made_absolute_for_the_filesystem() -> None:
    url = Url.from_string("logs/app.txt")
    _, path = url.into_filesystem()
    assert path == os.path.abspath("logs/app.txt")
    assert url.store_path == path, "and `store_path` is the same answer, not a second one"


def settings_of(filesystem: pyarrow.fs.S3FileSystem) -> dict:
    """What an `S3FileSystem` was built with -- there is no reader for it.

    `repr` is the default one, and every option is write-only; pickling is the
    only place Arrow hands the settings back, so that is where the tests below
    read the endpoint and the scheme from.
    """
    return filesystem.__reduce__()[1][0]


def test_an_endpoint_reaches_the_s3_filesystem() -> None:
    """The whole point: MinIO is an endpoint, and the bucket is below it."""
    filesystem, path = Url.from_string("s3://key:sec:ret@minio:9000/logs/a.txt").into_filesystem()
    assert isinstance(filesystem, pyarrow.fs.S3FileSystem)
    assert path == "logs/a.txt"
    settings = settings_of(filesystem)
    assert settings["endpoint_override"] == "minio:9000"
    assert (settings["access_key"], settings["secret_key"]) == ("key", "sec:ret")


def test_a_plain_endpoint_is_read_as_http_and_a_real_host_as_https() -> None:
    """A container or a laptop has no certificate; anything with a domain might."""
    local = Url.from_string("s3://k:s@minio:9000/b").into_filesystem()[0]
    named = Url.from_string("s3://k:s@store.example.com:9000/b").into_filesystem()[0]
    assert settings_of(local)["scheme"] == "http"
    assert settings_of(named)["scheme"] == "https"


def test_the_scheme_query_wins_over_the_guess() -> None:
    filesystem, _ = Url.from_string(
        "s3://k:s@store.example.com:9000/b?scheme=http"
    ).into_filesystem()
    assert settings_of(filesystem)["scheme"] == "http"


def test_resolve_goes_through_the_same_parser(tmp_path: Path) -> None:
    """`filesystems.resolve` is the cached front door, and it is this parser."""
    filesystem, path = resolve(str(tmp_path / "x.txt"))
    assert isinstance(filesystem, pyarrow.fs.LocalFileSystem)
    assert path == str(tmp_path / "x.txt")
    assert resolve(str(tmp_path / "x.txt"))[0] is filesystem, "cached on the location"


# -- what a location says a catalog needs to be told -------------------------


def test_a_location_translates_into_the_properties_that_say_the_same() -> None:
    """So a caller says where the store is once, not once per setting."""
    url = Url.from_string("s3://key:secret@minio:9000/warehouse")
    assert properties_of(url) == {
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "secret",
    }


def test_a_location_that_says_nothing_configures_nothing() -> None:
    assert properties_of(Url.from_string("s3://bucket/key")) == {}
