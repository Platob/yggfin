"""One parser for every location, and the four things it gets right.

The Windows branch is data (`urls._WINDOWS`), so a POSIX runner pins what a
Windows one would answer and the other way round -- path spelling is the half
of this module CI's two legs do not share, and it is where every Windows
failure this file has ever had came from.
"""

import os
from pathlib import Path

import pyarrow.fs
import pytest

from rekep import Url, urls
from rekep.filesystems import resolve
from rekep.urls import properties_of


@pytest.fixture
def windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(urls, "_WINDOWS", True)


@pytest.fixture
def posix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(urls, "_WINDOWS", False)


def posix_path(relative: str) -> str:
    """What this location resolves to here, spelled the way `Url` spells it."""
    return Path(os.path.abspath(relative)).as_posix()


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


def test_a_local_path_comes_back_posix_on_a_windows_host(windows: None) -> None:
    """One parser, one spelling: a backslash is a separator where it is one."""
    assert Url.from_string("C:\\warehouse\\x").store_path == "C:/warehouse/x"
    assert Url.from_string("logs\\app.txt").path == "logs/app.txt"


def test_a_backslash_stays_a_character_in_a_name_on_a_posix_host(posix: None) -> None:
    """Where it is not a separator it is data, and normalising it renames a file."""
    assert Url.from_string("logs/a\\b.txt").path == "logs/a\\b.txt"


def test_a_drive_is_read_as_a_drive_on_either_host(posix: None) -> None:
    """A recorded `C:\\x` has to mean the same thing wherever it is compared."""
    assert Url.from_string("C:\\warehouse\\x") == Url.from_string("C:/warehouse/x")


def test_an_absolute_path_does_not_collect_the_working_drive(windows: None) -> None:
    """`abspath` answers `/var/log` with whichever drive the process is on.

    That is a guess, it differs per process, and it is what stopped
    `file:///var/log/app.txt` round tripping on the Windows CI leg.
    """
    url = Url.from_string("file:///var/log/app.txt")
    assert url.store_path == "/var/log/app.txt"
    assert url.into_string() == "file:///var/log/app.txt"


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


@pytest.mark.parametrize("text", ["s3://my.logs.2026/key", "s3://logs.internal/key"])
def test_a_dotted_name_that_is_no_hostname_is_still_a_bucket(text: str) -> None:
    """A bucket may carry dots, so a dot decides nothing -- the last label does."""
    url = Url.from_string(text)
    assert url.endpoint is None
    assert url.bucket == url.host
    assert url.key == "key"


def test_a_hostname_without_a_port_is_still_a_store() -> None:
    """The gap a port-only rule leaves: every hosted S3 answers on 443.

    `minio.corp.com` behind a certificate carries no port at all, and reading
    it as a bucket loses the real one -- `logs` -- into the key.
    """
    url = Url.from_string("s3://k:s@minio.corp.com/logs/2026/app.txt")
    assert url.hosts_a_store is True
    assert url.endpoint == "minio.corp.com"
    assert (url.bucket, url.key) == ("logs", "2026/app.txt")
    assert url.store_path == "logs/2026/app.txt"


@pytest.mark.parametrize(
    ("text", "endpoint", "region"),
    [
        ("s3://logs.s3.amazonaws.com/a.txt", "s3.amazonaws.com", None),
        ("s3://logs.s3.eu-west-1.amazonaws.com/a.txt", "s3.eu-west-1.amazonaws.com", "eu-west-1"),
        ("s3://logs.s3-eu-west-1.amazonaws.com/a.txt", "s3-eu-west-1.amazonaws.com", "eu-west-1"),
        (
            "s3://logs.s3.dualstack.eu-west-1.amazonaws.com/a.txt",
            "s3.dualstack.eu-west-1.amazonaws.com",
            "eu-west-1",
        ),
        ("s3://logs.s3-accelerate.amazonaws.com/a.txt", "s3-accelerate.amazonaws.com", None),
        (
            "s3://logs.s3-fips.us-gov-west-1.amazonaws.com/a.txt",
            "s3-fips.us-gov-west-1.amazonaws.com",
            "us-gov-west-1",
        ),
        (
            "s3://logs.s3.cn-north-1.amazonaws.com.cn/a.txt",
            "s3.cn-north-1.amazonaws.com.cn",
            "cn-north-1",
        ),
    ],
)
def test_a_virtual_hosted_aws_location_keeps_the_bucket_in_front_of_it(
    text: str, endpoint: str, region: str | None
) -> None:
    """The spelling a console copies out, and the one that loses a bucket worst.

    `logs.s3.eu-west-1.amazonaws.com` read as a name addresses a bucket nobody
    created -- and every published form of the hostname has to split the same
    way, or one of them silently does not.
    """
    url = Url.from_string(text)
    assert url.hosted_bucket == "logs"
    assert (url.bucket, url.key, url.store_path) == ("logs", "a.txt", "logs/a.txt")
    assert url.endpoint == endpoint
    assert url.region == region


@pytest.mark.parametrize(
    ("text", "region"),
    [
        ("s3://s3.amazonaws.com/logs/a.txt", None),
        ("s3://s3.eu-west-1.amazonaws.com/logs/a.txt", "eu-west-1"),
        ("s3://s3-eu-west-1.amazonaws.com/logs/a.txt", "eu-west-1"),
        ("s3://s3.dualstack.eu-west-1.amazonaws.com/logs/a.txt", "eu-west-1"),
    ],
)
def test_a_path_style_aws_location_finds_its_bucket_below_the_endpoint(
    text: str, region: str | None
) -> None:
    """The same store, addressed the other way: nothing in front, bucket below."""
    url = Url.from_string(text)
    assert url.hosted_bucket == ""
    assert (url.bucket, url.key, url.store_path) == ("logs", "a.txt", "logs/a.txt")
    assert url.region == region


def test_the_rightmost_s3_label_is_the_service_and_the_rest_is_the_bucket() -> None:
    """So a bucket that is *named* `s3logs` keeps its name, dots and all."""
    assert Url.from_string("s3://s3logs.s3.amazonaws.com/k").bucket == "s3logs"
    assert Url.from_string("s3://my.dotted.logs.s3.amazonaws.com/k").bucket == "my.dotted.logs"


def test_a_hostname_is_read_for_s3_and_a_port_is_read_everywhere() -> None:
    """S3 is what addresses one store two ways; the others have one spelling.

    `gs://bucket/key` is the only shape GCS has, so a name in it is a name
    however it ends -- and reading it as a store would move the bucket into
    the key on a location nobody spelled wrong.
    """
    assert Url.from_string("gs://bucket.example.com/key").bucket == "bucket.example.com"
    assert Url.from_string("gs://store.example.com:9000/b/k").bucket == "b"
    assert Url.from_string("abfss://container@account.dfs.core.windows.net/p").endpoint is None


def test_an_explicit_endpoint_override_leaves_the_bucket_where_it_was() -> None:
    """It is a setting beside the location, not a claim about the netloc."""
    url = Url.from_string("s3://bucket/key?endpoint_override=minio:9000")
    assert url.endpoint == "minio:9000"
    assert url.hosts_a_store is False
    assert (url.bucket, url.key, url.store_path) == ("bucket", "key", "bucket/key")


def test_an_explicit_endpoint_is_how_a_bucket_named_for_a_domain_keeps_its_name() -> None:
    """The static-website pattern is the one place a bucket really is a host.

    `s3://www.example.com/index.html` has to be readable as the bucket AWS
    requires that site to be named -- and a decision stated in the location
    beats a shape inferred from it, so `?endpoint_override=` is the way out.
    """
    url = Url.from_string("s3://www.example.com/index.html?endpoint_override=s3.amazonaws.com")
    assert url.hosts_a_store is False
    assert (url.bucket, url.key) == ("www.example.com", "index.html")


# -- spelling it back --------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "s3://bucket/key",
        "s3://key:secret@bucket/key",
        "s3://key:sec%3Aret@minio:9000/logs/a.txt",
        "s3://k:s@minio:9000/b?region=eu-west-1",
        "s3://logs.s3.eu-west-1.amazonaws.com/a.txt",
        "s3://s3.eu-west-1.amazonaws.com/logs/a.txt",
        "s3://k:s@minio.corp.com/logs/a.txt",
        "file:///var/log/app.txt",
        "gs://bucket/key",
    ],
)
def test_a_location_round_trips_through_its_own_spelling(text: str) -> None:
    """Including on Windows, where an absolute path used to collect a drive."""
    url = Url.from_string(text)
    assert Url.from_string(url.into_string()) == url


def test_a_relative_path_becomes_absolute_where_it_has_to_be_a_uri() -> None:
    """There is no relative `file://` URI, so it is resolved once, here.

    Spelled POSIX, because a `%5C` in the middle of a path is what a Windows
    `abspath` used to put there and nothing reads that back.
    """
    url = Url.from_string("logs/app.txt")
    assert url.path == "logs/app.txt"
    assert url.into_string() == f"file:///{posix_path('logs/app.txt').lstrip('/')}"
    assert "%5C" not in url.into_string()


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
    assert path == (tmp_path / "x.txt").as_posix(), "the one spelling, on either host"


def test_a_relative_local_location_is_made_absolute_for_the_filesystem() -> None:
    url = Url.from_string("logs/app.txt")
    _, path = url.into_filesystem()
    assert path == posix_path("logs/app.txt")
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


def test_amazons_own_endpoint_is_configured_as_a_region_and_not_an_override() -> None:
    """Arrow builds `bucket.s3.<region>.amazonaws.com` from the region itself.

    Overriding AWS with AWS only forces path-style addressing -- which
    `s3-accelerate` refuses outright -- and the region is the half of that
    hostname that does have to travel, because SigV4 signs with it.
    """
    filesystem, path = Url.from_string(
        "s3://logs.s3.eu-west-1.amazonaws.com/a.txt"
    ).into_filesystem()
    assert path == "logs/a.txt"
    settings = settings_of(filesystem)
    assert settings["region"] == "eu-west-1"
    assert not settings["endpoint_override"]


def test_the_two_spellings_of_one_aws_location_build_the_same_filesystem() -> None:
    """Which is the point of reading the hostname: they are the same location."""
    hostname = Url.from_string("s3://logs.s3.eu-west-1.amazonaws.com/a.txt").into_filesystem()
    named = Url.from_string("s3://logs/a.txt?region=eu-west-1").into_filesystem()
    assert hostname[1] == named[1] == "logs/a.txt"
    assert settings_of(hostname[0]) == settings_of(named[0])


def test_a_hosted_endpoint_that_is_nobody_elses_is_configured_outright() -> None:
    """Arrow cannot build `minio.corp.com`, so this is where it is told."""
    filesystem, path = Url.from_string("s3://k:s@minio.corp.com/logs/a.txt").into_filesystem()
    assert path == "logs/a.txt"
    settings = settings_of(filesystem)
    assert settings["endpoint_override"] == "minio.corp.com"
    assert settings["scheme"] == "https", "a real host may have a certificate"


def test_an_endpoint_named_only_in_the_query_is_still_built_here() -> None:
    """A location with no port, no store hostname and no credentials at all --
    everything it says about the store, it says in `?endpoint_override=`.

    Arrow reads that query key too, so the endpoint and the path come out the
    same either way and asserting them proves nothing. The **scheme** is what
    differs: Arrow defaults to `https`, and a MinIO in a container has no
    certificate to serve -- so a location handed off rather than built here is
    a connection that is refused.
    """
    filesystem, path = Url.from_string(
        "s3://bucket/key?endpoint_override=minio:9000"
    ).into_filesystem()
    assert isinstance(filesystem, pyarrow.fs.S3FileSystem)
    assert path == "bucket/key"
    settings = settings_of(filesystem)
    assert settings["endpoint_override"] == "minio:9000"
    assert settings["scheme"] == "http", "which is the half Arrow's own parser guesses wrong"


def test_the_scheme_query_wins_over_the_guess() -> None:
    filesystem, _ = Url.from_string(
        "s3://k:s@store.example.com:9000/b?scheme=http"
    ).into_filesystem()
    assert settings_of(filesystem)["scheme"] == "http"


def test_resolve_goes_through_the_same_parser(tmp_path: Path) -> None:
    """`filesystems.resolve` is the cached front door, and it is this parser."""
    filesystem, path = resolve(str(tmp_path / "x.txt"))
    assert isinstance(filesystem, pyarrow.fs.LocalFileSystem)
    assert path == (tmp_path / "x.txt").as_posix()
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


def test_an_aws_location_tells_a_catalog_its_region_and_not_its_endpoint() -> None:
    """pyiceberg passes `s3.endpoint` straight to `endpoint_override`, so the
    reason `_s3_filesystem` leaves AWS out is the reason this leaves it out."""
    url = Url.from_string("s3://key:secret@logs.s3.eu-west-1.amazonaws.com/warehouse")
    assert properties_of(url) == {
        "s3.access-key-id": "key",
        "s3.secret-access-key": "secret",
        "s3.region": "eu-west-1",
    }


def test_a_hosted_endpoint_reaches_a_catalog_with_the_scheme_it_is_served_on() -> None:
    assert properties_of(Url.from_string("s3://minio.corp.com/warehouse")) == {
        "s3.endpoint": "https://minio.corp.com"
    }


def test_a_location_that_says_nothing_configures_nothing() -> None:
    assert properties_of(Url.from_string("s3://bucket/key")) == {}
