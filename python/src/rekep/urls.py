"""One place a location is parsed, decoded and put back together."""

from __future__ import annotations

import functools
import os
import re
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any

import pyarrow.fs

#: Whether a backslash in a path is a separator rather than a character in a
#: name. It is on Windows, and it is nowhere else -- so the branch is data, and
#: a POSIX runner can pin what a Windows one would answer and the other way
#: round, which is the half of this module CI's two legs do not share.
_WINDOWS = os.name == "nt"

#: A path whose first segment is a drive letter -- `C:/x` or `C:\\x`.
DRIVE = re.compile(r"^[A-Za-z]:[/\\]")

#: The slash a URI split leaves in front of one -- `/C:/x`.
ROOTED_DRIVE = re.compile(r"^/+(?=[A-Za-z]:[/\\])")

#: A scheme and what follows it. Two letters at least, which is what tells a
#: scheme from a Windows drive: `C:/x` is a path, `s3://b/k` and `file:/x` are
#: URIs -- the second being the authority-less spelling a store writes into
#: Iceberg metadata, and one this used to read as a relative path.
SCHEME = re.compile(
    r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]+):(?P<slashes>//)?(?P<rest>.*)$", re.DOTALL
)

#: Schemes that name a file on this machine.
LOCAL = frozenset({"", "file", "local"})

#: Schemes whose byte transport is HTTP rather than a `pyarrow.fs` filesystem.
HTTP = frozenset({"http", "https"})

#: Schemes pyarrow serves with `S3FileSystem`, which is the one whose URI
#: parsing this module has to correct.
S3 = frozenset({"s3", "s3a", "s3n"})

#: Query keys an S3 location may carry, and what they configure. Anything else
#: is left in `query` for whoever put it there.
S3_SETTINGS = ("region", "scheme", "endpoint_override", "allow_bucket_creation")

#: Portable process defaults and their Iceberg property suffixes. AWS
#: credentials stay in Arrow's provider chain; these names are the explicit
#: S3-compatible-store layer above it.
S3_ENVIRONMENT = (
    ("S3_ACCESS_KEY_ID", "access-key-id"),
    ("S3_SECRET_ACCESS_KEY", "secret-access-key"),
    ("S3_SESSION_TOKEN", "session-token"),
    ("S3_REGION", "region"),
)

#: Endpoint defaults, from the store-specific spelling to AWS's global one.
S3_ENDPOINT_ENVIRONMENT = ("S3_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL")

#: A netloc that is a *hostname* rather than a bucket name. A bucket may carry
#: dots -- `my.logs.2026` is a legal name -- so a dot decides nothing; what
#: decides is the last label, because a name ending in a public suffix is
#: something somebody registered and pointed at a store. `.com` is the one that
#: carries S3: every AWS endpoint, R2, Spaces, Wasabi, Backblaze, and a MinIO
#: behind a real certificate. A location whose bucket really *is* named for a
#: domain -- the S3 static-website pattern, `s3://www.example.com/index.html` --
#: says so with `?endpoint_override=`, which is a decision and beats a shape.
STORE_HOST = re.compile(r"\.com$", re.IGNORECASE)

#: One of Amazon's own S3 hostnames, and the bucket in front of it when the
#: location is spelled virtual-hosted style. Every published form is here:
#: `s3.amazonaws.com`, `s3.<region>.amazonaws.com`, the legacy
#: `s3-<region>.amazonaws.com`, `s3-fips`/`s3-accelerate`/`s3-accesspoint`,
#: `.dualstack`, China's `.amazonaws.com.cn`, and any of them with a bucket
#: label in front. The bucket is greedy so the *rightmost* `s3` label is the
#: service, which is what keeps a bucket named `s3logs` its own name.
AWS_HOST = re.compile(
    r"^(?:(?P<bucket>.+)\.)?"
    r"s3(?:-(?P<qualifier>[a-z0-9\-]+))?"
    r"(?:\.dualstack)?"
    r"(?:\.(?P<region>[a-z]{2}-[a-z0-9\-]+-\d+))?"
    r"\.amazonaws\.com(?:\.cn)?$",
    re.IGNORECASE,
)

#: A region label, which is what tells the legacy `s3-eu-west-1` from the
#: `s3-accelerate` and `s3-fips` that are spelled exactly like it.
REGION = re.compile(r"^[a-z]{2}-[a-z0-9\-]+-\d+$", re.IGNORECASE)


@dataclass(eq=True)
class Url:
    """One location, in parts, mutable.

    Fields are plain and assignable -- `url.path = "..."`, `url.scheme = "s3"`
    -- because a location is a value a job adjusts as it walks: a warehouse
    root becomes a table directory becomes a data file. `join` is that walk,
    in place, so the object a caller holds stays the object they hold.

    `Url()` with nothing set is the current directory as a local path, which
    is what an empty location means to every filesystem here.
    """

    scheme: str = "file"
    user: str | None = None
    password: str | None = None
    host: str = ""
    port: int | None = None
    path: str = ""
    query: dict[str, str] = dataclass_field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalise the parts so two spellings of one location compare equal."""
        self.scheme = (self.scheme or "file").lower()
        self.query = {str(key): str(value) for key, value in (self.query or {}).items()}
        if self.port is not None:
            self.port = int(self.port)
        if self.scheme in LOCAL:
            self.path = _posix(self.path)

    # -- building -----------------------------------------------------------

    @classmethod
    def from_string(cls, text: str) -> Url:
        """Parse a URI, a local path, or a Windows path.

        The scheme is what precedes `://`, so a drive letter is never mistaken
        for one. Everything after is split by `urllib`, and then decoded: the
        userinfo on its *first* colon, and user, password and path through
        `unquote`, because a URL is transport and the values are not.
        """
        text = os.fspath(text)
        matched = SCHEME.match(text)
        if matched is None:
            return cls(scheme="file", path=_drive_path(text))
        scheme = matched["scheme"].lower()
        # `s3://host/path` has an authority to read; `file:/path` has none, and
        # reading one there would take the first segment for a host.
        parsed = urllib.parse.urlsplit(f"//{matched['rest']}" if matched["slashes"] else text)
        user, password = _credentials(parsed.netloc)
        path = urllib.parse.unquote(parsed.path)
        return cls(
            scheme=scheme,
            user=user,
            password=password,
            host=_host(parsed),
            port=parsed.port,
            path=_drive_path(path) if scheme in LOCAL else path.lstrip("/"),
            query=dict(urllib.parse.parse_qsl(parsed.query)),
        )

    @classmethod
    def from_path(cls, path: str | os.PathLike[str]) -> Url:
        """A local path as a location, absolute from here."""
        return cls(scheme="file", path=_drive_path(os.path.abspath(os.fspath(path))))

    def copy(self) -> Url:
        """This location again, so a walk can branch without moving the root."""
        return Url(
            scheme=self.scheme,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            path=self.path,
            query=dict(self.query),
        )

    # -- walking ------------------------------------------------------------

    def join(self, *segments: str) -> Url:
        """Walk down, in place, and hand back the same object.

        Separators are collapsed rather than counted: `join("a/", "/b")` and
        `join("a", "b")` are the same location, because a path built from a
        configured root and a name is exactly where the double slash comes
        from. An absolute segment is still a segment -- this walks a location,
        it does not replace one.
        """
        parts = [part.strip("/") for part in (self.path, *segments)]
        joined = "/".join(part for part in parts if part)
        self.path = f"/{joined}" if self.path.startswith("/") else joined
        return self

    def parent(self) -> Url:
        """Walk up one segment, in place. The root is its own parent.

        An absolute path stays absolute all the way up: the parent of `/var`
        is `/`, and not the empty path -- which a local filesystem would then
        read as the working directory, so a walk up would land somewhere else
        entirely.
        """
        absolute = self.path.startswith("/")
        head, separator, _ = self.path.rstrip("/").rpartition("/")
        root = "/" if absolute else ""
        self.path = (head or root) if separator else root
        return self

    # -- the parts, as the store reads them ---------------------------------

    @property
    def netloc(self) -> str:
        """`host`, `host:port`, or nothing."""
        return _netloc(self.host, self.port)

    @property
    def endpoint(self) -> str | None:
        """The store this location names, when it names one rather than a bucket.

        `?endpoint_override=` says it outright and wins. Otherwise the netloc
        says it -- a port does, and so does a hostname -- and what comes back
        is the *store's* netloc, which is the host without the bucket label a
        virtual-hosted spelling puts in front of it.
        """
        override = self.query.get("endpoint_override")
        if override:
            return override
        if not self.hosts_a_store:
            return None
        store = _split_host(self.host)[1] if self.scheme in S3 else ""
        return _netloc(store or self.host, self.port)

    @property
    def hosts_a_store(self) -> bool:
        """Whether the netloc is the store rather than the bucket."""
        if not self.host:
            return False
        if self.port:
            return True
        if self.scheme not in S3 or self.query.get("endpoint_override"):
            return False
        return bool(_split_host(self.host)[1])

    @property
    def hosted_bucket(self) -> str:
        """The bucket a virtual-hosted spelling puts in front of the store."""
        if self.scheme not in S3 or not self.hosts_a_store:
            return ""
        bucket, store = _split_host(self.host)
        return bucket if store else ""

    @property
    def region(self) -> str | None:
        """The region this location names, when it names one.

        `?region=` says it outright and wins; otherwise a regional hostname
        says it. It matters wherever an endpoint is configured: SigV4 signs for
        a region, so a location that named `s3.eu-west-1.amazonaws.com` and
        left the region to a default would be signed for `us-east-1` and
        refused. Where nothing names one, `into_filesystem` asks Arrow to
        resolve it from the bucket.
        """
        declared = self.query.get("region")
        if declared:
            return declared
        for host in (self.query.get("endpoint_override", ""), self.host):
            region = _amazon(_bare(host))[1] if host else None
            if region:
                return region
        return None

    @property
    def bucket(self) -> str:
        """The bucket this location is in -- an object store's view of it.

        The label in front of a virtual-hosted store, the first path segment
        when the netloc is a store addressed path-style, and the host itself
        when the host is the bucket -- which is what `s3://bucket/key` means
        everywhere and is the common case.

        A local path has no bucket and says so with an empty string.
        """
        hosted = self.hosted_bucket
        if hosted:
            return hosted
        if not self.hosts_a_store:
            return self.host
        return self.path.strip("/").split("/", 1)[0]

    @property
    def key(self) -> str:
        """Everything below the bucket -- an object store's view, like `bucket`."""
        stripped = self.path.strip("/")
        if not self.hosts_a_store or self.hosted_bucket:
            return stripped
        _, _, rest = stripped.partition("/")
        return rest

    @property
    def store_path(self) -> str:
        """The path the location's own filesystem understands."""
        if self.scheme in LOCAL:
            return self._local_path()
        return "/".join(part for part in (self.bucket, self.key) if part)

    @property
    def masked(self) -> str:
        """This location with the secret taken out, for anything that prints."""
        return self._spelled(secret="***")

    # -- converting ---------------------------------------------------------

    def into_string(self) -> str:
        """The location as a URI, secret included, every part re-encoded.

        The one method that writes a password out. `repr` does not, so a
        location that reaches a log or a traceback does not take the secret
        with it.
        """
        return self._spelled()

    def into_filesystem(self) -> tuple[pyarrow.fs.FileSystem, str]:
        """The `pyarrow.fs` handle for this location, and the path on it."""
        if self.scheme in LOCAL:
            return pyarrow.fs.LocalFileSystem(), self._local_path()
        environment = s3_environment() if self.scheme in S3 else {}
        if self.scheme in S3 and (
            self.endpoint is not None or self.user is not None or environment
        ):
            return self._s3_filesystem(environment), self.store_path
        location = self.into_string()
        if self.scheme in S3:
            # Arrow's native parser accepts `s3`, not Hadoop's `s3a`/`s3n` aliases.
            location = f"s3:{location.partition(':')[2]}"
        filesystem, path = pyarrow.fs.FileSystem.from_uri(location)
        return filesystem, path

    def _s3_filesystem(self, environment: Mapping[str, str]) -> pyarrow.fs.FileSystem:
        """`S3FileSystem` configured from process defaults and this location."""
        arguments = (
            ("access_key", "s3.access-key-id"),
            ("secret_key", "s3.secret-access-key"),
            ("session_token", "s3.session-token"),
            ("region", "s3.region"),
        )
        settings: dict[str, Any] = {
            argument: environment[property_name]
            for argument, property_name in arguments
            if property_name in environment
        }
        environment_endpoint = environment.get("s3.endpoint")
        if environment_endpoint:
            settings["scheme"], settings["endpoint_override"] = _endpoint_parts(
                environment_endpoint
            )
        settings.update(
            {
                key: self.query[key]
                for key in S3_SETTINGS
                if key in self.query and key != "endpoint_override"
            }
        )
        if self.user is not None:
            settings["access_key"] = self.user
            settings["secret_key"] = self.password or ""
            settings.pop("session_token", None)
        region = self.region
        if region is not None:
            settings["region"] = region
        endpoint = self.endpoint
        if endpoint is not None:
            if not _amazon(_bare(endpoint))[0]:
                settings["endpoint_override"] = endpoint
                if "scheme" not in self.query:
                    settings["scheme"] = "http" if _plain(endpoint) else "https"
            else:
                # Naming AWS in the location is an explicit store choice, so
                # a process-wide compatible-store endpoint cannot survive it.
                settings.pop("endpoint_override", None)
                if "scheme" not in self.query:
                    settings.pop("scheme", None)
        if "endpoint_override" not in settings and "region" not in settings:
            settings["region"] = _region_of(self.bucket)
            if settings["region"] is None:
                settings.pop("region")
        if "allow_bucket_creation" in settings:
            settings["allow_bucket_creation"] = settings["allow_bucket_creation"] == "true"
        return pyarrow.fs.S3FileSystem(**settings)

    def _local_path(self) -> str:
        """A local path, absolute and POSIX-spelled, both because they compare."""
        path = self.path or "."
        if DRIVE.match(path) or path.startswith("/"):
            return _posix(os.path.normpath(path))
        return _posix(os.path.abspath(path))

    def _spelled(self, secret: str | None = None) -> str:
        """The URI, with the password written as `secret` when one is given."""
        if self.scheme in LOCAL:
            # A relative path stays relative in the field and becomes absolute
            # here, because there is no such thing as a relative `file://` URI.
            # It is POSIX by then, so nothing is left for `quote` to escape --
            # a `%5C` in the middle of a path is a spelling nothing reads back.
            local = self._local_path()
            spelled = urllib.parse.quote(local, safe="/:" if DRIVE.match(local) else "/")
            return f"file:///{spelled.lstrip('/')}"
        quoted = urllib.parse.quote(self.path, safe="/")
        credentials = ""
        if self.user is not None:
            password = secret if secret is not None else self.password
            credentials = urllib.parse.quote(self.user, safe="")
            if self.password is not None:
                credentials += ":" + (
                    password if secret is not None else urllib.parse.quote(self.password, safe="")
                )
            credentials += "@"
        location = f"{self.scheme}://{credentials}{self.netloc}/{quoted.lstrip('/')}"
        if self.query:
            location += "?" + urllib.parse.urlencode(self.query)
        return location

    def __repr__(self) -> str:
        """Masked, always: a `repr` is what a log and a traceback print."""
        return f"Url({self.masked!r})"


def _credentials(netloc: str) -> tuple[str | None, str | None]:
    """The userinfo of a netloc, split on the first colon and decoded.

    On the *first*, because everything after it is the secret -- and a secret
    with a colon in it is a secret, not a malformed URL. Decoded, because a
    secret with a `/` or an `@` has to arrive percent-encoded to survive the
    URL at all.
    """
    userinfo, separator, _ = netloc.rpartition("@")
    if not separator:
        return None, None
    user, colon, password = userinfo.partition(":")
    return urllib.parse.unquote(user), urllib.parse.unquote(password) if colon else None


def _host(parsed: urllib.parse.SplitResult) -> str:
    """The host of a netloc, as it was written.

    `SplitResult.hostname` lowercases it, and a bucket is a host here -- so a
    bucket that was named with a capital in it would be silently addressed as
    a different bucket. The raw spelling is used whenever it is the same name,
    which is every case except one nobody can write on purpose.
    """
    hostname = parsed.hostname or ""
    written = parsed.netloc.rpartition("@")[2]
    if parsed.port is not None:
        written = written.rpartition(":")[0]
    written = written.removeprefix("[").removesuffix("]")
    return written if written.lower() == hostname else hostname


def _netloc(host: str, port: int | None) -> str:
    """`host`, `host:port`, or nothing.

    An IPv6 host is put back in the brackets that told it from a port in the
    first place, so what this spells parses back to what it is.
    """
    if not host:
        return ""
    host = f"[{host}]" if ":" in host else host
    return f"{host}:{port}" if port else host


def _bare(netloc: str) -> str:
    """A netloc as just its host -- no port, no brackets around an IPv6 one."""
    host = netloc.rsplit(":", 1)[0] if not netloc.endswith("]") else netloc
    return host.removeprefix("[").removesuffix("]")


def _amazon(host: str) -> tuple[str, str | None]:
    """`(store, region)` for one of Amazon's own S3 hostnames."""
    matched = AWS_HOST.match(host)
    if matched is None:
        return "", None
    bucket = matched["bucket"] or ""
    region = matched["region"]
    if region is None and matched["qualifier"] and REGION.match(matched["qualifier"]):
        region = matched["qualifier"]
    return host[len(bucket) + 1 :] if bucket else host, region


def _split_host(host: str) -> tuple[str, str]:
    """A host as `(the bucket it names, the store it names)`."""
    store, _ = _amazon(host)
    if store:
        # `- 1` for the dot the bucket label is joined on; without a bucket the
        # store *is* the host, and slicing a length off it would take a
        # character of the hostname with it.
        return ("" if store == host else host[: len(host) - len(store) - 1]), store
    if STORE_HOST.search(host):
        return "", host
    return host, ""


def _posix(path: str) -> str:
    """A local path spelled with `/`, where a backslash is a separator.

    On Windows it always is. Elsewhere it is a character a filename may legally
    contain, and the one exception is a path behind a drive letter -- which is
    a Windows path somebody carried across, and is read as one on either host
    so that a recorded location means the same thing wherever it is compared.
    """
    if _WINDOWS or DRIVE.match(path):
        return path.replace("\\", "/")
    return path


def _drive_path(path: str) -> str:
    """A local path with the slash a URI split leaves before a drive removed."""
    return ROOTED_DRIVE.sub("", path)


def _plain(endpoint: str) -> bool:
    """Whether an endpoint is one nobody put TLS in front of.

    A hostname without a domain -- `minio:9000`, `localhost:9000` -- is a
    container or a laptop, and neither has a certificate. Anything with a dot
    in it is treated as a real host, so the guess never downgrades a URL that
    reaches the internet. `?scheme=` says it outright, and wins.
    """
    host = endpoint.rsplit(":", 1)[0]
    return "." not in host or host.startswith("127.") or host == "localhost"


def _endpoint_parts(endpoint: str) -> tuple[str, str]:
    """An endpoint URL as Arrow's separate transport and connect string."""
    parsed = urllib.parse.urlsplit(endpoint)
    if parsed.scheme in HTTP and parsed.netloc:
        return parsed.scheme, f"{parsed.netloc}{parsed.path}".rstrip("/")
    return ("http" if _plain(endpoint) else "https"), endpoint.rstrip("/")


@functools.lru_cache(maxsize=64)
def _region_of(bucket: str) -> str | None:
    """The region a bucket lives in, when Arrow can be asked and knows.

    `FileSystem.from_uri` resolves it, so building the filesystem here would
    lose it -- and a bucket read against the wrong region fails on every call.
    A resolution that cannot happen (no network, no such bucket, MinIO) is not
    an error here: Arrow's own default is what a caller who named no region
    would have got anyway.
    """
    if not bucket:
        return None
    try:
        return pyarrow.fs.resolve_s3_region(bucket)
    except Exception:  # noqa: BLE001 - any failure means "nobody knows", not "refuse"
        return None


def s3_environment(environ: Mapping[str, str] = os.environ, prefix: str = "s3") -> dict[str, str]:
    """Portable S3 process defaults in Iceberg catalog spelling."""
    settings = {
        f"{prefix}.{suffix}": value
        for name, suffix in S3_ENVIRONMENT
        if (value := environ.get(name))
    }
    endpoint = next(
        (value for name in S3_ENDPOINT_ENVIRONMENT if (value := environ.get(name))), None
    )
    if endpoint:
        settings[f"{prefix}.endpoint"] = endpoint
    return settings


def properties_of(url: Url, prefix: str = "s3") -> Mapping[str, str]:
    """What a location says, as the catalog properties that say the same thing.

    pyiceberg configures its filesystems from properties (`s3.endpoint`,
    `s3.access-key-id`), and a warehouse URL that carries an endpoint and
    credentials is saying exactly those. This is the translation, so a caller
    can hand one location to a catalog rather than repeating it as three
    settings.
    """
    settings: dict[str, str] = {}
    endpoint = url.endpoint
    # Amazon's own hostnames are left out for the reason `_s3_filesystem`
    # leaves them out: pyiceberg passes `s3.endpoint` straight to
    # `endpoint_override`, and overriding AWS with AWS only forces path-style
    # addressing. The region below is the part of such a location that has to
    # be carried, and it is carried.
    if endpoint is not None and not _amazon(_bare(endpoint))[0]:
        scheme = url.query.get("scheme", "http" if _plain(endpoint) else "https")
        settings[f"{prefix}.endpoint"] = f"{scheme}://{endpoint}"
    if url.user is not None:
        settings[f"{prefix}.access-key-id"] = url.user
        settings[f"{prefix}.secret-access-key"] = url.password or ""
    region = url.region
    if region:
        settings[f"{prefix}.region"] = region
    return settings
