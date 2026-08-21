"""One place a location is parsed, decoded and put back together.

A path is not a string. `file:///C:/warehouse`, `C:\\warehouse`,
`s3://bucket/key`, `s3://key:secret@minio:9000/bucket/key` and `logs/app.txt`
all name a file, and every one of them is read differently -- so anything that
compares two locations, or hands one to a filesystem, has to reduce both
through the *same* parser. That parser is here, and `Url` is what it produces:
a mutable dataclass whose fields are the parts, whose `into_filesystem` hands
back the `pyarrow.fs` handle and the path that filesystem understands, and
whose `join` walks it without any call site doing string arithmetic.

Three things it gets right that a `urlparse` at the call site does not:

- **A secret may contain a colon.** Userinfo splits on the *first* one, so
  `s3://AKIA:sec:ret@bucket/key` is the key `AKIA` and the secret `sec:ret`,
  and every part is percent-decoded (`%2F` in a secret, `%20` in a name).
- **A port means an endpoint, not a bucket.** `s3://key:secret@minio:9000/logs`
  is MinIO holding a bucket called `logs`; `s3://logs/app.txt` is AWS holding
  the same bucket. `pyarrow`'s own URI parser reads the endpoint host *as* the
  bucket and drops the port, which turns a MinIO warehouse into a bucket named
  `minio` -- silently, since the name is legal.
- **A Windows drive is not a scheme.** `C:/warehouse` parses as scheme `c`
  everywhere else; here it is a local path, and `file:///C:/x` sheds the slash
  a URI split leaves in front of the drive.

A password never reaches a log by accident: `repr` masks it, and only
`into_string()` writes it out.
"""

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

#: Schemes pyarrow serves with `S3FileSystem`, which is the one whose URI
#: parsing this module has to correct.
S3 = frozenset({"s3", "s3a", "s3n"})

#: Query keys an S3 location may carry, and what they configure. Anything else
#: is left in `query` for whoever put it there.
S3_SETTINGS = ("region", "scheme", "endpoint_override", "allow_bucket_creation")


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
        if self.scheme in LOCAL and DRIVE.match(self.path):
            # A Windows path is the one place a backslash is a separator and
            # not a character in a name, so it is normalised where it is known
            # to be one -- and nowhere else.
            self.path = self.path.replace("\\", "/")

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
        """`host`, `host:port`, or nothing.

        An IPv6 host is put back in the brackets that told it from a port in
        the first place, so what this spells parses back to what it is.
        """
        if not self.host:
            return ""
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{host}:{self.port}" if self.port else host

    @property
    def endpoint(self) -> str | None:
        """The store this location names, when it names one rather than a bucket.

        `?endpoint_override=` says it outright; otherwise a port in the netloc
        says it, and `hosts_a_store` is then true because the bucket has moved
        into the path. Without either, the host *is* the bucket, which is what
        an `s3://bucket/key` URL means everywhere.
        """
        override = self.query.get("endpoint_override")
        if override:
            return override
        return self.netloc if self.hosts_a_store else None

    @property
    def hosts_a_store(self) -> bool:
        """Whether the netloc is the store rather than the bucket.

        A port is what says so, and only a port: `?endpoint_override=` is a
        setting added *beside* the location, so `s3://bucket/key?...` still
        names the bucket `bucket`. Nothing else is read -- a bucket name may
        carry dots, so the shape of a hostname decides nothing.
        """
        return bool(self.host and self.port)

    @property
    def bucket(self) -> str:
        """The bucket this location is in -- an object store's view of it.

        The host, or the first path segment when the host was the store.
        A local path has no bucket and says so with an empty string.
        """
        if not self.hosts_a_store:
            return self.host
        return self.path.strip("/").split("/", 1)[0]

    @property
    def key(self) -> str:
        """Everything below the bucket -- an object store's view, like `bucket`."""
        stripped = self.path.strip("/")
        if not self.hosts_a_store:
            return stripped
        _, _, rest = stripped.partition("/")
        return rest

    @property
    def store_path(self) -> str:
        """The path the location's own filesystem understands.

        A local file keeps its whole path, drive letter included. An object
        store wants `bucket/key`, which is the path with the bucket put back
        in front of it when the host was an endpoint rather than the bucket.

        This is the path for the filesystems this module builds itself -- the
        local one and S3. For a scheme Arrow builds, `into_filesystem` hands
        back Arrow's own answer, which is the one that filesystem takes.
        """
        if self.scheme in LOCAL:
            return self._local_path()
        if self.hosts_a_store:
            return self.path.strip("/")
        return "/".join(part for part in (self.host, self.path.strip("/")) if part)

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
        """The `pyarrow.fs` handle for this location, and the path on it.

        Arrow owns every scheme it already knows, so this delegates -- with
        two exceptions it demonstrably reads wrong. A **local** path is built
        directly, because a Windows drive letter has to survive. An **S3**
        location that names an endpoint or carries credentials is built
        directly too: Arrow's parser takes `minio:9000` for a bucket, drops
        the port, and never sees the endpoint at all.
        """
        if self.scheme in LOCAL:
            return pyarrow.fs.LocalFileSystem(), self._local_path()
        if self.scheme in S3 and (self.endpoint is not None or self.user is not None):
            return self._s3_filesystem(), self.store_path
        filesystem, path = pyarrow.fs.FileSystem.from_uri(self.into_string())
        return filesystem, path

    def _s3_filesystem(self) -> pyarrow.fs.FileSystem:
        """`S3FileSystem` configured from the parts of the location itself.

        The region is only asked for when the location does not say and there
        is no endpoint: an AWS bucket has one to resolve, a MinIO endpoint does
        not, and a resolution that fails is not a reason to refuse the write --
        Arrow's own default stands.
        """
        settings: dict[str, Any] = {
            key: self.query[key] for key in S3_SETTINGS if key in self.query
        }
        settings.pop("endpoint_override", None)
        if self.user is not None:
            settings["access_key"] = self.user
            settings["secret_key"] = self.password or ""
        endpoint = self.endpoint
        if endpoint is not None:
            settings["endpoint_override"] = endpoint
            settings.setdefault("scheme", "http" if _plain(endpoint) else "https")
        elif "region" not in settings:
            settings["region"] = _region_of(self.bucket)
            if settings["region"] is None:
                settings.pop("region")
        if "allow_bucket_creation" in settings:
            settings["allow_bucket_creation"] = settings["allow_bucket_creation"] == "true"
        return pyarrow.fs.S3FileSystem(**settings)

    def _local_path(self) -> str:
        """A local path, absolute, because that is what a filesystem takes."""
        if DRIVE.match(self.path):
            return self.path
        return os.path.abspath(self.path or ".")

    def _spelled(self, secret: str | None = None) -> str:
        """The URI, with the password written as `secret` when one is given."""
        if self.scheme in LOCAL:
            # A relative path stays relative in the field and becomes absolute
            # here, because there is no such thing as a relative `file://` URI.
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
    if endpoint is not None:
        scheme = url.query.get("scheme", "http" if _plain(endpoint) else "https")
        settings[f"{prefix}.endpoint"] = f"{scheme}://{endpoint}"
    if url.user is not None:
        settings[f"{prefix}.access-key-id"] = url.user
        settings[f"{prefix}.secret-access-key"] = url.password or ""
    region = url.query.get("region")
    if region:
        settings[f"{prefix}.region"] = region
    return settings
