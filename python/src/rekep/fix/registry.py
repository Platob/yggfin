"""Every FIX field of every FIX version, scraped once and kept for offline use."""

from __future__ import annotations

import concurrent.futures
import dataclasses
import html
import importlib.resources
import io
import json
import os
import pathlib
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
import warnings
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from functools import cache, cached_property
from types import MappingProxyType
from typing import Any, Self

import pyarrow.fs

from rekep.convert import Convertible
from rekep.enums import EventType, State
from rekep.fields import Field
from rekep.filesystems import local_path, read_bytes, resolve, write_bytes
from rekep.fix.entries import (
    ANY_VERSION,
    NAMESPACE,
    Alias,
    ComponentEntry,
    FieldEntry,
    fold,
    name_of,
    newest_rank,
)
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import (
    QUICKFIX_URL,
    SpecComponent,
    SpecField,
    SpecGroup,
    declared_group,
    first_declared_name,
    parse_components,
    parse_session,
    parse_spec,
    spec_name,
)
from rekep.fix.store import (
    COMPONENTS,
    DECLARED,
    DOCUMENT_SUFFIX,
    FIELDS,
    SESSIONS,
    STORED,
    VERSIONS_FILE,
    ArchiveDocuments,
    ConflictReport,
    DirectoryDocuments,
    Documents,
    ShardedLayout,
    collapse,
    document_of,
    documents_of,
    field_document,
    slug_collisions,
    write_archive,
)
from rekep.urls import HTTP, LOCAL, Url

#: The dictionary that is scraped: OnixS publishes every FIX version as one
#: page per version listing the tags, and one page per field carrying the
#: name, datatype, description and enumerated values.
BASE_URL = "https://www.onixs.biz/fix-dictionary"

# Sent with every request so scrape traffic identifies its client.
_USER_AGENT = "rekep-fix-registry (+https://github.com/Platob/yggfin)"

#: Where the scrape persists: the tag shards, the components and the version
#: list, so everything after the first scrape works offline -- including on a
#: machine that was never online, by copying the directory.
CACHE_DIRECTORY = pathlib.Path.home() / ".config" / "fix"

#: Optional full-registry archive and bearer token used to fill a cold default store.
REGISTRY_URL_ENVIRONMENT = "REKEP_FIX_REGISTRY_URL"
REGISTRY_TOKEN_ENVIRONMENT = "REKEP_FIX_REGISTRY_TOKEN"

#: A registry is currently under 3 MiB expanded; this bounds corrupt or hostile archives.
_REGISTRY_ARCHIVE_MAX_BYTES = 64 * 1024 * 1024
_REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES = 16 * 1024 * 1024
_REGISTRY_ARCHIVE_MAX_DOCUMENTS = 10_000
_REGISTRY_ARCHIVE_READ_BYTE_SIZE = 64 * 1024


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse authenticated redirects so a bearer token reaches one HTTPS URL."""

    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


#: What one bootstrap costs, said before it starts. The dictionary is a page
#: per field per version and the site throttles a long walk, so the number is
#: an order of magnitude and the duration a warning, not a promise.
BOOTSTRAP_PAGES = 7000
BOOTSTRAP_DURATION = "several minutes"

#: Versions in a per-version directory name: `4.4`, `5.0.SP2`, `FIXT1.1`.
_VERSION_LINK = re.compile(r"/fix-dictionary/([^/\"'#]+)/index\.html")

#: One field on a `fields_by_tag.html` page: the link and the text inside it.
_TAG_LINK = re.compile(r"<a[^>]+href=\"tagNum_(\d+)\.html\"[^>]*>(.*?)</a>", re.DOTALL)

#: The `Type: <a ...>char</a>` line of a field page, tags tolerated anywhere.
_TYPE = re.compile(r"Type:\s*(?:<[^>]*>\s*)*([A-Za-z][A-Za-z0-9]*)")

#: The field page title: `FIX 4.4 : TimeInForce <59> field`, entities or not.
_TITLE = re.compile(r"<h\d[^>]*>(?:[^<]*:)?\s*([^<>&]+?)\s*(?:&lt;|<)\s*(\d+)\s*(?:&gt;|>)")

#: One enumerated value: `0 = Day (or session)`. Some versions spell an
#: enumeration as a list and others as one paragraph per value, so an item is
#: either -- a scrape that only read `<li>` came back from the live pages with
#: every field's values empty and no error to say so.
_VALUE_ITEM = re.compile(r"<(li|p)[^>]*>\s*(.*?)\s*</\1\s*>", re.DOTALL | re.IGNORECASE)
_VALUE = re.compile(r"^\s*(\S+)\s*=\s*(.+?)\s*$", re.DOTALL)

#: What opens each part of a field page. The prose, the enumeration and the
#: messages are markers in one flat run of paragraphs, not containers, so each
#: part runs from its own opener to the next one. The `Description` heading is
#: preferred over the anchor that precedes it: cutting at the anchor leaves the
#: heading's own text in front of the prose.
_DESCRIPTION_HEADING = re.compile(r"<h\d[^>]*>\s*Description\s*</h\d\s*>", re.IGNORECASE)
_DESCRIPTION_ANCHOR = re.compile(r"<a[^>]+name=[\"']Description[\"'][^>]*>", re.IGNORECASE)
_VALID_VALUES = re.compile(r"Valid values[^<]*", re.IGNORECASE)

#: The messages section: the anchor the page links to, or the heading itself.
#: Never the `<a href="#UsedIn">` link that sits *above* the description -- it
#: is navigation, and cutting there would take the description with it.
_USED_IN = re.compile(
    r"<a[^>]+name=[\"']UsedIn[\"'][^>]*>|<h\d[^>]*>\s*Used\s+in\s*</h\d\s*>", re.IGNORECASE
)

#: What a `Used In` link points at: one message, or one component block. Both
#: kinds sit in the same section, and reading only the first left every field
#: FIX carries inside a component recorded as used nowhere.
MESSAGE_LINK = "msgType"
COMPONENT_LINK = "compBlock"

#: A component link's whole text is `<Name>`; the name is what is inside it.
_COMPONENT_NAME = re.compile(r"^\s*<\s*(.+?)\s*>\s*$")

#: A parenthetical note beside a name on the by-tag page -- `(no longer
#: used)`, `(replaced)` -- which is the one deprecation signal the site has.
_NOTE = re.compile(r"\(([^)]*)\)\s*$")

#: The tiers `resolve` walks, in the order it walks them: an identity's own
#: name, then a declared alias -- a rendered spelling, a legacy name, or the
#: name an older version gave the tag, which the collapse records as one.
_CANONICAL = "canonical"
_ALIASED = "aliased"
TIERS: tuple[str, ...] = (_CANONICAL, _ALIASED)

#: Where a field FIX never numbered sorts among the ones it did: after all of
#: them, in one place, rather than at tag zero beside `BeginString`.
_NO_TAG = 1 << 31


def builtin_projection() -> str:
    """Where the packaged projection lives: 900 entries against the whole 6802.

    `rekep.fix.publish` calls it a projection because that is what it is -- it
    parses common traffic and misses the long tail -- and nothing here ever
    presents it as the whole dictionary.
    """
    return os.fspath(importlib.resources.files(__package__).joinpath("registry.zip"))


_TAGS = re.compile(r"<[^>]+>")
_SPACES = re.compile(r"\s+")
_DEFAULT = object()


@dataclasses.dataclass(eq=False)
class FixRegistry(Convertible):
    """The FIX dictionary as local `Field` declarations."""

    #: Where the dictionary lives; override to scrape a mirror.
    base_url: str = BASE_URL

    #: Where the QuickFIX spec files live, which is the *second* source. The
    #: dictionary is prose written for people and the spec is the same standard
    #: written for programs, so each has what the other lacks: descriptions
    #: there, the symbolic name of every enumerated value here. One file per
    #: version against the site's one page per field, so this costs a request
    #: and enriches a whole version.
    spec_url: str = QUICKFIX_URL

    #: Where scrapes persist: a directory of JSON, or a `.zip` of the same
    #: files. The extension is what says which -- like every other inference
    #: here -- so one path names either, and a dictionary that travels as one
    #: file and a dictionary that travels as a directory are the same
    #: dictionary. `None` means `CACHE_DIRECTORY`, and is the only spelling
    #: that asks to be bootstrapped: a store somebody named is that store,
    #: cold or not.
    cache_dir: str | os.PathLike[str] | None = None

    #: Optional filesystem for `cache_dir`, whose value is then a path on it.
    filesystem: pyarrow.fs.FileSystem | None = None

    #: Full registry archive tried before scraping when the default store is empty.
    registry_url: str | None = None

    #: Optional bearer token for `registry_url`; consumed and never serialised.
    registry_token: dataclasses.InitVar[str | None] = None

    #: Seconds one page fetch may take, and how many fetch at once. The site
    #: is a static dictionary; eight lanes drain a version in seconds without
    #: leaning on it.
    timeout: float = 30.0
    max_workers: int = 8

    #: How many times a fetch that was refused *for now* is asked again, and
    #: the first pause before it is. The pause doubles per attempt (capped at
    #: a minute each), so six retries wait about two minutes in total: the
    #: dictionary is seven thousand pages, the site throttles harder the
    #: further in a scrape gets, and half a minute of patience was measured
    #: to be too little to finish one. Still short enough that a site which is
    #: really down is reported as down rather than waited on.
    retries: int = 6
    backoff: float = 2.0

    #: Whether this registry may reach the site at all. False is the default
    #: and what a person at a prompt wants: ask for `4.4`, get `4.4`, paying
    #: for one scrape at construction and never again.
    #:
    #: True is what a **pipeline** wants, and it is not the same wish. A parse
    #: that meets its first bridge line must not answer it by starting a
    #: seven-thousand-page scrape in the middle of a batch -- and because the
    #: only scrape happens while this is being built, a pipeline that got past
    #: construction never meets one. An offline registry with no store serves
    #: the packaged projection and says so.
    offline: bool = False

    #: How long a local store may go without being checked against upstream,
    #: in seconds. `0` -- the default -- never refetches: the local copy is
    #: what this registry serves, which is the whole of what "offline-first"
    #: means and what every pipeline reading a packaged dictionary wants.
    #:
    #: Above zero, a store older than this is regenerated from the spec before
    #: it is served, once per registry. A refetch that fails is reported and
    #: the local copy is served anyway: a dictionary that is a day stale still
    #: parses every message, and one that raises parses none.
    cache_ttl: float = 0.0

    #: Where a bootstrap's start and finish lines are surfaced. `stderr` by
    #: default, so a person waiting on the fetch sees it; the CLI and the
    #: notebooks pass their own writer. `warnings.warn` carries the same lines
    #: for the record, because a warning is filterable and shows once, which is
    #: wrong for an operation somebody is waiting on.
    announce: Any = None

    def __post_init__(self, registry_token: str | None) -> None:
        """Normalise the locations once, then bootstrap the default store."""
        self.base_url = Url.from_string(str(self.base_url)).into_string().rstrip("/")
        self.spec_url = Url.from_string(str(self.spec_url)).into_string().rstrip("/")
        if self.registry_url is None:
            self.registry_url = os.environ.get(REGISTRY_URL_ENVIRONMENT)
        self.__dict__["_registry_token"] = (
            registry_token
            if registry_token is not None
            else os.environ.get(REGISTRY_TOKEN_ENVIRONMENT)
        )
        if self.registry_url:
            source = Url.from_string(self.registry_url)
            if source.user is not None:
                raise ValueError("registry_url cannot contain credentials; use registry_token")
            if source.query:
                raise ValueError("registry_url cannot contain a query")
            if self.__dict__["_registry_token"] and source.scheme == "http":
                raise ValueError("registry_token requires an HTTPS registry_url")
            self.registry_url = source.into_string()
        if self.cache_dir is None:
            self.cache_dir = CACHE_DIRECTORY
        if self.cache_ttl < 0:
            raise ValueError(f"a FIX registry cache TTL cannot be negative: {self.cache_ttl}")
        self.__dict__["_installed"] = self.bootstrap()

    @property
    def revision(self) -> int:
        """Generation of the in-memory views over this mutable store."""
        return self.__dict__.get("_revision", 0)

    @classmethod
    @cache
    def from_builtin(cls, cache_ttl: float = 0.0) -> Self:
        """The packaged field projection, offline and shared by declarations.

        `cache_ttl` above zero makes this registry check its age against the
        spec before serving -- which needs the network, so it also lifts
        `offline`. Zero, the default, is the packaged copy and nothing else.
        """
        return cls(cache_dir=builtin_projection(), offline=not cache_ttl, cache_ttl=cache_ttl)

    @classmethod
    def scrape(
        cls,
        dump_folder: str | os.PathLike[str] | None = None,
        **configuration: Any,
    ) -> Self:
        """Scrape a fresh registry and replace one local directory with it."""
        reserved = {"cache_dir", "filesystem", "offline"} & configuration.keys()
        if reserved:
            raise TypeError(f"scrape configures {sorted(reserved)} through dump_folder")
        location = Url.from_string(os.fspath(dump_folder or CACHE_DIRECTORY))
        if location.scheme not in LOCAL or pathlib.PurePath(location.path).suffix.lower() == ".zip":
            raise ValueError("FixRegistry.scrape requires a local dump folder")
        target = pathlib.Path(local_path(location.into_string()))
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and (not target.is_dir() or target.is_symlink()):
            raise ValueError(f"the FIX registry dump target is not a directory: {target}")

        with tempfile.TemporaryDirectory(prefix=f".{target.name}-", dir=target.parent) as scratch:
            root = pathlib.Path(scratch)
            staged = root / target.name
            source = cls(cache_dir=staged, offline=False, **configuration)
            source.rebuild()
            cls._validate_registry_store(source._documents)

            previous = root / "previous"
            if target.exists():
                target.replace(previous)
            try:
                staged.replace(target)
            except BaseException:
                if previous.exists() and not target.exists():
                    previous.replace(target)
                raise
        return cls(cache_dir=target, offline=True, **configuration)

    # -- bootstrapping the default store --------------------------------------

    def bootstrap(self) -> bool:
        """Fill the default store, once, saying so before and after; True when it did.

        A cold online store tries the configured full archive, then the source
        dictionaries. A failed source or a cold offline store serves the
        packaged projection and says that it is reduced.

        Only the *default* store is bootstrapped. A `cache_dir` somebody named
        is that store, cold or not -- it is about to be written, or it is a
        projection that is complete for what it projects.

        Both channels carry it: `warnings.warn` is the record, and `announce`
        is the foreground line a person waiting on the fetch reads.
        """
        if os.fspath(self.cache_dir) != os.fspath(CACHE_DIRECTORY):
            return False
        if self._documents.names():
            return False
        if not self.offline and self.registry_url:
            source = Url.from_string(self.registry_url)
            self._say(
                f"no FIX registry at {self.cache_dir}; downloading the full dictionary "
                f"from {source.masked}"
            )
            started = time.monotonic()
            try:
                counted = self._install_registry_archive()
            except (OSError, ValueError, zipfile.BadZipFile, pyarrow.ArrowException) as error:
                self._say(
                    f"the FIX registry archive at {source.masked} could not be installed "
                    f"({error}); falling back to the source dictionaries"
                )
            else:
                self._say(
                    f"the FIX registry is installed at {self.cache_dir}: {counted} documents, "
                    f"in {time.monotonic() - started:.0f}s"
                )
                return True
        if self.offline:
            self._reduced("this registry is offline")
            return False
        self._say(
            f"no FIX registry at {self.cache_dir}; fetching the dictionary from "
            f"{self.base_url} and the spec from {self.spec_url} -- about {BOOTSTRAP_PAGES} "
            f"pages across every FIX version, {BOOTSTRAP_DURATION}. It installs to "
            f"{self.cache_dir} and is never fetched again. To skip it, pass offline=True "
            "or point cache_dir at a store you already have."
        )
        started = time.monotonic()
        try:
            report = self.rebuild()
        except (OSError, ValueError) as error:
            self._reduced(f"{self.base_url} could not be read ({error})")
            return False
        counted = len(self._layout.field_entries)
        self._say(
            f"the FIX registry is installed at {self.cache_dir}: {counted} fields, "
            f"{len(self._layout.component_entries)} components, "
            f"{sum(report.counts().values())} collapses, in {time.monotonic() - started:.0f}s"
        )
        return True

    def _install_registry_archive(self) -> int:
        """Download, validate and expand the configured registry archive."""
        if not self.registry_url:  # pragma: no cover - guarded by bootstrap
            raise ValueError("a FIX registry archive URL is required")
        payload = self._registry_archive_payload()
        documents = self._registry_archive_documents(payload)
        return self._install_registry_documents(documents)

    def _registry_archive_payload(self) -> bytes:
        """Read the compressed archive in bounded chunks."""
        if not self.registry_url:  # pragma: no cover - guarded by bootstrap
            raise ValueError("a FIX registry archive URL is required")
        source = Url.from_string(self.registry_url)
        token = self.__dict__.get("_registry_token")
        if source.scheme in HTTP:
            headers = {"User-Agent": _USER_AGENT}
            if token and source.scheme == "https":
                headers["Authorization"] = f"Bearer {token}"
            request = urllib.request.Request(self.registry_url, headers=headers)
            if "Authorization" in headers:
                response = urllib.request.build_opener(_NoRedirect()).open(
                    request, timeout=self.timeout
                )
            else:
                response = urllib.request.urlopen(request, timeout=self.timeout)  # noqa: S310
            with response:
                return self._bounded_registry_archive(response)
        filesystem, path = resolve(self.registry_url)
        info = filesystem.get_file_info(path)
        if info.type == pyarrow.fs.FileType.NotFound:
            raise FileNotFoundError(self.registry_url)
        if info.size > _REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES:
            raise ValueError(
                "the FIX registry archive exceeds "
                f"{_REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES} compressed bytes"
            )
        with filesystem.open_input_stream(path) as stream:
            return self._bounded_registry_archive(stream)

    @staticmethod
    def _bounded_registry_archive(stream: Any) -> bytes:
        """Read at most the configured compressed archive size."""
        length = getattr(stream, "headers", {}).get("Content-Length")
        if str(length or "").isdigit() and int(length) > _REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES:
            raise ValueError(
                "the FIX registry archive exceeds "
                f"{_REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES} compressed bytes"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            read_size = min(
                _REGISTRY_ARCHIVE_READ_BYTE_SIZE,
                _REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES - total + 1,
            )
            try:
                chunk = stream.read(read_size)
            except (OSError, EOFError, RuntimeError, pyarrow.ArrowException) as error:
                raise OSError(f"the FIX registry archive download failed: {error}") from error
            if not chunk:
                break
            payload = bytes(chunk)
            total += len(payload)
            if total > _REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES:
                raise ValueError(
                    "the FIX registry archive exceeds "
                    f"{_REGISTRY_ARCHIVE_MAX_COMPRESSED_BYTES} compressed bytes"
                )
            chunks.append(payload)
        return b"".join(chunks)

    def _install_registry_documents(self, documents: Mapping[str, Mapping[str, Any]]) -> int:
        """Stage a complete default store beside its final path, then rename it."""
        if self.filesystem is not None:
            raise ValueError("a downloaded FIX registry requires the local default store")
        target = pathlib.Path(os.fspath(self.cache_dir))
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=f".{target.name}-", dir=target.parent) as scratch:
            staged = pathlib.Path(scratch) / target.name
            place = DirectoryDocuments(pyarrow.fs.LocalFileSystem(), staged.as_posix())
            for name in sorted(documents):
                place.write(name, documents[name])
            if set(place.names()) != set(documents):
                raise OSError("the staged FIX registry is incomplete")
            counted = self._validate_registry_store(place)
            try:
                target.rmdir()
            except FileNotFoundError:
                pass
            except OSError:
                return self._accept_registry_winner(target)
            try:
                staged.replace(target)
            except OSError:
                return self._accept_registry_winner(target)
        self._forget()
        return counted

    def _accept_registry_winner(self, target: pathlib.Path) -> int:
        """Accept a complete store installed by a concurrent process."""
        place = DirectoryDocuments(pyarrow.fs.LocalFileSystem(), target.as_posix())
        counted = self._validate_registry_store(place)
        self._forget()
        return counted

    @staticmethod
    def _validate_registry_store(place: Documents) -> int:
        """Validate every staged document as the record it declares."""
        names = place.names()
        allowed_index = {"versions", SESSIONS, STORED, DECLARED}
        index = place.read(VERSIONS_FILE)
        if not isinstance(index, Mapping):
            raise ValueError("the FIX registry has no readable version index")
        unknown = sorted(set(index) - allowed_index)
        if unknown:
            raise ValueError(f"the FIX registry version index declares unknown {unknown}")
        versions = index.get("versions")
        if (
            not isinstance(versions, list)
            or not versions
            or any(type(version) is not str or not version.strip() for version in versions)
            or len(set(versions)) != len(versions)
        ):
            raise ValueError("the FIX registry version index needs distinct version names")
        known_versions = set(versions)
        for key in (STORED, DECLARED):
            declared = index.get(key, [])
            if (
                not isinstance(declared, list)
                or any(type(version) is not str for version in declared)
                or len(set(declared)) != len(declared)
                or not set(declared).issubset(known_versions)
            ):
                raise ValueError(f"the FIX registry version index has invalid {key}")
        sessions = index.get(SESSIONS, {})
        if not isinstance(sessions, Mapping) or not set(sessions).issubset(known_versions):
            raise ValueError("the FIX registry version index has invalid sessions")
        for version, members in sessions.items():
            if not isinstance(members, list):
                raise ValueError(f"the FIX {version} session is not a sequence")
            seen: set[str] = set()
            for member in members:
                if (
                    not isinstance(member, list | tuple)
                    or len(member) != 2
                    or type(member[0]) is not str
                    or not member[0].strip()
                    or type(member[1]) is not bool
                    or member[0] in seen
                ):
                    raise ValueError(f"the FIX {version} session has an invalid field")
                seen.add(member[0])

        fields: dict[int | str, FieldEntry] = {}
        components: dict[str, ComponentEntry] = {}
        component_tags: set[int] = set()
        component_refs: set[str] = set()
        for name in names:
            if name == VERSIONS_FILE:
                continue
            document = place.read(name)
            if not isinstance(document, Mapping) or not document:
                raise ValueError(f"FIX registry document {name!r} is empty or unreadable")
            if name.startswith(f"{FIELDS}/"):
                for stored, record in document.items():
                    if not isinstance(record, Mapping):
                        raise ValueError(f"FIX field {stored!r} in {name!r} is not an object")
                    record_versions = record.get("versions")
                    if (
                        type(record.get("name")) is not str
                        or not isinstance(record_versions, list)
                        or any(type(version) is not str for version in record_versions)
                        or not set(record_versions).issubset(known_versions | {ANY_VERSION})
                        or not isinstance(record.get("aliases", []), list)
                        or any(
                            key in record and not isinstance(record[key], Mapping)
                            for key in (
                                "values",
                                "value_names",
                                "event_types",
                                "states",
                                "encoded",
                                "decoded",
                            )
                        )
                        or any(
                            key in record and not isinstance(record[key], list)
                            for key in ("used_in", "components")
                        )
                    ):
                        raise ValueError(f"FIX field {stored!r} in {name!r} has invalid metadata")
                    try:
                        entry = FieldEntry.from_dict(record)
                    except (AttributeError, KeyError, TypeError, ValueError) as error:
                        raise ValueError(
                            f"FIX field {stored!r} in {name!r} is invalid: {error}"
                        ) from error
                    expected_key = str(entry.tag) if entry.tag is not None else entry.name
                    if str(stored) != expected_key or field_document(entry) != name:
                        raise ValueError(f"FIX field {stored!r} is stored in the wrong shard")
                    if entry.key in fields:
                        raise ValueError(f"FIX field {stored!r} is stored more than once")
                    fields[entry.key] = entry
                continue
            if not name.startswith(f"{COMPONENTS}/"):
                raise ValueError(f"unexpected FIX registry document {name!r}")
            unknown = sorted(set(document) - {"name", "versions", "members", "msg_type", "aliases"})
            component_versions = document.get("versions")
            members = document.get("members", [])
            if (
                unknown
                or type(document.get("name")) is not str
                or not isinstance(component_versions, list)
                or any(type(version) is not str for version in component_versions)
                or not set(component_versions).issubset(known_versions | {ANY_VERSION})
                or not isinstance(members, list)
                or not isinstance(document.get("aliases", []), list)
            ):
                raise ValueError(f"FIX component in {name!r} has invalid metadata")
            pending = list(members)
            while pending:
                member = pending.pop()
                if not isinstance(member, Mapping):
                    raise ValueError(f"FIX component in {name!r} has a non-object member")
                kind = member.get("kind")
                allowed = {"kind", "name", "required"}
                if kind in ("field", "group"):
                    allowed.add("tag")
                if kind == "group":
                    allowed.add("members")
                if (
                    kind not in ("field", "component", "group")
                    or set(member) - allowed
                    or type(member.get("name")) is not str
                    or not member["name"].strip()
                    or type(member.get("required")) is not bool
                    or (
                        kind in ("field", "group")
                        and (type(member.get("tag")) is not int or member["tag"] <= 0)
                    )
                    or (kind == "group" and not isinstance(member.get("members"), list))
                ):
                    raise ValueError(f"FIX component in {name!r} has an invalid member")
                if kind == "group":
                    pending.extend(member["members"])
                if kind in ("field", "group"):
                    component_tags.add(member["tag"])
                elif kind == "component":
                    component_refs.add(fold(member["name"]))
            try:
                entry = ComponentEntry.from_dict(document)
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                raise ValueError(f"FIX component in {name!r} is invalid: {error}") from error
            expected = f"{COMPONENTS}/{entry.slug}{DOCUMENT_SUFFIX}"
            if name != expected or entry.slug in components:
                raise ValueError(f"FIX component {entry.name!r} is stored under the wrong name")
            components[entry.slug] = entry
        if not fields:
            raise ValueError("the FIX registry has no fields")
        field_names = {
            fold(spelling) for entry in fields.values() for spelling in entry.spellings()
        }
        for version, members in sessions.items():
            missing = [name for name, _required in members if fold(name) not in field_names]
            if missing:
                raise ValueError(f"the FIX {version} session names unknown fields {missing}")
        missing_tags = sorted(component_tags - {entry.tag for entry in fields.values()})
        if missing_tags:
            raise ValueError(f"FIX components name unknown field tags {missing_tags[:5]}")
        missing_components = sorted(
            component_refs - {entry.folded for entry in components.values()}
        )
        if missing_components:
            raise ValueError(f"FIX components name unknown components {missing_components[:5]}")
        problems = _problems((fields, components))
        if problems:
            raise ValueError(f"the FIX registry is inconsistent: {problems[0]}")
        return len(names)

    @staticmethod
    def _registry_archive_documents(payload: bytes) -> dict[str, dict[str, Any]]:
        """Read only root registry documents from a bounded ZIP archive."""
        documents: dict[str, dict[str, Any]] = {}
        expanded = 0
        try:
            archive = zipfile.ZipFile(io.BytesIO(payload))
        except (OSError, EOFError, RuntimeError, NotImplementedError, zipfile.BadZipFile) as error:
            raise ValueError(f"the FIX registry archive cannot be decoded: {error}") from error
        with archive:
            try:
                members = [member for member in archive.infolist() if not member.is_dir()]
            except (OSError, EOFError, RuntimeError, zipfile.BadZipFile) as error:
                raise ValueError(f"the FIX registry archive cannot be decoded: {error}") from error
            if len(members) > _REGISTRY_ARCHIVE_MAX_DOCUMENTS:
                raise ValueError("the FIX registry archive has too many documents")
            for member in members:
                name = member.filename
                path = pathlib.PurePosixPath(name)
                parts = path.parts
                nested = len(parts) == 2 and parts[0] in (FIELDS, COMPONENTS)
                if (
                    not parts
                    or path.is_absolute()
                    or "\\" in name
                    or any(part in ("", ".", "..") for part in parts)
                    or not (name == VERSIONS_FILE or nested)
                    or path.suffix != DOCUMENT_SUFFIX
                ):
                    raise ValueError(f"unsafe FIX registry archive member {name!r}")
                if name in documents:
                    raise ValueError(f"duplicate FIX registry archive member {name!r}")
                if member.flag_bits & 1:
                    raise ValueError(f"FIX registry archive member {name!r} is encrypted")
                expanded += member.file_size
                if expanded > _REGISTRY_ARCHIVE_MAX_BYTES:
                    raise ValueError("the FIX registry archive expands beyond 64 MiB")
                try:
                    document = document_of(archive.read(member), name)
                except (
                    OSError,
                    EOFError,
                    RuntimeError,
                    NotImplementedError,
                    UnicodeError,
                    ValueError,
                    zipfile.BadZipFile,
                ) as error:
                    raise ValueError(
                        f"FIX registry archive member {name!r} cannot be decoded: {error}"
                    ) from error
                if not isinstance(document, dict):
                    raise ValueError(f"FIX registry archive member {name!r} is not an object")
                documents[name] = document
        index = documents.get(VERSIONS_FILE)
        versions = index.get("versions") if index is not None else None
        if (
            not isinstance(versions, list)
            or not versions
            or not any(name.startswith(f"{FIELDS}/") for name in documents)
        ):
            raise ValueError("the FIX registry archive has no version index or fields")
        return documents

    @property
    def installed(self) -> bool:
        """Whether construction filled the default store."""
        return bool(self.__dict__.get("_installed"))

    def _reduced(self, why: str) -> None:
        """Serve the packaged projection instead, and never quietly."""
        self.__dict__["_documents"] = ArchiveDocuments(builtin_projection())
        self._say(
            f"no FIX registry at {self.cache_dir} and {why}, so a reduced one is served: "
            "the packaged projection parses common traffic and misses the long tail. "
            "Run `rekep fix registry scrape` to install the whole dictionary."
        )

    def _say(self, line: str) -> None:
        """One bootstrap line, on both channels: the record, then the foreground."""
        warnings.warn(line, RuntimeWarning, stacklevel=3)
        announce = self.announce
        if announce is None:
            print(line, file=sys.stderr)
        else:
            announce(line)

    def rebuild(self, *versions: str) -> ConflictReport:
        """Scrape whole versions and write the store, collapsed, in one pass.

        Where `fields` folds one version into what is held, this is the bulk
        build: a version at a time cannot see what two versions disagree about,
        so this is the only place the collapse -- and its report -- is whole.
        """
        order = tuple(self._spelling(version) for version in versions) or self.versions
        declarations: dict[str, list[Field]] = {}
        sessions: dict[str, Sequence[tuple[str, bool]]] = {}
        components: dict[str, Sequence[SpecComponent]] = {}
        for version in order:
            document = self._spec_document(version)
            declarations[version] = self._scrape_version(version, document)
            sessions[version] = parse_session(document)
            components[version] = tuple(parse_components(document).values())
        entries, component_entries, report = collapse(order, declarations, components)
        self._write(documents_of(order, entries, component_entries, sessions, components))
        self.__dict__["_conflicts"] = report
        return report

    @property
    def conflicts(self) -> ConflictReport:
        """What the last `rebuild` collapsed; empty for a store it did not build."""
        return self.__dict__.get("_conflicts") or ConflictReport()

    def _write(self, documents: Mapping[str, Mapping[str, Any]]) -> None:
        """Replace this store with `documents`, in one pass over the place."""
        if self.archived:
            ArchiveDocuments(self._cache_path).write_all(documents)
            self._sync_archive()
        else:
            for name, document in documents.items():
                self._documents.write(name, document)
        self._forget()

    # -- versions ------------------------------------------------------------

    @cached_property
    def versions(self) -> tuple[str, ...]:
        """Every FIX version the dictionary carries, newest first."""
        self.refresh_if_stale()
        stored = self._stored_versions()
        if stored:
            return stored
        if self.offline:
            return self._known_versions()
        try:
            versions = self._scrape_versions()
        except OSError:
            # Offline before the index was ever stored: the versions that
            # *were* scraped are the ones this registry can honestly serve.
            known = self._known_versions()
            if known:
                return known
            raise
        self._store_versions(versions)
        return versions

    def _scrape_versions(self) -> tuple[str, ...]:
        """The version list off the dictionary's front page, newest first."""
        page = self._fetch(f"{self.base_url}.html")
        found = dict.fromkeys(_VERSION_LINK.findall(page))
        found.pop("latest", None)
        versions = tuple(sorted(found, key=newest_rank, reverse=True))
        if not versions:
            raise ValueError(f"{self.base_url}.html lists no FIX versions; is the layout new?")
        return versions

    def _versions(self, version: str | None) -> tuple[str, ...]:
        """The versions a call walks: all of them, or the one it named.

        Case-insensitive like every other name here -- `fixt1.1` finds
        `FIXT1.1` -- and the canonical spelling is what comes back, so the
        cache files and the site's directories are always addressed the one
        way they are spelled.
        """
        if version is None:
            return self.versions
        wanted = str(version).strip().lower()
        for candidate in self.versions:
            if candidate.lower() == wanted:
                return (candidate,)
        raise KeyError(f"{version!r} is not a FIX version here; one of {self.versions}")

    # -- fields --------------------------------------------------------------

    def fields(self, version: str, *, refresh: bool = False) -> list[Field]:
        """Every field of one FIX version, from the cache or from one scrape."""
        version = self._spelling(version)
        if not refresh and not self._torn():
            self.refresh_if_stale()
            stored = self._stored_fields(version)
            if stored is not None:
                return stored
        document = self._spec_document(version)
        fields = self._scrape_version(version, document)
        self._store_fields(
            version,
            fields,
            parse_session(document),
            tuple(parse_components(document).values()),
        )
        self._indexes.pop(version, None)
        return fields

    def fields_available(self, version: str | None = None) -> bool:
        """Whether at least one selected version's fields can be read now."""
        try:
            candidates = self._versions(version)
        except (KeyError, OSError, ValueError):
            return False
        for candidate in candidates:
            if self._stored_fields(candidate) is not None:
                return True
            if not self.offline:
                try:
                    self.fields(candidate)
                except (OSError, ValueError):
                    continue
                return True
        return False

    def _spelling(self, version: str) -> str:
        """The canonical spelling of `version`, and a refusal of non-names."""
        wanted = str(version).strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._\-]*", wanted):
            raise ValueError(f"{version!r} does not name a FIX version")
        lowered = wanted.lower()
        for candidate in self.__dict__.get("versions") or ():
            if candidate.lower() == lowered:
                return candidate
        for candidate in self._stored_spellings():
            if candidate.lower() == lowered:
                return candidate
        return wanted

    def load(self, *versions: str, refresh: bool = False) -> dict[str, int]:
        """Scrape (or verify) whole versions into the cache: `{version: fields}`.

        The bulk form of `fields`, for priming a cache that then travels to
        machines without network access. No versions named means all of them.
        """
        return {
            version: len(self.fields(version, refresh=refresh))
            for version in (versions or self.versions)
        }

    def session(self, version: str) -> tuple[tuple[str, bool], ...]:
        """`((name, required), ...)`: the standard header, then the trailer.

        What every message of a version carries whatever it says, and which of
        those it must -- the spec's own answer, read from the store so it costs
        nothing and works offline. Empty for a version stored before this was
        kept, or one whose spec could not be read.
        """
        return self._stored_session(self._spelling(version))

    def components(self, version: str, *, refresh: bool = False) -> list[SpecComponent]:
        """Every reusable component of one FIX version, in spec order."""
        version = self._spelling(version)
        stored = self._stored_components(version)
        if stored is not None and not refresh:
            return stored
        if self.offline:
            return stored or []
        if self._stored_fields(version) is None:
            self.fields(version, refresh=refresh)
        else:
            self.enrich(version)
        refreshed = self._stored_components(version)
        return refreshed if refreshed is not None else (stored or [])

    def group_count_tags(self, version: str | None = None) -> frozenset[int]:
        """Tags that open declared repeating groups in one or every FIX version."""
        spelling = "*" if version is None else self._spelling(version)
        cache = self.__dict__.setdefault("_group_count_tags", {})
        found = cache.get(spelling)
        if found is not None:
            return found

        if version is None:
            found = frozenset(
                tag for candidate in self.versions for tag in self.group_count_tags(candidate)
            )
            cache[spelling] = found
            return found

        counts: set[int] = set()

        def visit(members: Sequence[Any]) -> None:
            for member in members:
                if isinstance(member, SpecGroup):
                    if member.tag:
                        counts.add(int(member.tag))
                    visit(member.members)

        for component in self.components(spelling):
            visit(component.members)
        found = frozenset(counts)
        cache[spelling] = found
        return found

    def group_delimiters(
        self, root: str, groups: Sequence[str], version: str | None = None
    ) -> tuple[str, ...] | None:
        """First declared field of each group in `groups`, nested under `root`.

        Each group is looked for inside the previous one's declaration, so
        `("NoQuoteSets", "NoQuoteEntries")` answers the outer and inner
        delimiter together. Resolved against `version` when given, else every
        stored version newest first; None when no version declares the chain.
        """
        candidates = (version,) if version else self.versions
        for candidate in candidates:
            try:
                components = self.components(candidate)
            except (KeyError, OSError, ValueError):
                continue
            by_name = {component.name.lower(): component for component in components}
            node = by_name.get(root.lower())
            if node is None:
                continue
            members: Sequence[Any] = node.members
            found: list[str] = []
            for group in groups:
                declared = declared_group(members, group, by_name)
                named = None if declared is None else first_declared_name(declared.members, by_name)
                if not named:
                    break
                found.append(named)
                members = declared.members
            else:
                return tuple(found)
        return None

    def components_available(self, version: str) -> bool:
        """Whether this store holds component declarations for `version` at all.

        `components()` answers `[]` twice over: for a version whose spec
        declares none -- 4.0 through 4.2 predate them -- and for a store
        written before this package kept any. The first is the standard, the
        second is a stale artifact, and only this tells them apart.
        """
        try:
            return self._stored_components(self._spelling(version)) is not None
        except (KeyError, OSError, ValueError):
            return False

    def component(self, name: str, version: str | None = None) -> SpecComponent:
        """The newest declaration of one component, matched case-insensitively."""
        wanted = str(name).strip().lower()
        candidates = (self._spelling(version),) if version is not None else self.versions
        for candidate in candidates:
            try:
                components = self.components(candidate)
            except (OSError, ValueError):
                continue
            for component in components:
                if component.name.lower() == wanted:
                    return component
        where = version or "any version"
        raise KeyError(f"no FIX component {name!r} in {where}")

    def enrich(self, *versions: str) -> dict[str, int]:
        """Add the spec's value symbols to versions already stored: `{version: fields}`."""
        counted: dict[str, int] = {}
        for version in versions or self._stored_spellings():
            version = self._spelling(version)
            stored = self._stored_fields(version)
            if stored is None:
                continue
            document = self._spec_document(version)
            spec = parse_spec(document)
            if not spec:
                counted[version] = 0
            enriched = 0
            for member in stored:
                tag = member.fix.get("tag")
                known = spec.get(int(tag)) if tag and tag.isdigit() else None
                if known and known.values:
                    member.fix["value_names"] = json.dumps(known.values, separators=(",", ":"))
                    enriched += 1
            session = parse_session(document) or self.session(version)
            components = list(parse_components(document).values())
            if not components and not spec:
                # A spec that could not be read says nothing about components,
                # so what is stored stands. One that *was* read and declares
                # none -- 4.0 through 4.2 ship an empty `<components/>` -- is
                # answering the question, and storing that empty answer is what
                # separates "this version has none" from "this store never had
                # any". `components()` is `[]` either way; only the store knows
                # which, and a caller that cannot tell degrades silently.
                components = self._stored_components(version)
            self._store_fields(version, stored, session, components)
            self._indexes.pop(version, None)
            counted[version] = enriched
        return counted

    # -- lookup --------------------------------------------------------------

    def lookup(self, key: int | str, version: str | None = None) -> list[Field]:
        """One field as each version that declares it has it, newest version first.

        `key` is a tag (`54`, `"54"`) or any name the record answers to
        (`"Side"`, case-insensitive). `version` narrows to one version; the
        default walks them all in descending order, which is also the order of
        the result.

        A tag reads the one shard that can hold it. A name needs the name
        index, which is every shard -- a name has no arithmetic behind it.
        """
        order = self._versions(version)
        entry = self._record(key)
        return entry.into_fields(order) if entry is not None else []

    def _record(self, key: int | str) -> FieldEntry | None:
        """One field record: by tag out of its shard alone, by name out of the index."""
        if _is_tag(key):
            return self._layout.record(int(key))
        return self.resolve(str(key))

    def entry(self, key: int | str) -> FieldEntry | None:
        """The stored record one tag or name resolves to, or None.

        The record, not a projected `Field`: `FieldEntry.encode` and the
        alias spellings live on it, and `FieldAccess` reads both.
        """
        return self._record(key)

    def field(self, key: int | str, version: str | None = None) -> Field:
        """The newest definition of one field; `KeyError` when no version has it."""
        found = self.lookup(key, version)
        if not found:
            where = version or "any version"
            raise KeyError(f"no FIX field {key!r} in {where}")
        return found[0]

    def msg_type_event_types(self) -> Mapping[str, EventType]:
        """Known MsgTypes to their configured market kind or MISC."""
        return self._msg_type_event_types

    @cached_property
    def _msg_type_event_types(self) -> Mapping[str, EventType]:
        """Registry-owned classification index, built once per store revision."""
        entry = self.entry(35)
        if entry is None:
            return MappingProxyType({})
        msg_types = dict.fromkeys((*entry.values, *entry.value_names, *entry.event_types))
        return MappingProxyType({value: entry.event_type(value) for value in msg_types})

    def msg_types(self, event_type: EventType | int) -> frozenset[str]:
        """Configured MsgTypes belonging to one stored event kind."""
        return self._msg_types.get(EventType(event_type), frozenset())

    @cached_property
    def _msg_types(self) -> Mapping[EventType, frozenset[str]]:
        """Event-kind groups built once per store revision."""
        grouped: dict[EventType, set[str]] = {}
        for msg_type, event_type in self.msg_type_event_types().items():
            grouped.setdefault(event_type, set()).add(msg_type)
        return MappingProxyType(
            {event_type: frozenset(values) for event_type, values in grouped.items()}
        )

    def msg_type_handlers(self) -> Mapping[str, str]:
        """Known MsgTypes to their canonical normalized decoded name."""
        return self._msg_type_handlers

    @cached_property
    def _msg_type_handlers(self) -> Mapping[str, str]:
        """MsgType codes to their canonical decoded spelling."""
        entry = self.entry(35)
        if entry is None:
            return MappingProxyType({})
        return MappingProxyType(dict(entry.decoded))

    def state_values(self, field: int | str) -> Mapping[str, State]:
        """Configured market states for one FIX field's wire values."""
        entry = self.entry(field)
        return MappingProxyType({}) if entry is None else self._state_values.get(entry.key, {})

    @cached_property
    def _state_values(self) -> Mapping[int | str, Mapping[str, State]]:
        """Field-state maps built once per store revision."""
        return MappingProxyType(
            {
                entry.key: MappingProxyType(dict(entry.states))
                for entry in self._entries[0].values()
                if entry.states
            }
        )

    def scalar(
        self,
        key: int | str,
        *,
        version: str | None = None,
        name: str = "",
        dtype: Any = _DEFAULT,
        nullable: bool | None = None,
        metadata: dict[str, str] | None = None,
    ) -> Field:
        """A fresh scalar declaration, exact by version or merged across versions."""
        source = self.field(key, version) if version is not None else self._scalar_of(key)
        # Protocol identity is the registry's. Other declarations can add
        # metadata, but cannot silently retag or retype a standard field.
        declared = {**(metadata or {}), **source.metadata, "fix:name": source.name}
        return Field(
            name=name or source.name,
            dtype=source.dtype if dtype is _DEFAULT else dtype,
            nullable=nullable,
            metadata=declared,
        )

    def tags(self, version: str | None = None) -> dict[str, int]:
        """Every field name to its tag number, lowercased, newest version winning."""
        mapping: dict[str, int] = {}
        candidates = self._versions(version) if version is not None else self._versions(None)
        for candidate in candidates:
            members = self.fields(candidate) if version is not None else self._members(candidate)
            for member in members:
                tag = member.fix.get("tag")
                if tag:
                    mapping.setdefault(member.name.lower(), int(tag))
        return mapping

    def search(
        self,
        text: int | str,
        version: str | None = None,
        *,
        limit: int = 10,
        fuzzy: bool = True,
    ) -> list[Field]:
        """Distinct field identities matching `text`, best first."""
        wanted = str(text).strip().lower()
        if not wanted or limit <= 0:
            return []
        ranked: list[tuple[int, int, int, Field]] = []
        for order, candidate in enumerate(self._versions(version)):
            for member in self._members(candidate):
                rank = _rank(member, wanted)
                if rank is not None:
                    ranked.append((rank, order, int(member.fix.get("tag") or _NO_TAG), member))
        if not ranked and fuzzy and not _is_tag(wanted):
            ceiling = max(2, len(wanted) // 3)
            for order, candidate in enumerate(self._versions(version)):
                for member in self._members(candidate):
                    distance = _levenshtein(wanted, member.name.lower(), ceiling)
                    if distance is not None:
                        ranked.append(
                            (100 + distance, order, int(member.fix.get("tag") or _NO_TAG), member)
                        )
        ranked.sort(key=lambda entry: entry[:3])
        found: list[Field] = []
        seen: set[int | str] = set()
        for *_, member in ranked:
            entry = self.entry(member.fix.get("tag") or member.name)
            identity = entry.key if entry is not None else member.name
            if identity in seen:
                continue
            seen.add(identity)
            found.append(member)
            if len(found) == limit:
                break
        return found

    # -- one identity, every version -----------------------------------------
    #
    # `lookup` and `scalar` answer about one key at a time and read a version
    # at a time. These answer about the whole dictionary at once, and about an
    # identity rather than a version's reading of one -- which is what a tool
    # comparing a capture's key names against the standard needs, and what
    # nine per-version documents could not be asked.

    def field_entries(self) -> Mapping[str, FieldEntry]:
        """Every field identity this registry holds, keyed by canonical name."""
        return MappingProxyType({entry.name: entry for entry in self._entries[0].values()})

    def component_entries(self) -> Mapping[str, ComponentEntry]:
        """Every component identity this registry holds, keyed by canonical name."""
        return MappingProxyType({entry.name: entry for entry in self._entries[1].values()})

    def merged_fields(self) -> Mapping[str, Field]:
        """The whole unified field table: `{canonical name: merged declaration}`.

        `scalar()` for every field at once, and the same declaration it builds:
        one record, and the versions that declare it, rather than a version's
        reading of it.
        """
        order = self.versions
        return MappingProxyType(
            {entry.name: entry.into_merged(order) for entry in self._entries[0].values()}
        )

    def merged_components(self) -> Mapping[str, ComponentEntry]:
        """The whole unified component table: `{canonical name: record}`.

        A record, not a declaration, because `paths()` and `delimiters()` are
        the questions worth asking of one and neither is on a member tree.
        """
        return self.component_entries()

    def merged_component(self, name: str) -> ComponentEntry:
        """One component across every version it is declared for."""
        wanted = fold(name)
        for entry in self._entries[1].values():
            if wanted in {fold(spelled) for spelled in entry.spellings()}:
                return entry
        raise KeyError(f"no FIX component {name!r} in any version")

    def component_field(self, name: str, version: str) -> Field | None:
        """One component's declaration as an Arrow field, or None for that version.

        The spec's `required` rules decide nullability and the dictionary
        decides each member's type, so a component projects into a shape a
        reader can trust rather than into a struct of nullable strings.
        """
        entry = self.merged_component(name)
        return entry.into_field(
            self._spelling(version),
            types=self._component_types(version),
            components={found.folded: found for found in self._entries[1].values()},
        )

    def _component_types(self, version: str) -> dict[str, Any]:
        """`{FIX member name: Arrow type}` for one version, for a projection."""
        return {
            member.name: member.dtype
            for member in self.fields(self._spelling(version))
            if member.dtype is not None
        }

    def resolve(self, name: str) -> FieldEntry | None:
        """The identity a rendered name means, or None when nothing here is it.

        Deterministic, in three tiers, and the tiers are the whole rule:

        1. the canonical name of an identity;
        2. a name some version spells for it (tag 64 is `FutSettDate` through
           4.3 and `SettlDate` after, and both are that identity);
        3. a declared alias -- a rendered or namespaced spelling, a legacy name, a
           near miss confirmed against a capture.

        A later tier is only consulted when every earlier one missed, so
        adding an alias can never take a name away from a field that already
        claims it. Two identities claiming one name in the same tier is a
        defect in the store, not something to resolve at read time:
        `alias_conflicts` finds it and `check` fails the build on it.
        """
        return self._resolutions.get(fold(name))

    def alias_conflicts(self) -> dict[str, list[str]]:
        """`{name: the identities claiming it}` for every name two fields claim.

        Read per tier, because a canonical name overruling an alias is the
        rule and not a conflict -- what this reports is two identities meeting
        inside one tier, where nothing decides between them.
        """
        conflicts: dict[str, list[str]] = {}
        claimed: set[str] = set()
        for tier in TIERS:
            names: dict[str, list[str]] = {}
            for entry in self._entries[0].values():
                for spelled in _tier(entry, tier):
                    names.setdefault(fold(spelled), []).append(entry.name)
            for folded, owners in names.items():
                unique = list(dict.fromkeys(owners))
                if len(unique) > 1 and folded not in claimed:
                    conflicts[folded] = unique
                claimed.add(folded)
        return conflicts

    def check(self) -> list[str]:
        """Everything wrong with this store, as lines; empty when it is sound.

        What a build runs. Three failures, and each one is silent otherwise: an
        alias two fields claim resolves a rendered key to whichever was read
        first; two identities stored in one file lose one of themselves on the
        next write; an entry declared for no version answers nothing.
        """
        return _problems(self._entries)

    @cached_property
    def _entries(self) -> tuple[dict[int | str, FieldEntry], dict[str, ComponentEntry]]:
        """Every record this store holds, keyed as the store keys them."""
        return self._layout.field_entries, self._layout.component_entries

    @cached_property
    def _resolutions(self) -> dict[str, FieldEntry]:
        """`{folded name: identity}`, built once in tier order."""
        found: dict[str, FieldEntry] = {}
        for tier in TIERS:
            for entry in self._entries[0].values():
                for spelled in _tier(entry, tier):
                    found.setdefault(fold(spelled), entry)
        return found

    # -- editing the store ----------------------------------------------------
    #
    # Registering a newly observed namespaced field or a newly confirmed alias is
    # a supported operation, not a hand edit of a JSON file: every change goes
    # through one of these, is checked against the schema and against what the
    # store already holds, and is refused whole rather than written half.

    def add_field(self, entry: FieldEntry) -> FieldEntry:
        """Store one new field identity; `KeyError` when it is already here.

        The duplicate tag and duplicate name checks are in `_validated`, which
        every write goes through; this one is only the file it would land in.
        """
        held = self._entries[0].get(entry.key)
        if held is not None and held.folded == entry.folded:
            raise KeyError(f"FIX field {entry.name!r} is already stored in {field_document(entry)}")
        if held is not None:
            claimed = f"tag {entry.tag}" if entry.tag is not None else f"the name {entry.name!r}"
            raise KeyError(
                f"FIX field {entry.name!r} cannot be added: {claimed} is already claimed by "
                f"{held.name!r}, in {field_document(entry)}"
            )
        return self._write_field(entry)

    def update_field(self, entry: FieldEntry) -> FieldEntry:
        """Replace one stored field identity; `KeyError` when there is none."""
        if entry.key not in self._entries[0]:
            raise KeyError(f"no FIX field stored in {field_document(entry)}")
        return self._write_field(entry)

    def remove_field(self, name: str) -> bool:
        """Delete one field identity, by any name it answers to.

        By any name, because every other verb here takes one: a spelling good
        enough to resolve a rendered key is good enough to name the entry it
        resolves to.
        """
        entry = self.resolve(name)
        if entry is None:
            return False
        removed = self._layout.remove_field(entry.key)
        self._forget()
        return removed

    def add_component(self, entry: ComponentEntry) -> ComponentEntry:
        """Store one new component identity; `KeyError` when it is already here."""
        if entry.slug in self._entries[1]:
            raise KeyError(f"FIX component {entry.name!r} is already stored")
        return self._write_component(entry)

    def update_component(self, entry: ComponentEntry) -> ComponentEntry:
        """Replace one stored component identity; `KeyError` when there is none."""
        if entry.slug not in self._entries[1]:
            raise KeyError(f"no FIX component stored as {entry.slug}{DOCUMENT_SUFFIX}")
        return self._write_component(entry)

    def remove_component(self, name: str) -> bool:
        """Delete one component identity, by any name it answers to."""
        try:
            entry = self.merged_component(name)
        except KeyError:
            return False
        removed = self._layout.remove_component(entry.slug)
        self._forget()
        return removed

    def alias_field(self, name: str, *aliases: Alias | str) -> FieldEntry:
        """Add spellings one field has been observed under, and keep the entry.

        The operation a classification run produces: a near miss confirmed
        against a capture becomes a data change here, never a branch in a
        resolver.
        """
        entry = self.resolve(name)
        if entry is None:
            raise KeyError(f"no FIX field {name!r} in this registry")
        added = tuple(alias if isinstance(alias, Alias) else Alias(name=alias) for alias in aliases)
        held = {alias.folded for alias in entry.aliases}
        return self.update_field(
            dataclasses.replace(
                entry,
                aliases=(*entry.aliases, *(a for a in added if a.folded not in held)),
            )
        )

    def promote_field(
        self,
        name: str,
        column: str,
        *,
        type: str = "",
        description: str = "",
        aliases: Sequence[Alias | str] = (),
    ) -> FieldEntry:
        """Register a rendered name and the column it is lifted into, in one call.

        The one entry point for promoting a bridge-proprietary spelling into a
        typed column. A name the store has never seen becomes a new namespaced
        entry carrying `column`; a name a classification run already
        registered without one -- the half-done state the `add_field` plus
        `into_entry` two-step leaves -- is completed in place, keeping every
        alias and count it has gathered. A `type` or `description` said here
        is the newest reading and wins; one left unsaid keeps what the entry
        holds -- `String`, for a type nobody has ever said, because that is
        what every rendered value is until someone says otherwise.
        Three refusals, all data problems to resolve rather than
        overwrite: a standard tagged field, whose column the dictionary
        decides; an entry already lifted into a different column; and a
        column some other field already landed in -- runs disagreeing about
        where a field lands are a conflict, not a newer answer.
        """
        if not str(name).strip():
            raise ValueError("promoting a FIX field requires its name")
        column = str(column).strip()
        if not column:
            raise ValueError(f"promoting FIX field {name!r} requires the column it is lifted into")
        added: tuple[Alias, ...] = ()
        for alias in aliases:
            one = alias if isinstance(alias, Alias) else Alias(name=alias)
            if one.folded not in {a.folded for a in added}:
                added = (*added, one)
        held = self.resolve(name)
        claimed = next(
            (
                one
                for one in self._entries[0].values()
                if one.column == column and (held is None or one.key != held.key)
            ),
            None,
        )
        if claimed is not None:
            raise ValueError(
                f"column {column!r} is already {claimed.name!r}'s; "
                "two fields cannot land in one column"
            )
        if held is None:
            return self.add_field(
                FieldEntry(
                    name=name,
                    kind=NAMESPACE,
                    versions=(ANY_VERSION,),
                    type=type or "String",
                    description=description,
                    aliases=added,
                    column=column,
                )
            )
        if held.kind != NAMESPACE:
            raise KeyError(
                f"FIX field {held.name!r} is standard, with tag {held.tag}; promotion "
                "registers rendered bridge fields only"
            )
        if held.column and held.column != column:
            raise ValueError(
                f"FIX field {held.name!r} is already lifted into {held.column!r}; "
                f"refusing to move it to {column!r}"
            )
        spelled = {held.folded, *(alias.folded for alias in held.aliases)}
        return self.update_field(
            dataclasses.replace(
                held,
                type=type or held.type or "String",
                description=description or held.description,
                aliases=(*held.aliases, *(a for a in added if a.folded not in spelled)),
                column=column,
            )
        )

    def _write_field(self, entry: FieldEntry) -> FieldEntry:
        """Validate one field record against the whole store, then write it."""
        self._validated(fields={**self._entries[0], entry.key: entry})
        self._layout.store_field(entry)
        self._forget()
        return entry

    def _write_component(self, entry: ComponentEntry) -> ComponentEntry:
        """Validate one component record against the whole store, then write it."""
        self._validated(components={**self._entries[1], entry.slug: entry})
        self._layout.store_component(entry)
        self._forget()
        return entry

    def _validated(
        self,
        fields: Mapping[int | str, FieldEntry] | None = None,
        components: Mapping[str, ComponentEntry] | None = None,
    ) -> None:
        """Refuse a change that would make the store inconsistent, before writing.

        Checked against what the store would hold *after* the change rather
        than against the change alone: an alias is only a collision beside the
        entry that already claims it.
        """
        held = (fields or self._entries[0], components or self._entries[1])
        problems = _problems(held)
        if problems:
            counted = f"{len(problems)} inconsistenc{'y' if len(problems) == 1 else 'ies'}"
            raise ValueError(
                f"this change would leave {counted} in the registry, so nothing was written: "
                + "; ".join(problems)
            )

    # -- checking one store against another ------------------------------------

    def verify(self, other: FixRegistry) -> list[str]:
        """Every question these two registries answer differently, as lines.

        Every version, every field, every component, every session layer and
        the whole name-to-tag mapping -- the readings a parse depends on. Empty
        means the two are the same dictionary however either is stored.
        """
        differences = []
        if self.versions != other.versions:
            differences.append(f"versions {self.versions} != {other.versions}")
        for version in self.versions:
            mine, theirs = self.fields(version), other.fields(version)
            if mine != theirs:
                names = {member.name for member in mine} ^ {member.name for member in theirs}
                differences.append(
                    f"{version} fields differ ({len(mine)} vs {len(theirs)}"
                    + (f", {sorted(names)[:5]}" if names else "")
                    + ")"
                )
            if self._stored_components(version) != other._stored_components(version):
                differences.append(f"{version} components differ")
            if self.session(version) != other.session(version):
                differences.append(f"{version} session layer differs")
        if self.tags() != other.tags():
            differences.append("the name to tag mapping differs")
        for key in sorted(self.tags().values()):
            if self.scalar(key) != other.scalar(key):
                differences.append(f"the merged declaration of tag {key} differs")
        return differences

    # -- keeping the store fresh ----------------------------------------------

    def refresh_if_stale(self) -> bool:
        """Regenerate the store from upstream when it is older than `cache_ttl`.

        True when a refetch ran and the store was written. False when the TTL
        is off, when the store is young enough, or when the refetch failed --
        the last of which is reported and then served stale, because a
        dictionary a day old parses every message and one that raises parses
        none.
        """
        if not self.cache_ttl or self.__dict__.get("_refreshed"):
            return False
        self.__dict__["_refreshed"] = True
        age = self._store_age()
        if age is not None and age <= self.cache_ttl:
            return False
        try:
            refreshed = self._refresh()
        except (OSError, ValueError) as error:
            aged = "of unknown age" if age is None else f"{age:.0f}s old"
            warnings.warn(
                f"the FIX registry at {self.cache_dir} is {aged} and could not be "
                f"refreshed ({error}); serving the local copy",
                RuntimeWarning,
                stacklevel=3,
            )
            return False
        return refreshed

    def _store_age(self) -> float | None:
        """How many seconds since this store was last written; None when never."""
        stamps = [
            stamp for name in self._documents.names() if (stamp := self._documents.stamp(name))
        ]
        return time.time() - max(stamps) if stamps else None

    def _refresh(self) -> bool:
        """Read the spec for every stored version and write what it says."""
        spellings = self._stored_spellings()
        if not spellings:
            raise ValueError("this FIX registry store holds no version to refresh")
        refreshed = False
        for version in spellings:
            document = self._fetch(f"{self.spec_url}/{spec_name(version)}")
            spec = parse_spec(document)
            if not spec:
                raise ValueError(f"the spec for {version} says nothing")
            stored = self._stored_fields(version) or []
            for member in stored:
                tag = member.fix.get("tag")
                known = spec.get(int(tag)) if tag and tag.isdigit() else None
                if known and known.values:
                    member.fix["value_names"] = json.dumps(known.values, separators=(",", ":"))
            self._store_fields(
                version,
                stored,
                parse_session(document) or self.session(version),
                list(parse_components(document).values()),
            )
            refreshed = True
        return refreshed

    @cached_property
    def _indexes(self) -> dict[str, list[Field] | None]:
        return {}

    @cached_property
    def _scalars(self) -> dict[int | str, Field]:
        return {}

    def _scalar_of(self, key: int | str) -> Field:
        """The cached cross-version declaration behind `scalar`."""
        entry = self._record(key)
        if entry is None:
            raise KeyError(f"no FIX field {key!r} in any version")
        built = self._scalars.get(entry.key)
        if built is None:
            built = self._scalars[entry.key] = entry.into_merged(self.versions)
        return built

    def _members(self, version: str) -> list[Field]:
        """`fields(version)` for a walk over many versions: absent means empty.

        The miss is remembered per registry, not per call, or an offline walk
        would retry the network once per query.
        """
        held = self._indexes.get(version, ())
        if held == ():
            try:
                held = self._indexes[version] = self.fields(version)
            except OSError:
                held = self._indexes[version] = None
        return list(held or ())

    # -- scraping ------------------------------------------------------------

    def _scrape_version(self, version: str, document: str | None = None) -> list[Field]:
        """One version, whole: the spec, the by-tag list, then every field page.

        Two sources merged, each supplying what the other has not. The spec is
        fetched first because it is one request and answers for the whole
        version; the site is then read for the prose it alone has. A tag only
        the spec knows still becomes a field -- typed and named, with no
        description -- because a field nobody wrote up is still a field.
        """
        spec = self._spec(version, document)
        listed = self._scrape_tags(version)
        with concurrent.futures.ThreadPoolExecutor(self.max_workers) as pool:
            read = pool.map(lambda tag: self._scrape_field(version, tag), listed)
            details = dict(zip(listed, read, strict=True))
        fields = []
        for tag in sorted(listed.keys() | spec.keys()):
            name, note = listed.get(tag, ("", ""))
            detail = details.get(tag, {})
            known = spec.get(tag)
            built = fix_field(
                name_of(detail.get("name") or name or (known.name if known else str(tag))),
                tag,
                detail.get("type") or (known.datatype if known else None),
                description=detail.get("description"),
                version=version,
                values=detail.get("values"),
            )
            if note:
                built.fix["note"] = note
            used = detail.get("used_in")
            if used:
                built.fix["msgtypes"] = json.dumps(used, separators=(",", ":"))
            components = detail.get("components")
            if components:
                built.fix["components"] = json.dumps(components, separators=(",", ":"))
            if known and known.values:
                # The symbol, beside the description and never over it: the
                # spec's `description=` attribute holds `BUY`, which is the
                # value's *name*, and writing that where the prose goes would
                # replace "Buy" with shouting.
                built.fix["value_names"] = json.dumps(known.values, separators=(",", ":"))
            fields.append(built)
        return fields

    def _spec(self, version: str, document: str | None = None) -> dict[int, SpecField]:
        """The QuickFIX spec for one version, or nothing when it cannot be had.

        Empty rather than raised, because this is the *enriching* source: a
        scrape that failed here still has a whole dictionary, only without the
        value symbols. The site is the one whose failure is fatal.
        """
        return parse_spec(self._spec_document(version) if document is None else document)

    def _spec_document(self, version: str) -> str:
        """One spec file, or empty text when it cannot be had."""
        try:
            return self._fetch(f"{self.spec_url}/{spec_name(version)}")
        except (OSError, ValueError):
            return ""

    def _scrape_tags(self, version: str) -> dict[int, tuple[str, str]]:
        """`{tag: (name, note)}` off the by-tag page, in tag order.

        The page links each field twice -- once as the tag, once as the name
        -- so the name is the first link text that is not the bare number,
        and a trailing parenthetical (`(no longer used)`) is split off as the
        note, because it is annotation and not the name.
        """
        page = self._fetch(f"{self.base_url}/{version}/fields_by_tag.html")
        listed: dict[int, tuple[str, str]] = {}
        for tag_text, label in _TAG_LINK.findall(page):
            tag = int(tag_text)
            text = _text(label)
            if not text or text == tag_text:
                listed.setdefault(tag, ("", ""))
                continue
            name, note = _split_note(text)
            name = name_of(name)
            known = listed.get(tag)
            if known is None or not known[0]:
                listed[tag] = (name, note)
        if not listed:
            raise ValueError(
                f"{self.base_url}/{version}/fields_by_tag.html lists no fields; is the layout new?"
            )
        return {tag: listed[tag] for tag in sorted(listed)}

    def _scrape_field(self, version: str, tag: int) -> dict[str, Any]:
        """What one field page says: name, type, description, values, messages.

        Every part is optional on purpose -- the pages drift across versions,
        and a field whose page cannot be read is still a field, just one that
        stays a string with no comment.
        """
        try:
            page = self._fetch(f"{self.base_url}/{version}/tagNum_{tag}.html")
        except OSError as error:
            # A page that is *not there* is a field the site never wrote up,
            # and the by-tag row alone is still a field. A page that was
            # refused -- throttled, or a server that stayed broken past the
            # retries -- is not: swallowing it writes a typeless, commentless
            # field into the cache, where it then answers every later call.
            if _is_transient(error):
                raise
            return {}
        detail: dict[str, Any] = {}
        title = _TITLE.search(page)
        if title and title[2] == str(tag):
            detail["name"] = name_of(_text(title[1]))
        typed = _TYPE.search(page)
        if typed:
            detail["type"] = typed[1]
        prose, listed, carried = _sections(page, typed.end() if typed else 0)
        description = _description(prose)
        if description:
            detail["description"] = description
        values = _values(listed, names=tag == 35)
        if values:
            detail["values"] = values
        used = _used_in(carried)
        if used:
            detail["used_in"] = used
        components = _used_in(carried, COMPONENT_LINK)
        if components:
            detail["components"] = components
        return detail

    # -- the store -----------------------------------------------------------
    #
    # Seven methods, and they are the whole of where the dictionary is kept.
    # Everything above -- the scraping, the version rules, the ordering, the
    # searching -- is written against these and is the same wherever the fields
    # live: a directory or a zip. `rekep.fix.store` owns that choice and the
    # shard arithmetic; nothing here reads a path or a member name.

    def _stored_versions(self) -> tuple[str, ...]:
        """The version list this store already holds; empty when it holds none."""
        return self._layout.versions()

    def _store_versions(self, versions: tuple[str, ...]) -> None:
        """Keep the version list, so the front page is fetched once."""
        self._layout.store_versions(versions)

    def _known_versions(self) -> tuple[str, ...]:
        """The versions this store has *fields* for, newest first.

        What an offline registry can honestly serve when it never saw the
        front page. Deduplicated case-blind, because a copied-in cache can
        hold two spellings of one version and they are one version here.
        """
        seen: set[str] = set()
        unique: list[str] = []
        for candidate in sorted(self._stored_spellings(), key=newest_rank, reverse=True):
            if candidate.lower() not in seen:
                seen.add(candidate.lower())
                unique.append(candidate)
        return tuple(unique)

    def _stored_spellings(self) -> tuple[str, ...]:
        """Every version this store has fields for, spelled as it stored them."""
        return self._layout.spellings()

    def _stored_fields(self, version: str) -> list[Field] | None:
        """One version's fields as this store holds them; None when it does not."""
        return self._layout.fields(version)

    def _stored_components(self, version: str) -> list[SpecComponent] | None:
        """Stored component declarations; None means this predates them."""
        return self._layout.components(version)

    def _stored_session(self, version: str) -> tuple[tuple[str, bool], ...]:
        """One version's session layer as this store holds it."""
        return self._layout.session(version)

    def _store_fields(
        self,
        version: str,
        fields: list[Field],
        session: Sequence[tuple[str, bool]] = (),
        components: Sequence[SpecComponent] | None = None,
    ) -> None:
        """Keep one version's fields and optional spec declarations."""
        self._layout.store(version, fields, session, components, url=f"{self.base_url}/{version}/")
        self._forget()

    def _torn(self) -> bool:
        """Whether this store holds documents it cannot read, said out loud.

        A torn write used to cost a whole version, which read as a cold cache
        and was scraped over. One identity per file makes it cost one field --
        and a version that still answers, one field short, is exactly the
        silence this store exists to avoid. So it is written again, and an
        offline registry that cannot says so.
        """
        torn = getattr(self._layout, "torn", ())
        if not torn:
            return False
        warnings.warn(
            f"the FIX registry at {self.cache_dir} cannot read {list(torn[:5])}"
            + ("" if self.offline else "; scraping over them"),
            RuntimeWarning,
            stacklevel=3,
        )
        return not self.offline

    def _forget(self) -> None:
        """Drop everything derived from the store, after the store changed."""
        forget = getattr(self._layout, "forget", None)
        if forget is not None:
            forget()
        self._indexes.clear()
        self._scalars.clear()
        self.__dict__.pop("_entries", None)
        self.__dict__.pop("_resolutions", None)
        self.__dict__.pop("_msg_type_event_types", None)
        self.__dict__.pop("_msg_types", None)
        self.__dict__.pop("_msg_type_handlers", None)
        self.__dict__.pop("_state_values", None)
        self.__dict__.pop("_group_count_tags", None)
        self.__dict__["_revision"] = self.revision + 1

    # -- the cache files and the wire -----------------------------------------

    @property
    def archived(self) -> bool:
        """Whether this registry keeps its dictionary in a zip rather than a directory.

        Read off the extension, and only off the extension: a path that does
        not exist yet has to say what it will be before anything is written
        to it, and `data/fix.zip` says it.
        """
        location = Url.from_string(os.fspath(self.cache_dir))
        return pathlib.PurePosixPath(location.path).suffix.lower() == ".zip"

    @cached_property
    def _cache_source(self) -> tuple[pyarrow.fs.FileSystem, str] | None:
        """The configured cache before an archive is localized."""
        location = os.fspath(self.cache_dir)
        if self.filesystem is not None:
            return self.filesystem, location
        if Url.from_string(location).scheme in HTTP:
            return None
        return resolve(location)

    @cached_property
    def _cache(self) -> tuple[pyarrow.fs.FileSystem, str]:
        """The filesystem and path used by cache operations, resolved once."""
        source = self._cache_source
        location = os.fspath(self.cache_dir)
        if source is None:
            if not self.archived:
                raise ValueError("an HTTP FIX registry cache must be an archive")
            return pyarrow.fs.LocalFileSystem(), local_path(location)
        filesystem, path = source
        if self.archived and not isinstance(filesystem, pyarrow.fs.LocalFileSystem):
            path = local_path(path, filesystem, missing_ok=True)
            return pyarrow.fs.LocalFileSystem(), path
        return filesystem, path

    @property
    def _cache_path(self) -> str:
        """OS path of an archive after any required one-time localization."""
        filesystem, path = self._cache
        if not isinstance(filesystem, pyarrow.fs.LocalFileSystem):  # pragma: no cover - invariant
            raise RuntimeError("a FIX registry archive was not localized")
        return path

    def _sync_archive(self) -> None:
        """Copy a modified localized archive back to its Arrow filesystem."""
        source = self._cache_source
        if source is None:
            if Url.from_string(os.fspath(self.cache_dir)).scheme in HTTP:
                raise OSError("an HTTP FIX registry archive is read-only")
            return
        filesystem, path = source
        if isinstance(filesystem, pyarrow.fs.LocalFileSystem):
            return
        write_bytes(pathlib.Path(self._cache_path).read_bytes(), path, filesystem)

    @cached_property
    def _documents(self) -> Documents:
        """Where this registry's documents are read and written, resolved once."""
        if self.archived:
            return ArchiveDocuments(self._cache_path, self._sync_archive)
        filesystem, directory = self._cache
        return DirectoryDocuments(filesystem, directory)

    @cached_property
    def _layout(self) -> ShardedLayout:
        """This store's documents as records: tag shards, and one file per component."""
        return ShardedLayout(self._documents)

    def into_zip(self, target: str | os.PathLike[str]) -> pathlib.Path | str:
        """Write everything this registry holds into one archive, and name it.

        The documents are copied verbatim, so this is how a store travels
        between a directory and a zip and back without being rebuilt.
        """
        return write_archive(target, self._documents.read_many(""))

    def into_projection(
        self,
        target: str | os.PathLike[str],
        keys: Sequence[int | str],
        fields: Sequence[Field] = (),
    ) -> pathlib.Path | str:
        """Write a deterministic offline registry containing only `keys`.

        The records themselves, copied rather than rebuilt: a record already
        holds every version's contribution, and re-deriving one from a version
        walk would drop the aliases and the hand-written encodings on it.
        """
        if not keys:
            raise ValueError("a FIX registry projection needs at least one field")
        extra: dict[str, list[Field]] = {}
        for member in fields:
            version = member.fix.get("version")
            if not version:
                raise ValueError(f"projected FIX field {member.name!r} has no fix:version")
            extra.setdefault(version, []).append(member)
        overlap = set(extra).intersection(self.versions)
        if overlap:
            raise ValueError(f"projected FIX versions already exist: {sorted(overlap)}")
        if _resource_identity(target) == _resource_identity(self.cache_dir, self.filesystem):
            raise ValueError("a registry projection cannot replace its source")

        held = self._entries[0]
        selected: dict[int | str, FieldEntry] = {}
        missing = []
        for key in keys:
            entry = self._record(key)
            if entry is not None and entry.key in held:
                selected[entry.key] = entry
                continue
            found = [
                member
                for members in extra.values()
                for member in members
                if (
                    int(member.fix.get("tag") or 0) == int(key)
                    if _is_tag(key)
                    else member.name.lower() == str(key).strip().lower()
                )
            ]
            if not found:
                missing.append(key)
        if missing:
            raise KeyError(f"no FIX fields {missing!r} in this registry")
        versions = (*extra, *self.versions)
        added, _, _ = collapse(versions, extra, {})
        selected.update(added)

        sessions: dict[str, Sequence[tuple[str, bool]]] = {}
        declared: dict[str, Sequence[SpecComponent]] = {}
        for version in self.versions:
            names = {entry.name for entry in selected.values() if entry.declares(version)}
            sessions[version] = [
                (name, required) for name, required in self.session(version) if name in names
            ]
            # Whole, never projected: a component declares where a group starts
            # and ends, and a tree missing the members whose tags this
            # projection did not select would split the group somewhere else.
            # Dropping them entirely is worse still -- `components()` then
            # answers `[]` for every version and the Parties extractor silently
            # produces nothing.
            stored = self._stored_components(version)
            if stored is not None:
                declared[version] = stored
        return write_archive(
            target,
            documents_of(versions, selected, self._entries[1], sessions, declared),
        )

    def _fetch(self, url: str) -> str:
        """One page, as text, retried while the site says "later"."""
        if self.offline:
            raise OSError(f"{url} was not fetched: this registry is offline")
        request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        pause = self.backoff
        for _ in range(self.retries):
            try:
                return self._read(request)
            except OSError as error:
                if not _is_transient(error):
                    raise
                time.sleep(_wait_for(error, pause))
                pause *= 2
        # The last attempt is the one whose failure is the caller's.
        return self._read(request)

    def _read(self, request: urllib.request.Request) -> str:
        """One page fetch, once. The single place the network is touched."""
        return read_bytes(request, timeout=self.timeout).decode("utf-8", "replace")


# -- the store, as a directory or as a zip ------------------------------------


def _resource_identity(
    resource: str | os.PathLike[str], filesystem: pyarrow.fs.FileSystem | None = None
) -> str:
    """Canonical identity used when comparing two registry resources."""
    location = os.fspath(resource)
    if filesystem is not None:
        return f"{id(filesystem)}:{location}"
    return Url.from_string(location).into_string()


# -- the wire ----------------------------------------------------------------

#: Answers that mean "later" rather than "no": a scrape waits these out.
_RETRIED = frozenset({408, 425, 429, 500, 502, 503, 504})

#: A pause the site asked for is honoured up to this, so one absurd
#: `Retry-After` cannot park a whole scrape for an afternoon.
_MAX_WAIT = 60.0


def _is_transient(error: Exception) -> bool:
    """Whether an error means "ask again later" rather than "there is none"."""
    if isinstance(error, urllib.error.HTTPError):
        return error.code in _RETRIED
    return isinstance(error, urllib.error.URLError | TimeoutError | ConnectionError)


def _wait_for(error: Exception, pause: float) -> float:
    """How long to wait: what the site asked for, else the caller's pause.

    `Retry-After` is seconds or an HTTP date, and only the seconds spelling is
    read -- a date is the site's clock, and the two clocks are not the same.
    """
    headers = getattr(error, "headers", None)
    asked = headers.get("Retry-After", "") if headers is not None else ""
    seconds = float(asked) if str(asked).strip().isdigit() else pause
    return min(seconds, _MAX_WAIT)


# -- page parsing ------------------------------------------------------------


def _text(markup: str) -> str:
    """Markup as one line of text: tags out, entities decoded, spaces folded."""
    return _SPACES.sub(" ", html.unescape(_TAGS.sub(" ", markup))).strip()


def _split_note(label: str) -> tuple[str, str]:
    """`Name (no longer used)` -> `("Name", "no longer used")`."""
    note = _NOTE.search(label)
    if note is None:
        return label, ""
    return label[: note.start()].strip(), note[1].strip()


def _sections(page: str, start: int) -> tuple[str, str, str]:
    """A field page cut into its three parts: prose, values, messages."""
    body = page[start:]
    used_in = ""
    carried = _USED_IN.search(body)
    if carried is not None:
        body, used_in = body[: carried.start()], body[carried.end() :]
    values = ""
    valid = _VALID_VALUES.search(body)
    if valid is not None:
        body, values = body[: valid.start()], body[valid.end() :]
    described = _DESCRIPTION_HEADING.search(body) or _DESCRIPTION_ANCHOR.search(body)
    prose = body[described.end() :] if described is not None else _until_section(body)
    return prose, values, used_in


def _until_section(markup: str) -> str:
    """The prose of a page that never says `Description`, up to what follows it.

    The markers are what the older pages put after the field's own paragraph;
    the length cap is what stops a page carrying *none* of them from making
    its whole body one description.
    """
    window = markup[:8000]
    for marker in ("Used in", "Used In", "<h3", "<ul", "<table"):
        cut = window.find(marker)
        if cut >= 0:
            window = window[:cut]
    return window


def _description(prose: str) -> str:
    """The field's own paragraphs, as one line of text."""
    return _text(prose[:8000])


def _values(markup: str, *, names: bool = False) -> dict[str, str]:
    """The enumerated values a field page lists: `{"1": "Buy", ...}`."""
    found: dict[str, str] = {}
    for _, item in _VALUE_ITEM.findall(markup):
        text = _text(item)
        value = _VALUE.match(text)
        if value:
            label = name_of(value[2]) if names else value[2]
            if label:
                found.setdefault(value[1], label)
    return found


def _used_in(markup: str, kind: str = MESSAGE_LINK) -> list[str]:
    """What a field page's `Used In` links name: its messages, or its components.

    Two kinds of link sit in the same section, and reading only the first left
    every field that FIX carries *inside a component block* -- `TrdRegTimestamp
    <769>`, and three hundred others in 4.4 alone -- recorded as used nowhere.
    """
    names = []
    seen: set[str] = set()
    pattern = rf"<a[^>]+href=\"{kind}_[^\"]+\"[^>]*>(.*?)</a>"
    for match in re.finditer(pattern, markup, re.DOTALL):
        name, _ = _split_note(_text(match[1]))
        # A component link's own angle brackets are its name, unlike the tag
        # suffix on a message link.
        name = _COMPONENT_NAME.sub(r"\1", name) if kind == COMPONENT_LINK else name
        name = name_of(name)
        folded = name.casefold()
        if name and folded not in seen:
            names.append(name)
            seen.add(folded)
    return names


# -- ordering and matching ---------------------------------------------------


def _tier(entry: FieldEntry, tier: str) -> tuple[str, ...]:
    """The names one record claims in one resolution tier."""
    if tier == _CANONICAL:
        return (entry.name,)
    return tuple(alias.name for alias in entry.aliases)


def _problems(
    held: tuple[Mapping[int | str, FieldEntry], Mapping[str, ComponentEntry]],
) -> list[str]:
    """Everything inconsistent about a set of records, as lines.

    Written against the records rather than against a registry, so a change
    can be checked before it is written and refused whole.
    """
    problems = list(_duplicates(held[0]))
    claimed: dict[str, str] = {}
    for tier in TIERS:
        names: dict[str, list[str]] = {}
        for entry in held[0].values():
            for spelled in _tier(entry, tier):
                names.setdefault(fold(spelled), []).append(entry.name)
        for folded, owners in sorted(names.items()):
            unique = list(dict.fromkeys(owners))
            held_by = claimed.get(folded)
            if held_by is None and len(unique) > 1:
                problems.append(f"{folded!r} is claimed by {unique}")
            elif held_by is not None and held_by not in unique:
                # An earlier tier already answers for this name, so what was
                # recorded here can never resolve. Precedence is the rule and
                # not the problem; a spelling nothing will ever reach is.
                problems.append(f"{folded!r} is {tier} for {unique} and already {held_by}'s")
            claimed.setdefault(folded, unique[0])
    for slug, shared in sorted(slug_collisions(e.name for e in held[1].values()).items()):
        problems.append(
            f"FIX components {shared} are all stored as components/{slug}{DOCUMENT_SUFFIX}"
        )
    return problems


def _duplicates(entries: Mapping[int | str, FieldEntry]) -> Iterator[str]:
    """Two identities claiming one tag, or one canonical name.

    The two ways a store answers a lookup with whichever entry it happened to
    read first, which is the same store answering differently on two machines.
    Reported before anything else, because every later line is about spellings
    and these are about identity.
    """
    by_tag: dict[int, list[str]] = {}
    by_name: dict[str, list[str]] = {}
    for entry in entries.values():
        if entry.tag is not None:
            by_tag.setdefault(int(entry.tag), []).append(entry.name)
        by_name.setdefault(entry.folded, []).append(entry.name)
    for tag, shared in sorted(by_tag.items()):
        if len(shared) > 1:
            yield (
                f"FIX tag {tag} is claimed by {sorted(shared)}: one tag is one identity, so "
                "give the newer spelling its own tag or record it as an alias of the older"
            )
    for folded, shared in sorted(by_name.items()):
        if len(shared) > 1:
            yield (
                f"FIX field name {folded!r} is claimed by {sorted(shared)}: one name is one "
                "identity, so rename one of them or record it as an alias"
            )


def _is_tag(key: Any) -> bool:
    return isinstance(key, int) or str(key).strip().isdigit()


def _rank(member: Field, wanted: str) -> int | None:
    """How well one field matches a lowercased query; None is not at all."""
    name = member.name.lower()
    if wanted == member.fix.get("tag") or wanted == name:
        return 0
    if name.startswith(wanted):
        return 1
    if wanted in name:
        return 2
    if wanted in member.description.lower():
        return 3
    return None


def _levenshtein(one: str, other: str, ceiling: int) -> int | None:
    """Edit distance, or None once it exceeds `ceiling`.

    Two rows and an early exit: the fallback runs over every field name, so
    a distance that is already past the ceiling must stop paying for the
    rest of the matrix.
    """
    if abs(len(one) - len(other)) > ceiling:
        return None
    if one == other:
        return 0
    previous = list(range(len(other) + 1))
    for row, left in enumerate(one, start=1):
        current = [row]
        best = row
        for column, right in enumerate(other, start=1):
            cost = min(
                previous[column] + 1,
                current[column - 1] + 1,
                previous[column - 1] + (left != right),
            )
            current.append(cost)
            best = min(best, cost)
        if best > ceiling:
            return None
        previous = current
    return previous[-1] if previous[-1] <= ceiling else None
