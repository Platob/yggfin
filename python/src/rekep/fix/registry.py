"""Every FIX field of every FIX version, scraped once and kept for offline use."""

from __future__ import annotations

import concurrent.futures
import contextlib
import dataclasses
import hashlib
import html
import importlib.resources
import io
import os
import pathlib
import re
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request
import warnings
import zipfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from functools import cached_property, lru_cache
from types import MappingProxyType
from typing import Any, Self

import pyarrow.fs

from rekep.arrow_path import ArrowPath
from rekep.convert import Convertible
from rekep.enums import EventType, State
from rekep.fields import Field, column_name, newest_rank
from rekep.fields.metadata import encoded_key, values_of
from rekep.filesystems import local_path, read_bytes, resolve, spill_path
from rekep.fix.entries import (
    ANY_VERSION,
    NAMESPACE,
    Alias,
    ComponentRecord,
    FixFieldValue,
    fold,
    merged_record,
    name_of,
    record_copy,
    record_kind,
    records_for,
    refuse_record,
)
from rekep.fix.fields import datatype_identity, fix_field, namespaced_field
from rekep.fix.quickfix import (
    QUICKFIX_URL,
    SPEC_VERSIONS,
    SpecField,
    declared_group,
    entry_of,
    first_declared_name,
    is_group,
    is_reference,
    members_of,
    parse_declarations,
    parse_session,
    parse_session_components,
    parse_spec,
    spec_name,
    walk,
)
from rekep.fix.store import (
    ADDED,
    ALIASES,
    COMPONENTS,
    DECLARED,
    DOCUMENT_SUFFIX,
    FIELDS,
    NAME,
    NOTE,
    SESSIONS,
    STORED,
    TYPE,
    VALUES,
    VERSIONS_FILE,
    ArchiveDocuments,
    Collapse,
    ConflictReport,
    DirectoryDocuments,
    Documents,
    Dropped,
    ShardedLayout,
    collapse,
    component_from_document,
    document_of,
    documents_of,
    field_document,
    field_from_document,
    slug_collisions,
    write_archive,
)
from rekep.urls import HTTP, LOCAL, Url

#: The ordered prose dictionaries: Nanoconda names every enumerated value and
#: OnixS fills anything it does not carry.
NANOCONDA_URL = "https://nanoconda.com/fix-reference"
ONIXS_URL = "https://www.onixs.biz/fix-dictionary"

# Sent with every request so scrape traffic identifies its client.
_USER_AGENT = "rekep-fix-registry"

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
BOOTSTRAP_PAGES = 14_000
BOOTSTRAP_DURATION = "several hours"

#: Versions in a per-version directory name: `4.4`, `5.0.SP2`, `FIXT1.1`.
_VERSION_LINK = re.compile(r"/fix-dictionary/([^/\"'#]+)/index\.html")

#: Nanoconda spells application versions with the `FIX.` prefix and service
#: packs without the dot used by the local canonical spelling.
_NANOCONDA_VERSION_LINK = re.compile(r'href="(FIX(?:T)?\.[^/\"#]+?)/index\.html"')

#: One field on a `fields_by_tag.html` page: the link and the text inside it.
_TAG_LINK = re.compile(r"<a[^>]+href=\"tagNum_(\d+)\.html\"[^>]*>(.*?)</a>", re.DOTALL)
_NANOCONDA_TAG_LINK = re.compile(r'<a[^>]+href="fields/(\d+)\.html"[^>]*>(.*?)</a>', re.DOTALL)

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

_NANOCONDA_TITLE = re.compile(
    r'<div[^>]+class="tag-number"[^>]*>\s*Tag\s+(\d+)\s*</div>\s*<h1[^>]*>(.*?)</h1>',
    re.DOTALL | re.IGNORECASE,
)
_NANOCONDA_META = re.compile(
    r'<span[^>]+class="label"[^>]*>(.*?)</span>\s*'
    r'<span[^>]+class="value"[^>]*>(.*?)</span>',
    re.DOTALL | re.IGNORECASE,
)
_NANOCONDA_DESCRIPTION = re.compile(
    r'<div[^>]+class="card-header"[^>]*>\s*<h3>\s*Description\s*</h3>\s*</div>\s*'
    r'<div[^>]+class="card-body"[^>]*>(.*?)</div>',
    re.DOTALL | re.IGNORECASE,
)
_NANOCONDA_ENUMERATED = re.compile(
    r"<h3>\s*Enumerated Values\s*</h3>.*?<tbody>(.*?)</tbody>",
    re.DOTALL | re.IGNORECASE,
)
_NANOCONDA_VALUE = re.compile(
    r'<tr>\s*<td>\s*<span[^>]+class="enum-value"[^>]*>(.*?)</span>\s*</td>\s*'
    r'<td[^>]+class="enum-name"[^>]*>(.*?)</td>\s*'
    r'<td[^>]+class="enum-desc"[^>]*>(.*?)</td>',
    re.DOTALL | re.IGNORECASE,
)
_NANOCONDA_USED = re.compile(r'href="\.\./messages/([^/\"#]+)\.html"')

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


#: The dictionary every unconfigured lookup resolves through, and the one
#: thing `set_builtin` moves. Held here rather than in a `functools.cache` on
#: `from_builtin`, because a cache keyed on the arguments cannot be *replaced*
#: -- only cleared -- and an installed default is a value, not a memo.
_BUILTIN: Any = None


#: What memoizes an answer derived from the default under a key that does not
#: mention it, as `module, attribute path`. Everything else either keys on the
#: registry object -- and so misses a new default by construction -- or
#: resolves per call. Read out of `sys.modules` rather than imported: this
#: runs while `rekep.fix.columns` is still executing its own module body the
#: first time a default is installed, and a module nobody has imported yet has
#: nothing cached to forget.
_BUILTIN_VIEWS: tuple[tuple[str, str], ...] = (
    ("rekep.text.fixmsg", "FixMsg.into_registry"),
    ("rekep.fix.columns", "_schemes"),
    ("rekep.market.fix", "_tag_of"),
    ("rekep.market.fix", "MarketTags.standard"),
    ("rekep.market.ticker", "SymbolTicker.from_str"),
)


def _forget_builtin_views() -> None:
    """Drop what memoized the previous default without holding it in a key."""
    for name, path in _BUILTIN_VIEWS:
        module = sys.modules.get(name)
        if module is None:
            continue
        held: Any = module
        for step in path.split("."):
            held = getattr(held, step, None)
            if held is None:
                break
        clear = getattr(held, "cache_clear", None)
        if clear is not None:
            clear()
    columns = sys.modules.get("rekep.fix.columns")
    if columns is not None and hasattr(columns, "_REGISTRY"):
        # Read by `_schemes`, which the loop above only emptied.
        columns._REGISTRY = _BUILTIN


def remote_cache() -> pathlib.Path:
    """Where a store this machine cannot open a file on is kept once fetched.

    Beside the default store rather than inside it: what lands here is a copy
    of somebody else's dictionary, reused across processes by identity and
    size. A `TemporaryDirectory` would make every process fetch it again --
    the whole point of pointing `cache_dir` at a bucket is that the fetch
    happens once.

    A function and not a constant, so it follows `CACHE_DIRECTORY` wherever a
    caller has moved it.
    """
    return CACHE_DIRECTORY.with_name(f"{CACHE_DIRECTORY.name}-remote")


def builtin_projection() -> str:
    """Where the packaged standard projection and rekep vocabulary live.

    `rekep.fix.publish` calls it a projection because that is what it is -- it
    parses common traffic and misses the long tail -- and nothing here ever
    presents it as the whole dictionary.
    """
    return os.fspath(importlib.resources.files(__package__).joinpath("registry.zip"))


_TAGS = re.compile(r"<[^>]+>")
_SPACES = re.compile(r"\s+")
_DEFAULT = object()


@dataclasses.dataclass(frozen=True)
class RegistrySource:
    """One FIX dictionary with the three readings a scrape needs."""

    name: str
    url: str
    workers: int = 8
    shared_field_page: bool = False
    field_pause_seconds: float = 0.0

    def versions(self, fetch: Callable[[str], str]) -> tuple[str, ...]:
        """Every version this source carries."""
        raise NotImplementedError

    def tags(self, fetch: Callable[[str], str], version: str) -> dict[int, tuple[str, str]]:
        """The tags and names this source carries for one version."""
        raise NotImplementedError

    def field(self, fetch: Callable[[str], str], version: str, tag: int) -> dict[str, Any]:
        """One field reading, or nothing where this source has no page."""
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class NanocondaSource(RegistrySource):
    """Nanoconda's FIX reference, including symbolic value names."""

    name: str = "nanoconda"
    url: str = NANOCONDA_URL

    def versions(self, fetch: Callable[[str], str]) -> tuple[str, ...]:
        url = f"{self.url}/index.html"
        page = fetch(url)
        found = dict.fromkeys(
            _from_nanoconda_version(version) for version in _NANOCONDA_VERSION_LINK.findall(page)
        )
        if not found:
            raise ValueError(f"{url} lists no FIX versions; is the source layout new?")
        return tuple(sorted(found, key=newest_rank, reverse=True))

    def tags(self, fetch: Callable[[str], str], version: str) -> dict[int, tuple[str, str]]:
        url = f"{self.url}/{_into_nanoconda_version(version)}/fields.html"
        listed = _linked_tags(fetch(url), _NANOCONDA_TAG_LINK)
        if not listed:
            raise ValueError(f"{url} lists no FIX fields; is the source layout new?")
        return listed

    def field(self, fetch: Callable[[str], str], version: str, tag: int) -> dict[str, Any]:
        url = f"{self.url}/{_into_nanoconda_version(version)}/fields/{tag}.html"
        page = _optional_page(fetch, url)
        if not page:
            return {}
        detail: dict[str, Any] = {}
        title = _NANOCONDA_TITLE.search(page)
        if not title or title[1] != str(tag):
            raise ValueError(f"{url} does not describe FIX field {tag}; is the source layout new?")
        detail["name"] = name_of(_text(title[2]))
        metadata = {
            _text(key).casefold(): _text(value) for key, value in _NANOCONDA_META.findall(page)
        }
        if metadata.get("type"):
            detail["type"] = metadata["type"]
        if metadata.get("added"):
            detail["added"] = _from_nanoconda_version(metadata["added"])
        described = _NANOCONDA_DESCRIPTION.search(page)
        if described and (description := _text(described[1])):
            detail["description"] = description
        enumerated = _NANOCONDA_ENUMERATED.search(page)
        values: list[FixFieldValue] = []
        if enumerated:
            for value, name, meaning in _NANOCONDA_VALUE.findall(enumerated[1]):
                wire, alias, prose = _text(value), _text(name), _text(meaning)
                if wire:
                    values.append(
                        FixFieldValue(
                            value=wire,
                            meaning=prose,
                            # `encoded_key` already erases case and punctuation; one
                            # source spelling serves Fill, fill and FILL.
                            aliases=(alias,) if alias else (),
                        )
                    )
        if values:
            detail["values"] = tuple(values)
        used_at = enumerated.end() if enumerated else 0
        used = list(dict.fromkeys(_NANOCONDA_USED.findall(page[used_at:])))
        if used:
            detail["used_in"] = used
        return detail


@dataclasses.dataclass(frozen=True)
class OnixSSource(RegistrySource):
    """OnixS's FIX dictionary."""

    name: str = "onixs"
    url: str = ONIXS_URL
    workers: int = 1
    field_pause_seconds: float = 1.0

    def versions(self, fetch: Callable[[str], str]) -> tuple[str, ...]:
        url = f"{self.url}.html"
        page = fetch(url)
        found = dict.fromkeys(_VERSION_LINK.findall(page))
        found.pop("latest", None)
        if not found:
            raise ValueError(f"{url} lists no FIX versions; is the source layout new?")
        return tuple(sorted(found, key=newest_rank, reverse=True))

    def tags(self, fetch: Callable[[str], str], version: str) -> dict[int, tuple[str, str]]:
        url = f"{self.url}/{version}/fields_by_tag.html"
        listed = _linked_tags(fetch(url), _TAG_LINK)
        if not listed:
            raise ValueError(f"{url} lists no FIX fields; is the source layout new?")
        return listed

    def field(self, fetch: Callable[[str], str], version: str, tag: int) -> dict[str, Any]:
        url = f"{self.url}/{version}/tagNum_{tag}.html"
        page = _optional_page(fetch, url)
        if not page:
            return {}
        detail: dict[str, Any] = {}
        title = _TITLE.search(page)
        if not title or title[2] != str(tag):
            raise ValueError(f"{url} does not describe FIX field {tag}; is the source layout new?")
        detail["name"] = name_of(_text(title[1]))
        typed = _TYPE.search(page)
        if typed:
            detail["type"] = typed[1]
        prose, listed, carried = _sections(page, typed.end() if typed else 0)
        if description := _description(prose):
            detail["description"] = description
        if values := _values(listed, names=tag == 35):
            detail["values"] = values
        if used := _used_in(carried):
            detail["used_in"] = used
        if components := _used_in(carried, COMPONENT_LINK):
            detail["components"] = components
        return detail


@dataclasses.dataclass(frozen=True)
class QuickFixSource(RegistrySource):
    """The QuickFIX machine-readable specification."""

    name: str = "quickfix"
    url: str = QUICKFIX_URL
    workers: int = 1
    shared_field_page: bool = True

    def versions(self, fetch: Callable[[str], str]) -> tuple[str, ...]:
        available = _optional_page(fetch, f"{self.url}/{spec_name('4.4')}")
        return SPEC_VERSIONS if available else ()

    def tags(self, fetch: Callable[[str], str], version: str) -> dict[int, tuple[str, str]]:
        document = self.document(fetch, version)
        if not document:
            return {}
        reading = _parsed_quickfix(document)
        if not reading.fields:
            raise ValueError(f"{self.url}/{spec_name(version)} lists no FIX fields")
        return {tag: (field.name, "") for tag, field in reading.fields.items()}

    def field(self, fetch: Callable[[str], str], version: str, tag: int) -> dict[str, Any]:
        document = self.document(fetch, version)
        if not document:
            return {}
        reading = _parsed_quickfix(document)
        known = reading.fields.get(tag)
        if known is None:
            return {}
        values = tuple(
            FixFieldValue(value=value, aliases=(symbol,)) for value, symbol in known.values.items()
        )
        detail: dict[str, Any] = {"name": known.name, "type": known.datatype, "values": values}
        used_in, components = reading.usage.get(known.name, ((), ()))
        if used_in:
            detail["used_in"] = used_in
        if components:
            detail["components"] = components
        return detail

    def document(self, fetch: Callable[[str], str], version: str) -> str:
        """The XML document that also owns sessions and components."""
        return _optional_page(fetch, f"{self.url}/{spec_name(version)}")


DEFAULT_SOURCES: tuple[RegistrySource, ...] = (
    NanocondaSource(),
    OnixSSource(),
    QuickFixSource(),
)


@dataclasses.dataclass(frozen=True)
class _QuickFixReading:
    """One parsed QuickFIX document and its field usage."""

    fields: Mapping[int, SpecField]
    declarations: Mapping[str, Field]
    usage: Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]]


@lru_cache(maxsize=len(SPEC_VERSIONS) * 2)
def _parsed_quickfix(document: str) -> _QuickFixReading:
    """A bounded immutable reading per exact QuickFIX document."""
    fields = MappingProxyType(parse_spec(document))
    declarations = MappingProxyType(parse_declarations(document))
    usage: dict[str, tuple[list[str], list[str]]] = {}

    def add(field: str, slot: int, owner: str) -> None:
        owners = usage.setdefault(field, ([], []))[slot]
        if owner not in owners:
            owners.append(owner)

    # Direct owners lead the graph-reachable parents in stored metadata.
    for declared in declarations.values():
        slot = 0 if declared.fix.msgtype else 1
        for member, _ in walk(declared):
            if is_reference(member):
                continue
            add(member.name, slot, declared.name)

    expanded: dict[str, tuple[str, ...]] = {}

    def fields_of(name: str) -> tuple[str, ...]:
        held = expanded.get(name)
        if held is not None:
            return held
        names: list[str] = []
        for member, _ in walk(declarations[name]):
            if is_reference(member):
                names.extend(fields_of(member.name))
            else:
                names.append(member.name)
        found = tuple(dict.fromkeys(names))
        expanded[name] = found
        return found

    for declared in declarations.values():
        slot = 0 if declared.fix.msgtype else 1
        for name in fields_of(declared.name):
            add(name, slot, declared.name)
    for name, component in parse_session_components(document):
        add(name, 1, component)
    return _QuickFixReading(
        fields,
        declarations,
        MappingProxyType(
            {
                name: (tuple(messages), tuple(components))
                for name, (messages, components) in usage.items()
            }
        ),
    )


@dataclasses.dataclass(eq=False)
class FixRegistry(Convertible):
    """The FIX dictionary as local `Field` declarations."""

    #: Dictionaries in priority order; the first stated reading wins and later
    #: sources fill its gaps.
    sources: tuple[RegistrySource, ...] = DEFAULT_SOURCES

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

    #: Seconds one page fetch may take, and the maximum fetched at once. Each
    #: source may lower the latter to stay within its own throttle.
    timeout: float = 30.0
    max_workers: int = 8

    #: How many times a fetch that was refused *for now* is asked again, and
    #: the first pause before it is. The pause doubles per attempt (capped at
    #: a minute each), so six retries wait about two minutes in total: the
    #: dictionary is fourteen thousand pages, the sites throttle harder the
    #: further in a scrape gets, and half a minute of patience was measured
    #: to be too little to finish one. Still short enough that a site which is
    #: really down is reported as down rather than waited on.
    retries: int = 6
    backoff: float = 2.0

    #: How long a local store may go without being checked against upstream,
    #: in seconds. `0` -- the default -- never refetches: the local copy is
    #: what this registry serves, which is the whole of what "offline-first"
    #: means and what every pipeline reading a packaged dictionary wants.
    #:
    #: Above zero, a store older than this is regenerated from the sources before
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

    #: Successful raw pages held outside an incomplete explicit scrape.
    _source_page_cache: pathlib.Path | None = dataclasses.field(
        init=False, default=None, repr=False
    )

    def __post_init__(self, registry_token: str | None) -> None:
        """Normalise the locations once, then bootstrap the default store."""
        self.sources = tuple(
            dataclasses.replace(
                source,
                url=Url.from_string(str(source.url)).into_string().rstrip("/"),
            )
            for source in self.sources
        )
        if not self.sources:
            raise ValueError("a FIX registry needs at least one source")
        names = [source.name for source in self.sources]
        if any(not name for name in names) or len(set(names)) != len(names):
            raise ValueError("FIX registry source names must be non-empty and distinct")
        if self.max_workers < 1 or any(source.workers < 1 for source in self.sources):
            raise ValueError("FIX registry source worker counts must be positive")
        if any(source.field_pause_seconds < 0 for source in self.sources):
            raise ValueError("FIX registry source field pauses cannot be negative")
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
        named = self.cache_dir is not None
        if self.cache_dir is None:
            self.cache_dir = CACHE_DIRECTORY
        if self.cache_ttl < 0:
            raise ValueError(f"a FIX registry cache TTL cannot be negative: {self.cache_ttl}")
        # Asked once, of the location it was pointed at, before anything here
        # can write to it. Asked again later it would answer differently the
        # moment a scrape wrote its first document, and stop the scrape it is
        # inside.
        self.__dict__["_found_a_store"] = named or bool(self._documents.names())
        self.__dict__["_installed"] = self.bootstrap()

    @property
    def offline(self) -> bool:
        """Whether this registry answers from its store alone.

        Inferred rather than declared, and inferred from the one thing that
        says it: which store this registry was pointed at. Naming a
        `cache_dir` *is* saying where the answers come from -- `data/fix.zip`,
        a directory a worker mounts, a bucket -- so a registry that was given
        one serves it, warm or cold, and never reaches a source. That is what
        a pipeline needs and what every caller used to have to remember to
        ask for.

        The default store, `~/.config/fix`, is the only one nobody chose. Cold,
        it may fill itself, and says what that will cost before it starts;
        once it holds documents it answers from them like any other.

        A fetch verb -- `scrape`, `rebuild`, `fields(refresh=True)`, a
        `cache_ttl` that came due -- opens the door explicitly. Nothing else
        does, so no read can turn into a fourteen-thousand-page scrape.
        """
        return self.__dict__.get("_found_a_store", False)

    @property
    def _serves_stored(self) -> bool:
        """`offline`, unless a fetch verb has opened the door right now."""
        return self.offline and not self.__dict__.get("_may_fetch")

    @contextlib.contextmanager
    def _fetching(self) -> Iterator[None]:
        """The window in which this registry may reach its sources."""
        held = self.__dict__.get("_may_fetch", 0)
        self.__dict__["_may_fetch"] = held + 1
        try:
            yield
        finally:
            self.__dict__["_may_fetch"] = held

    @property
    def revision(self) -> int:
        """Generation of the in-memory views over this mutable store."""
        return self.__dict__.get("_revision", 0)

    @classmethod
    def from_builtin(cls, cache_ttl: float = 0.0) -> Self:
        """The default dictionary every unconfigured lookup resolves through.

        The packaged standard projection and rekep vocabulary, unless
        `set_builtin` installed another. `cache_ttl` above zero builds a fresh
        registry that checks its age against the configured sources before
        serving -- which reaches the network, so it is never the installed
        default. Zero, the default, is the one held below.
        """
        held = _BUILTIN
        if cache_ttl:
            return cls._checked_builtin(cache_ttl)
        if held is not None and isinstance(held, cls):
            return held
        return cls.set_builtin(cls._checked_builtin(0.0))

    @classmethod
    def set_builtin(cls, registry: Self | None = None) -> Self:
        """Install the default `from_builtin` hands back, and return it.

        None restores the packaged projection. The registry has to carry
        rekep's own vocabulary -- the 36 identities every product
        contract is declared against -- so an installed one that does not is
        refused here rather than reported as a missing field halfway through a
        parse.

        Its scope is what an *unconfigured* lookup resolves through:
        `FixMsg().registry`, `FixMsg.into_codec()`, `MarketTags.of()`,
        `FieldAccess.of()`, `Ascii32`'s declaration. It is deliberately not
        the published Arrow contracts -- `rekep.fix.columns` reads the default
        while its module body runs, and those declarations are what
        `schemas/rekep/*.yaml` publishes. Call this at startup, before the
        first parse; calling it mid-pipeline moves what later rows resolve
        through and leaves the rows already parsed reading the old one.
        """
        from rekep.fix.rekep import rekep_is_registered

        global _BUILTIN
        installed = cls._checked_builtin(0.0) if registry is None else registry
        if not rekep_is_registered(installed):
            raise RuntimeError("the FIX registry lacks rekep's declared vocabulary")
        held = _BUILTIN
        _BUILTIN = installed
        if held is not None and held is not installed:
            _forget_builtin_views()
        return installed

    @classmethod
    def _checked_builtin(cls, cache_ttl: float) -> Self:
        """The packaged projection, refusing one that lost rekep's vocabulary."""
        from rekep.fix.rekep import rekep_is_registered

        registry = cls(cache_dir=builtin_projection(), cache_ttl=cache_ttl)
        if not rekep_is_registered(registry):
            raise RuntimeError("the packaged FIX registry lacks rekep's declared vocabulary")
        return registry

    @classmethod
    def scrape(
        cls,
        dump_folder: str | os.PathLike[str] | None = None,
        **configuration: Any,
    ) -> Self:
        """Scrape a fresh registry, resuming pages, then replace one local directory."""
        reserved = {"cache_dir", "filesystem"} & configuration.keys()
        if reserved:
            raise TypeError(f"scrape configures {sorted(reserved)} through dump_folder")
        location = Url.from_string(os.fspath(dump_folder or CACHE_DIRECTORY))
        if location.scheme not in LOCAL or pathlib.PurePath(location.path).suffix.lower() == ".zip":
            raise ValueError("FixRegistry.scrape requires a local dump folder")
        target = pathlib.Path(local_path(location.into_string()))
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and (not target.is_dir() or target.is_symlink()):
            raise ValueError(f"the FIX registry dump target is not a directory: {target}")
        page_cache = target.with_name(f".{target.name}-source-pages")
        if page_cache.exists() and (not page_cache.is_dir() or page_cache.is_symlink()):
            raise ValueError(f"the FIX registry source page cache is not a directory: {page_cache}")
        local_fields, local_components, local_overlays = cls._local_declarations(target)

        report = ConflictReport()
        with tempfile.TemporaryDirectory(prefix=f".{target.name}-", dir=target.parent) as scratch:
            root = pathlib.Path(scratch)
            staged = root / target.name
            source = cls(cache_dir=staged, **configuration)
            source._source_page_cache = page_cache
            report = source.rebuild()
            source._restore_local_declarations(local_fields, local_components, local_overlays)
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
        installed = cls(cache_dir=target, **configuration)
        installed.__dict__["_conflicts"] = report
        if page_cache.exists():
            shutil.rmtree(page_cache)
        return installed

    @classmethod
    def _local_declarations(
        cls, target: pathlib.Path
    ) -> tuple[tuple[Field, ...], tuple[ComponentRecord, ...], tuple[Field, ...]]:
        """Local declarations and field configuration a refresh carries forward."""
        if not target.exists():
            return (), (), ()
        try:
            held = cls(cache_dir=target)
            declarations = held._preserved_declarations()
        except (OSError, TypeError, ValueError, pyarrow.ArrowException):
            return (), (), ()
        return declarations

    def _preserved_declarations(
        self,
    ) -> tuple[tuple[Field, ...], tuple[ComponentRecord, ...], tuple[Field, ...]]:
        """Local declarations and overlays not owned by a source dictionary."""
        fields = tuple(
            record_copy(entry)
            for entry in self.field_records().values()
            if ANY_VERSION in entry.fix.versions
        )
        components = tuple(
            ComponentRecord.from_dict(entry.into_dict())
            for entry in self.component_records().values()
            if ANY_VERSION in entry.versions
        )
        overlays = tuple(
            record_copy(entry)
            for entry in self.field_records().values()
            if ANY_VERSION not in entry.fix.versions
            and (
                entry.fix.event_types
                or entry.fix.states
                or entry.fix.column
                or _local_aliases(entry)
            )
        )
        return fields, components, overlays

    def _restore_local_declarations(
        self,
        fields: Sequence[Field],
        components: Sequence[ComponentRecord],
        overlays: Sequence[Field],
    ) -> None:
        """Restore local declarations and overlays after a source rebuild."""
        for entry in fields:
            self.add_field(entry)
        for entry in components:
            self.add_component(entry)
        for overlay in overlays:
            held = self._entries[0].get(overlay.fix.key)
            if held is None:
                continue
            restored = record_copy(held)
            restored.fix.event_types = {**held.fix.event_types, **overlay.fix.event_types}
            restored.fix.states = {**held.fix.states, **overlay.fix.states}
            restored.fix.column = overlay.fix.column or held.fix.column
            aliases = held.fix.named_aliases
            folded = {alias.folded for alias in aliases}
            restored.fix.named_aliases = (
                *aliases,
                *(alias for alias in _local_aliases(overlay) if alias.folded not in folded),
            )
            self.update_field(restored)

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
            self._reduced("it serves a stored dictionary")
            return False
        self._say(
            f"no FIX registry at {self.cache_dir}; fetching the dictionary from "
            f"{', '.join(source.url for source in self.sources)} -- about {BOOTSTRAP_PAGES} "
            f"pages across every FIX version, {BOOTSTRAP_DURATION}. It installs to "
            f"{self.cache_dir} and is never fetched again. To skip it, point "
            "cache_dir at a store you already have."
        )
        started = time.monotonic()
        try:
            report = self.rebuild()
        except (OSError, ValueError) as error:
            self._reduced(f"the configured sources could not be read ({error})")
            return False
        counted = len(self._layout.field_records)
        self._say(
            f"the FIX registry is installed at {self.cache_dir}: {counted} fields, "
            f"{len(self._layout.component_records)} components, "
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
        archive = ArrowPath(
            self.registry_url,
            filesystem,
            filesystem_path=path,
        )
        # The configured archive is a required input. Opening it stays strict,
        # while the bounded reader below remains the one size authority.
        with archive.open_input_stream() as stream:
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

        fields: dict[int | str, Field] = {}
        components: dict[str, ComponentRecord] = {}
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
                    # A record validates by being read, the way a component's
                    # declaration does: a document `Field` cannot parse is not
                    # one, and `refuse_record` says the rest.
                    try:
                        entry = refuse_record(field_from_document(record))
                    except (AttributeError, KeyError, TypeError, ValueError) as error:
                        raise ValueError(
                            f"FIX field {stored!r} in {name!r} is invalid: {error}"
                        ) from error
                    if not set(entry.fix.versions).issubset(known_versions | {ANY_VERSION}):
                        raise ValueError(
                            f"FIX field {stored!r} in {name!r} names a version this store "
                            "does not declare"
                        )
                    declared_tag = entry.fix.tag
                    expected_key = (
                        str(declared_tag) if declared_tag is not None else entry.fix.canonical
                    )
                    if str(stored) != expected_key or field_document(entry.fix.key) != name:
                        raise ValueError(f"FIX field {stored!r} is stored in the wrong shard")
                    if entry.fix.key in fields:
                        raise ValueError(f"FIX field {stored!r} is stored more than once")
                    fields[entry.fix.key] = entry
                continue
            if not name.startswith(f"{COMPONENTS}/"):
                raise ValueError(f"unexpected FIX registry document {name!r}")
            unknown = sorted(set(document) - {"name", "versions", "declaration", "aliases"})
            component_versions = document.get("versions")
            if (
                unknown
                or type(document.get("name")) is not str
                or not isinstance(component_versions, list)
                or any(type(version) is not str for version in component_versions)
                or not set(component_versions).issubset(known_versions | {ANY_VERSION})
                or not isinstance(document.get("declaration"), Mapping)
                or not isinstance(document.get("aliases", []), list)
            ):
                raise ValueError(f"FIX component in {name!r} has invalid metadata")
            try:
                entry = component_from_document(document)
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                raise ValueError(f"FIX component in {name!r} is invalid: {error}") from error
            # The declaration validates by being read: a document Field cannot
            # parse is not one. What is left is the cross-check the shape alone
            # cannot make -- that every tag and every reference it names is
            # something this store actually holds.
            for member, _ in walk(entry.declaration):
                if is_reference(member):
                    component_refs.add(fold(member.name))
                    continue
                tag = member.fix.tag
                if tag is None or tag <= 0:
                    raise ValueError(
                        f"FIX component in {name!r} declares {member.name!r} with no tag"
                    )
                component_tags.add(tag)
            expected = f"{COMPONENTS}/{entry.slug}{DOCUMENT_SUFFIX}"
            if name != expected or entry.slug in components:
                raise ValueError(f"FIX component {entry.name!r} is stored under the wrong name")
            components[entry.slug] = entry
        if not fields:
            raise ValueError("the FIX registry has no fields")
        field_names = {
            fold(spelling) for entry in fields.values() for spelling in entry.fix.spellings()
        }
        for version, members in sessions.items():
            missing = [name for name, _required in members if fold(name) not in field_names]
            if missing:
                raise ValueError(f"the FIX {version} session names unknown fields {missing}")
        missing_tags = sorted(component_tags - {entry.fix.tag for entry in fields.values()})
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
        local_fields, local_components, local_overlays = self._preserved_declarations()
        self.__dict__.pop("_source_pages", None)
        self.__dict__.pop("_source_conflicts", None)
        with self._fetching():
            return self._rebuilt(versions, local_fields, local_components, local_overlays)

    def _rebuilt(
        self,
        versions: Sequence[str],
        local_fields: Sequence[Field],
        local_components: Sequence[ComponentRecord],
        local_overlays: Sequence[Field],
    ) -> ConflictReport:
        """`rebuild`'s body, inside the window that lets it reach a source."""
        try:
            order = tuple(self._spelling(version) for version in versions) or self.versions
            declarations: dict[str, list[Field]] = {}
            sessions: dict[str, Sequence[tuple[str, bool]]] = {}
            components: dict[str, Sequence[Field]] = {}
            reads_spec = any(isinstance(source, QuickFixSource) for source in self.sources)
            for version in order:
                document = self._spec_document(version)
                if reads_spec and not document:
                    raise ValueError(f"the QuickFIX source has no document for {version}")
                declarations[version] = self._scrape_version(version)
                if document:
                    sessions[version] = parse_session(document)
                    components[version] = self._component_declarations(document)
            entries, component_records, report = collapse(order, declarations, components)
            report = dataclasses.replace(
                report,
                collapses=(*report.collapses, *self.__dict__.get("_source_conflicts", ())),
            )
            self._write(documents_of(order, entries, component_records, sessions, components))
            self._restore_local_declarations(local_fields, local_components, local_overlays)
            self.__dict__["_conflicts"] = report
            return report
        finally:
            self.__dict__.pop("_source_pages", None)
            self.__dict__.pop("_source_conflicts", None)

    @property
    def conflicts(self) -> ConflictReport:
        """What the last `rebuild` collapsed; empty for a store it did not build."""
        return self.__dict__.get("_conflicts") or ConflictReport()

    def _write(self, documents: Mapping[str, Mapping[str, Any]]) -> None:
        """Replace this store with `documents`, in one pass over the place."""
        self._documents.write_all(documents)
        self._forget()

    # -- versions ------------------------------------------------------------

    @cached_property
    def versions(self) -> tuple[str, ...]:
        """Every FIX version the dictionary carries, newest first."""
        self.refresh_if_stale()
        stored = self._stored_versions()
        if stored:
            return stored
        if self._serves_stored:
            return self._known_versions()
        try:
            versions = self._scrape_versions()
        except (OSError, ValueError):
            # Offline before the index was ever stored: the versions that
            # *were* scraped are the ones this registry can honestly serve.
            known = self._known_versions()
            if known:
                return known
            raise
        self._store_versions(versions)
        return versions

    def _scrape_versions(self) -> tuple[str, ...]:
        """The union of the configured sources' versions, newest first."""
        found: dict[str, None] = {}
        for source in self.sources:
            try:
                versions = self._read_source(source.versions)
            except OSError as error:
                if not _is_missing(error):
                    raise
                continue
            for version in versions:
                found.setdefault(version, None)
        versions = tuple(sorted(found, key=newest_rank, reverse=True))
        if not versions:
            raise ValueError("the configured FIX sources list no versions")
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
        available = self.__dict__.get("versions")
        if available is not None:
            for candidate in available:
                if candidate.lower() == wanted:
                    return (candidate,)
        # A store can predate versions.json or receive one version's shards
        # independently. Only an index miss earns the wider store scan.
        stored = tuple(dict.fromkeys((*self._stored_versions(), *self._known_versions())))
        for candidate in stored:
            if candidate.lower() == wanted:
                return (candidate,)
        if available is None:
            available = self.versions
            for candidate in available:
                if candidate.lower() == wanted:
                    return (candidate,)
        raise KeyError(f"{version!r} is not a FIX version here; one of {available}")

    # -- fields --------------------------------------------------------------

    def fields(self, version: str, *, refresh: bool = False) -> list[Field]:
        """Every field of one FIX version, from the cache or from one scrape."""
        version = self._spelling(version)
        if not refresh and not self._torn():
            self.refresh_if_stale()
            stored = self._stored_fields(version)
            if stored is not None:
                return stored
        if refresh:
            self.__dict__.pop("_source_pages", None)
        self.__dict__.pop("_source_conflicts", None)
        with self._fetching() if refresh else contextlib.nullcontext():
            return self._scraped_fields(version)

    def _scraped_fields(self, version: str) -> list[Field]:
        """`fields`'s scrape half, once the store had no answer."""
        try:
            document = self._spec_document(version)
            fields = self._scrape_version(version)
            self._store_fields(
                version,
                fields,
                parse_session(document),
                self._component_declarations(document),
            )
            self._indexes.pop(version, None)
            return fields
        finally:
            self.__dict__.pop("_source_pages", None)
            self.__dict__.pop("_source_conflicts", None)

    def fields_available(self, version: str | None = None) -> bool:
        """Whether at least one selected version's fields can be read now."""
        try:
            candidates = self._versions(version)
        except (KeyError, OSError, ValueError):
            return False
        for candidate in candidates:
            if self._stored_fields(candidate) is not None:
                return True
            if self._serves_stored:
                continue
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
        nothing and works offline. Empty when the version has no session layer.
        """
        return self._stored_session(self._spelling(version))

    def components(self, version: str, *, refresh: bool = False) -> list[Field]:
        """Every reusable component of one FIX version, in spec order."""
        version = self._spelling(version)
        stored = self._stored_components(version)
        if stored is not None and not refresh:
            return stored
        if self._serves_stored:
            return stored or []
        if self._stored_fields(version) is None:
            self.fields(version, refresh=refresh)
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

        def visit(declared: Any) -> None:
            for member in members_of(declared):
                if is_group(member):
                    if member.fix.tag:
                        counts.add(int(member.fix.tag))
                    visit(entry_of(member))

        for component in self.components(spelling):
            visit(component)
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
            by_name = {fold(component.name): component for component in components}
            node = by_name.get(fold(root))
            if node is None:
                continue
            declared_in: Any = node
            found: list[str] = []
            for group in groups:
                declared = declared_group(declared_in, group, by_name)
                named = (
                    None if declared is None else first_declared_name(entry_of(declared), by_name)
                )
                if not named:
                    break
                found.append(named)
                declared_in = entry_of(declared)
            else:
                return tuple(found)
        return None

    def components_available(self, version: str) -> bool:
        """Whether this store holds component declarations for `version` at all.

        `components()` answers `[]` both for no declarations and for an
        undeclared version; this method distinguishes those states.
        """
        try:
            return self._stored_components(self._spelling(version)) is not None
        except (KeyError, OSError, ValueError):
            return False

    def component(self, name: str, version: str | None = None) -> Field:
        """The newest declaration of one component, matched by its fold."""
        wanted = fold(name)
        candidates = (self._spelling(version),) if version is not None else self.versions
        for candidate in candidates:
            try:
                components = self.components(candidate)
            except (OSError, ValueError):
                continue
            for component in components:
                if fold(component.name) == wanted:
                    return component
        where = version or "any version"
        raise KeyError(f"no FIX component {name!r} in {where}")

    # -- lookup --------------------------------------------------------------

    def lookup(self, key: int | str, version: str | None = None) -> list[Field]:
        """One field as each version that declares it has it, newest version first.

        `key` is a tag (`54`, `"54"`) or any name the record answers to
        (`"Side"`, matched by its fold). `version` narrows to one version; the
        default walks them all in descending order, which is also the order of
        the result.

        A tag reads the one shard that can hold it. A name needs the name
        index, which is every shard -- a name has no arithmetic behind it.
        """
        order = self._versions(version)
        entry = self._record(key)
        return records_for(entry, order) if entry is not None else []

    def _record(self, key: int | str) -> Field | None:
        """One field record: by tag out of its shard alone, by name out of the index."""
        if _is_tag(key):
            return self._layout.record(int(key))
        return self.resolve(str(key))

    def field(self, key: int | str, version: str | None = None) -> Field | None:
        """One field by tag or by any name it answers to; None when nothing is.

        The whole **record** when no version is named: the newest reading, the
        versions that declare it, its aliases and the values it enumerates --
        which is what `record.fix.encode` and `FieldAccess` read. One version's
        reading of it when a version is named, which is the same declaration
        under that version alone.
        """
        record = self._record(key)
        if record is None:
            return None
        if version is None:
            return merged_record(record, self.versions)
        found = records_for(record, self._versions(version))
        return found[0] if found else None

    def msg_type_event_types(self) -> Mapping[str, EventType]:
        """Known MsgTypes to their configured market kind or MISC."""
        return self._msg_type_event_types

    @cached_property
    def _msg_type_event_types(self) -> Mapping[str, EventType]:
        """Registry-owned classification index, built once per store revision."""
        entry = self.field(35)
        if entry is None:
            return MappingProxyType({})
        fix = entry.fix
        msg_types = dict.fromkeys((*(one.value for one in fix.enumerated), *fix.event_types))
        return MappingProxyType({value: fix.event_type(value) for value in msg_types})

    def state_values(self, field: int | str) -> Mapping[str, State]:
        """Configured market states for one FIX field's wire values."""
        entry = self.field(field)
        return MappingProxyType({}) if entry is None else self._state_values.get(entry.fix.key, {})

    @cached_property
    def _state_values(self) -> Mapping[int | str, Mapping[str, State]]:
        """Field-state maps built once per store revision."""
        return MappingProxyType(
            {
                entry.fix.key: MappingProxyType(states)
                for entry in self._entries[0].values()
                if (states := entry.fix.states)
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
        """A fresh scalar declaration, exact by version or merged across versions.

        Unlike `field`, which answers None for a key the registry does not
        have, this is a *declaration* a caller is about to build a column from:
        there is nothing to hand back, so it raises.
        """
        source = self.field(key, version) if version is not None else self._scalar_of(key)
        if source is None:
            raise KeyError(f"no FIX field {key!r} in {version}")
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
        """Every folded field name to its tag number, newest version winning."""
        mapping: dict[str, int] = {}
        candidates = self._versions(version) if version is not None else self._versions(None)
        declared: list[tuple[Field, int]] = []
        for candidate in candidates:
            members = self.fields(candidate) if version is not None else self._members(candidate)
            for member in members:
                tag = member.fix.get("tag")
                if tag:
                    declared.append((member, int(tag)))
        # A canonical name always owns its fold; aliases fill only the names
        # no canonical declaration answered across the selected versions.
        for member, tag in declared:
            mapping.setdefault(fold(member.name), tag)
        for member, tag in declared:
            record = self.resolve(member.name)
            for spelling in (record or member).fix.spellings():
                mapping.setdefault(fold(spelling), tag)
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
                rank = _rank(self.resolve(member.name) or member, wanted)
                if rank is not None:
                    ranked.append((rank, order, int(member.fix.get("tag") or _NO_TAG), member))
        if not ranked and fuzzy and not _is_tag(wanted):
            ceiling = max(2, len(wanted) // 3)
            for order, candidate in enumerate(self._versions(version)):
                for member in self._members(candidate):
                    record = self.resolve(member.name) or member
                    distance = min(
                        (
                            found
                            for spelling in record.fix.spellings()
                            if (found := _levenshtein(fold(wanted), fold(spelling), ceiling))
                            is not None
                        ),
                        default=None,
                    )
                    if distance is not None:
                        ranked.append(
                            (100 + distance, order, int(member.fix.get("tag") or _NO_TAG), member)
                        )
        ranked.sort(key=lambda entry: entry[:3])
        if ranked and ranked[0][0] < _BY_DESCRIPTION:
            # Something answered the query by name or by tag, so nothing that
            # only mentions it in prose is an answer to the same question.
            ranked = [entry for entry in ranked if entry[0] < _BY_DESCRIPTION]
        found: list[Field] = []
        seen: set[int | str] = set()
        for *_, member in ranked:
            entry = self.field(member.fix.get("tag") or member.name)
            identity = entry.fix.key if entry is not None else member.name
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

    def field_records(self) -> Mapping[str, Field]:
        """Every field identity this registry holds, keyed by canonical name."""
        return MappingProxyType({entry.fix.canonical: entry for entry in self._entries[0].values()})

    def tag_numbers(self) -> frozenset[int]:
        """Every numeric identity present in any locally stored version."""
        records = list(self.field_records().values())
        for version in self._stored_spellings():
            records.extend(self._stored_fields(version) or ())
        return frozenset(int(field.fix.tag) for field in records if field.fix.tag)

    def source_coverage(self) -> Mapping[str, Mapping[str, int]]:
        """Field counts each source led and answered for."""
        counted: dict[str, dict[str, int]] = {}
        for entry in self.field_records().values():
            primary = entry.fix.source
            sources = entry.fix.sources or ((primary,) if primary else ())
            for source in sources:
                counted.setdefault(source, {"primary": 0, "fields": 0})["fields"] += 1
            if primary:
                counted.setdefault(primary, {"primary": 0, "fields": 0})["primary"] += 1
        return {source: counted[source] for source in sorted(counted)}

    def component_records(self) -> Mapping[str, ComponentRecord]:
        """Every component identity this registry holds, keyed by canonical name.

        Messages are among them: a message is a component that arrives on the
        wire under a code, so `record.msg_type` is what tells the two apart
        and `message_records()` is the index keyed by that code.
        """
        return MappingProxyType({entry.name: entry for entry in self._entries[1].values()})

    def message_records(self) -> Mapping[str, ComponentRecord]:
        """Every message this registry holds, keyed by the MsgType it arrives under.

        Newest declaration wins a code two names claim: `J` is `Allocation`
        through 4.2 and `AllocationInstruction` after, and a reader parsing
        today's traffic wants the reading today's traffic is written to.
        """
        return self._messages

    def merged_fields(self) -> Mapping[str, Field]:
        """The whole unified field table: `{canonical name: merged declaration}`.

        `scalar()` for every field at once, and the same declaration it builds:
        one record, and the versions that declare it, rather than a version's
        reading of it.
        """
        order = self.versions
        return MappingProxyType(
            {
                entry.fix.canonical: merged_record(entry, order)
                for entry in self._entries[0].values()
            }
        )

    def merged_component(self, name: str) -> ComponentRecord:
        """One component across every version it is declared for.

        A name, one of its aliases, or a MsgType: a message is a component
        here, so `merged_component("D")` and `merged_component("NewOrderSingle")`
        answer the same record. The code is tried second, because a spelling
        somebody wrote down beats one this reads out of a wire value.
        """
        wanted = fold(name)
        for entry in self._entries[1].values():
            if wanted in {fold(spelled) for spelled in entry.spellings()}:
                return entry
        found = self.message_records().get(str(name))
        if found is not None:
            return found
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
            fields=self._component_fields_by_name(version),
            components={found.folded: found for found in self._entries[1].values()},
        )

    def component_scalar(self, name: str, version: str) -> type | None:
        """One component as a class, built from its declaration rather than by hand.

        The declaration already says every member's name, its Arrow type and
        whether a message must carry it, so the class is `into_dataclass` over
        the projection -- nested entry classes and all. Nothing is written
        twice, and a dictionary refresh moves the class with it.

        The handful of components this package projects into *published*
        columns keep their hand-written declarations: those are a contract,
        and a contract that changed shape whenever the dictionary was
        refreshed would not be one.
        """
        projected = self.component_field(name, version)
        return None if projected is None else projected.into_dataclass(projected.fix.component)

    def _component_fields_by_name(self, version: str) -> dict[str, Field]:
        """`{folded FIX member name: field}` for one version projection."""
        return {
            column_name(member.name): member
            for member in self.fields(self._spelling(version))
            if member.dtype is not None
        }

    def resolve(self, name: str) -> Field | None:
        """The identity a rendered name means, or None when nothing here is it.

        Deterministic, in the two tiers `TIERS` names, and they are the whole rule:

        1. the canonical name of an identity;
        2. a declared alias -- a rendered or namespaced spelling, a near miss
           confirmed against a capture, or the name an older version gave the
           tag (64 is `FutSettDate` through 4.3 and `SettlDate` after, and both
           are that identity).

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
                    names.setdefault(fold(spelled), []).append(entry.fix.canonical)
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
    def _entries(self) -> tuple[dict[int | str, Field], dict[str, ComponentRecord]]:
        """Every record this store holds, keyed as the store keys them."""
        return self._layout.field_records, self._layout.component_records

    @cached_property
    def _messages(self) -> Mapping[str, ComponentRecord]:
        """`{MsgType: record}`, built once: a lookup by code walks no records."""
        found: dict[str, ComponentRecord] = {}
        for entry in self._entries[1].values():
            code = entry.msg_type
            if not code:
                continue
            held = found.get(code)
            if held is None or newest_rank(entry.newest) > newest_rank(held.newest):
                found[code] = entry
        return MappingProxyType(found)

    @cached_property
    def _resolutions(self) -> dict[str, Field]:
        """`{folded name: identity}`, built once in tier order."""
        found: dict[str, Field] = {}
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

    def add_field(self, entry: Field) -> Field:
        """Store one new field identity; `KeyError` when it is already here.

        The duplicate tag and duplicate name checks are in `_validated`, which
        every write goes through; this one is only the file it would land in.
        """
        fix = entry.fix
        held = self._entries[0].get(fix.key)
        if held is not None and held.fix.folded == fix.folded:
            raise KeyError(
                f"FIX field {fix.canonical!r} is already stored in {field_document(entry.fix.key)}"
            )
        if held is not None:
            claimed = f"tag {fix.tag}" if fix.tag is not None else f"the name {fix.canonical!r}"
            raise KeyError(
                f"FIX field {fix.canonical!r} cannot be added: {claimed} is already claimed by "
                f"{held.fix.canonical!r}, in {field_document(entry.fix.key)}"
            )
        return self._write_field(entry)

    def update_field(self, entry: Field) -> Field:
        """Replace one stored field identity; `KeyError` when there is none."""
        if entry.fix.key not in self._entries[0]:
            raise KeyError(f"no FIX field stored in {field_document(entry.fix.key)}")
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
        removed = self._layout.remove_field(entry.fix.key)
        self._forget()
        return removed

    def add_component(self, entry: ComponentRecord) -> ComponentRecord:
        """Store one new component identity; `KeyError` when it is already here."""
        if entry.slug in self._entries[1]:
            raise KeyError(f"FIX component {entry.name!r} is already stored")
        return self._write_component(entry)

    def update_component(self, entry: ComponentRecord) -> ComponentRecord:
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

    def alias_field(self, name: str, *aliases: Alias | str) -> Field:
        """Add spellings one field has been observed under, and keep the entry.

        The operation a classification run produces: a near miss confirmed
        against a capture becomes a data change here, never a branch in a
        resolver.
        """
        entry = self.resolve(name)
        if entry is None:
            raise KeyError(f"no FIX field {name!r} in this registry")
        added = tuple(alias if isinstance(alias, Alias) else Alias(name=alias) for alias in aliases)
        spelled = entry.fix.named_aliases
        held = {alias.folded for alias in spelled}
        aliased = record_copy(entry)
        aliased.fix.named_aliases = (*spelled, *(a for a in added if a.folded not in held))
        return self.update_field(aliased)

    def promote_field(
        self,
        name: str,
        column: str,
        *,
        type: str = "",
        description: str = "",
        aliases: Sequence[Alias | str] = (),
    ) -> Field:
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
                if one.fix.column == column and (held is None or one.fix.key != held.fix.key)
            ),
            None,
        )
        if claimed is not None:
            raise ValueError(
                f"column {column!r} is already {claimed.fix.canonical!r}'s; "
                "two fields cannot land in one column"
            )
        if held is None:
            return self.add_field(
                namespaced_field(
                    name,
                    type or "String",
                    description=description,
                    column=column,
                    aliases=added,
                )
            )
        fix = held.fix
        if record_kind(held) != NAMESPACE:
            raise KeyError(
                f"FIX field {fix.canonical!r} is standard, with tag {fix.tag}; promotion "
                "registers rendered bridge fields only"
            )
        if fix.column and fix.column != column:
            raise ValueError(
                f"FIX field {fix.canonical!r} is already lifted into {fix.column!r}; "
                f"refusing to move it to {column!r}"
            )
        aliased = fix.named_aliases
        spelled = {fix.folded, *(alias.folded for alias in aliased)}
        promoted = record_copy(held)
        promoted.fix.type = type or fix.type or "String"
        if description:
            promoted.description = description
        promoted.fix.named_aliases = (*aliased, *(a for a in added if a.folded not in spelled))
        promoted.fix.column = column
        return self.update_field(promoted)

    def _write_field(self, entry: Field) -> Field:
        """Validate one field record against the whole store, then write it."""
        self._validated(fields={**self._entries[0], entry.fix.key: entry})
        self._layout.store_field(entry)
        self._forget()
        return entry

    def _write_component(self, entry: ComponentRecord) -> ComponentRecord:
        """Validate one component record against the whole store, then write it."""
        self._validated(components={**self._entries[1], entry.slug: entry})
        self._layout.store_component(entry)
        self._forget()
        return entry

    def _validated(
        self,
        fields: Mapping[int | str, Field] | None = None,
        components: Mapping[str, ComponentRecord] | None = None,
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
        """Read every source for every stored version and write what it says."""
        spellings = self._stored_spellings()
        if not spellings:
            raise ValueError("this FIX registry store holds no version to refresh")
        self.rebuild(*spellings)
        return True

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
        built = self._scalars.get(entry.fix.key)
        if built is None:
            built = self._scalars[entry.fix.key] = merged_record(entry, self.versions)
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
            except (OSError, ValueError):
                held = self._indexes[version] = None
        return list(held or ())

    # -- scraping ------------------------------------------------------------

    def _scrape_version(self, version: str) -> list[Field]:
        """One version, with each source filling what higher priorities omit."""
        listings: list[tuple[RegistrySource, dict[int, tuple[str, str]]]] = []
        missing: list[OSError] = []
        for source in self.sources:
            try:
                listed = self._read_source(source.tags, version)
            except OSError as error:
                if not _is_missing(error):
                    raise
                missing.append(error)
                continue
            if listed:
                listings.append((source, listed))
        tags = sorted({tag for _, listed in listings for tag in listed})
        if not tags:
            if missing:
                raise missing[-1]
            raise ValueError(f"the configured FIX sources list no fields for {version}")

        def read(job: tuple[RegistrySource, int]) -> tuple[RegistrySource, int, dict[str, Any]]:
            source, tag = job
            detail = self._read_source(
                source.field,
                version,
                tag,
                shared=source.shared_field_page,
                pause_seconds=source.field_pause_seconds,
            )
            return source, tag, detail

        by_source: dict[tuple[str, int], dict[str, Any]] = {}

        def fetch_fields(source: RegistrySource, tags: Sequence[int]) -> None:
            if not tags:
                return
            workers = 1 if source.shared_field_page else min(self.max_workers, source.workers)
            with concurrent.futures.ThreadPoolExecutor(workers) as pool:
                fetched = pool.map(read, ((source, tag) for tag in tags))
                by_source.update(
                    ((found.name, tag), detail) for found, tag, detail in fetched if detail
                )

        # A lower-priority page can carry usage or an alias no higher source
        # exposes, so every source is asked about every tag it lists.
        for source, listed in listings:
            fetch_fields(source, tuple(listed))

        fields: list[Field] = []
        for tag in tags:
            readings = _ordered_source_readings(tag, listings, by_source)
            if not readings:
                continue
            detail = _merged_source_field(readings, version, tag)
            self.__dict__.setdefault("_source_conflicts", []).extend(detail.pop("conflicts", ()))
            built = fix_field(
                name_of(detail.get("name") or str(tag)),
                tag,
                detail.get("type"),
                description=detail.get("description"),
                version=version,
                values=detail.get("values"),
            )
            built.fix.source = detail["source"]
            built.fix.sources = detail["sources"]
            built.fix.origins = detail["origins"]
            if detail.get("added"):
                built.fix.added = detail["added"]
            if detail.get("note"):
                built.fix.note = detail["note"]
            used = detail.get("used_in")
            if used:
                built.fix.msgtypes = used
            components = detail.get("components")
            if components:
                built.fix.components = components
            fields.append(built)
        return fields

    def _spec_document(self, version: str) -> str:
        """The configured QuickFIX document, or empty text when absent."""
        for source in self.sources:
            if isinstance(source, QuickFixSource):
                return self._read_source(source.document, version)
        return ""

    def _component_declarations(self, document: str) -> tuple[Field, ...]:
        """QuickFIX component trees stamped with their source."""
        source = next((one.name for one in self.sources if isinstance(one, QuickFixSource)), "")
        declarations: list[Field] = []
        for entry in _parsed_quickfix(document).declarations.values():
            declared = record_copy(entry)
            declared.fix.source = source
            declared.fix.sources = (source,) if source else ()
            declarations.append(declared)
        return tuple(declarations)

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

    def _stored_components(self, version: str) -> list[Field] | None:
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
        components: Sequence[Field] | None = None,
    ) -> None:
        """Keep one version's fields and optional spec declarations."""
        self._layout.store(version, fields, session, components)
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
        self.__dict__.pop("_messages", None)
        self.__dict__.pop("_resolutions", None)
        self.__dict__.pop("_msg_type_event_types", None)
        self.__dict__.pop("_state_values", None)
        self.__dict__.pop("_group_count_tags", None)
        self.__dict__.pop("versions", None)
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
        """The filesystem and path used by cache operations, resolved once.

        A directory is read where it is, local or not. An archive somewhere
        this process cannot open a file on -- `s3://bucket/fix.zip`, a store
        on a bucket a worker shares -- is copied down to `REMOTE_CACHE` once
        and read from there: a zip is opened by seeking, and seeking over an
        object store reads it whole for every lookup. Copied by identity and
        size, so a second process reuses the first one's copy rather than
        fetching it again.
        """
        source = self._cache_source
        location = os.fspath(self.cache_dir)
        if source is None:
            if not self.archived:
                raise ValueError("an HTTP FIX registry cache must be an archive")
            return pyarrow.fs.LocalFileSystem(), local_path(location)
        filesystem, path = source
        if self.archived and not isinstance(filesystem, pyarrow.fs.LocalFileSystem):
            return pyarrow.fs.LocalFileSystem(), self._localized(filesystem, path)
        return filesystem, path

    def _localized(self, filesystem: pyarrow.fs.FileSystem, path: str) -> str:
        """A remote archive's OS path, fetched into `remote_cache()` when it exists.

        Keyed by the location, not by the filesystem handle that read it: two
        registries over one bucket are two objects, and identifying the copy
        by the handle would fetch the archive again for each of them -- which
        is the cost this cache exists to pay once.

        `spill_path` answers None for a remote that is not there, which is a
        store about to be written rather than one that failed to read -- so
        the local path it *would* have is what a cold remote store gets.
        """
        cache = remote_cache()
        cache.mkdir(parents=True, exist_ok=True)
        identity = Url.from_string(os.fspath(self.cache_dir)).into_string()
        found = spill_path(path, filesystem, cache, identity=identity, temporary=False)
        if found is not None:
            return found
        return local_path(path, filesystem, missing_ok=True)

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
        ArrowPath(path, filesystem).write_bytes(pathlib.Path(self._cache_path).read_bytes())

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
        selected: dict[int | str, Field] = {}
        missing = []
        for key in keys:
            entry = self._record(key)
            if entry is not None and entry.fix.key in held:
                selected[entry.fix.key] = entry
                continue
            found = [
                member
                for members in extra.values()
                for member in members
                if (
                    int(member.fix.get("tag") or 0) == int(key)
                    if _is_tag(key)
                    else fold(member.name) == fold(key)
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
        declared: dict[str, Sequence[Field]] = {}
        for version in self.versions:
            names = {
                entry.fix.canonical for entry in selected.values() if entry.fix.declares(version)
            }
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

    def _source_fetch(self, url: str) -> str:
        """One source page per registry operation, shared by every source question."""
        pages = self.__dict__.setdefault("_source_pages", {})
        if url not in pages:
            pages[url] = self._fetch(url)
        return pages[url]

    def _read_source(
        self,
        reader: Callable[..., Any],
        *arguments: Any,
        shared: bool = True,
        pause_seconds: float = 0.0,
    ) -> Any:
        """One validated source answer, resuming pages an interrupted scrape fetched."""
        pending: dict[str, str] = {}
        used: dict[str, None] = {}

        def fetch(url: str) -> str:
            used[url] = None
            pages = self.__dict__.setdefault("_source_pages", {}) if shared else None
            if pages is not None and url in pages:
                return pages[url]
            cached = self._cached_source_page(url)
            if cached is not None:
                if pages is not None:
                    pages[url] = cached
                return cached
            if pause_seconds:
                time.sleep(pause_seconds)
            page = (self._source_fetch if shared else self._fetch)(url)
            pending[url] = page
            return page

        try:
            answer = reader(fetch, *arguments)
        except ValueError:
            for url in used:
                self._remove_source_page(url)
            raise
        for url, page in pending.items():
            self._store_source_page(url, page)
        return answer

    def _cached_source_page(self, url: str) -> str | None:
        """One successful page from an interrupted scrape, or None when absent."""
        path = self._source_page_path(url)
        if path is None:
            return None
        try:
            return path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            return None

    def _store_source_page(self, url: str, page: str) -> None:
        """Keep one successful page atomically outside the published registry."""
        path = self._source_page_path(url)
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        scratch = path.with_suffix(".tmp")
        scratch.write_text(page, encoding="utf-8")
        scratch.replace(path)

    def _remove_source_page(self, url: str) -> None:
        """Forget a page that failed its source parser."""
        path = self._source_page_path(url)
        if path is not None:
            path.unlink(missing_ok=True)
        self.__dict__.get("_source_pages", {}).pop(url, None)

    def _source_page_path(self, url: str) -> pathlib.Path | None:
        """The sharded cache path for one exact source URL."""
        if self._source_page_cache is None:
            return None
        digest = hashlib.sha256(url.encode()).hexdigest()
        return self._source_page_cache / digest[:2] / f"{digest[2:]}.page"

    def _fetch(self, url: str) -> str:
        """One page, as text, retried while the site says "later".

        The one place the network is reached on a read, and so the one place
        a registry that serves a store refuses to: `_fetching` is what the
        verbs that mean to scrape open, and nothing else opens it.
        """
        if self._serves_stored:
            raise OSError(
                f"{url} was not fetched: {self.cache_dir} is the dictionary this "
                "registry serves. Pass refresh=True, or call rebuild() or scrape()."
            )
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
        try:
            return self._read(request)
        except urllib.error.HTTPError as error:
            error.msg = f"{error.msg}: {url}"
            raise

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


def _is_missing(error: Exception) -> bool:
    """Whether a source answered that this exact page does not exist."""
    if isinstance(error, urllib.error.HTTPError):
        return error.code == 404
    return bool(re.search(r"^\s*404(?:\D|$)", str(error)))


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


def _optional_page(fetch: Callable[[str], str], url: str) -> str:
    """One source page, empty only when that source never published it."""
    try:
        return fetch(url)
    except OSError as error:
        # Missing is no contribution. Refused is an incomplete scrape and must
        # not become the stored reading every later offline call trusts.
        if _is_missing(error):
            return ""
        raise


def _local_aliases(entry: Field) -> tuple[Alias, ...]:
    """Aliases attributed outside the source dictionaries."""
    source_owned = {
        str(source).casefold() for source in (*entry.fix.versions, *entry.fix.sources) if source
    }
    return tuple(
        alias
        for alias in entry.fix.named_aliases
        if not alias.source or alias.source.casefold() not in source_owned
    )


def _into_nanoconda_version(version: str) -> str:
    """`5.0.SP2` -> `FIX.5.0SP2`; `FIXT1.1` -> `FIXT.1.1`."""
    text = str(version).strip().upper()
    if text.startswith("FIXT"):
        return f"FIXT.{text.removeprefix('FIXT').lstrip('.')}"
    application = text.removeprefix("FIX.").removeprefix("FIX")
    return f"FIX.{application.replace('.SP', 'SP')}"


def _from_nanoconda_version(version: str) -> str:
    """Nanoconda's version spelling as the registry's canonical spelling."""
    text = str(version).strip().upper()
    if text.startswith("FIXT."):
        return f"FIXT{text.removeprefix('FIXT.')}"
    application = text.removeprefix("FIX.")
    return re.sub(r"(?<=\d)SP(?=\d+$)", ".SP", application)


def _linked_tags(page: str, pattern: re.Pattern[str]) -> dict[int, tuple[str, str]]:
    """A source's repeated tag/name links as `{tag: (name, note)}`."""
    listed: dict[int, tuple[str, str]] = {}
    for tag_text, label in pattern.findall(page):
        tag = int(tag_text)
        text = _text(label)
        if not text or text == tag_text:
            listed.setdefault(tag, ("", ""))
            continue
        name, note = _split_note(text)
        known = listed.get(tag)
        if known is None or not known[0]:
            listed[tag] = (name_of(name), note)
    return {tag: listed[tag] for tag in sorted(listed)}


def _ordered_source_readings(
    tag: int,
    listings: Sequence[tuple[RegistrySource, Mapping[int, tuple[str, str]]]],
    fetched: Mapping[tuple[str, int], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """One field's fetched readings in configured priority order."""
    readings: list[dict[str, Any]] = []
    for source, listed in listings:
        detail = fetched.get((source.name, tag))
        if detail is None:
            continue
        name, note = listed[tag]
        readings.append({"name": name, "note": note, **detail, "source": source.name})
    return readings


def _merged_source_field(
    readings: Sequence[Mapping[str, Any]], version: str, tag: int
) -> dict[str, Any]:
    """One field from ordered sources, first reading winning each stated part.

    Name, type, description and value meaning come from the first source that
    states them. Values merge by wire value and names remain aliases in source
    order, deduplicated by the same fold the value decoder uses. A valid-value
    row without a source name is prose or a constraint, not an enumeration.
    """
    merged: dict[str, Any] = {}
    origins: dict[str, Any] = {}
    sources: list[str] = []
    values: dict[str, FixFieldValue] = {}
    value_origins: dict[str, dict[str, str]] = {VALUES: {}, ALIASES: {}}
    for reading in readings:
        source = str(reading.get("source") or "")
        if source:
            sources.append(source)
        for part in ("name", "type", "description", "note", "added"):
            if part not in merged and reading.get(part):
                merged[part] = reading[part]
                if source:
                    origins[part] = source
        for part in ("used_in", "components"):
            if reading.get(part):
                merged[part] = list(dict.fromkeys((*merged.get(part, ()), *reading[part])))
        for fresh in values_of(reading.get("values")):
            held = values.get(fresh.value)
            if held is None:
                values[fresh.value] = fresh
                if source and fresh.meaning:
                    value_origins[VALUES][fresh.value] = source
                if source and fresh.aliases:
                    value_origins[ALIASES][fresh.value] = source
                continue
            aliases = list(held.aliases)
            folded = {encoded_key(alias) for alias in aliases}
            for alias in fresh.aliases:
                key = encoded_key(alias)
                if key not in folded:
                    folded.add(key)
                    aliases.append(alias)
            if source and not held.meaning and fresh.meaning:
                value_origins[VALUES][fresh.value] = source
            if source and not held.aliases and aliases:
                value_origins[ALIASES][fresh.value] = source
            values[fresh.value] = dataclasses.replace(
                held,
                meaning=held.meaning or fresh.meaning,
                aliases=tuple(aliases),
            )
    merged["source"] = sources[0]
    merged["sources"] = tuple(dict.fromkeys(sources))
    enumerated = {wire: value for wire, value in values.items() if value.aliases}
    if enumerated:
        merged["values"] = tuple(enumerated.values())
    for part, stated in value_origins.items():
        kept = {wire: source for wire, source in stated.items() if wire in enumerated}
        if kept:
            origins[part] = kept
    merged["origins"] = origins
    merged["conflicts"] = _source_conflicts(readings, merged, version, tag)
    return merged


def _source_conflicts(
    readings: Sequence[Mapping[str, Any]],
    merged: Mapping[str, Any],
    version: str,
    tag: int,
) -> tuple[Collapse, ...]:
    """Same-version source disagreements in the registry's conflict shape."""
    name = name_of(str(merged.get("name") or tag))
    conflicts: list[Collapse] = []
    for key, part, compared in (
        ("name", NAME, fold),
        ("added", ADDED, str),
        ("type", TYPE, datatype_identity),
        ("note", NOTE, str),
    ):
        kept = str(merged.get(key) or "")
        keptsource = next(
            (str(reading.get("source") or "") for reading in readings if reading.get(key)),
            "",
        )
        dropped = tuple(
            Dropped(
                version=version,
                reading=str(reading[key]),
                source=str(reading.get("source") or ""),
            )
            for reading in readings
            if reading.get(key) and compared(str(reading[key])) != compared(kept)
        )
        if dropped:
            conflicts.append(Collapse(name, part, version, dropped, tag, keptsource))

    by_value: dict[str, list[tuple[str, FixFieldValue]]] = {}
    named = {value.value for value in values_of(merged.get("values"))}
    for reading in readings:
        source = str(reading.get("source") or "")
        for value in values_of(reading.get("values")):
            if value.value in named:
                by_value.setdefault(value.value, []).append((source, value))
    for part, read in (
        (VALUES, lambda value: value.meaning),
        (ALIASES, lambda value: value.aliases[0] if value.aliases else ""),
    ):
        grouped: dict[str, list[Dropped]] = {}
        for value, stated in sorted(by_value.items()):
            keptsource, kept = next(
                ((source, read(one)) for source, one in stated if read(one)),
                ("", ""),
            )
            compared = encoded_key if part == ALIASES else str
            for source, one in stated:
                reading = read(one)
                if reading and compared(reading) != compared(kept):
                    grouped.setdefault(keptsource, []).append(
                        Dropped(version, reading, value, source)
                    )
        conflicts.extend(
            Collapse(name, part, version, tuple(dropped), tag, keptsource)
            for keptsource, dropped in grouped.items()
        )
    return tuple(conflicts)


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


def _values(markup: str, *, names: bool = False) -> tuple[FixFieldValue, ...]:
    """The enumerated values a field page lists, in their wire order."""
    found: dict[str, FixFieldValue] = {}
    for _, item in _VALUE_ITEM.findall(markup):
        text = _text(item)
        value = _VALUE.match(text)
        if value:
            label = name_of(value[2]) if names else value[2]
            if label:
                found.setdefault(
                    value[1],
                    FixFieldValue(
                        value=value[1],
                        meaning="" if names else label,
                        aliases=(label,) if names else (),
                    ),
                )
    return tuple(found.values())


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
        folded = fold(name)
        if name and folded not in seen:
            names.append(name)
            seen.add(folded)
    return names


# -- ordering and matching ---------------------------------------------------


def _tier(entry: Field, tier: str) -> tuple[str, ...]:
    """The names one record claims in one resolution tier."""
    if tier == _CANONICAL:
        return (entry.fix.canonical,)
    return tuple(alias.name for alias in entry.fix.named_aliases)


def _problems(
    held: tuple[Mapping[int | str, Field], Mapping[str, ComponentRecord]],
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
                names.setdefault(fold(spelled), []).append(entry.fix.canonical)
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


def _duplicates(entries: Mapping[int | str, Field]) -> Iterator[str]:
    """Two identities claiming one tag, or one canonical name.

    The two ways a store answers a lookup with whichever entry it happened to
    read first, which is the same store answering differently on two machines.
    Reported before anything else, because every later line is about spellings
    and these are about identity.
    """
    by_tag: dict[int, list[str]] = {}
    by_name: dict[str, list[str]] = {}
    for entry in entries.values():
        fix = entry.fix
        if fix.tag is not None:
            by_tag.setdefault(int(fix.tag), []).append(fix.canonical)
        by_name.setdefault(fix.folded, []).append(fix.canonical)
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


#: The rank a match by *description* gets, and so the first rank that is not a
#: match on the identity itself. A query that named something -- a tag, a name,
#: part of one -- has been answered, and padding the answer with fields whose
#: prose happens to contain it buries what was asked for: `54` named `Side`,
#: and nine fields whose descriptions mention 54 followed it.
_BY_DESCRIPTION = 3


def _rank(member: Field, wanted: str) -> int | None:
    """How well one field matches a lowercased query; None is not at all.

    A query of several words is every one of them, so `settlement date` reaches
    `UnderlyingSettlementDate` by its name rather than by prose that happens to
    spell the two together. The browser's registry ranks by this same rule.
    """
    folded = fold(wanted)
    if wanted == str(member.fix.get("tag") or ""):
        return 0
    parts = wanted.split()
    named: list[int] = []
    for spelling in member.fix.spellings():
        name = fold(spelling)
        if folded == name:
            named.append(0)
        elif name.startswith(folded):
            named.append(1)
        elif all(fold(part) in name for part in parts):
            named.append(2)
    if named:
        return min(named)
    described = member.description.lower()
    if all(part in described for part in parts):
        return _BY_DESCRIPTION
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
