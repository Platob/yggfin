"""Every FIX field of every FIX version, scraped once and kept for offline use."""

from __future__ import annotations

import concurrent.futures
import dataclasses
import html
import importlib.resources
import json
import os
import pathlib
import re
import time
import urllib.error
import urllib.request
import warnings
from collections.abc import Mapping, Sequence
from functools import cache, cached_property
from types import MappingProxyType
from typing import Any, Self

import pyarrow.fs

from rekep.convert import Convertible
from rekep.fields import Field
from rekep.filesystems import local_path, read_bytes, resolve, write_bytes
from rekep.fix.entries import Alias, ComponentEntry, FieldEntry, fold, merged_field
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import (
    QUICKFIX_URL,
    SpecComponent,
    SpecField,
    parse_components,
    parse_session,
    parse_spec,
    spec_name,
)
from rekep.fix.store import (
    EXPLODED,
    LAYOUTS,
    ArchiveDocuments,
    DirectoryDocuments,
    Documents,
    ExplodedLayout,
    Layout,
    documents_of,
    entry_identity,
    explode,
    layout_of,
    slug_collisions,
    write_archive,
)
from rekep.urls import HTTP, Url

#: The dictionary that is scraped: OnixS publishes every FIX version as one
#: page per version listing the tags, and one page per field carrying the
#: name, datatype, description and enumerated values.
BASE_URL = "https://www.onixs.biz/fix-dictionary"

# Sent with every request so scrape traffic identifies its client.
_USER_AGENT = "rekep-fix-registry (+https://github.com/Platob/rekep)"

#: Where the scrape persists, one JSON per version plus the version list, so
#: everything after the first scrape works offline -- including on a machine
#: that was never online, by copying the directory.
CACHE_DIRECTORY = pathlib.Path.home() / ".config" / "fix"

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

#: A parenthetical note beside a name on the by-tag page -- `(no longer
#: used)`, `(replaced)` -- which is the one deprecation signal the site has.
_NOTE = re.compile(r"\(([^)]*)\)\s*$")

#: The tiers `resolve` walks, in the order it walks them: an identity's own
#: name, then a name some version spells for it, then a declared alias.
_CANONICAL = "canonical"
_VERSIONED = "versioned"
_ALIASED = "aliased"

#: Where a field FIX never numbered sorts among the ones it did: after all of
#: them, in one place, rather than at tag zero beside `BeginString`.
_NO_TAG = 1 << 31

#: One version's fields, three ways: by tag, by lowercased name, in order.
_Index = tuple[dict[int, Field], dict[str, Field], list[Field]]

_TAGS = re.compile(r"<[^>]+>")
_SPACES = re.compile(r"\s+")
_DEFAULT = object()


@dataclasses.dataclass(eq=False)
class FixRegistry(Convertible):
    """The OnixS FIX dictionary as `Field`s, one scrape then offline forever."""

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
    #: dictionary. One member per version, `versions.json` beside them, all
    #: plain JSON.
    cache_dir: str | os.PathLike[str] = CACHE_DIRECTORY

    #: Optional filesystem for `cache_dir`, whose value is then a path on it.
    filesystem: pyarrow.fs.FileSystem | None = None

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
    #: and what a person at a prompt wants: ask for `4.4`, get `4.4`.
    #:
    #: True is what a **pipeline** wants, and it is not the same wish. A parse
    #: that meets its first bridge line must not answer it by starting a
    #: seven-thousand-page scrape in the middle of a batch -- so an offline
    #: registry serves whatever the store already holds and reports the rest as
    #: unavailable, down the same path an outage takes. Which is why this is
    #: one flag rather than a second registry: every caller here already
    #: handles "cannot be had right now".
    offline: bool = False

    #: Which shape a **cold** store is written in. An existing store says what
    #: it is by what it holds, and is never rewritten into the other shape by
    #: being opened -- `migrate` is how a store changes layout, deliberately.
    layout: str = EXPLODED

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

    def __post_init__(self) -> None:
        """Normalise the two locations once, so everything downstream agrees."""
        self.base_url = Url.from_string(str(self.base_url)).into_string().rstrip("/")
        self.spec_url = Url.from_string(str(self.spec_url)).into_string().rstrip("/")
        if self.layout not in LAYOUTS:
            raise ValueError(f"unknown FIX registry layout {self.layout!r}; one of {list(LAYOUTS)}")
        if self.cache_ttl < 0:
            raise ValueError(f"a FIX registry cache TTL cannot be negative: {self.cache_ttl}")

    @classmethod
    @cache
    def from_builtin(cls, cache_ttl: float = 0.0) -> Self:
        """The packaged field projection, offline and shared by declarations.

        `cache_ttl` above zero makes this registry check its age against the
        spec before serving -- which needs the network, so it also lifts
        `offline`. Zero, the default, is the packaged copy and nothing else.
        """
        stored = importlib.resources.files(__package__).joinpath("registry.zip")
        return cls(cache_dir=os.fspath(stored), offline=not cache_ttl, cache_ttl=cache_ttl)

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
        versions = tuple(sorted(found, key=_version_key, reverse=True))
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
        """Every version's definition of one field, newest version first.

        `key` is a tag (`54`, `"54"`) or a name (`"Side"`, case-insensitive).
        `version` narrows to one version; the default walks them all in
        descending order, which is also the order of the result.
        """
        found = []
        for candidate in self._versions(version):
            indexed = self._index(candidate)
            if indexed is None:
                continue
            by_tag, by_name, _ = indexed
            member = by_tag.get(int(key)) if _is_tag(key) else by_name.get(str(key).strip().lower())
            if member is not None:
                found.append(member)
        return found

    def field(self, key: int | str, version: str | None = None) -> Field:
        """The newest definition of one field; `KeyError` when no version has it."""
        found = self.lookup(key, version)
        if not found:
            where = version or "any version"
            raise KeyError(f"no FIX field {key!r} in {where}")
        return found[0]

    def scalar(
        self,
        key: int | str,
        *,
        version: str | None = None,
        name: str = "",
        arrow_type: Any = _DEFAULT,
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
            arrow_type=source.arrow_type if arrow_type is _DEFAULT else arrow_type,
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
        """Fields matching `text` by tag, name or description, best first."""
        wanted = str(text).strip().lower()
        if not wanted:
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
        return [member for *_, member in ranked[:limit]]

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

        `scalar()` for every field at once, and the same declaration it builds
        -- one canonical identity carrying each version's disagreement as
        metadata rather than resolving it away.
        """
        order = self.versions
        found: dict[str, Field] = {}
        for entry in self._entries[0].values():
            members = entry.into_fields(order)
            if not members:
                continue
            merged = merged_field(members)
            # Protocol identity is the entry's, not the newest reading's: the
            # canonical name and the spellings it also answers to belong to
            # the identity and would otherwise be lost in the merge.
            merged.fix["name"] = entry.name
            if entry.aliases:
                merged.fix["aliases"] = _json([alias.into_dict() for alias in entry.aliases])
            found[entry.name] = merged
        return MappingProxyType(found)

    def merged_components(self) -> Mapping[str, ComponentEntry]:
        """The whole unified component table: `{canonical name: entry}`.

        An entry, not a declaration, because "the Parties component across all
        versions" is not one member tree -- it is every version's tree, and
        `paths`, `delimiters` and `diff` are the questions worth asking of it.
        """
        return self.component_entries()

    def merged_component(self, name: str) -> ComponentEntry:
        """One component across every version it is declared for."""
        wanted = fold(name)
        for entry in self._entries[1].values():
            if wanted in {fold(spelled) for spelled in entry.spellings()}:
                return entry
        raise KeyError(f"no FIX component {name!r} in any version")

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
        for tier in (_CANONICAL, _VERSIONED, _ALIASED):
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
    def _entries(self) -> tuple[dict[str, FieldEntry], dict[str, ComponentEntry]]:
        """The identity view of this store, however the store itself is laid out.

        Read straight off an exploded store, and derived from a versioned one,
        so every reading above answers the same for both and a migration has
        something to be checked against.
        """
        layout = self._layout
        if isinstance(layout, ExplodedLayout):
            return layout.field_entries, layout.component_entries
        order = self.versions
        return explode(
            order,
            {version: self._members(version) for version in order},
            {version: self._stored_components(version) or () for version in order},
        )

    @cached_property
    def _resolutions(self) -> dict[str, FieldEntry]:
        """`{folded name: identity}`, built once in tier order."""
        found: dict[str, FieldEntry] = {}
        for tier in (_CANONICAL, _VERSIONED, _ALIASED):
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
        """Store one new field identity; `KeyError` when it is already here."""
        held = self._entries[0]
        if entry.slug in held:
            raise KeyError(f"FIX field {entry.name!r} is already stored as {entry.slug}.json")
        return self._write_field(entry)

    def update_field(self, entry: FieldEntry) -> FieldEntry:
        """Replace one stored field identity; `KeyError` when there is none."""
        if entry.slug not in self._entries[0]:
            raise KeyError(f"no FIX field stored as {entry.slug}.json")
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
        slug = self._editable.slugs.get(entry_identity(entry), entry.slug)
        removed = self._editable.remove_field(slug)
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
            raise KeyError(f"no FIX component stored as {entry.slug}.json")
        return self._write_component(entry)

    def remove_component(self, name: str) -> bool:
        """Delete one component identity, by any name it answers to."""
        try:
            entry = self.merged_component(name)
        except KeyError:
            return False
        removed = self._editable.remove_component(entry.slug)
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

    @property
    def _editable(self) -> ExplodedLayout:
        """The store, refusing the edit when it is not one that can be edited."""
        layout = self._layout
        if not isinstance(layout, ExplodedLayout):
            raise TypeError(
                "this FIX registry store keeps one file per version and cannot be edited "
                "one identity at a time; migrate it first"
            )
        return layout

    def _write_field(self, entry: FieldEntry) -> FieldEntry:
        """Validate one field entry against the whole store, then write it."""
        layout = self._editable
        self._validated(fields={**self._entries[0], entry.slug: entry})
        layout.store_field(entry)
        self._forget()
        return entry

    def _write_component(self, entry: ComponentEntry) -> ComponentEntry:
        """Validate one component entry against the whole store, then write it."""
        layout = self._editable
        self._validated(components={**self._entries[1], entry.slug: entry})
        layout.store_component(entry)
        self._forget()
        return entry

    def _validated(
        self,
        fields: Mapping[str, FieldEntry] | None = None,
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
            raise ValueError("; ".join(problems))

    # -- changing the layout --------------------------------------------------

    def migrate(
        self,
        target: str | os.PathLike[str],
        filesystem: pyarrow.fs.FileSystem | None = None,
    ) -> Self:
        """Write this store's dictionary into `target` as one file per identity.

        The whole of it, in one pass, and then read back: what comes out has to
        answer every version of every question the same as what went in, or the
        migration failed and says so rather than shipping a store that answers
        differently. `verify` is that comparison on its own, for a target
        somebody else wrote.
        """
        order = self.versions
        components = {
            version: stored
            for version in order
            if (stored := self._stored_components(version)) is not None
        }
        sessions = {version: self.session(version) for version in order}
        # The identity view, not a fresh explosion of the per-version one: a
        # store that already holds identities holds the spellings each answers
        # to, and re-deriving them from the fields would migrate a dictionary
        # into one that had forgotten every alias in it.
        field_entries, component_entries = self._entries
        documents = documents_of(order, field_entries, component_entries, sessions, components)
        migrated = dataclasses.replace(
            self, cache_dir=os.fspath(target), filesystem=filesystem, offline=True, cache_ttl=0.0
        )
        if migrated.archived:
            ArchiveDocuments(migrated._cache_path).write_all(documents)
            migrated._sync_archive()
        else:
            for name, document in documents.items():
                migrated._documents.write(name, document)
        differences = self.verify(migrated)
        if differences:
            raise ValueError(
                f"migrating the FIX registry to {target} changed what it answers: "
                + "; ".join(differences[:5])
            )
        return migrated

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
    def _indexes(self) -> dict[str, _Index | None]:
        return {}

    @cached_property
    def _scalars(self) -> dict[tuple[str, int | str], Field]:
        return {}

    def _scalar_of(self, key: int | str) -> Field:
        """The cached cross-version declaration behind `scalar`."""
        identity = ("tag", int(key)) if _is_tag(key) else ("name", str(key).strip().lower())
        built = self._scalars.get(identity)
        if built is not None:
            return built
        found = self.lookup(key)
        if not found:
            raise KeyError(f"no FIX field {key!r} in any version")
        # A version may annotate the canonical name (`Field(Deprecated)`) while
        # retaining its tag. Once the newest name resolves, the tag is the
        # cross-version identity and must bring that version back into history.
        if not _is_tag(key) and found[0].fix.get("tag"):
            found = self.lookup(int(found[0].fix["tag"]))
        built = self._scalars[identity] = _merged_scalar(found)
        return built

    def _index(self, version: str) -> _Index | None:
        """`(by tag, by lowercased name)` for one version, built once.

        None for a version that cannot be had right now -- offline with no
        cache for it -- so a lookup across versions answers from the versions
        it *does* hold rather than dying on the one it does not. The miss is
        remembered per registry, not per call, or an offline lookup would
        retry the network once per query.
        """
        built = self._indexes.get(version, ())
        if built == ():
            try:
                members = self.fields(version)
            except OSError:
                built = self._indexes[version] = None
            else:
                # A field FIX never numbered -- a bridge's rendered
                # `AMON.ISINCODE` -- has no tag and belongs to the name index
                # alone. Writing it under `0` would make every such field the
                # same field.
                by_tag = {int(tag): member for member in members if (tag := member.fix.get("tag"))}
                # First declaration wins on a duplicated name, matching
                # `tags()` -- so the tag a name resolves to and the field a
                # lookup returns can never disagree about each other.
                by_name: dict[str, Field] = {}
                for member in members:
                    by_name.setdefault(member.name.lower(), member)
                built = self._indexes[version] = (by_tag, by_name, members)
        return built

    def _members(self, version: str) -> list[Field]:
        """`fields(version)` for a walk over many versions: absent means empty."""
        indexed = self._index(version)
        return [] if indexed is None else list(indexed[2])

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
                detail.get("name") or name or (known.name if known else str(tag)),
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
                built.fix["used_in"] = json.dumps(used, separators=(",", ":"))
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
            detail["name"] = _text(title[1])
        typed = _TYPE.search(page)
        if typed:
            detail["type"] = typed[1]
        prose, listed, carried = _sections(page, typed.end() if typed else 0)
        description = _description(prose)
        if description:
            detail["description"] = description
        values = _values(listed)
        if values:
            detail["values"] = values
        used = _used_in(carried)
        if used:
            detail["used_in"] = used
        return detail

    # -- the store -----------------------------------------------------------
    #
    # Seven methods, and they are the whole of where the dictionary is kept.
    # Everything above -- the scraping, the version rules, the ordering, the
    # searching -- is written against these and is the same wherever the fields
    # live and in whichever layout: a directory or a zip, one file per version
    # or one file per identity. `rekep.fix.store` owns both of those choices;
    # nothing here reads a path or a member name.

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
        for candidate in sorted(self._stored_spellings(), key=_version_key, reverse=True):
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
    def _layout(self) -> Layout:
        """Which shape this store's documents are in, read off what it holds."""
        return layout_of(self._documents, self.layout)

    def into_zip(self, target: str | os.PathLike[str]) -> pathlib.Path | str:
        """Write everything this registry holds into one archive, and name it.

        Whatever it holds, in whichever layout: the documents are copied
        verbatim, so an archive of an exploded store is an exploded store and
        an archive of a versioned one is a versioned one.
        """
        return write_archive(target, self._documents.read_many(""))

    def into_projection(
        self,
        target: str | os.PathLike[str],
        keys: Sequence[int | str],
        fields: Sequence[Field] = (),
    ) -> pathlib.Path | str:
        """Write a deterministic offline registry containing only `keys`."""
        if not keys:
            raise ValueError("a FIX registry projection needs at least one field")
        extra_by_version: dict[str, list[Field]] = {}
        for member in fields:
            version = member.fix.get("version")
            if not version:
                raise ValueError(f"projected FIX field {member.name!r} has no fix:version")
            extra_by_version.setdefault(version, []).append(member)
        wanted: set[int] = set()
        # A field FIX never numbered is selected by name, because that is all
        # it has -- and a projection that could only select tags would leave
        # every rendered namespaced field behind.
        named: set[str] = set()
        missing = []
        for key in keys:
            found = self.lookup(key)
            if not found:
                found = [
                    member
                    for members in extra_by_version.values()
                    for member in members
                    if (
                        int(member.fix.get("tag") or 0) == int(key)
                        if _is_tag(key)
                        else member.name.lower() == str(key).strip().lower()
                    )
                ]
            if not found:
                missing.append(key)
                continue
            for member in found:
                tag = member.fix.get("tag")
                if tag:
                    wanted.add(int(tag))
                else:
                    named.add(fold(member.name))
        if missing:
            raise KeyError(f"no FIX fields {missing!r} in this registry")
        overlap = set(extra_by_version).intersection(self.versions)
        if overlap:
            raise ValueError(f"projected FIX versions already exist: {sorted(overlap)}")

        if _resource_identity(target) == _resource_identity(self.cache_dir, self.filesystem):
            raise ValueError("a registry projection cannot replace its source")
        versions = (*extra_by_version, *self.versions)
        projected: dict[str, list[Field]] = {}
        sessions: dict[str, Sequence[tuple[str, bool]]] = {}
        declared: dict[str, Sequence[SpecComponent]] = {}
        for version in versions:
            if version in extra_by_version:
                projected[version] = sorted(
                    extra_by_version[version], key=lambda member: int(member.fix["tag"])
                )
                continue
            selected = [
                member
                for member in self.fields(version)
                if (tag := member.fix.get("tag")) and int(tag) in wanted
            ]
            projected[version] = selected
            names = {member.name for member in selected}
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
        field_entries, component_entries = explode(versions, projected, declared)
        # A field FIX never numbered travels as the identity it already is: it
        # holds for every version rather than for the ones a per-version walk
        # happened to hand it, and a projection that restated it per version
        # would say it arrived with 5.0.SP2.
        for entry in self._entries[0].values():
            if entry.tag is None and fold(entry.name) in named:
                field_entries[entry.slug] = entry
        return write_archive(
            target,
            documents_of(versions, field_entries, component_entries, sessions, declared),
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


def _values(markup: str) -> dict[str, str]:
    """The enumerated values a field page lists: `{"1": "Buy", ...}`."""
    found: dict[str, str] = {}
    for _, item in _VALUE_ITEM.findall(markup):
        text = _text(item)
        value = _VALUE.match(text)
        if value:
            found.setdefault(value[1], value[2])
    return found


def _used_in(markup: str) -> list[str]:
    """The messages a field page says carry it, names only."""
    names = []
    for match in re.finditer(r"<a[^>]+href=\"msgType_[^\"]+\"[^>]*>(.*?)</a>", markup, re.DOTALL):
        name, _ = _split_note(_text(match[1]))
        name = re.sub(r"\s*<\s*\w+\s*>$", "", name).strip()
        if name and name not in names:
            names.append(name)
    return names


# -- projected declarations -------------------------------------------------


def _merged_scalar(fields: Sequence[Field]) -> Field:
    """Newest field identity with every version's non-conflicting knowledge."""
    latest = fields[0]
    typed = next((member for member in fields if member.fix.get("type")), latest)
    metadata = dict(latest.metadata)
    metadata["fix:versions"] = _json([member.fix["version"] for member in fields])
    metadata["fix:types"] = _json(
        {member.fix["version"]: member.fix["type"] for member in fields if member.fix.get("type")}
    )

    names = {member.fix["version"]: member.name for member in fields}
    if len(set(names.values())) > 1:
        metadata["fix:names"] = _json(names)
    tags = {member.fix["version"]: member.fix["tag"] for member in fields}
    if len(set(tags.values())) > 1:
        metadata["fix:tags"] = _json(tags)

    for key in ("values", "value_names"):
        combined: dict[str, str] = {}
        # Oldest first, so a newer correction of one code wins without losing
        # a value that disappeared from the newest prose/spec page.
        for member in reversed(fields):
            combined.update(_json_mapping(member.fix.get(key)))
        if combined:
            metadata[f"fix:{key}"] = _json(combined)

    used: list[str] = []
    for member in fields:
        for message in _json_sequence(member.fix.get("used_in")):
            if message not in used:
                used.append(message)
    if used:
        metadata["fix:used_in"] = _json(used)

    description = next((member.description for member in fields if member.description), "")
    if description:
        metadata["description"] = description
    if typed.fix.get("type"):
        metadata["fix:type"] = typed.fix["type"]
    return Field(
        name=latest.name,
        arrow_type=typed.arrow_type,
        nullable=True,
        metadata=metadata,
    )


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _json_mapping(value: str | None) -> dict[str, str]:
    try:
        decoded = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return (
        {str(key): str(item) for key, item in decoded.items()} if isinstance(decoded, dict) else {}
    )


def _json_sequence(value: str | None) -> list[str]:
    try:
        decoded = json.loads(value or "[]")
    except (TypeError, ValueError):
        return []
    return [str(item) for item in decoded] if isinstance(decoded, list) else []


# -- ordering and matching ---------------------------------------------------


def _version_key(version: str) -> tuple[int, ...]:
    """A sortable reading of `4.4`, `5.0.SP2`, `FIXT1.1`.

    Application versions rank above the transport (`FIXT1.1`): the session
    layer defines a handful of fields the application versions redefine, so
    "newest first" should hand back the application's reading.
    """
    transport = 0 if version.upper().startswith("FIXT") else 1
    numbers = tuple(int(part) for part in re.findall(r"\d+", version))
    return (transport, *numbers)


def _tier(entry: FieldEntry, tier: str) -> tuple[str, ...]:
    """The names one entry claims in one resolution tier."""
    if tier == _CANONICAL:
        return (entry.name,)
    if tier == _VERSIONED:
        return tuple(dict.fromkeys(entry.names().values()))
    return tuple(alias.name for alias in entry.aliases)


def _problems(
    held: tuple[Mapping[str, FieldEntry], Mapping[str, ComponentEntry]],
) -> list[str]:
    """Everything inconsistent about a set of entries, as lines.

    Written against the entries rather than against a registry, so a change
    can be checked before it is written and refused whole.
    """
    problems = []
    claimed: dict[str, str] = {}
    for tier in (_CANONICAL, _VERSIONED, _ALIASED):
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
    for slug, shared in sorted(slug_collisions(e.name for e in held[0].values()).items()):
        problems.append(f"FIX fields {shared} are all stored as fields/{slug}.json")
    for slug, shared in sorted(slug_collisions(e.name for e in held[1].values()).items()):
        problems.append(f"FIX components {shared} are all stored as components/{slug}.json")
    return problems


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
