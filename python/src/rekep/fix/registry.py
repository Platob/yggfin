"""The stored FIX dictionary and its explicit complete-file refresh."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import pathlib
import re
import shutil
import sys
import tempfile
import warnings
from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import cache, cached_property
from types import MappingProxyType
from typing import Any, Self, cast

import pyarrow
import pyarrow.compute as compute
import pyarrow.fs

from rekep.arrow_path import ArrowPath
from rekep.convert import Convertible
from rekep.enums import EventType, State
from rekep.fields import Field, ListField, StructField, column_name, newest_rank
from rekep.fields.metadata import canonical_versions
from rekep.filesystems import local_path, resolve, spill_path
from rekep.fix.entries import (
    ANY_VERSION,
    NAMESPACE,
    Alias,
    ComponentRecord,
    fold,
    merged_record,
    record_copy,
    record_kind,
    records_for,
    refuse_record,
)
from rekep.fix.fields import cast_arrow_field, datatype_identity, namespaced_field
from rekep.fix.quickfix import (
    declared_group,
    entry_of,
    first_declared_name,
    is_group,
    is_reference,
    walk,
)
from rekep.fix.store import (
    ALIASES,
    COMPONENTS,
    DECLARED,
    DOCUMENT_SUFFIX,
    FIELDS,
    NAME,
    NAMESPACES,
    NOTE,
    REPGROUP,
    SESSIONS,
    SOURCES_FILE,
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
    component_document,
    component_from_document,
    documents_of,
    field_document,
    field_from_document,
    repeating_groups_of,
    slug_collisions,
    write_archive,
)
from rekep.urls import HTTP, LOCAL, Url

#: Where the scrape persists: the tag shards, the components and the version
#: list, so everything after the first scrape works offline -- including on a
#: machine that was never online, by copying the directory.
CACHE_DIRECTORY = pathlib.Path.home() / ".config" / "fix"

#: Lookup priority is a protocol contract. Standard definitions always win;
#: registered UDFs fill the numeric space FIX reserved for them; configured
#: venue dictionaries are consulted only after both.
STANDARD_NAMESPACE = "standard"
UDF_NAMESPACE = "fixtrading-udf"

# Bump when parsed records project or reconcile differently without source
# bytes changing. The parsed-output checksum catches parser and mapping changes;
# this marker makes registry-only changes invalidate an otherwise warm refresh.
_ADAPTER_PROJECTION = "rekep-fix-registry-v1"


#: The tiers `resolve` walks, in the order it walks them: an identity's own
#: name, then a declared alias -- a rendered spelling or a prior version's
#: name for the same tag, which the collapse records as one.
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
        cast(Any, columns)._REGISTRY = _BUILTIN


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


#: The dictionary's path inside the repository, relative to its root.
REPOSITORY_REGISTRY = ("data", "fix")


@cache
def repository_registry() -> str:
    """Where the repository's FIX dictionary lives.

    Walked up from this module rather than bundled into the wheel, so the
    checked-in `data/fix` is the one dictionary every unconfigured lookup
    reads and there is no second copy to keep byte-identical. A package
    installed away from its repository has no default and must be told.
    """
    for parent in pathlib.Path(__file__).resolve().parents:
        found = parent.joinpath(*REPOSITORY_REGISTRY)
        if found.is_dir():
            return os.fspath(found)
    raise RuntimeError(
        "no FIX dictionary above "
        f"{pathlib.Path(__file__).resolve()}: name one with `cache_dir`, or run "
        f"inside a checkout carrying {'/'.join(REPOSITORY_REGISTRY)}"
    )


_DEFAULT = object()


@dataclasses.dataclass(eq=False)
class FixRegistry(Convertible):
    """The FIX dictionary as local `Field` declarations."""

    #: Venue namespaces after the two protocol-owned tiers. A copied store
    #: still discovers every namespace, but configuration decides which venue
    #: wins an otherwise unscoped lookup.
    namespace_priority: tuple[str, ...] = ()

    #: A directory of JSON, or a `.zip` of the same files. `None` names the
    #: repository's `data/fix`; only `scrape` writes or refreshes a registry.
    cache_dir: str | os.PathLike[str] | None = None

    #: Optional filesystem for `cache_dir`, whose value is then a path on it.
    filesystem: pyarrow.fs.FileSystem | None = None

    #: Complete-source request timeout and retry policy used only by `scrape`.
    timeout: float = 30.0
    retries: int = 6
    backoff: float = 2.0

    def __post_init__(self) -> None:
        """Normalise the store location and lookup priority once."""
        if self.timeout <= 0 or self.retries < 0 or self.backoff < 0:
            raise ValueError("FIX registry timeout must be positive and retries non-negative")
        self.namespace_priority = tuple(
            dict.fromkeys(str(namespace).strip().lower() for namespace in self.namespace_priority)
        )
        reserved = {STANDARD_NAMESPACE, UDF_NAMESPACE} & set(self.namespace_priority)
        if reserved:
            raise ValueError(
                f"configured FIX venue namespaces cannot repeat protocol tiers {sorted(reserved)}"
            )
        if any(
            not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", namespace)
            for namespace in self.namespace_priority
        ):
            raise ValueError("configured FIX venue namespaces must be lowercase path-safe names")
        if self.cache_dir is None:
            self.cache_dir = repository_registry()

    @property
    def offline(self) -> bool:
        """Whether reads are confined to the named local or remote store."""
        return True

    @property
    def revision(self) -> int:
        """Generation of the in-memory views over this mutable store."""
        return self.__dict__.get("_revision", 0)

    @classmethod
    def from_builtin(cls) -> Self:
        """The default dictionary every unconfigured lookup resolves through.

        The repository's dictionary, unless `set_builtin` installed another.
        """
        held = _BUILTIN
        if held is not None and isinstance(held, cls):
            return held
        return cls.set_builtin(cls._checked_builtin())

    @classmethod
    def set_builtin(cls, registry: Self | None = None) -> Self:
        """Install the default `from_builtin` hands back, and return it.

        None restores the repository's dictionary. The registry has to carry
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
        installed = cls._checked_builtin() if registry is None else registry
        if not rekep_is_registered(installed):
            raise RuntimeError("the FIX registry lacks rekep's declared vocabulary")
        held = _BUILTIN
        _BUILTIN = installed
        if held is not None and held is not installed:
            _forget_builtin_views()
        return installed

    @classmethod
    def _checked_builtin(cls) -> Self:
        """The repository's registry, refusing one that lost rekep's vocabulary."""
        from rekep.fix.rekep import rekep_is_registered

        registry = cls(cache_dir=repository_registry())
        if not rekep_is_registered(registry):
            raise RuntimeError("the repository FIX registry lacks rekep's declared vocabulary")
        return registry

    @classmethod
    def scrape(
        cls,
        dump_folder: str | os.PathLike[str] | None = None,
        **configuration: Any,
    ) -> Self:
        """Refresh selected complete-file adapters, then replace one local directory."""
        from rekep.fix.adapters import ADAPTERS_BY_ID

        source_ids = tuple(
            dict.fromkeys(str(source) for source in configuration.pop("source_ids", ()))
        ) or tuple(source_id for source_id, adapter in ADAPTERS_BY_ID.items() if adapter.default)
        offline = bool(configuration.pop("offline", False))
        refresh_sources = bool(configuration.pop("refresh_sources", False))
        source_cache_value = configuration.pop("source_cache", None)
        if offline and refresh_sources:
            raise ValueError("offline FIX refresh cannot redownload source files")
        unknown = [source_id for source_id in source_ids if source_id not in ADAPTERS_BY_ID]
        if unknown:
            raise KeyError(f"unknown FIX registry sources {unknown!r}")
        restricted = [
            source_id
            for source_id in source_ids
            if not offline and not ADAPTERS_BY_ID[source_id].fetch_allowed
        ]
        if restricted:
            raise ValueError(
                f"FIX sources {restricted!r} cannot be fetched; cache their files and use offline"
            )
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
        source_cache = pathlib.Path(
            source_cache_value or target.with_name(f".{target.name}-sources")
        )
        if source_cache.exists() and (not source_cache.is_dir() or source_cache.is_symlink()):
            raise ValueError(f"the FIX registry source cache is not a directory: {source_cache}")

        report = ConflictReport()
        unchanged = False
        with tempfile.TemporaryDirectory(prefix=f".{target.name}-", dir=target.parent) as scratch:
            root = pathlib.Path(scratch)
            staged = root / target.name
            if target.exists():
                shutil.copytree(target, staged, copy_function=_stage_registry_file)
            source = cls(cache_dir=staged, **configuration)
            source.__dict__["_conflicts"] = report
            source._ingest_adapters(
                source_ids, source_cache, offline=offline, refresh=refresh_sources
            )
            report = source.conflicts
            unchanged = target.exists() and not source.__dict__.get("_refresh_changed", True)
            if not unchanged:
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
        installed.__dict__["_source_status"] = source.__dict__.get("_source_status", ())
        return installed

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

        fields: dict[tuple[str, int | str], Field] = {}
        components: dict[str, dict[str, ComponentRecord]] = {}
        repeating_groups: dict[str, dict[str, ComponentRecord]] = {}
        component_tags: dict[str, set[int]] = {}
        component_refs: dict[str, set[str]] = {}
        for name in names:
            if name == VERSIONS_FILE:
                continue
            document = place.read(name)
            if not isinstance(document, Mapping) or not document:
                raise ValueError(f"FIX registry document {name!r} is empty or unreadable")
            if name == SOURCES_FILE:
                sources = document.get("sources")
                required = {
                    "source_id",
                    "namespace",
                    "url",
                    "version",
                    "format",
                    "checksum",
                    "license_url",
                }
                if not isinstance(sources, list) or any(
                    not isinstance(source, Mapping) or not required.issubset(source)
                    for source in sources
                ):
                    raise ValueError("the FIX registry source manifest is invalid")
                continue
            namespaced = re.fullmatch(
                rf"{NAMESPACES}/([^/]+)/{FIELDS}/\d{{6}}{re.escape(DOCUMENT_SUFFIX)}", name
            )
            if name.startswith(f"{FIELDS}/") or namespaced:
                namespace = namespaced.group(1) if namespaced else ""
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
                    if not namespace and not set(entry.fix.versions).issubset(
                        known_versions | {ANY_VERSION}
                    ):
                        raise ValueError(
                            f"FIX field {stored!r} in {name!r} names a version this store "
                            "does not declare"
                        )
                    declared_tag = entry.fix.tag
                    expected_key = (
                        str(declared_tag) if declared_tag is not None else entry.fix.canonical
                    )
                    if (
                        str(stored) != expected_key
                        or field_document(entry.fix.key, namespace) != name
                    ):
                        raise ValueError(f"FIX field {stored!r} is stored in the wrong shard")
                    identity = (namespace, entry.fix.key)
                    if identity in fields:
                        raise ValueError(f"FIX field {stored!r} is stored more than once")
                    fields[identity] = entry
                continue
            scoped_component = re.fullmatch(
                rf"{NAMESPACES}/([^/]+)/({COMPONENTS}|{REPGROUP})/[^/]+"
                rf"{re.escape(DOCUMENT_SUFFIX)}",
                name,
            )
            namespace = scoped_component.group(1) if scoped_component else ""
            component_file = name.startswith(f"{COMPONENTS}/") or bool(
                scoped_component and scoped_component.group(2) == COMPONENTS
            )
            group_file = name.startswith(f"{REPGROUP}/") or bool(
                scoped_component and scoped_component.group(2) == REPGROUP
            )
            if not component_file and not group_file:
                raise ValueError(f"unexpected FIX registry document {name!r}")
            unknown = sorted(set(document) - {"name", "versions", "declaration", "aliases"})
            component_versions = document.get("versions")
            if (
                unknown
                or type(document.get("name")) is not str
                or not isinstance(component_versions, list)
                or any(type(version) is not str for version in component_versions)
                or len(set(component_versions)) != len(component_versions)
                or (
                    not namespace
                    and not set(component_versions).issubset(known_versions | {ANY_VERSION})
                )
                or not isinstance(document.get("declaration"), Mapping)
                or not isinstance(document.get("aliases", []), list)
            ):
                kind = "repeating group" if group_file else "component"
                raise ValueError(f"FIX {kind} in {name!r} has invalid metadata")
            try:
                entry = component_from_document(document)
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                kind = "repeating group" if group_file else "component"
                raise ValueError(f"FIX {kind} in {name!r} is invalid: {error}") from error
            # The declaration validates by being read: a document Field cannot
            # parse is not one. What is left is the cross-check the shape alone
            # cannot make -- that every tag and every reference it names is
            # something this store actually holds.
            if is_group(entry.declaration) != group_file:
                expected_kind = "a list" if group_file else "a struct"
                raise ValueError(f"FIX declaration in {name!r} must be {expected_kind}")
            declared = entry_of(entry.declaration) if group_file else entry.declaration
            members = list(walk(declared))
            if group_file:
                members.insert(0, (entry.declaration, ()))
            for member, _ in members:
                if is_reference(member):
                    component_refs.setdefault(namespace, set()).add(fold(member.name))
                    continue
                tag = member.fix.tag
                if tag is None or tag <= 0:
                    raise ValueError(
                        f"FIX component in {name!r} declares {member.name!r} with no tag"
                    )
                component_tags.setdefault(namespace, set()).add(tag)
            records_by_namespace = repeating_groups if group_file else components
            records = records_by_namespace.setdefault(namespace, {})
            expected = component_document(entry.slug, namespace, group=group_file)
            if name != expected or entry.slug in records:
                kind = "repeating group" if group_file else "component"
                raise ValueError(f"FIX {kind} {entry.name!r} is stored under the wrong name")
            records[entry.slug] = entry
        if not fields:
            raise ValueError("the FIX registry has no fields")
        standard_fields = {
            key: entry for (namespace, key), entry in fields.items() if not namespace
        }
        if standard_fields and not versions:
            raise ValueError("the FIX registry version index needs a standard version")
        field_names = {
            fold(spelling)
            for entry in standard_fields.values()
            for spelling in entry.fix.spellings()
        }
        for version, members in sessions.items():
            missing = [name for name, _required in members if fold(name) not in field_names]
            if missing:
                raise ValueError(f"the FIX {version} session names unknown fields {missing}")
        for namespace, records in components.items():
            scoped_fields = {
                entry.fix.tag
                for (field_namespace, _), entry in fields.items()
                if field_namespace in {"", namespace}
            }
            missing_tags = sorted(component_tags.get(namespace, set()) - scoped_fields)
            if missing_tags:
                raise ValueError(f"FIX components name unknown field tags {missing_tags[:5]}")
            available_components = {
                entry.folded
                for candidate in (components.get("", {}), records)
                for entry in candidate.values()
            }
            missing_components = sorted(component_refs.get(namespace, set()) - available_components)
            if missing_components:
                raise ValueError(f"FIX components name unknown components {missing_components[:5]}")
            if repeating_groups.get(namespace, {}) != repeating_groups_of(records):
                raise ValueError("the FIX repeating-group index does not match the component trees")
        standard_components = components.get("", {})
        problems = _problems((standard_fields, standard_components))
        if problems:
            raise ValueError(f"the FIX registry is inconsistent: {problems[0]}")
        return len(names)

    @property
    def conflicts(self) -> ConflictReport:
        """What the last `scrape` collapsed; empty for a store it did not build."""
        return self.__dict__.get("_conflicts") or ConflictReport()

    def _ingest_adapters(
        self,
        source_ids: Sequence[str],
        cache: pathlib.Path,
        *,
        offline: bool,
        refresh: bool = False,
    ) -> None:
        """Load complete cached sources and store their field definitions."""
        from rekep.fix.adapters import ADAPTERS_BY_ID

        unknown = [source_id for source_id in source_ids if source_id not in ADAPTERS_BY_ID]
        if unknown:
            raise KeyError(f"unknown FIX registry sources {unknown!r}")
        cache.mkdir(parents=True, exist_ok=True)
        parsed = [
            ADAPTERS_BY_ID[source_id].load(
                cache,
                offline=offline,
                refresh=refresh,
                timeout=self.timeout,
                retries=self.retries,
                backoff=self.backoff,
            )
            for source_id in source_ids
        ]
        previous_versions = self._layout.versions()
        standard_versions = tuple(
            dict.fromkeys(
                registry.declaration_version
                for registry in parsed
                if registry.source.namespace == STANDARD_NAMESPACE and registry.declaration_version
            )
        )
        versions = tuple(
            sorted(
                {
                    *self._layout.versions(),
                    *standard_versions,
                },
                key=newest_rank,
                reverse=True,
            )
        )
        index_changed = versions != previous_versions or any(
            not self._layout.stored(version) or not self._layout.declared(version)
            for version in standard_versions
        )
        if parsed:
            self._layout.store_versions(versions)
            for version in standard_versions:
                self._layout.store_stored(version)
                self._layout.store_declared(version)
            self._forget()
        priorities = {
            source_id: int(ADAPTERS_BY_ID[source_id].priority) for source_id in source_ids
        }
        lookup_order = {source_id: order for order, source_id in enumerate(source_ids)}
        self.__dict__["_refresh_priorities"] = priorities
        manifest = {
            str(source.get("source_id")): dict(source) for source in self._layout.source_manifest()
        }
        statuses: list[dict[str, Any]] = []
        standard_registries: list[Any] = []
        namespaced_registries: dict[str, list[Any]] = {}
        changed = index_changed
        try:
            for registry in parsed:
                conflict_count = len(self.conflicts.collapses) + len(self.conflicts.collisions)
                provenance = {
                    **registry.source.into_dict(),
                    "priority": ADAPTERS_BY_ID[registry.source.source_id].priority,
                    "lookup_order": lookup_order[registry.source.source_id],
                    "projection": _ADAPTER_PROJECTION,
                    "definitions_checksum": _source_definitions_checksum(registry),
                }
                source_id = str(provenance["source_id"])
                previous = manifest.get(source_id)
                replayed = (
                    not refresh
                    and previous is not None
                    and all(
                        previous.get(key) == provenance.get(key)
                        for key in (
                            "namespace",
                            "version",
                            "checksum",
                            "priority",
                            "projection",
                            "definitions_checksum",
                        )
                    )
                )
                source_conflicts = () if replayed else registry.conflicts
                if source_conflicts:
                    report = self.conflicts
                    attributed: list[Collapse] = []
                    for conflict in source_conflicts:
                        tag = int(conflict.key) if conflict.key.isdigit() else None
                        field = registry.field(tag) if tag is not None else None
                        attributed.append(
                            Collapse(
                                name=field.name if field is not None else conflict.key,
                                tag=tag,
                                part=TYPE if conflict.part == "datatype" else conflict.part,
                                kept=conflict.resolution,
                                keptsource=source_id,
                                dropped=tuple(
                                    Dropped(
                                        version=registry.source.version,
                                        reading=reading,
                                        source=source_id,
                                    )
                                    for reading in conflict.readings
                                    if reading != conflict.resolution
                                ),
                            )
                        )
                    self.__dict__["_conflicts"] = dataclasses.replace(
                        report, collapses=(*report.collapses, *attributed)
                    )
                if replayed:
                    changes = MappingProxyType({"additions": 0, "updates": 0})
                else:
                    definitions = tuple(
                        field.into_field() if hasattr(field, "into_field") else field
                        for field in registry.fields
                    )
                    changes = self.add_definitions(definitions, registry.source.namespace)
                changed = changed or bool(changes["additions"] or changes["updates"])
                if registry.source.namespace == STANDARD_NAMESPACE and not replayed:
                    standard_registries.append(registry)
                elif registry.source.namespace != STANDARD_NAMESPACE and not replayed:
                    namespaced_registries.setdefault(registry.source.namespace, []).append(registry)
                manifest[source_id] = provenance
                statuses.append(
                    {
                        "source_id": source_id,
                        "namespace": registry.source.namespace,
                        "fields": len(registry.fields),
                        "additions": changes["additions"],
                        "updates": changes["updates"],
                        "fallbacks": len(registry.fallbacks),
                        "conflicts": len(self.conflicts.collapses)
                        + len(self.conflicts.collisions)
                        - conflict_count,
                        "messages": len(registry.messages),
                        "components": len(registry.components),
                        "groups": len(registry.groups),
                    }
                )
            changed = self._ingest_standard_components(standard_registries) or changed
            for namespace, registries in sorted(namespaced_registries.items()):
                changed = self._ingest_namespaced_components(namespace, registries) or changed
            final_manifest = tuple(manifest.values())
            current_manifest = {
                source["source_id"]: source for source in self._layout.source_manifest()
            }
            changed = (
                changed
                or {source["source_id"]: source for source in final_manifest} != current_manifest
            )
            self.store_source_manifest(final_manifest)
        finally:
            self.__dict__.pop("_refresh_priorities", None)
        self.__dict__["_source_status"] = tuple(statuses)
        self.__dict__["_refresh_changed"] = changed

    def _ingest_standard_components(self, registries: Sequence[Any]) -> bool:
        """Reconcile all standard blocks, then rebuild derived groups once."""
        # Repeating groups stay derived from the component/message trees. A
        # second top-level copy would be a list under `components/`, where the
        # store contract admits structs only.
        if not registries:
            return False
        records = dict(self._entries[1])
        preserved_carriage = {slug: entry.msgtypes for slug, entry in records.items()}
        changed: set[str] = set()
        ordered = sorted(
            registries,
            key=lambda registry: (
                self._source_priority(registry.source.source_id),
                registry.source.source_id,
            ),
        )
        for registry in ordered:
            source = registry.source.source_id
            version = registry.declaration_version
            for declaration in registry.declarations().values():
                declared = record_copy(declaration)
                declared.fix.source = source
                declared.fix.sources = (source,)
                declared.fix["source-version"] = registry.source.version
                declared.fix["source-url"] = registry.source.url
                declared.fix["source-checksum"] = registry.source.checksum
                fresh = ComponentRecord.from_components((declared,), (version,))
                held = records.get(fresh.slug)
                if held is None:
                    merged = fresh
                else:
                    held_source = held.declaration.fix.source
                    fresh_priority = self._source_priority(source)
                    held_priority = self._source_priority(held_source)
                    if fresh_priority > held_priority or (
                        fresh_priority == held_priority and source != held_source
                    ):
                        # Lower-priority dictionaries are structural fallbacks:
                        # once an authoritative block exists they do not widen
                        # its versions or manufacture membership conflicts.
                        continue
                    merged = dataclasses.replace(
                        fresh,
                        versions=canonical_versions((*held.versions, version)),
                        aliases=held.aliases,
                    )
                records[fresh.slug] = merged
                if held is None or merged != held:
                    changed.add(fresh.slug)
        for slug in sorted(changed):
            self._layout._store_component(records[slug])
        carriage_changed = self._layout._store_carriage(preserved_carriage)
        groups_changed = self._layout._sync_repeating_groups()
        if changed or carriage_changed or groups_changed:
            self._forget()
        return bool(changed) or carriage_changed or groups_changed

    def _ingest_namespaced_components(self, namespace: str, registries: Sequence[Any]) -> bool:
        """Reconcile extension blocks without flattening them into standard."""
        records = dict(self._layout.namespace_component_records(namespace))
        changed = False
        ordered = sorted(
            registries,
            key=lambda registry: (
                self._source_priority(registry.source.source_id),
                registry.source.source_id,
            ),
        )
        for registry in ordered:
            source = registry.source.source_id
            version = registry.declaration_version
            for declaration in registry.declarations().values():
                declared = record_copy(declaration)
                declared.fix.source = source
                declared.fix.sources = (source,)
                declared.fix["namespace"] = namespace
                declared.fix["source-version"] = registry.source.version
                declared.fix["source-url"] = registry.source.url
                declared.fix["source-checksum"] = registry.source.checksum
                fresh = ComponentRecord.from_components((declared,), (version,))
                held = records.get(fresh.slug)
                if held is not None:
                    held_source = held.declaration.fix.source
                    fresh_priority = self._source_priority(source)
                    held_priority = self._source_priority(held_source)
                    if fresh_priority > held_priority or (
                        fresh_priority == held_priority and source != held_source
                    ):
                        continue
                    fresh = dataclasses.replace(
                        fresh,
                        versions=canonical_versions((*held.versions, version)),
                        aliases=held.aliases,
                    )
                if held is None or fresh != held:
                    records[fresh.slug] = fresh
                    changed = True
        if changed:
            self._layout.store_namespace_components(namespace, records)
            self._forget()
        return changed

    @property
    def source_status(self) -> tuple[Mapping[str, Any], ...]:
        """Per-source results from the last in-process refresh."""
        return tuple(MappingProxyType(status) for status in self.__dict__.get("_source_status", ()))

    def _write(self, documents: Mapping[str, Mapping[str, Any]]) -> None:
        """Replace this store with `documents`, in one pass over the place."""
        self._documents.write_all(documents)
        self._forget()

    # -- versions ------------------------------------------------------------

    @cached_property
    def versions(self) -> tuple[str, ...]:
        """Every FIX version the dictionary carries, newest first."""
        return self._stored_versions() or self._known_versions()

    @property
    def latest_application_version(self) -> str | None:
        """Newest application version this dictionary can resolve."""
        return next(
            (version for version in self.versions if not version.upper().startswith("FIXT")),
            None,
        )

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

    def fields(self, version: str) -> list[Field]:
        """Every stored field of one FIX version, in tag order."""
        version = self._spelling(version)
        if self._torn():
            raise OSError(
                f"the FIX registry at {self.cache_dir} is torn; refresh it with "
                "FixRegistry.scrape()"
            )
        return self._stored_fields(version) or []

    def fields_available(self, version: str | None = None) -> bool:
        """Whether at least one selected version's fields can be read now."""
        try:
            candidates = self._versions(version)
        except (KeyError, OSError, ValueError):
            return False
        return any(self._stored_fields(candidate) is not None for candidate in candidates)

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

    def load(self, *versions: str) -> dict[str, int]:
        """Stored field counts by version; all stored versions when omitted."""
        return {version: len(self.fields(version)) for version in (versions or self.versions)}

    def session(self, version: str) -> tuple[tuple[str, bool], ...]:
        """`((name, required), ...)`: the standard header, then the trailer.

        What every message of a version carries whatever it says, and which of
        those it must -- the spec's own answer, read from the store so it costs
        nothing and works offline. Empty when the version has no session layer.
        """
        return self._stored_session(self._spelling(version))

    def components(self, version: str) -> list[Field]:
        """Every reusable component of one FIX version, in spec order."""
        version = self._spelling(version)
        return self._stored_components(version) or []

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

        found = frozenset(
            int(group.fix.tag)
            for group in self.repeating_groups(spelling)
            if group.fix.tag is not None
        )
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
                if declared is None:
                    break
                declared_in = entry_of(declared)
                named = first_declared_name(declared_in, by_name)
                if not named:
                    break
                found.append(named)
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

    def lookup(
        self,
        key: int | str,
        version: str | None = None,
        *,
        namespace: str | None = None,
    ) -> list[Field]:
        """One field as each version that declares it has it, newest version first.

        `key` is a tag (`54`, `"54"`) or any name the record answers to
        (`"Side"`, matched by its fold). `version` narrows to one version; the
        default walks them all in descending order, which is also the order of
        the result.

        A tag reads the one shard that can hold it. A name needs the name
        index, which is every shard -- a name has no arithmetic behind it.
        """
        entry = self._record(key, namespace=namespace)
        if (namespace and namespace != STANDARD_NAMESPACE) or (
            entry is not None and entry.fix.get("namespace")
        ):
            order = (version,) if version is not None else entry.fix.versions if entry else ()
        else:
            order = self._versions(version)
        return records_for(entry, order) if entry is not None else []

    def _record(self, key: int | str, *, namespace: str | None = None) -> Field | None:
        """One field record under an exact namespace or protocol priority."""
        if namespace is not None:
            normalized = str(namespace).strip().lower()
            if normalized == STANDARD_NAMESPACE:
                return (
                    self._layout.record(int(key))
                    if _is_tag(key)
                    else self._resolutions.get(fold(str(key)))
                )
            return self._layout.namespace_record(normalized, key)
        if _is_tag(key):
            standard = self._layout.record(int(key))
        else:
            standard = self._resolutions.get(fold(str(key)))
        if standard is not None:
            return standard
        for candidate in self._namespace_order:
            if candidate == STANDARD_NAMESPACE:
                continue
            found = self._layout.namespace_record(candidate, key)
            if found is not None:
                return found
        return None

    def field(
        self,
        key: int | str,
        version: str | None = None,
        *,
        namespace: str | None = None,
    ) -> Field | None:
        """One field by tag or by any name it answers to; None when nothing is.

        The whole **record** when no version is named: the newest reading, the
        versions that declare it, its aliases and the values it enumerates --
        which is what `record.fix.encode` and `FieldAccess` read. One version's
        reading of it when a version is named, which is the same declaration
        under that version alone.
        """
        record = self._record(key, namespace=namespace)
        if record is None:
            return None
        if version is None:
            built = merged_record(record, self.versions)
            if namespace is not None:
                built.fix["namespace"] = str(namespace).strip().lower()
            return built
        extension = bool(record.fix.get("namespace")) or bool(
            namespace and namespace != STANDARD_NAMESPACE
        )
        order = (version,) if extension else self._versions(version)
        found = records_for(record, order)
        return found[0] if found else None

    def definitions(self, key: int | str) -> tuple[Field, ...]:
        """Every definition of one tag or name, in unscoped lookup order."""
        found: list[Field] = []
        for namespace in self._namespace_order:
            record = self._record(key, namespace=namespace)
            if record is None:
                continue
            definition = merged_record(record, self.versions)
            definition.fix["namespace"] = namespace
            found.append(definition)
        return tuple(found)

    def definition(self, key: int | str, namespace: str) -> Field | None:
        """One definition under exactly one namespace."""
        return self.field(key, namespace=namespace)

    def namespaces(self) -> tuple[str, ...]:
        """Standard and stored extension namespaces in lookup order."""
        return self._namespace_order

    @cached_property
    def _namespace_order(self) -> tuple[str, ...]:
        stored = self._layout.namespaces()
        configured = tuple(
            namespace for namespace in self.namespace_priority if namespace in stored
        )
        manifest_order: dict[str, int] = {}
        for position, source in enumerate(self._layout.source_manifest()):
            namespace = str(source.get("namespace") or STANDARD_NAMESPACE)
            order = int(source.get("lookup_order", position))
            manifest_order[namespace] = min(order, manifest_order.get(namespace, order))
        remaining = tuple(
            sorted(
                (
                    namespace
                    for namespace in stored
                    if namespace not in {UDF_NAMESPACE, *configured}
                ),
                key=lambda namespace: (manifest_order.get(namespace, 2**31 - 1), namespace),
            )
        )
        return tuple(
            dict.fromkeys(
                (
                    STANDARD_NAMESPACE,
                    *((UDF_NAMESPACE,) if UDF_NAMESPACE in stored else ()),
                    *configured,
                    *remaining,
                )
            )
        )

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
        declared = {**(metadata or {}), **(source.metadata or {}), "fix:name": source.name}
        return Field(
            name=name or source.name,
            dtype=source.dtype if dtype is _DEFAULT else dtype,
            nullable=nullable,
            metadata=declared,
        )

    def field_tags(
        self,
        key: int | str,
        version: str | None = None,
        *,
        namespace: str | None = None,
    ) -> tuple[int, ...]:
        """Canonical and equivalent numeric tags in lift priority."""
        source = self.field(key, version, namespace=namespace)
        return () if source is None else source.fix.tag_priority

    def coalesce_tags(
        self,
        key: int | str,
        values: Mapping[int | str, Any],
        *,
        version: str | None = None,
        namespace: str | None = None,
        default: Any = None,
    ) -> Any:
        """First non-null scalar carried by a field's equivalent tags."""
        source = self.field(key, version, namespace=namespace)
        if source is None:
            raise KeyError(f"no FIX field {key!r} in {version or 'the registry'}")
        for tag in source.fix.tag_priority:
            value = values.get(tag, values.get(str(tag)))
            if value is not None:
                return value
        return default

    def arrow_coalesce_tags(
        self,
        key: int | str,
        values: Mapping[int | str, Any],
        rows: int,
        *,
        version: str | None = None,
        namespace: str | None = None,
    ) -> Any:
        """One typed Arrow column from a field's equivalent tag columns."""
        source = self.field(key, version, namespace=namespace)
        if source is None:
            raise KeyError(f"no FIX field {key!r} in {version or 'the registry'}")
        if rows < 0:
            raise ValueError("Arrow tag coalescing needs a non-negative row count")
        columns = []
        for tag in source.fix.tag_priority:
            column = values.get(tag, values.get(str(tag)))
            if column is None:
                continue
            if len(column) != rows:
                raise ValueError(f"FIX tag {tag} has {len(column)} rows; expected {rows}")
            columns.append(cast_arrow_field(column, source, source.dtype))
        if not columns:
            return pyarrow.nulls(rows, type=source.dtype)
        return columns[0] if len(columns) == 1 else compute.coalesce(*columns)

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
        namespace: str | None = None,
    ) -> list[Field]:
        """Distinct field identities matching `text`, best first."""
        wanted = str(text).strip().lower()
        if not wanted or limit <= 0:
            return []
        if namespace is not None and namespace != STANDARD_NAMESPACE:
            return _search_records(
                self.field_records(namespace).values(), wanted, limit=limit, fuzzy=fuzzy
            )
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
        if not found and namespace is None:
            extensions = (
                entry
                for candidate in self._namespace_order
                if candidate != STANDARD_NAMESPACE
                for entry in self.field_records(candidate).values()
            )
            found.extend(_search_records(extensions, wanted, limit=limit, fuzzy=fuzzy))
        return found

    # -- one identity, every version -----------------------------------------
    #
    # `lookup` and `scalar` answer about one key at a time and read a version
    # at a time. These answer about the whole dictionary at once, and about an
    # identity rather than a version's reading of one -- which is what a tool
    # comparing a capture's key names against the standard needs, and what
    # nine per-version documents could not be asked.

    def field_records(self, namespace: str = STANDARD_NAMESPACE) -> Mapping[str, Field]:
        """Every field identity in one namespace, keyed by canonical name."""
        normalized = str(namespace).strip().lower()
        records = (
            self._entries[0]
            if normalized == STANDARD_NAMESPACE
            else self._layout.namespace_field_records(normalized)
        )
        return MappingProxyType({entry.fix.canonical: entry for entry in records.values()})

    def standardizes(self, record: Field) -> int | str | None:
        """Which standard identity a namespace record is a venue reading of.

        Two ways a namespace says "this is the standard's field under our own
        tag": the registry's own `replacement-tag`, which names the tag the
        UDF was folded into, and a canonical name the standard already owns.
        Both are only taken when the datatypes agree -- a `char` reading of a
        `String` field is a different reading, not the same one renumbered.
        """
        fix = record.fix
        replacement = fix.get("replacement-tag")
        candidates: list[int | str] = []
        if replacement:
            candidates.append(int(replacement))
        if name := fix.get("replacement-name") or fix.canonical:
            candidates.append(str(name))
        for candidate in candidates:
            held = self._record(candidate, namespace=STANDARD_NAMESPACE)
            if held is None or held.fix.tag == fix.tag:
                continue
            if held.fix.type and fix.type and held.fix.type != fix.type:
                continue
            return held.fix.key
        return None

    def unified(self, key: int | str) -> Field | None:
        """One identity with every namespace reading of it folded in.

        The unique record a caller reads: the standard declaration, carrying
        each venue's own tag in `fix:tags`, each venue's spelling as an alias
        attributed to that namespace, and any value only a venue enumerates.
        A namespace whose reading is a *different* identity -- tag 9001 is
        `MaxShow` to one venue and `TradeType` to another -- is not folded in
        and stays reachable through `definitions`.
        """
        held = self._record(key, namespace=STANDARD_NAMESPACE)
        if held is None:
            return None
        built = merged_record(record_copy(held), self.versions)
        for namespace, record in self._standardized.get(built.fix.key, ()):
            built.fix.merge(merged_record(record, self.versions).fix, source=namespace)
        return built

    def unified_records(self) -> Mapping[str, Field]:
        """Every identity as one record, namespace readings folded into each."""
        standardized = self._standardized
        found: dict[str, Field] = {}
        for entry in self._entries[0].values():
            built = merged_record(record_copy(entry), self.versions)
            for namespace, record in standardized.get(built.fix.key, ()):
                built.fix.merge(merged_record(record, self.versions).fix, source=namespace)
            found[built.fix.canonical] = built
        return MappingProxyType(found)

    @cached_property
    def _standardized(self) -> Mapping[int | str, tuple[tuple[str, Field], ...]]:
        """`{standard key: the namespace readings of it}`, built once.

        Every namespace field is asked once whether it standardizes, because
        the answer needs a lookup per candidate and a unified read wants them
        all -- not one scan of every namespace per identity.
        """
        found: dict[int | str, list[tuple[str, Field]]] = {}
        for namespace in self._namespace_order:
            if namespace == STANDARD_NAMESPACE:
                continue
            for record in self._layout.namespace_field_records(namespace).values():
                target = self.standardizes(record)
                if target is not None:
                    found.setdefault(target, []).append((namespace, record))
        return MappingProxyType({key: tuple(value) for key, value in found.items()})

    def source_manifest(self) -> tuple[Mapping[str, Any], ...]:
        """Deterministic complete-source provenance carried by this store."""
        return tuple(MappingProxyType(source) for source in self._layout.source_manifest())

    def store_source_manifest(self, sources: Sequence[Mapping[str, Any]]) -> None:
        """Replace the store's complete-source provenance."""
        self._layout.store_source_manifest(sources)
        self._forget()

    def tag_numbers(self) -> frozenset[int]:
        """Every numeric identity present in any locally stored version."""
        records = list(self.field_records().values())
        for version in self._stored_spellings():
            records.extend(self._stored_fields(version) or ())
        return frozenset(int(field.fix.tag) for field in records if field.fix.tag)

    def source_coverage(self) -> Mapping[str, Mapping[str, int]]:
        """Field counts each source led and answered for."""
        counted: dict[str, dict[str, int]] = {}
        for namespace in self._namespace_order:
            for entry in self.field_records(namespace).values():
                primary = entry.fix.source
                sources = entry.fix.sources or ((primary,) if primary else ())
                for source in sources:
                    counted.setdefault(source, {"primary": 0, "fields": 0})["fields"] += 1
                if primary:
                    coverage = counted.setdefault(primary, {"primary": 0, "fields": 0})
                    coverage["primary"] += 1
                    if entry.fix.get("type-fallback"):
                        coverage["fallbacks"] = coverage.get("fallbacks", 0) + 1
        return {source: counted[source] for source in sorted(counted)}

    def component_records(
        self, namespace: str = STANDARD_NAMESPACE
    ) -> Mapping[str, ComponentRecord]:
        """Every component identity in one namespace, keyed by canonical name.

        Messages are among them: a message is a component that arrives on the
        wire under a code, so `record.msg_type` is what tells the two apart
        and `message_records()` is the index keyed by that code.
        """
        normalized = str(namespace).strip().lower()
        records = (
            self._entries[1]
            if normalized == STANDARD_NAMESPACE
            else self._layout.namespace_component_records(normalized)
        )
        return MappingProxyType({entry.name: entry for entry in records.values()})

    def repeating_group_records(
        self, namespace: str = STANDARD_NAMESPACE
    ) -> Mapping[str, ComponentRecord]:
        """Every derived group in one namespace, keyed by canonical name."""
        normalized = str(namespace).strip().lower()
        records = (
            self._layout.repeating_group_records
            if normalized == STANDARD_NAMESPACE
            else self._layout.namespace_repeating_group_records(normalized)
        )
        return MappingProxyType({entry.name: entry for entry in records.values()})

    def repeating_groups(self, version: str | None = None) -> list[Field]:
        """Stored repeating-group declarations, optionally for one version."""
        spelling = None if version is None else self._spelling(version)
        return [
            entry.declaration
            for _, entry in sorted(self._layout.repeating_group_records.items())
            if spelling is None or entry.declares(spelling)
        ]

    def repeating_group(self, name: str) -> ComponentRecord:
        """One repeating group matched by canonical name or alias."""
        wanted = fold(name)
        for entry in self._layout.repeating_group_records.values():
            if wanted in {fold(spelled) for spelled in entry.spellings()}:
                return entry
        raise KeyError(f"no FIX repeating group {name!r}")

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

    def component_group_field(
        self,
        root: str,
        groups: str | Sequence[str],
        version: str | None = None,
    ) -> Field | None:
        """Terminal repeating-group field under one component or message root."""
        path = (groups,) if isinstance(groups, str) else tuple(groups)
        if not path:
            raise ValueError("a FIX component group path cannot be empty")
        try:
            entry = self.merged_component(root)
        except KeyError:
            return None
        try:
            spelling = self._spelling(version) if version is not None else entry.newest
            projected = self.component_field(root, spelling)
        except KeyError:
            return None
        if projected is None:
            return None
        members = projected.fields
        selected: Field | None = None
        for group in path:
            wanted = column_name(group)
            matches = [
                member
                for member in members
                if wanted in {column_name(member.name), column_name(member.fix.name)}
            ]
            if not matches:
                return None
            if len(matches) != 1:
                raise ValueError(f"FIX group {group!r} is ambiguous under {root!r}")
            selected = matches[0]
            if not isinstance(selected, ListField):
                raise ValueError(f"FIX member {group!r} under {root!r} is not a group")
            members = selected.item.fields
        return selected

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
        if projected is None:
            return None
        return cast(StructField, projected).into_dataclass(projected.fix.component)

    def _component_fields_by_name(self, version: str) -> dict[str, Field]:
        """`{folded FIX member name: field}` for one version projection."""
        return {
            column_name(member.name): member
            for member in self.fields(self._spelling(version))
            if member.dtype is not None
        }

    def resolve(self, name: str, *, namespace: str | None = None) -> Field | None:
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
        return self._record(str(name), namespace=namespace)

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
        entry = self.resolve(name, namespace=STANDARD_NAMESPACE)
        if entry is None:
            return False
        removed = self._layout.remove_field(entry.fix.key)
        self._forget()
        return removed

    def add_definition(self, entry: Field, namespace: str) -> Field:
        """Store or reconcile one extension definition in its namespace.

        Multiple authorities may publish the same venue dictionary. Their
        disagreement is data to report, not a reason to lose the remaining
        refresh; disputed datatypes deliberately become Arrow strings.
        """
        self.add_definitions((entry,), namespace)
        found = self.definition(entry.fix.key, namespace) or self.definition(
            entry.fix.canonical, namespace
        )
        if found is None:  # pragma: no cover - the bulk writer just stored it
            raise RuntimeError(f"FIX definition {entry.fix.key!r} was not stored")
        return found

    def add_definitions(self, entries: Sequence[Field], namespace: str) -> Mapping[str, int]:
        """Reconcile a source in memory, then write each affected shard once."""
        normalized = str(namespace).strip().lower()
        standard = normalized == STANDARD_NAMESPACE
        records = dict(
            self._layout.field_records
            if standard
            else self._layout.namespace_field_records(normalized)
        )
        stored_keys = set(records)
        names = {entry.fix.folded: entry for entry in records.values()}
        additions = 0
        updates = 0
        changed: set[int | str] = set()
        for entry in entries:
            fresh = record_copy(entry)
            if fresh.fix.version and not fresh.fix.versions:
                fresh.fix.versions = (fresh.fix.version,)
                fresh.fix.pop("version", None)
            if fresh.fix.source and not fresh.fix.sources:
                fresh.fix.sources = (fresh.fix.source,)
            if standard:
                fresh.fix.pop("namespace", None)
            else:
                fresh.fix["namespace"] = normalized
            refuse_record(fresh)
            held = records.get(fresh.fix.key)
            same_name = names.get(fresh.fix.folded)
            if held is None and same_name is not None and same_name.fix.key != fresh.fix.key:
                fresh_wins = self._source_priority(fresh.fix.source) < self._source_priority(
                    same_name.fix.source
                )
                winner, dropped = (fresh, same_name) if fresh_wins else (same_name, fresh)
                self._record_namespace_conflict(
                    winner,
                    dropped,
                    NAME,
                    dropped_reading=f"{dropped.fix.canonical} <{dropped.fix.key}>",
                )
                merged = _with_disputed_key(winner, dropped)
                held = same_name
                if fresh_wins:
                    records.pop(same_name.fix.key)
                    changed.add(same_name.fix.key)
            else:
                merged = fresh if held is None else self._merge_definition(held, fresh)
            if standard:
                merged.fix.pop("namespace", None)
            records[merged.fix.key] = merged
            names[merged.fix.folded] = merged
            additions += held is None
            modified = held is None or merged != held
            updates += held is not None and modified
            if modified:
                changed.add(merged.fix.key)
        records, shadowed = self._without_shadowed_aliases(records)
        for key in shadowed:
            if key not in changed and key in stored_keys:
                updates += 1
            changed.add(key)
        if standard:
            self._validated(fields=records)
        if changed:
            self._layout.store_field_records(
                records, "" if standard else normalized, changed=changed
            )
            self._forget()
        return MappingProxyType({"additions": additions, "updates": updates})

    def _without_shadowed_aliases(
        self, records: Mapping[int | str, Field]
    ) -> tuple[dict[int | str, Field], set[int | str]]:
        """Drop aliases which a canonical identity in the namespace now owns."""
        canonical = {entry.fix.folded: entry for entry in records.values()}
        built = dict(records)
        changed: set[int | str] = set()
        for key, entry in records.items():
            kept: list[Alias] = []
            for alias in entry.fix.named_aliases:
                owner = canonical.get(alias.folded)
                if owner is None or owner.fix.key == entry.fix.key:
                    kept.append(alias)
                    continue
                attributed = record_copy(entry)
                attributed.fix.source = alias.source or entry.fix.source
                self._record_namespace_conflict(
                    owner,
                    attributed,
                    ALIASES,
                    dropped_reading=alias.name,
                )
            if len(kept) == len(entry.fix.named_aliases):
                continue
            cleaned = record_copy(entry)
            cleaned.fix.named_aliases = tuple(kept)
            built[key] = cleaned
            changed.add(key)
        return built, changed

    def update_definition(self, entry: Field, namespace: str) -> Field:
        """Replace one exact extension definition."""
        normalized = str(namespace).strip().lower()
        if normalized == STANDARD_NAMESPACE:
            return self.update_field(entry)
        fresh = record_copy(entry)
        fresh.fix["namespace"] = normalized
        if self._layout.namespace_record(normalized, fresh.fix.key) is None:
            raise KeyError(f"no FIX definition {fresh.fix.key!r} in namespace {normalized!r}")
        self._layout.store_namespace_field(normalized, refuse_record(fresh))
        self._forget()
        return merged_record(fresh)

    def remove_definition(self, key: int | str, namespace: str) -> bool:
        """Delete one exact extension definition."""
        normalized = str(namespace).strip().lower()
        if normalized == STANDARD_NAMESPACE:
            found = self._record(key, namespace=STANDARD_NAMESPACE)
            return False if found is None else self.remove_field(found.fix.canonical)
        found = self._layout.namespace_record(normalized, key)
        if found is None:
            return False
        removed = self._layout.remove_namespace_field(normalized, found.fix.key)
        self._forget()
        return removed

    def _merge_definition(self, held: Field, fresh: Field) -> Field:
        """Two same-namespace readings under source priority."""
        held_priority = self._source_priority(held.fix.source)
        fresh_priority = self._source_priority(fresh.fix.source)
        winner, dropped = (fresh, held) if fresh_priority < held_priority else (held, fresh)
        built = record_copy(winner)
        built.fix.versions = canonical_versions((*held.fix.versions, *fresh.fix.versions))
        built.fix.sources = tuple(dict.fromkeys((*winner.fix.sources, *dropped.fix.sources)))
        built.fix.source = winner.fix.source or dropped.fix.source
        built.fix.tags = tuple(dict.fromkeys((*winner.fix.tags, *dropped.fix.tags)))
        aliases = list(winner.fix.named_aliases)
        aliased = {alias.folded for alias in aliases}
        for alias in dropped.fix.named_aliases:
            if alias.folded != winner.fix.folded and alias.folded not in aliased:
                aliases.append(alias)
                aliased.add(alias.folded)
        values = {value.value: value for value in winner.fix.enumerated}
        for value in dropped.fix.enumerated:
            kept = values.get(value.value)
            if kept is None:
                values[value.value] = value
                continue
            value_aliases = tuple(dict.fromkeys((*kept.aliases, *value.aliases)))
            if kept.meaning and value.meaning and kept.meaning != value.meaning:
                self._record_namespace_conflict(
                    winner,
                    dropped,
                    VALUES,
                    dropped_key=value.value,
                    dropped_reading=value.meaning,
                )
            values[value.value] = dataclasses.replace(
                kept,
                meaning=kept.meaning or value.meaning,
                aliases=value_aliases,
            )
        built.fix.enumerated = tuple(values.values())
        if fold(winner.fix.canonical) != fold(dropped.fix.canonical):
            displaced = Alias(name=dropped.fix.canonical, source=dropped.fix.source)
            if displaced.folded not in aliased:
                aliases.append(displaced)
                aliased.add(displaced.folded)
            if winner is fresh or displaced.folded not in {
                fold(spelling) for spelling in held.fix.spellings()
            }:
                self._record_namespace_conflict(winner, dropped, NAME)
        built.fix.named_aliases = tuple(aliases)
        built.fix.event_types = {**dropped.fix.event_types, **winner.fix.event_types}
        built.fix.states = {**dropped.fix.states, **winner.fix.states}
        built.fix.msgtypes = tuple(dict.fromkeys((*winner.fix.msgtypes, *dropped.fix.msgtypes)))
        built.fix.components = tuple(
            dict.fromkeys((*winner.fix.components, *dropped.fix.components))
        )
        built.fix.column = winner.fix.column or dropped.fix.column
        built_dtype = built.dtype
        dropped_dtype = dropped.dtype
        if (
            isinstance(built_dtype, pyarrow.TimestampType)
            and isinstance(dropped_dtype, pyarrow.TimestampType)
            and built_dtype.unit == dropped_dtype.unit
            and built_dtype.tz is None
            and dropped_dtype.tz == "UTC"
        ):
            built.dtype = dropped_dtype
        if not winner.description and dropped.description:
            built.description = dropped.description
        elif (
            winner.description and dropped.description and winner.description != dropped.description
        ):
            self._record_namespace_conflict(
                winner,
                dropped,
                NOTE,
                dropped_reading=dropped.description,
            )
        held_types = _definition_types(held)
        fresh_types = _definition_types(fresh)
        readings = tuple(dict.fromkeys((*held_types, *fresh_types)))
        identities = {datatype_identity(reading) for reading in readings if reading}
        if len(identities) > 1:
            built.dtype = pyarrow.string()
            built.fix.type = "String"
            built.fix["disputed_types"] = json.dumps(readings, separators=(",", ":"))
            held_identities = {datatype_identity(reading) for reading in held_types if reading}
            fresh_identities = {datatype_identity(reading) for reading in fresh_types if reading}
            if not fresh_identities.issubset(held_identities):
                self._record_namespace_conflict(winner, dropped, TYPE, kept_reading="string")
        return built

    def _source_priority(self, source: str) -> int:
        """Active adapter priority, then the stored source manifest."""
        active = self.__dict__.get("_refresh_priorities", {})
        if source in active:
            return int(active[source])
        manifest = self._layout.source_manifest()
        for order, declared in enumerate(manifest):
            if str(declared.get("source_id") or "") == source:
                return int(declared.get("priority", order))
        return 20_000 + len(manifest)

    def _record_namespace_conflict(
        self,
        winner: Field,
        dropped: Field,
        part: str,
        *,
        kept_reading: str | None = None,
        dropped_key: str = "",
        dropped_reading: str | None = None,
    ) -> None:
        """Add one attributed namespace conflict to the refresh report."""
        report = self.conflicts
        conflict = Collapse(
            name=winner.fix.canonical,
            tag=winner.fix.tag,
            part=part,
            kept=kept_reading or winner.fix.newest,
            keptsource=winner.fix.source,
            dropped=(
                Dropped(
                    version=dropped.fix.newest,
                    source=dropped.fix.source,
                    key=dropped_key,
                    reading=dropped_reading
                    or (dropped.fix.type if part == TYPE else dropped.fix.canonical),
                ),
            ),
        )
        self.__dict__["_conflicts"] = dataclasses.replace(
            report, collapses=(*report.collapses, conflict)
        )

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
        entry = self.resolve(name, namespace=STANDARD_NAMESPACE)
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
        held = self.resolve(name, namespace=STANDARD_NAMESPACE)
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

        One identity per file makes a torn write cost one shard. It is still
        reported rather than served short, and only explicit `scrape` may
        replace it.
        """
        torn = getattr(self._layout, "torn", ())
        if not torn:
            return False
        warnings.warn(
            f"the FIX registry at {self.cache_dir} cannot read {list(torn[:5])}",
            RuntimeWarning,
            stacklevel=3,
        )
        return True

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
        self.__dict__.pop("_namespace_order", None)
        self.__dict__.pop("versions", None)
        self.__dict__["_revision"] = self.revision + 1

    # -- the cache files and the wire -----------------------------------------

    @property
    def archived(self) -> bool:
        """Whether this registry keeps its dictionary in a zip rather than a directory.

        Read off the extension, and only off the extension: a path that does
        not exist yet has to say what it will be before anything is written
        to it, and its `.zip` suffix says it.
        """
        location = Url.from_string(os.fspath(cast(str | os.PathLike[str], self.cache_dir)))
        return pathlib.PurePosixPath(location.path).suffix.lower() == ".zip"

    @cached_property
    def _cache_source(self) -> tuple[pyarrow.fs.FileSystem, str] | None:
        """The configured cache before an archive is localized."""
        location = os.fspath(cast(str | os.PathLike[str], self.cache_dir))
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
        location = os.fspath(cast(str | os.PathLike[str], self.cache_dir))
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
        identity = Url.from_string(
            os.fspath(cast(str | os.PathLike[str], self.cache_dir))
        ).into_string()
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
            cache_dir = cast(str | os.PathLike[str], self.cache_dir)
            if Url.from_string(os.fspath(cache_dir)).scheme in HTTP:
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
        cache_dir = cast(str | os.PathLike[str], self.cache_dir)
        if _resource_identity(target) == _resource_identity(cache_dir, self.filesystem):
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
                    else fold(member.name) == fold(str(key))
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


# -- the store, as a directory or as a zip ------------------------------------


def _resource_identity(
    resource: str | os.PathLike[str], filesystem: pyarrow.fs.FileSystem | None = None
) -> str:
    """Canonical identity used when comparing two registry resources."""
    location = os.fspath(resource)
    if filesystem is not None:
        return f"{id(filesystem)}:{location}"
    return Url.from_string(location).into_string()


def _stage_registry_file(source: str, target: str) -> str:
    """Hard-link an immutable store document, copying where links are unavailable."""
    # A write creates a sibling and replaces this path, so a hard link never
    # mutates the published document it shares and makes a 1,600-file replay
    # metadata-only on local filesystems.
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)
    return target


def _source_definitions_checksum(registry: Any) -> str:
    """Digest the normalized parser output which feeds registry projection."""
    digest = hashlib.sha256()
    digest.update(
        repr(
            (
                registry.repository_name,
                registry.repository_version,
                registry.declaration_version,
            )
        ).encode()
    )
    digest.update(b"\0")
    for family in (
        "metadata",
        "datatypes",
        "code_sets",
        "fields",
        "messages",
        "components",
        "groups",
        "conflicts",
    ):
        digest.update(family.encode())
        digest.update(b"\0")
        for definition in getattr(registry, family, ()):
            digest.update(repr(definition).encode())
            digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def _definition_types(entry: Field) -> tuple[str, ...]:
    """Authoritative type readings retained by one reconciled definition."""
    encoded = entry.fix.get("disputed_types")
    if encoded:
        try:
            readings = json.loads(encoded)
        except (TypeError, ValueError):
            readings = ()
        if isinstance(readings, list) and all(isinstance(reading, str) for reading in readings):
            return tuple(dict.fromkeys(readings))
    return (entry.fix.type,) if entry.fix.type else ()


def _with_disputed_key(winner: Field, dropped: Field) -> Field:
    """The winning same-name identity with every disputed key attributed."""
    built = record_copy(winner)
    built.fix.sources = tuple(dict.fromkeys((*winner.fix.sources, *dropped.fix.sources)))
    readings: list[dict[str, str]] = []
    encoded = winner.fix.get("disputed_keys")
    if encoded:
        try:
            stored = json.loads(encoded)
        except (TypeError, ValueError):
            stored = ()
        if isinstance(stored, list):
            readings.extend(reading for reading in stored if isinstance(reading, dict))
    readings.extend(
        (
            {"key": str(winner.fix.key), "source": winner.fix.source},
            {"key": str(dropped.fix.key), "source": dropped.fix.source},
        )
    )
    unique = {
        (str(reading.get("key", "")), str(reading.get("source", ""))): {
            "key": str(reading.get("key", "")),
            "source": str(reading.get("source", "")),
        }
        for reading in readings
    }
    built.fix["disputed_keys"] = json.dumps(
        [unique[key] for key in sorted(unique)], separators=(",", ":"), sort_keys=True
    )
    return built


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


def _search_records(
    records: Iterable[Field],
    wanted: str,
    *,
    limit: int,
    fuzzy: bool,
) -> list[Field]:
    """Rank one namespace's cross-version records without a version scan."""
    members = tuple(records)
    ranked = [
        (rank, int(member.fix.tag or _NO_TAG), member)
        for member in members
        if (rank := _rank(member, wanted)) is not None
    ]
    if not ranked and fuzzy and not _is_tag(wanted):
        ceiling = max(2, len(wanted) // 3)
        for member in members:
            distance = min(
                (
                    found
                    for spelling in member.fix.spellings()
                    if (found := _levenshtein(fold(wanted), fold(spelling), ceiling)) is not None
                ),
                default=None,
            )
            if distance is not None:
                ranked.append((100 + distance, int(member.fix.tag or _NO_TAG), member))
    ranked.sort(key=lambda item: item[:2])
    if ranked and ranked[0][0] < _BY_DESCRIPTION:
        ranked = [item for item in ranked if item[0] < _BY_DESCRIPTION]
    return [member for _, _, member in ranked[:limit]]


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
