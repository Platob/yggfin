"""Where a FIX dictionary is kept: fields, components, and repeating groups.

One layout. Fields live in shards of one thousand tags under `fields/`, named
by the shard index, so the file holding a tag is arithmetic -- no index, no
lookup table, no scan -- and a single-tag lookup reads one document rather than
the dictionary. The tag space is sparse, and an empty shard is simply absent:
ten files hold the tagged and named fields. Fields FIX never numbered have no tag
to shard on: they key by their name and share `999999`, the one shard index the
arithmetic never reaches, so every field document is named the same way and
nothing has to ask what kind of record it is about to read.

A shard is a *list* of its records. Every record states its own tag or its own
name, so keying the list by that identity wrote it a second time, and two
spellings of one fact are one fact that can contradict itself.

Components stay one document per identity under `components/`, because they are
keyed by name and there is no arithmetic to do.

Repeating groups are derived from those component trees and stored under
`repgroup/`. Their declaration is the same list `Field` carried by the tree,
so tools can inspect a group without first finding a component which embeds it.

Every document here is a field document -- a component and a group are the
`Field` they declare, carrying the versions declaring them in their own `fix`
metadata exactly as a field record does. One serialization, so a reader that
can read a field can read the whole dictionary.

A store lives on a directory or inside a zip -- the extension decides -- and
never reaches the network.
"""

from __future__ import annotations

import dataclasses
import io
import json
import os
import pathlib
import posixpath
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Protocol

import pyarrow.fs

from rekep.arrow_path import ArrowPath
from rekep.convert import Convertible
from rekep.enums import State
from rekep.fields import Field, encodings_of, newest_rank
from rekep.filesystems import write_bytes
from rekep.fix.entries import (
    ANY_VERSION,
    Alias,
    ComponentRecord,
    FixFieldValue,
    canonical_versions,
    collapsed_record,
    fold,
    folded_field_values,
    newest_of,
    record_copy,
    record_for,
    refuse_record,
    slug_of,
)
from rekep.fix.fields import datatype_identity
from rekep.fix.quickfix import is_group, is_reference, walk
from rekep.require import require
from rekep.urls import LOCAL, Url

#: What the layout calls its three folders. Named here because the reader, the
#: writer and the tests must all spell them alike.
FIELDS = "fields"
COMPONENTS = "components"
REPGROUP = "repgroup"
NAMESPACES = "namespaces"

#: Complete-source provenance is one store-level document. Keeping it beside
#: the declarations makes a copied directory or archive self-describing while
#: leaving the arithmetic standard shard layout unchanged.
SOURCES_FILE = "sources.json"

#: What a stored document is named, and so what a store is written in. JSON,
#: and measured: the dictionary is nearly a thousand documents and every
#: process that imports this package parses a projection of it, where
#: pure-Python YAML costs 25 seconds against a tenth of one for JSON. One
#: spelling, so a read is one open and a name is one document.
DOCUMENT_SUFFIX = ".json"

#: What a declaration a *person* hands in may be written in -- a field record
#: dropped on `rekep fix add-field`, not a document this store wrote. A store
#: is the format above; this is the courtesy at the edge, and the two being
#: one tuple is what made a store read every document name twice.
DECLARATION_SUFFIXES: tuple[str, ...] = (".json", ".yaml")

#: What one stored document holds. A mapping where the file is keyed -- the
#: version list, the source manifest, one component -- and a list of records
#: where it is a shard, because a shard's records already carry their own key
#: and writing it twice is a document that can disagree with itself.
Document = Mapping[str, Any] | Sequence[Mapping[str, Any]]


#: Where the layout keeps what belongs to no single identity: the version
#: list, each version's session layer, and which versions have had their
#: components read at all.
VERSIONS_FILE = f"versions{DOCUMENT_SUFFIX}"
SESSIONS = "sessions"
STORED = "stored"
DECLARED = "declared"

#: How many tags one shard holds. One thousand keeps one lookup bounded while
#: halving object-store listings and archive members for the sparse registry.
SHARD_SPAN = 1_000

#: The shard the fields FIX never numbered share. An index, not a name, so
#: every field document is `fields/NNNNNN.json` under one rule and there is no
#: second kind of document for a reader or a writer to test for. It sorts
#: after every populated range, which is where a nameless-tag record belongs.
NAMED_SHARD = 999_999

#: The largest tag the shard arithmetic can carry without colliding with
#: `NAMED_SHARD`. FIX's own tags stop five orders of magnitude below it.
MAX_TAG = NAMED_SHARD * SHARD_SPAN - 1


class Documents(Protocol):
    """Reading and writing named JSON documents, wherever they are kept.

    The whole of what the layout needs from a place. `FixRegistry` owns one of
    these; a directory and a zip are two implementations of it, and the layout
    below never knows which it has.
    """

    def read(self, name: str) -> Document | None:
        """One document, or None when the store does not hold it."""
        ...

    def write(self, name: str, payload: Document) -> None:
        """Keep one document, replacing whatever was under that name."""
        ...

    def remove(self, name: str) -> bool:
        """Drop one document; False when there was none."""
        ...

    def names(self) -> tuple[str, ...]:
        """Every document this store holds, as posix-style relative names."""
        ...

    def read_many(self, prefix: str) -> dict[str, Document]:
        """Every document under `prefix`, in one pass over the place.

        "The fields of 4.4" is a question about every identity, so the shards
        are read together; a place that can answer many documents more cheaply
        at once than one at a time says so here.
        """
        ...

    def write_all(self, documents: Mapping[str, Document]) -> None:
        """Replace this store with the complete document mapping."""
        ...


def is_document(name: str) -> bool:
    """Whether `name` is a stored document."""
    return name.endswith(DOCUMENT_SUFFIX)


def is_declaration(name: str) -> bool:
    """Whether `name` is a declaration this can read, in either format."""
    return name.endswith(DECLARATION_SUFFIXES)


def document_stem(name: str) -> str:
    """`fields/party_role.json` -> `fields/party_role`; the name without a format."""
    return name[: -len(DOCUMENT_SUFFIX)] if name.endswith(DOCUMENT_SUFFIX) else name


def document_text(payload: Document) -> str:
    """One stored document's text. The one place the on-disk spelling is decided."""
    return json.dumps(payload, indent=1)


def document_of(payload: bytes, name: str) -> Any:
    """One supplied document read back, by the format its name names.

    A torn document raises `ValueError` whichever format it is in, because
    that is the one thing every caller here already handles: a half-written
    file is a cold cache, not a dead registry.
    """
    if not name.endswith(".yaml"):
        return json.loads(payload.decode("utf-8"))
    yaml = require("yaml", "yaml")
    try:
        return yaml.safe_load(payload.decode("utf-8"))
    except yaml.YAMLError as error:
        raise ValueError(f"{name} is not a document this can read: {error}") from error


# -- the two places ----------------------------------------------------------


@dataclasses.dataclass(eq=False)
class DirectoryDocuments:
    """Documents under a directory, on any Arrow filesystem."""

    filesystem: pyarrow.fs.FileSystem
    directory: str

    def read(self, name: str) -> Document | None:
        """One document, or None for anything that cannot be read as one."""
        try:
            payload = self._path(name).read_bytes()
            return None if payload is None else document_of(payload, name)
        except (OSError, ValueError, pyarrow.ArrowException):
            # A torn write or someone else's file: write over it rather than
            # refuse to run offline forever.
            return None

    def write(self, name: str, payload: Document) -> None:
        """Written beside, then renamed, so a reader never sees half a file."""
        path = self._path(name)
        scratch = path.with_name(f"{path.name}.tmp")
        scratch.write_bytes(document_text(payload).encode())
        scratch.replace(path)

    def remove(self, name: str) -> bool:
        """Delete one document; False when it is not there."""
        return self._path(name).delete()

    def names(self) -> tuple[str, ...]:
        """Every document under the directory, folders included."""
        prefix = self.directory.replace("\\", "/").rstrip("/") + "/"
        found = []
        root = ArrowPath(self.directory, self.filesystem)
        for path, info in root.ls_with_info(recursive=True):
            if info.type != pyarrow.fs.FileType.File or not is_document(info.path):
                continue
            spelled = path.path
            found.append(spelled[len(prefix) :] if spelled.startswith(prefix) else spelled)
        return tuple(sorted(found))

    def read_many(self, prefix: str) -> dict[str, Document]:
        """Every document under `prefix`, one file open each."""
        return {
            name: document
            for name in self.names()
            if name.startswith(prefix) and (document := self.read(name)) is not None
        }

    def write_all(self, documents: Mapping[str, Document]) -> None:
        """Replace every registry document, dropping identities no longer declared."""
        kept = set(documents)
        stale = [name for name in self.names() if name not in kept]
        for name, document in documents.items():
            self.write(name, document)
        for name in stale:
            self.remove(name)

    def _path(self, name: str) -> ArrowPath:
        return ArrowPath(posixpath.join(self.directory, name), self.filesystem)


@dataclasses.dataclass(eq=False)
class ArchiveDocuments:
    """Documents inside one zip, a store and not only a way to publish one.

    A `zip -r fix.zip fix/` archive prefixes every member with its folder, so
    what a name means is resolved against what the archive already holds rather
    than assumed to sit at the root.
    """

    archive: str | os.PathLike[str]
    #: Called after a write, to copy a localized archive back where it came from.
    synchronise: Any = None

    def read(self, name: str) -> Document | None:
        """One member, as the document it holds; None when it is not there."""
        held = self._members()
        if name not in held:
            return None
        try:
            with zipfile.ZipFile(self.archive) as opened:
                return document_of(opened.read(held[name]), name)
        except (OSError, ValueError, zipfile.BadZipFile):
            # A torn archive is a cold cache, not a dead registry.
            return None

    def write(self, name: str, payload: Document) -> None:
        """Put one member in, replacing what was there under that name."""
        self._rewrite({name: document_text(payload)})

    def remove(self, name: str) -> bool:
        """Drop one member; False when the archive did not hold it."""
        if name not in self._members():
            return False
        self._rewrite({}, drop=(name,))
        return True

    def names(self) -> tuple[str, ...]:
        """Every member, under the name the layout addresses it by."""
        return tuple(sorted(self._members()))

    def read_many(self, prefix: str) -> dict[str, Document]:
        """Every member under `prefix`, opening the archive once.

        Once, and that is the whole point: a dictionary is two thousand
        members, and opening the zip per member read the central directory two
        thousand times -- fourteen seconds where this takes a tenth of one.
        """
        wanted = {
            name: member for name, member in self._members().items() if name.startswith(prefix)
        }
        if not wanted:
            return {}
        found: dict[str, dict[str, Any]] = {}
        try:
            with zipfile.ZipFile(self.archive) as opened:
                for name, member in wanted.items():
                    try:
                        found[name] = document_of(opened.read(member), name)
                    except (OSError, ValueError, zipfile.BadZipFile):
                        continue
        except (OSError, zipfile.BadZipFile):
            return {}
        return found

    def write_all(self, documents: Mapping[str, Document]) -> None:
        """Replace the whole archive with `documents`, in one deterministic pass.

        Writing a member at a time rebuilds the zip once per member, which for
        a dictionary of two thousand identities is two thousand rebuilds. A
        store being filled says so and pays for one.
        """
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as fresh:
            for name in sorted(documents):
                fresh.writestr(archive_member(name), document_text(documents[name]))
        write_bytes(output.getvalue(), self.archive)
        self._cached_members = None
        self._synchronise()

    _cached_members: dict[str, str] | None = None

    def _members(self) -> dict[str, str]:
        """`{addressed name: member name}`, resolving any folder prefix."""
        if self._cached_members is not None:
            return self._cached_members
        try:
            with zipfile.ZipFile(self.archive) as opened:
                members = opened.namelist()
        except (OSError, zipfile.BadZipFile):
            members = []
        prefix = _archive_prefix(members)
        found: dict[str, str] = {}
        for member in sorted(members):
            if not is_document(member):
                continue
            found.setdefault(member[len(prefix) :] if prefix else member, member)
        self._cached_members = found
        return found

    def _rewrite(self, put: Mapping[str, str], drop: Sequence[str] = ()) -> None:
        """Rebuild the archive with `put` written and `drop` gone.

        Rewritten whole rather than appended to: a zip will happily hold two
        members of one name, and a reader then picks between a stale version
        and a fresh one. Built beside and renamed over, so a reader never sees
        half of it.
        """
        path = pathlib.Path(self.archive)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = self._members()
        prefix = _archive_prefix(list(existing.values()))
        replaced = {existing.get(name, f"{prefix}{name}") for name in (*put, *drop)}
        scratch = path.with_name(f"{path.name}.tmp")
        with zipfile.ZipFile(scratch, "w", zipfile.ZIP_DEFLATED) as fresh:
            try:
                with zipfile.ZipFile(path) as opened:
                    for member in opened.infolist():
                        if member.filename not in replaced:
                            fresh.writestr(member, opened.read(member))
            except (OSError, zipfile.BadZipFile):
                # Nothing there, or nothing readable there: written over rather
                # than mourned, and what could not be read was not being served.
                pass
            for name, text in put.items():
                fresh.writestr(archive_member(existing.get(name, f"{prefix}{name}")), text)
        scratch.replace(path)
        self._cached_members = None
        self._synchronise()

    def _synchronise(self) -> None:
        if self.synchronise is not None:
            self.synchronise()


def archive_member(name: str) -> zipfile.ZipInfo:
    """One archive member, stamped at the start of zip time and on no host."""
    entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    entry.compress_type = zipfile.ZIP_DEFLATED
    entry.create_system = 3
    entry.external_attr = 0o644 << 16
    return entry


def _archive_prefix(members: Sequence[str]) -> str:
    """The folder a `zip -r` archive keeps its members in, or none.

    `zip -r fix.zip fix/` puts every member under `fix/`, and a member written
    into such an archive has to join its neighbours rather than land at a root
    where nothing else is. So a prefix is a folder *every* member shares --
    never merely the first one's folder, which for this store is `fields/` and
    made the next write land in `fields/fields/`.
    """
    leading = {name.split("/", 1)[0] for name in members if is_document(name)}
    if len(leading) != 1:
        return ""
    (folder,) = leading
    if is_document(folder) or folder in (FIELDS, COMPONENTS, REPGROUP, NAMESPACES):
        return ""
    return f"{folder}/"


# -- the layout ---------------------------------------------------------------


def field_document(key: int | str, namespace: str = "") -> str:
    """The document holding one field key: `fields/000000.json` for tags 0-999.

    `tag // SHARD_SPAN` zero-padded, and `NAMED_SHARD` for a field FIX never
    numbered, which keys by its name instead. That is the whole mapping: no
    index in `versions.json`, no lookup table, no scan -- and one kind of
    document, so nothing downstream asks whether a record has a tag to decide
    where it is written or read.
    """
    if isinstance(key, int):
        if not 0 <= key <= MAX_TAG:
            raise ValueError(f"a FIX tag outside 0..{MAX_TAG} has no shard: {key}")
        index = key // SHARD_SPAN
    else:
        index = NAMED_SHARD
    relative = f"{FIELDS}/{index:06d}{DOCUMENT_SUFFIX}"
    return relative if not namespace else f"{NAMESPACES}/{_namespace(namespace)}/{relative}"


def component_document(slug: str, namespace: str = "", *, group: bool = False) -> str:
    """The component or derived-group document for one namespace."""
    folder = REPGROUP if group else COMPONENTS
    relative = f"{folder}/{slug}{DOCUMENT_SUFFIX}"
    return relative if not namespace else f"{NAMESPACES}/{_namespace(namespace)}/{relative}"


def _namespace(value: str) -> str:
    """One filesystem-safe registry namespace."""
    namespace = str(value).strip().lower()
    if not namespace or namespace == "standard":
        raise ValueError("a namespaced FIX definition needs a non-standard namespace")
    if not all(part and part.replace("-", "").isalnum() for part in namespace.split(".")):
        raise ValueError(f"{value!r} is not a FIX registry namespace")
    return namespace


@dataclasses.dataclass(eq=False)
class ShardedLayout:
    """A FIX dictionary as tag-range shards, cross-version records inside them.

    Each shard is `{"<tag>": {record}}` in numeric tag order, and is read and
    held whole -- a tag lookup parses at most one thousand records that share
    its range, never the dictionary. Questions about a whole version read every
    shard, once, and hold those too.
    """

    documents: Documents

    # -- what belongs to no identity -----------------------------------------

    def versions(self) -> tuple[str, ...]:
        """The version list the store holds; empty when it holds none."""
        return tuple(str(version) for version in self._index().get("versions", ()))

    def store_versions(self, versions: Sequence[str]) -> None:
        """Keep the version list, so the front page is fetched once."""
        index = self._index()
        declared = list(versions)
        if index.get("versions") != declared:
            self._store_index({**index, "versions": declared})

    def session(self, version: str) -> tuple[tuple[str, bool], ...]:
        """`((name, required), ...)`: the standard header, then the trailer."""
        stored = self._index().get(SESSIONS, {}).get(version, ())
        return tuple((str(name), bool(required)) for name, required in stored)

    def store_session(self, version: str, session: Sequence[tuple[str, bool]]) -> None:
        """Keep one version's session layer beside the records."""
        index = self._index()
        stored = dict(index.get(SESSIONS, {}))
        if session:
            stored[version] = [[name, required] for name, required in session]
        else:
            stored.pop(version, None)
        self._store_index({**index, SESSIONS: {key: stored[key] for key in sorted(stored)}})

    def store_declared(self, version: str) -> None:
        """Record that this version's components were read, however few it has.

        Without this an empty `components/` folder answers `[]` for a version
        the spec was never read for, which is the reading that made a stale
        artifact extract nothing and say nothing.
        """
        index = self._index()
        self._store_index({**index, DECLARED: sorted({*index.get(DECLARED, ()), version})})

    def declared(self, version: str) -> bool:
        """Whether this store has ever been told what components `version` has."""
        return version in set(self._index().get(DECLARED, ()))

    def store_stored(self, version: str) -> None:
        """Record that this version's fields were written, however few it has."""
        index = self._index()
        self._store_index({**index, STORED: sorted({*index.get(STORED, ()), version})})

    def stored(self, version: str) -> bool:
        """Whether this store has ever been written for `version` at all."""
        return version in set(self._index().get(STORED, ()))

    def spellings(self) -> tuple[str, ...]:
        """Every version any record is declared for, the session layer included.

        The wildcard a namespaced field carries is not a version and never
        appears here: it means "whichever version this store already has".
        """
        found = {
            version for record in self.field_records.values() for version in record.fix.versions
        }
        for entry in self.component_records.values():
            found.update(entry.versions)
        index = self._index()
        found.update(index.get(SESSIONS, {}))
        found.update(index.get(STORED, ()))
        found.update(index.get(DECLARED, ()))
        found.discard(ANY_VERSION)
        return tuple(sorted(found))

    def _index(self) -> dict[str, Any]:
        """`versions.json`: everything about a version that is not an identity."""
        return self.documents.read(VERSIONS_FILE) or {}

    def _store_index(self, payload: Mapping[str, Any]) -> None:
        self.documents.write(VERSIONS_FILE, {key: payload[key] for key in sorted(payload)})

    # -- one tag, one shard --------------------------------------------------

    def record(self, tag: int) -> Field | None:
        """One field by tag, reading only the shard that can hold it.

        The whole point of the arithmetic: asking what tag 54 is opens
        `fields/000000.json` and nothing else.
        """
        return self._shard(field_document(int(tag))).get(int(tag))

    def namespace_record(self, namespace: str, key: int | str) -> Field | None:
        """One definition from exactly one namespace."""
        namespace = _namespace(namespace)
        if isinstance(key, int) or str(key).isdigit():
            tag = int(key)
            return self._shard(field_document(tag, namespace)).get(tag)
        wanted = fold(str(key))
        for record in self.namespace_field_records(namespace).values():
            if wanted in {fold(spelling) for spelling in record.fix.spellings()}:
                return record
        return None

    def _shard(self, name: str) -> dict[int | str, Field]:
        """One shard's records, read once and held."""
        held = self.__dict__.setdefault("_shards", {})
        found = held.get(name)
        if found is None:
            found = held[name] = self._read_shard(name)
        return found

    def _read_shard(self, name: str) -> dict[int | str, Field]:
        document = self.documents.read(name)
        if document is None:
            if name in self.documents.names():
                self.__dict__.setdefault("_torn", set()).add(name)
            return {}
        if not isinstance(document, Sequence):
            raise ValueError(f"the FIX registry shard {name} is not a list of field records")
        records = (refuse_record(field_from_document(record)) for record in document)
        return {record.fix.key: record for record in records}

    @property
    def field_records(self) -> dict[int | str, Field]:
        """`{tag or folded name: record}` for every field, every shard read once."""
        held = self.__dict__.get("_fields")
        if held is None:
            shards = self.__dict__.setdefault("_shards", {})
            for name in self.documents.names():
                if name.startswith(f"{FIELDS}/"):
                    shards.setdefault(name, self._read_shard(name))
            held = self.__dict__["_fields"] = {
                key: record
                for name in sorted(shards)
                if name.startswith(f"{FIELDS}/")
                for key, record in shards[name].items()
            }
        return held

    def namespaces(self) -> tuple[str, ...]:
        """Stored definition namespaces in deterministic order."""
        prefix = f"{NAMESPACES}/"
        found = {
            name[len(prefix) :].split("/", 1)[0]
            for name in self.documents.names()
            if name.startswith(prefix) and f"/{FIELDS}/" in name
        }
        return tuple(sorted(found))

    def namespace_field_records(self, namespace: str) -> dict[int | str, Field]:
        """Every field definition stored under exactly one namespace."""
        namespace = _namespace(namespace)
        held = self.__dict__.setdefault("_namespace_fields", {})
        if namespace in held:
            return held[namespace]
        prefix = f"{NAMESPACES}/{namespace}/{FIELDS}/"
        shards = self.__dict__.setdefault("_shards", {})
        for name in self.documents.names():
            if name.startswith(prefix):
                shards.setdefault(name, self._read_shard(name))
        records = {
            key: record
            for name in sorted(shards)
            if name.startswith(prefix)
            for key, record in shards[name].items()
        }
        held[namespace] = records
        return records

    def namespace_component_records(self, namespace: str) -> dict[str, ComponentRecord]:
        """Every component and message stored under one extension namespace."""
        namespace = _namespace(namespace)
        held = self.__dict__.setdefault("_namespace_components", {})
        if namespace in held:
            return held[namespace]
        prefix = f"{NAMESPACES}/{namespace}/{COMPONENTS}/"
        readable = (
            self.documents.read_many(prefix)
            if any(name.startswith(prefix) for name in self.documents.names())
            else {}
        )
        records = {
            document_stem(name[len(prefix) :]): component_from_document(document)
            for name, document in sorted(readable.items())
        }
        held[namespace] = records
        return records

    def namespace_repeating_group_records(self, namespace: str) -> dict[str, ComponentRecord]:
        """Every derived group stored under one extension namespace."""
        namespace = _namespace(namespace)
        held = self.__dict__.setdefault("_namespace_repeating_groups", {})
        if namespace in held:
            return held[namespace]
        prefix = f"{NAMESPACES}/{namespace}/{REPGROUP}/"
        readable = (
            self.documents.read_many(prefix)
            if any(name.startswith(prefix) for name in self.documents.names())
            else {}
        )
        records = {
            document_stem(name[len(prefix) :]): component_from_document(document)
            for name, document in sorted(readable.items())
        }
        held[namespace] = records
        return records

    def source_manifest(self) -> tuple[dict[str, Any], ...]:
        """Complete-source provenance carried by this store."""
        document = self.documents.read(SOURCES_FILE) or {}
        sources = document.get("sources", ())
        return tuple(dict(source) for source in sources if isinstance(source, Mapping))

    def store_source_manifest(self, sources: Sequence[Mapping[str, Any]]) -> None:
        """Write deterministic complete-source provenance."""
        required = {
            "source_id",
            "namespace",
            "url",
            "version",
            "format",
            "checksum",
            "license_url",
        }
        normalized: list[dict[str, Any]] = []
        for source in sources:
            missing = required - source.keys()
            if missing:
                raise ValueError(f"a FIX source manifest entry lacks {sorted(missing)}")
            entry = {str(key): source[key] for key in sorted(source)}
            entry["namespace"] = str(entry["namespace"] or "standard").strip().lower()
            checksum = str(entry["checksum"])
            if (
                not checksum.startswith("sha256:")
                or len(checksum) != 71
                or checksum != checksum.lower()
            ):
                raise ValueError("a FIX source checksum must be sha256:<64 lowercase hex digits>")
            try:
                int(checksum[7:], 16)
            except ValueError as error:
                raise ValueError(
                    "a FIX source checksum must be sha256:<64 lowercase hex digits>"
                ) from error
            normalized.append(entry)
        normalized.sort(
            key=lambda entry: (
                int(entry.get("priority", 0)),
                str(entry["namespace"]),
                str(entry["source_id"]),
                str(entry["version"]),
            )
        )
        document = {"sources": normalized}
        if self.documents.read(SOURCES_FILE) != document:
            self.documents.write(SOURCES_FILE, document)

    @property
    def component_records(self) -> dict[str, ComponentRecord]:
        """`{slug: record}` for every component identity, read once and held."""
        held = self.__dict__.get("_components")
        if held is None:
            prefix = f"{COMPONENTS}/"
            readable = self.documents.read_many(prefix)
            torn = self.__dict__.setdefault("_torn", set())
            torn.update(name for name in self.documents.names() if name.startswith(prefix))
            torn.difference_update(readable)
            held = self.__dict__["_components"] = {
                document_stem(name[len(prefix) :]): component_from_document(document)
                for name, document in sorted(readable.items())
            }
        return held

    @property
    def repeating_group_records(self) -> dict[str, ComponentRecord]:
        """`{slug: record}` for every derived repeating-group identity."""
        held = self.__dict__.get("_repeating_groups")
        if held is None:
            prefix = f"{REPGROUP}/"
            readable = self.documents.read_many(prefix)
            torn = self.__dict__.setdefault("_torn", set())
            torn.update(name for name in self.documents.names() if name.startswith(prefix))
            torn.difference_update(readable)
            held = self.__dict__["_repeating_groups"] = {
                document_stem(name[len(prefix) :]): component_from_document(document)
                for name, document in sorted(readable.items())
            }
        return held

    def forget(self) -> None:
        """Drop the held records, so the next read sees what was just written."""
        self.__dict__.pop("_shards", None)
        self.__dict__.pop("_fields", None)
        self.__dict__.pop("_namespace_fields", None)
        self.__dict__.pop("_components", None)
        self.__dict__.pop("_repeating_groups", None)
        self.__dict__.pop("_namespace_components", None)
        self.__dict__.pop("_namespace_repeating_groups", None)
        self.__dict__.pop("_torn", None)

    @property
    def torn(self) -> tuple[str, ...]:
        """Documents this store holds and cannot read; empty when it is sound.

        A torn write costs one shard here, which is worse than one field and
        far better than a whole version answering short in silence. The
        registry treats a torn store as one to write again.
        """
        # Each store family is read once so a torn derived-group document is
        # reported with a torn field or component rather than hidden.
        self.field_records, self.component_records, self.repeating_group_records  # noqa: B018
        for namespace in self.namespaces():
            self.namespace_field_records(namespace)
            self.namespace_component_records(namespace)
            self.namespace_repeating_group_records(namespace)
        return tuple(sorted(self.__dict__.get("_torn", ())))

    # -- what a version declares ---------------------------------------------

    def fields(self, version: str) -> list[Field] | None:
        """One version's fields in tag order; None when it declares none."""
        records = self.field_records
        if not records:
            return None
        found = [
            member
            for record in records.values()
            if (member := record_for(record, version)) is not None
        ]
        if found:
            return sorted(found, key=_field_order)
        # No record declares this version. Whether that is "nobody has read it"
        # or "it has none of the fields this store keeps" -- which is what a
        # projection of two fields leaves FIXT1.1 as -- is what the index
        # remembers, and answering the second as the first would send an
        # offline registry to the network for a version it already holds.
        return [] if self.stored(version) else None

    def components(self, version: str) -> list[Field] | None:
        """What it takes to read one version's components, by name; None when it has none.

        The components that version declares, *and* the ones their trees
        reference: a record keeps the newest member tree, and 4.3's `Parties`
        is now the tree that reaches `PartySubID` through `PtysSubGrp` rather
        than naming it directly -- so a reader handed 4.3's declarations
        without `PtysSubGrp` would split the group and lose the member.

        None and `[]` are different answers -- "nobody ever read this version's
        spec" against "its spec declares none" -- and telling them apart is
        what makes a stale artifact detectable instead of silently extracting
        nothing. The version list is what remembers which.

        Messages are not among them, and do not seed the closure. A record
        keeps the newest member tree, so 4.2's `Allocation` is the tree
        5.0.SP2 declares -- and seeding from it would answer that 4.2 has
        `Parties`, whose group 4.2 traffic does not carry. The reusable
        blocks are what a group is read through; `merged_component()` and
        `message_records()` are how a message is asked about.
        """
        held = self.component_records
        wanted = {
            entry.folded
            for entry in held.values()
            if entry.declares(version) and not entry.msg_type
        }
        if not wanted:
            return [] if self.declared(version) else None
        by_name = {entry.folded: entry for entry in held.values()}
        wanted = component_closure(wanted, by_name)
        return [
            component
            for _, entry in sorted(held.items())
            if entry.folded in wanted and (component := entry.into_component()) is not None
        ]

    # -- writing -------------------------------------------------------------

    def store_field(self, record: Field) -> str:
        """Write one field record, and name the document it landed in."""
        name = field_document(record.fix.key)
        shard = self._shard(name)
        shard[record.fix.key] = record
        self.documents.write(name, _shard_document(shard))
        self.__dict__.pop("_fields", None)
        return name

    def store_field_records(
        self,
        records: Mapping[int | str, Field],
        namespace: str = "",
        changed: Iterable[int | str] | None = None,
    ) -> None:
        """Replace one namespace's fields with one write per affected shard."""
        normalized = _namespace(namespace) if namespace else ""
        changed_documents = (
            None if changed is None else {field_document(key, normalized) for key in changed}
        )
        shards: dict[str, dict[int | str, Field]] = {}
        for record in records.values():
            name = field_document(record.fix.key, normalized)
            stored = record_copy(record)
            if normalized:
                stored.fix["namespace"] = normalized
            else:
                stored.fix.pop("namespace", None)
            shards.setdefault(name, {})[stored.fix.key] = stored
        prefix = f"{NAMESPACES}/{normalized}/{FIELDS}/" if normalized else f"{FIELDS}/"
        existing = (
            {name for name in self.documents.names() if name.startswith(prefix)}
            if changed_documents is None
            else set()
        )
        for name in sorted(shards):
            if changed_documents is not None and name not in changed_documents:
                continue
            self.documents.write(name, _shard_document(shards[name]))
        if changed_documents is None:
            for name in sorted(existing - shards.keys()):
                self.documents.remove(name)
        else:
            for name in sorted(changed_documents - shards.keys()):
                self.documents.remove(name)
        self.forget()

    def store_namespace_field(self, namespace: str, record: Field) -> str:
        """Write one namespaced definition without touching the standard shard."""
        namespace = _namespace(namespace)
        name = field_document(record.fix.key, namespace)
        shard = self._shard(name)
        stored = record_copy(record)
        stored.fix["namespace"] = namespace
        shard[stored.fix.key] = stored
        self.documents.write(name, _shard_document(shard))
        self.__dict__.pop("_namespace_fields", None)
        return name

    def remove_namespace_field(self, namespace: str, key: int | str) -> bool:
        """Delete one exact namespaced definition."""
        namespace = _namespace(namespace)
        key = int(key) if isinstance(key, int) or str(key).isdigit() else fold(str(key))
        name = field_document(key, namespace)
        shard = self._shard(name)
        if key not in shard:
            return False
        del shard[key]
        self.__dict__.pop("_namespace_fields", None)
        if shard:
            self.documents.write(name, _shard_document(shard))
            return True
        return self.documents.remove(name)

    def remove_field(self, key: int | str) -> bool:
        """Delete one field record by tag or folded name; False when absent."""
        name = field_document(key)
        shard = self._shard(name)
        if key not in shard:
            return False
        del shard[key]
        self.__dict__.pop("_fields", None)
        if shard:
            self.documents.write(name, _shard_document(shard))
            return True
        return self.documents.remove(name)

    def store_component(self, entry: ComponentRecord) -> None:
        """Write one component record, replacing what was under its slug."""
        self._store_component(entry)
        self._sync_repeating_groups()

    def _store_component(self, entry: ComponentRecord) -> None:
        """Write one component without rebuilding the derived group index."""
        self.documents.write(
            f"{COMPONENTS}/{entry.slug}{DOCUMENT_SUFFIX}", component_record_document(entry)
        )
        self.component_records[entry.slug] = entry

    def store_namespace_components(
        self, namespace: str, records: Mapping[str, ComponentRecord]
    ) -> None:
        """Write changed extension blocks and derive their group index once."""
        namespace = _namespace(namespace)
        records = _used_in(records, {entry.folded: entry for entry in records.values()})
        held = self.namespace_component_records(namespace)
        for slug, entry in sorted(records.items()):
            if held.get(slug) != entry:
                self.documents.write(
                    component_document(slug, namespace), component_record_document(entry)
                )
        cached = self.__dict__.setdefault("_namespace_components", {})
        cached[namespace] = dict(records)
        groups = repeating_groups_of(records)
        held_groups = self.namespace_repeating_group_records(namespace)
        for slug in sorted(set(held_groups) - set(groups)):
            self.documents.remove(component_document(slug, namespace, group=True))
        for slug, entry in sorted(groups.items()):
            if held_groups.get(slug) != entry:
                self.documents.write(
                    component_document(slug, namespace, group=True),
                    component_record_document(entry),
                )
        self.__dict__.setdefault("_namespace_repeating_groups", {})[namespace] = groups

    def remove_component(self, slug: str) -> bool:
        """Delete one component identity; False when the store did not hold it."""
        removed = self._remove_component(slug)
        if removed:
            self._sync_repeating_groups()
        return removed

    def _remove_component(self, slug: str) -> bool:
        """Delete one component without rebuilding the derived group index."""
        self.component_records.pop(slug, None)
        return self.documents.remove(f"{COMPONENTS}/{slug}{DOCUMENT_SUFFIX}")

    def _sync_repeating_groups(self) -> bool:
        """Rewrite the group index from the component trees which own it."""
        records = repeating_groups_of(self.component_records)
        held = self.repeating_group_records
        changed = False
        prefix = f"{REPGROUP}/"
        for slug in sorted(set(held) - set(records)):
            changed = self.documents.remove(f"{prefix}{slug}{DOCUMENT_SUFFIX}") or changed
        for slug, entry in sorted(records.items()):
            if held.get(slug) != entry:
                self.documents.write(
                    f"{prefix}{slug}{DOCUMENT_SUFFIX}", component_record_document(entry)
                )
                changed = True
        self.__dict__["_repeating_groups"] = records
        return changed

    def store(
        self,
        version: str,
        fields: Sequence[Field],
        session: Sequence[tuple[str, bool]] = (),
        components: Sequence[Field] | None = None,
        url: str = "",
    ) -> None:
        """Fold one whole version into the records already stored.

        A scrape still arrives one version at a time, so this is where a
        version's reading meets the cross-version record: each field joins the
        record that owns its tag, and whether it owns the reading is decided
        the same way `collapse` decides it -- the newest application version
        wins, and everything older only adds enumerated values.
        """
        del url  # A record is not stored per version, so it carries no URL.
        held = dict(self.field_records)
        written: set[int | str] = set()
        for member in fields:
            key = member.fix.key
            record = fold_field(held.get(key), member, version)
            self.store_field(record)
            held[key] = record
            written.add(key)
        for key, record in held.items():
            if key in written or version not in record.fix.versions:
                continue
            # This call is what the version declares, so a field it no longer
            # names has lost that version.
            remaining = tuple(one for one in record.fix.versions if one != version)
            if remaining:
                shortened = record_copy(record)
                shortened.fix.versions = remaining
                self.store_field(shortened)
            else:
                self.remove_field(key)
        if components is not None:
            self._store_components(version, components)
            self.store_declared(version)
        self.store_stored(version)
        self.store_session(version, session)

    def _store_components(self, version: str, components: Sequence[Field]) -> None:
        """Fold one version's component declarations into the records."""
        declared = {found.name for found in components}
        for found in components:
            slug = slug_of(found.name)
            self._store_component(fold_component(self.component_records.get(slug), found, version))
        for slug, entry in list(self.component_records.items()):
            if entry.name in declared or version not in entry.versions:
                continue
            # The version declares components and not this one, so this
            # version's declaration of it is gone rather than merely unstated.
            remaining = tuple(one for one in entry.versions if one != version)
            if remaining:
                self._store_component(dataclasses.replace(entry, versions=remaining))
            else:
                self._remove_component(slug)
        self._store_carriage()
        self._sync_repeating_groups()

    def _store_carriage(self, preserved: Mapping[str, Sequence[str]] | None = None) -> bool:
        """Tell every block which messages carry it, once the trees settled.

        The same derivation a bulk collapse makes, run where a scrape folds
        one version at a time -- otherwise a store built a version at a time
        would answer `msgtypes` differently from one built in a single pass.
        Only what changed is written, so this costs nothing on a version whose
        messages reach the same blocks.
        """
        held = self.component_records
        changed = False
        derived = _used_in(held, {one.folded: one for one in held.values()})
        for slug, entry in derived.items():
            names = tuple(sorted({*entry.msgtypes, *(preserved or {}).get(slug, ())}))
            if names != entry.msgtypes:
                declared = record_copy(entry.declaration)
                declared.fix.msgtypes = list(names)
                entry = dataclasses.replace(entry, declaration=declared)
            if entry is not held[slug]:
                self._store_component(entry)
                changed = True
        return changed


def _shard_document(shard: Mapping[int | str, Field]) -> list[dict[str, Any]]:
    """One shard as it is written: numeric tag order, then the names.

    A list of the records themselves. Every record states its own tag or its
    own name, so a key above it was that identity written a second time --
    and a second spelling of one fact is a fact that can contradict itself.
    """
    tags = sorted(key for key in shard if isinstance(key, int))
    names = sorted(key for key in shard if not isinstance(key, int))
    return [field_record_document(shard[key]) for key in (*tags, *names)]


def field_record_document(record: Field) -> dict[str, Any]:
    """One registry field with its JSON metadata shown as the value it contains."""
    return _readable_declaration(record.into_dict())


def field_from_document(document: Mapping[str, Any]) -> Field:
    """Restore readable registry metadata to Arrow's string metadata."""
    return Field.from_dict(_stored_declaration(document))


def component_record_document(record: ComponentRecord) -> dict[str, Any]:
    """One component with readable metadata throughout its declaration tree."""
    return _readable_declaration(record.into_dict())


def component_from_document(document: Mapping[str, Any]) -> ComponentRecord:
    """Restore one readable component declaration to its stored metadata types."""
    return ComponentRecord.from_dict(_stored_declaration(document))


def _readable_declaration(value: Any) -> Any:
    """Render every FIX metadata object in a declaration tree as nested JSON."""
    if isinstance(value, Mapping):
        found = {key: _readable_declaration(member) for key, member in value.items()}
        fixed = found.get("fix")
        if isinstance(fixed, Mapping):
            found["fix"] = {key: _nested_metadata(member) for key, member in fixed.items()}
        return found
    if isinstance(value, list | tuple):
        return [_readable_declaration(member) for member in value]
    return value


def _stored_declaration(value: Any) -> Any:
    """Encode nested FIX metadata back to Arrow's string metadata contract."""
    if isinstance(value, Mapping):
        found = {key: _stored_declaration(member) for key, member in value.items()}
        fixed = value.get("fix")
        if isinstance(fixed, Mapping):
            found["fix"] = {
                key: member
                if isinstance(member, str)
                else json.dumps(member, separators=(",", ":"))
                for key, member in fixed.items()
            }
        return found
    if isinstance(value, list | tuple):
        return [_stored_declaration(member) for member in value]
    return value


def _nested_metadata(value: Any) -> Any:
    """Decode a JSON object or array while leaving scalar metadata as text."""
    if not isinstance(value, str) or not value.startswith(("[", "{")):
        return value
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return value
    return decoded if isinstance(decoded, (list, dict)) else value


def _field_order(member: Field) -> tuple[int, int, str]:
    """Tag order, with the fields FIX never numbered after them, by name."""
    tag = member.fix.get("tag")
    return (0, int(tag), "") if tag else (1, 0, member.name)


def fold_field(held: Field | None, member: Field, version: str) -> Field:
    """One version's reading of a field, folded into the record that owns it.

    The reading is kept when `version` is the newest application version the
    record then has; older versions only contribute enumerated values, which
    is what keeps a value that disappeared after 4.2 parsing. A spelling the
    newer reading displaces becomes an alias, so a rename -- tag 64 is
    `FutSettDate` through 4.3 and `SettlDate` after -- stays one identity that
    still answers to both.
    """
    fresh = collapsed_record([member], [version])
    if held is None:
        return fresh
    versions = canonical_versions((*held.fix.versions, version))
    if newest_of(versions) == version:
        # The newer reading owns the record, so it is the one written back to.
        built = fresh
        readings = (held, fresh)
        built.fix.named_aliases = _displaced(held, fresh.fix.canonical)
    else:
        built = record_copy(held)
        readings = (fresh, held)
    added = next((reading for reading in reversed(readings) if reading.fix.added), None)
    if added is not None:
        built.fix.added = added.fix.added
    values, origins = folded_field_values(readings)
    built.fix.enumerated = values
    scalar_origins = {
        part: source
        for part, source in built.fix.origins.items()
        if part not in (VALUES, ALIASES, ADDED)
    }
    if added is not None and (source := added.fix.source_of(ADDED)):
        scalar_origins[ADDED] = source
    built.fix.origins = {**scalar_origins, **origins}
    built.fix.versions = versions
    built.fix.event_types = {**fresh.fix.event_types, **held.fix.event_types}
    built.fix.states = {**fresh.fix.states, **held.fix.states}
    built.fix.msgtypes = _union(held.fix.msgtypes, fresh.fix.msgtypes)
    built.fix.components = _union(held.fix.components, fresh.fix.components)
    built.fix.sources = _union(_union(built.fix.sources, held.fix.sources), fresh.fix.sources)
    built.fix.source = built.fix.source or held.fix.source or fresh.fix.source
    built.fix.tags = tuple(dict.fromkeys((*built.fix.tags, *held.fix.tags, *fresh.fix.tags)))
    built.fix.column = held.fix.column or fresh.fix.column
    return built


def fold_component(held: ComponentRecord | None, declared: Field, version: str) -> ComponentRecord:
    """One version's component folded into the record that owns it."""
    fresh = ComponentRecord.from_components([declared], [version])
    if held is None:
        return fresh
    versions = (*held.versions, version)
    if newest_of(versions) == version:
        return dataclasses.replace(fresh, versions=versions, aliases=held.aliases)
    return dataclasses.replace(held, versions=versions)


def _displaced(held: Field, name: str) -> tuple[Alias, ...]:
    """The record's aliases, plus the spelling a newer reading just displaced."""
    canonical = held.fix.canonical
    aliases = held.fix.named_aliases
    if fold(canonical) == fold(name):
        return aliases
    spelled = {alias.folded for alias in aliases}
    if fold(canonical) in spelled:
        return aliases
    return (*aliases, Alias(name=canonical, source=held.fix.newest))


def _union(held: Sequence[str], fresh: Sequence[str]) -> tuple[str, ...]:
    """Both lists, in order, with nothing said twice."""
    return tuple(dict.fromkeys((*held, *fresh)))


# -- collapsing per-version declarations into records --------------------------


@dataclasses.dataclass(frozen=True)
class Dropped(Convertible):
    """One reading a collapse did not keep, and who stated it."""

    version: str
    reading: str
    #: The enumerated value this reading belongs to, where the part has keys.
    key: str = ""
    source: str = ""

    def into_dict(self) -> dict[str, Any]:
        """The dropped reading as the report holds it."""
        declared = {"version": self.version, "source": self.source, "reading": self.reading}
        return {**declared, "key": self.key} if self.key else declared

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Dropped:
        """Read one dropped reading back out of a report."""
        return cls(
            version=str(mapping.get("version") or ""),
            reading=str(mapping.get("reading") or ""),
            key=str(mapping.get("key") or ""),
            source=str(mapping.get("source") or ""),
        )


@dataclasses.dataclass(frozen=True)
class Collapse(Convertible):
    """What one identity lost when readings disagreed, and what was kept.

    Keyed conflicts sharing one winning version and source stay in one entry.
    `kept` names the version whose reading the record holds.
    """

    name: str
    part: str
    kept: str
    dropped: tuple[Dropped, ...] = ()
    tag: int | None = None
    keptsource: str = ""

    def into_dict(self) -> dict[str, Any]:
        """The collapse as the report holds it."""
        return {
            "name": self.name,
            "tag": self.tag,
            "part": self.part,
            "kept": self.kept,
            "keptsource": self.keptsource,
            "dropped": [one.into_dict() for one in self.dropped],
        }

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Collapse:
        """Read one collapse back out of a report."""
        tag = mapping.get("tag")
        return cls(
            name=str(mapping.get("name") or ""),
            part=str(mapping.get("part") or ""),
            kept=str(mapping.get("kept") or ""),
            dropped=tuple(Dropped.from_dict(one) for one in mapping.get("dropped") or ()),
            tag=int(tag) if tag is not None else None,
            keptsource=str(mapping.get("keptsource") or ""),
        )


@dataclasses.dataclass(frozen=True)
class Collision(Convertible):
    """One encoded spelling two values both normalize to, so neither has it."""

    name: str
    key: str
    values: tuple[str, ...] = ()
    tag: int | None = None
    sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Keep one source slot beside every colliding value."""
        if len(self.sources) != len(self.values):
            raise ValueError("a FIX encoding collision needs one source per value")

    def into_dict(self) -> dict[str, Any]:
        """The collision as the report holds it."""
        return {
            "name": self.name,
            "tag": self.tag,
            "key": self.key,
            "values": list(self.values),
            "sources": list(self.sources),
        }

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Collision:
        """Read one collision back out of a report."""
        tag = mapping.get("tag")
        return cls(
            name=str(mapping.get("name") or ""),
            key=str(mapping.get("key") or ""),
            values=tuple(str(value) for value in mapping.get("values") or ()),
            tag=int(tag) if tag is not None else None,
            sources=tuple(str(source) for source in mapping.get("sources") or ()),
        )


#: The parts of a reading a collapse can drop. Prose is not among them: a
#: description that grew a sentence between versions loses nothing by taking
#: the newest, and reporting six thousand of those would bury the ones that
#: matter.
VALUES = "values"
ALIASES = "aliases"
ADDED = "added"
TYPE = "type"
NAME = "name"
NOTE = "note"
MEMBERS = "members"
PARTS: tuple[str, ...] = (VALUES, ALIASES, ADDED, TYPE, NAME, NOTE, MEMBERS)


@dataclasses.dataclass(frozen=True)
class ConflictReport(Convertible):
    """Every reading a build dropped, and every encoding it could not spell.

    A dictionary is collapsed once and read forever, so the judgement it makes
    is written down rather than inferred: a silent drop is a reading nobody can
    find again. `counts` is what a build holds to its baseline.
    """

    collapses: tuple[Collapse, ...] = ()
    collisions: tuple[Collision, ...] = ()

    def counts(self) -> dict[str, int]:
        """`{part: conflict entries}`, with encoding collisions beside them."""
        counted = dict.fromkeys(PARTS, 0)
        for collapse in self.collapses:
            counted[collapse.part] = counted.get(collapse.part, 0) + 1
        counted["encoded"] = len(self.collisions)
        return counted

    def exceeds(self, baseline: Mapping[str, int]) -> list[str]:
        """Which counts grew past `baseline`, as lines; empty when none did."""
        counted = self.counts()
        return [
            f"{part}: {counted.get(part, 0)} conflicts against a baseline of {allowed}"
            for part, allowed in sorted(baseline.items())
            if counted.get(part, 0) > allowed
        ]

    def into_dict(self) -> dict[str, Any]:
        """The report as the committed artifact holds it."""
        return {
            "counts": self.counts(),
            "collapses": [collapse.into_dict() for collapse in self.collapses],
            "collisions": [collision.into_dict() for collision in self.collisions],
        }

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> ConflictReport:
        """Read a committed report back, so a test can hold a build to it."""
        return cls(
            collapses=tuple(Collapse.from_dict(one) for one in mapping.get("collapses") or ()),
            collisions=tuple(Collision.from_dict(one) for one in mapping.get("collisions") or ()),
        )


def collapse(
    order: Sequence[str],
    fields: Mapping[str, Sequence[Field]],
    components: Mapping[str, Sequence[Field]],
) -> tuple[dict[int | str, Field], dict[str, ComponentRecord], ConflictReport]:
    """Per-version declarations as cross-version records, and what that cost.

    The newest *application* version owns each reading -- name, datatype,
    prose, note -- and the enumerated values are the union across versions with
    the newest winning per key, so a value that only ever existed in 4.2 still
    parses. Every disagreement the collapse resolved is in the report; two
    identities claiming one tag or one name are refused outright, because a
    dictionary that ships with them answers differently on two machines.
    """
    readings: dict[int | str, list[tuple[str, Field]]] = {}
    for version in order:
        for member in fields.get(version, ()):
            readings.setdefault(member.fix.key, []).append((version, member))

    collapses: list[Collapse] = []
    collisions: list[Collision] = []
    entries: dict[int | str, Field] = {}
    by_name: dict[str, str] = {}
    for key, found in readings.items():
        found.sort(key=lambda pair: newest_rank(pair[0]))
        record = collapsed_record(
            [member for _, member in found], [version for version, _ in found]
        )
        canonical, folded = record.fix.canonical, record.fix.folded
        collapses.extend(_field_collapses(record, found))
        _, clashing = encodings_of(record.fix.enumerated)
        collisions.extend(
            Collision(
                name=canonical,
                key=spelling,
                values=tuple(owners),
                tag=record.fix.tag,
                sources=tuple(record.fix.source_of(ALIASES, owner) for owner in owners),
            )
            for spelling, owners in sorted(clashing.items())
        )
        held = by_name.get(folded)
        if held is not None:
            raise ValueError(
                f"FIX field name {folded!r} is claimed by {held!r} and {canonical!r}: "
                "one name is one identity, so rename one or record it as an alias"
            )
        by_name[folded] = canonical
        entries[key] = record

    entries = _aliased(entries, collapses, by_name)
    for tag, mapping in State.fix_mapping().items():
        record = entries.get(tag)
        if record is None:
            continue
        declared = {one.value for one in record.fix.enumerated}
        defaults = {code: state for code, state in mapping.items() if code in declared}
        stated = record_copy(record)
        stated.fix.states = {**defaults, **record.fix.states}
        entries[tag] = stated

    component_readings: dict[str, list[tuple[str, Field]]] = {}
    for version in order:
        for declared in components.get(version, ()):
            component_readings.setdefault(slug_of(declared.name), []).append((version, declared))
    component_records: dict[str, ComponentRecord] = {}
    for slug, found in sorted(component_readings.items()):
        found.sort(key=lambda pair: newest_rank(pair[0]))
        component_records[slug] = ComponentRecord.from_components(
            [declared for _, declared in found], [version for version, _ in found]
        )
    # After every record exists, because what a tree still reaches runs through
    # the components it references.
    by_component = {entry.folded: entry for entry in component_records.values()}
    for slug, entry in component_records.items():
        collapses.extend(_component_collapses(entry, component_readings[slug], by_component))
    component_records = _used_in(component_records, by_component)
    return entries, component_records, ConflictReport(tuple(collapses), tuple(collisions))


def _used_in(
    component_records: Mapping[str, ComponentRecord],
    by_component: Mapping[str, ComponentRecord],
) -> dict[str, ComponentRecord]:
    """Every reusable block, told which messages carry it.

    A field record carries `used_in` because the dictionary writes it down for
    a field and nobody writes it down for a component -- so it is derived here
    from the only source that knows: the message trees themselves, and the
    components those reference, however deeply. It is the same question and
    the same answer shape, so a component answers `msgtypes` exactly as a
    field does rather than needing a walk of its own at every call site.

    A message is left alone: it does not carry itself, and no message
    references another.
    """
    used: dict[str, set[str]] = {}
    for entry in component_records.values():
        if not entry.msg_type:
            continue
        for key in component_closure(
            (fold(member.name) for member, _ in walk(entry.declaration) if is_reference(member)),
            by_component,
        ):
            reached = by_component.get(key)
            if reached is not None and not reached.msg_type:
                used.setdefault(reached.folded, set()).add(entry.name)
    built: dict[str, ComponentRecord] = {}
    for slug, entry in component_records.items():
        names = tuple(sorted(used.get(entry.folded, ())))
        if names == entry.msgtypes:
            # Unchanged, and the same object: a caller storing what changed
            # writes nothing for a block whose messages did not move.
            built[slug] = entry
            continue
        declared = record_copy(entry.declaration)
        if names:
            declared.fix.msgtypes = list(names)
        else:
            declared.fix.pop("msgtypes", None)
        built[slug] = dataclasses.replace(entry, declaration=declared)
    return built


def _field_collapses(record: Field, found: Sequence[tuple[str, Field]]) -> list[Collapse]:
    """Every reading of one field the collapse dropped, one entry per part."""
    parts: dict[str, list[tuple[str, str, str]]] = {
        NAME: [(version, member.fix.source_of(NAME), member.name) for version, member in found],
        ADDED: [
            (version, member.fix.source_of(ADDED), str(member.fix.get("added") or ""))
            for version, member in found
            if member.fix.added
        ],
        TYPE: [
            (version, member.fix.source_of(TYPE), str(member.fix.get("type") or ""))
            for version, member in found
        ],
        NOTE: [
            (version, member.fix.source_of(NOTE), str(member.fix.get("note") or ""))
            for version, member in found
        ],
    }
    collapses = [
        one
        for part, readings in parts.items()
        if (one := _collapsed(record, part, readings)) is not None
    ]
    for part, reading_of in ((VALUES, _meaning_of), (ALIASES, _alias_of)):
        keyed: dict[str, list[tuple[str, str, str]]] = {}
        for version, member in found:
            for one in member.fix.enumerated:
                reading = reading_of(one)
                if reading:
                    keyed.setdefault(one.value, []).append(
                        (version, member.fix.source_of(part, one.value), reading)
                    )
        grouped: dict[tuple[str, str], list[Dropped]] = {}
        for value, readings in sorted(keyed.items()):
            kept_version, kept_source, kept = readings[-1]
            dropped = [
                Dropped(version, reading, value, source)
                for version, source, reading in readings
                if reading != kept
            ]
            if dropped:
                grouped.setdefault((kept_version, kept_source), []).extend(dropped)
        fix = record.fix
        for (kept_version, kept_source), dropped in grouped.items():
            collapses.append(
                Collapse(
                    fix.canonical,
                    part,
                    kept_version,
                    tuple(dropped),
                    fix.tag,
                    kept_source,
                )
            )
    return collapses


def _meaning_of(one: FixFieldValue) -> str:
    """The prose one value carries, for the collapse report."""
    return one.meaning


def _alias_of(one: FixFieldValue) -> str:
    """The spelling that leads one value's aliases, for the collapse report."""
    return one.aliases[0] if one.aliases else ""


def _collapsed(
    record: Field, part: str, readings: Sequence[tuple[str, str, str]]
) -> Collapse | None:
    """One part of a reading, as a collapse when the versions did not agree."""
    if not readings:
        return None
    kept_version, kept_source, kept = readings[-1]
    dropped = tuple(
        Dropped(version, reading, source=source)
        for version, source, reading in readings
        if reading and not _same_reading(part, reading, kept)
    )
    return (
        Collapse(record.fix.canonical, part, kept_version, dropped, record.fix.tag, kept_source)
        if dropped
        else None
    )


def _same_reading(part: str, left: str, right: str) -> bool:
    """Whether two readings state the same contract fact."""
    if part != TYPE:
        return left == right
    # FIX `char` constrains one string character; it does not change the wire
    # or Arrow type, so sources may use either spelling without a conflict.
    return datatype_identity(left) == datatype_identity(right)


def component_closure(wanted: Iterable[str], by_name: Mapping[str, ComponentRecord]) -> set[str]:
    """`wanted`, plus every component their trees reference, however deeply."""
    found: set[str] = set()
    pending = list(wanted)
    while pending:
        key = pending.pop()
        if key in found:
            continue
        found.add(key)
        entry = by_name.get(key)
        if entry is None:
            continue
        pending.extend(
            fold(member.name) for member, _ in walk(entry.declaration) if is_reference(member)
        )
    return found


def _component_collapses(
    entry: ComponentRecord,
    found: Sequence[tuple[str, Field]],
    by_name: Mapping[str, ComponentRecord],
) -> list[Collapse]:
    """Members an older version declared that the newest tree no longer reaches.

    Reaches, not names: a member the newest tree moved into a referenced
    component is still read, and reporting it as dropped would send a reader
    looking for a loss that is not there.
    """
    kept = _reachable(entry.declaration, by_name)
    dropped = tuple(
        Dropped(version, name, source=declared.fix.source)
        for version, declared in found
        for name in sorted(_reachable(declared, by_name) - kept)
    )
    return (
        [
            Collapse(
                entry.name,
                MEMBERS,
                entry.newest,
                dropped,
                keptsource=entry.declaration.fix.source,
            )
        ]
        if dropped
        else []
    )


def _reachable(declared: Field, by_name: Mapping[str, ComponentRecord]) -> set[str]:
    """Every member name one tree reads, following the components it references."""
    found = {member.name for member, _ in walk(declared)}
    for key in component_closure(
        (fold(member.name) for member, _ in walk(declared) if is_reference(member)),
        by_name,
    ):
        entry = by_name.get(key)
        if entry is not None:
            found.update(member.name for member, _ in walk(entry.declaration))
    return found


def _aliased(
    entries: Mapping[int | str, Field],
    collapses: Sequence[Collapse],
    by_name: Mapping[str, str],
) -> dict[int | str, Field]:
    """Records with every dropped spelling recorded as an alias that can resolve.

    A rename is a collapse like any other -- tag 64 is `FutSettDate` through
    4.3 and `SettlDate` after -- but the older spelling is a name real traffic
    still carries, so it is kept as an alias with the version that spelled it.
    One that another identity already claims as its canonical name cannot be,
    and stays in the report as the dropped reading it is.
    """
    dropped: dict[str, list[Dropped]] = {}
    for collapse in collapses:
        if collapse.part == NAME:
            dropped.setdefault(fold(collapse.name), []).extend(collapse.dropped)
    if not dropped:
        return dict(entries)
    aliased: dict[int | str, Field] = {}
    for key, record in entries.items():
        aliases = record.fix.named_aliases
        spellings = dropped.get(record.fix.folded, ())
        held = {alias.folded for alias in aliases}
        fresh = []
        for one in spellings:
            folded = fold(one.reading)
            if folded in held or folded in by_name:
                continue
            held.add(folded)
            fresh.append(Alias(name=one.reading, source=one.version))
        if not fresh:
            aliased[key] = record
            continue
        built = record_copy(record)
        built.fix.named_aliases = (*aliases, *fresh)
        aliased[key] = built
    return aliased


# -- a whole store, ready to write --------------------------------------------


def repeating_groups_of(
    component_records: Mapping[str, ComponentRecord],
) -> dict[str, ComponentRecord]:
    """Repeating-group records derived from component trees, keyed by slug.

    The newest declaration owns the entry shape and every declaring version
    is retained. Component order breaks a same-version tie, so publishing the
    same trees always writes the same record.
    """
    found: dict[str, ComponentRecord] = {}
    for component_slug in sorted(component_records):
        owner = component_records[component_slug]
        for member, _ in walk(owner.declaration):
            if not is_group(member):
                continue
            slug = slug_of(member.name)
            held = found.get(slug)
            if held is not None and held.name != member.name:
                raise ValueError(
                    f"FIX repeating groups {held.name!r} and {member.name!r} share {slug!r}"
                )
            versions = canonical_versions(
                (*(held.versions if held is not None else ()), *owner.versions)
            )
            declaration = member
            if held is not None and newest_rank(held.newest) >= newest_rank(owner.newest):
                declaration = held.declaration
            found[slug] = ComponentRecord(member.name, versions, declaration)
    return found


def documents_of(
    versions: Sequence[str],
    field_records: Mapping[int | str, Field],
    component_records: Mapping[str, ComponentRecord],
    sessions: Mapping[str, Sequence[tuple[str, bool]]],
    declared: Iterable[str] = (),
    namespace_records: Mapping[str, Mapping[int | str, Field]] | None = None,
    sources: Sequence[Mapping[str, Any]] = (),
) -> dict[str, dict[str, Any]]:
    """A whole store as `{document name: document}`, ready to write.

    `declared` names the versions whose components have been read, however few
    each has: a version missing from it answers "nobody asked" rather than
    "this version declares none".
    """
    documents: dict[str, dict[str, Any]] = {}
    index: dict[str, Any] = {}
    if versions:
        index["versions"] = list(versions)
    stored = {
        version: [[name, required] for name, required in session]
        for version, session in sorted(sessions.items())
        if session
    }
    if stored:
        index[SESSIONS] = stored
    if versions:
        index[STORED] = sorted(versions)
    if declared:
        index[DECLARED] = sorted(declared)
    if index:
        documents[VERSIONS_FILE] = {key: index[key] for key in sorted(index)}
    shards: dict[str, dict[int | str, Field]] = {}
    for record in field_records.values():
        name = field_document(record.fix.key)
        shards.setdefault(name, {})[record.fix.key] = record
    for name, shard in shards.items():
        documents[name] = _shard_document(shard)
    for namespace, records in sorted((namespace_records or {}).items()):
        namespaced: dict[str, dict[int | str, Field]] = {}
        for record in records.values():
            name = field_document(record.fix.key, namespace)
            namespaced.setdefault(name, {})[record.fix.key] = record
        for name, shard in namespaced.items():
            documents[name] = _shard_document(shard)
    for slug, entry in component_records.items():
        documents[f"{COMPONENTS}/{slug}{DOCUMENT_SUFFIX}"] = component_record_document(entry)
    for slug, entry in repeating_groups_of(component_records).items():
        documents[f"{REPGROUP}/{slug}{DOCUMENT_SUFFIX}"] = component_record_document(entry)
    if sources:
        ordered = sorted(
            (dict(source) for source in sources),
            key=lambda source: (
                int(source.get("priority", 0)),
                str(source.get("namespace", "standard")),
                str(source.get("source_id", "")),
                str(source.get("version", "")),
            ),
        )
        documents[SOURCES_FILE] = {"sources": ordered}
    return documents


def write_archive(
    target: str | os.PathLike[str], documents: Mapping[str, Mapping[str, Any]]
) -> pathlib.Path | str:
    """Write a whole store into one archive, and name what was written.

    Deterministic: the members are written in name order and stamped at the
    start of zip time on no host, so publishing the same dictionary twice is
    the same bytes and "nothing changed" looks like nothing changed.
    """
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(documents):
            archive.writestr(archive_member(name), document_text(documents[name]))
    write_bytes(output.getvalue(), target)
    parsed = Url.from_string(os.fspath(target))
    return pathlib.Path(parsed.store_path) if parsed.scheme in LOCAL else parsed.into_string()


def slug_collisions(names: Iterable[str]) -> dict[str, list[str]]:
    """`{slug: the names that share it}` -- empty when every identity is its own file."""
    found: dict[str, list[str]] = {}
    for name in names:
        found.setdefault(slug_of(name), []).append(name)
    return {slug: shared for slug, shared in found.items() if len(shared) > 1}
