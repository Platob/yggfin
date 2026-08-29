"""Where a FIX dictionary is kept: tag-range shards of fields, and components.

One layout. Fields live in shards of five hundred tags under `fields/`, named
by the shard index, so the file holding a tag is arithmetic -- no index, no
lookup table, no scan -- and a single-tag lookup reads one document rather than
the dictionary. The tag space is sparse (nothing between 2999 and 40000), and
an empty shard is simply absent: fourteen files answer for six thousand fields.
Fields FIX never numbered have no tag to shard on and share `fields/named.json`.

Components stay one document per identity under `components/`, because they are
keyed by name and there is no arithmetic to do.

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
from rekep.fix.quickfix import is_reference, walk
from rekep.require import require
from rekep.urls import LOCAL, Url

#: What the layout calls its two folders. Named here because the reader, the
#: writer and the tests must all spell them alike.
FIELDS = "fields"
COMPONENTS = "components"

#: What a stored document may be named, the one a store is written in first.
#: JSON, and measured: the dictionary is nearly a thousand documents and every
#: process that imports this package parses a projection of it, where
#: pure-Python YAML costs 25 seconds against a tenth of one for JSON. A store
#: somebody wrote in YAML still reads, and converts itself the first time
#: anything rewrites it -- the sibling under the other suffix is dropped with
#: that write, so one identity never sits in a store twice.
DOCUMENT_SUFFIXES: tuple[str, ...] = (".json", ".yaml")
DOCUMENT_SUFFIX = DOCUMENT_SUFFIXES[0]


#: Where the layout keeps what belongs to no single identity: the version
#: list, each version's session layer, and which versions have had their
#: components read at all.
VERSIONS_FILE = f"versions{DOCUMENT_SUFFIX}"
SESSIONS = "sessions"
STORED = "stored"
DECLARED = "declared"

#: How many tags one shard holds, and where the fields FIX never numbered go.
#: Five hundred: wide enough that the populated ranges are fourteen files
#: rather than a hundred, narrow enough that a single-tag lookup parses a few
#: hundred records instead of six thousand.
SHARD_SPAN = 500
NAMED_FILE = f"{FIELDS}/named{DOCUMENT_SUFFIX}"


class Documents(Protocol):
    """Reading and writing named JSON documents, wherever they are kept.

    The whole of what the layout needs from a place. `FixRegistry` owns one of
    these; a directory and a zip are two implementations of it, and the layout
    below never knows which it has.
    """

    def read(self, name: str) -> dict[str, Any] | None:
        """One document, or None when the store does not hold it."""
        ...

    def write(self, name: str, payload: Mapping[str, Any]) -> None:
        """Keep one document, replacing whatever was under that name."""
        ...

    def remove(self, name: str) -> bool:
        """Drop one document; False when there was none."""
        ...

    def names(self) -> tuple[str, ...]:
        """Every document this store holds, as posix-style relative names."""
        ...

    def stamp(self, name: str) -> float:
        """When one document was last written, in seconds since the epoch.

        Zero for a document the place cannot date, which is how an age check
        reads "I cannot tell" -- and a TTL that cannot tell refetches rather
        than assuming the copy is fresh.
        """
        ...

    def read_many(self, prefix: str) -> dict[str, dict[str, Any]]:
        """Every document under `prefix`, in one pass over the place.

        "The fields of 4.4" is a question about every identity, so the shards
        are read together; a place that can answer many documents more cheaply
        at once than one at a time says so here.
        """
        ...

    def write_all(self, documents: Mapping[str, Mapping[str, Any]]) -> None:
        """Replace this store with the complete document mapping."""
        ...


def is_document(name: str) -> bool:
    """Whether `name` is a stored document, whichever format it is written in."""
    return name.endswith(DOCUMENT_SUFFIXES)


def document_stem(name: str) -> str:
    """`fields/party_role.json` -> `fields/party_role`; the name without a format."""
    for suffix in DOCUMENT_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def document_names(name: str) -> tuple[str, ...]:
    """`name`, then the same document under every other format, in read order."""
    stem = document_stem(name)
    return tuple(f"{stem}{suffix}" for suffix in DOCUMENT_SUFFIXES)


def document_text(payload: Mapping[str, Any], name: str = DOCUMENT_SUFFIX) -> str:
    """One stored document's text. The one place the on-disk spelling is decided."""
    if name.endswith(".yaml"):
        return _yaml().safe_dump(dict(payload), sort_keys=False, allow_unicode=True)
    return json.dumps(payload, indent=1)


def document_of(payload: bytes, name: str) -> Any:
    """One stored document's text read back, by the format its name names.

    A torn document raises `ValueError` whichever format it is in, because
    that is the one thing every caller here already handles: a half-written
    file is a cold cache, not a dead registry.
    """
    if not name.endswith(".yaml"):
        return json.loads(payload.decode("utf-8"))
    yaml = _yaml()
    try:
        return yaml.safe_load(payload.decode("utf-8"))
    except yaml.YAMLError as error:
        raise ValueError(f"{name} is not a document this store can read: {error}") from error


def _yaml() -> Any:
    """The YAML reader, which only a store somebody wrote in YAML needs."""
    return require("yaml", "yaml")


# -- the two places ----------------------------------------------------------


@dataclasses.dataclass(eq=False)
class DirectoryDocuments:
    """Documents under a directory, on any Arrow filesystem."""

    filesystem: pyarrow.fs.FileSystem
    directory: str

    def read(self, name: str) -> dict[str, Any] | None:
        """One document, or None for anything that cannot be read as one."""
        for spelling in document_names(name):
            found = self._read_one(spelling)
            if found is not None:
                return found
        return None

    def _read_one(self, name: str) -> dict[str, Any] | None:
        try:
            with self.filesystem.open_input_stream(self._path(name)) as stream:
                return document_of(stream.read(), name)
        except FileNotFoundError:
            return None
        except (OSError, ValueError, pyarrow.ArrowException):
            # A torn write or someone else's file: write over it rather than
            # refuse to run offline forever.
            return None

    def write(self, name: str, payload: Mapping[str, Any]) -> None:
        """Written beside, then renamed, so a reader never sees half a file."""
        path = self._path(name)
        self.filesystem.create_dir(posixpath.dirname(path), recursive=True)
        scratch = f"{path}.tmp"
        with self.filesystem.open_output_stream(scratch) as stream:
            stream.write(document_text(payload, name).encode())
        self.filesystem.move(scratch, path)
        for stale in document_names(name):
            if stale != name:
                self._remove_one(stale)

    def remove(self, name: str) -> bool:
        """Delete one document, whichever format holds it; False when absent."""
        return any(self._remove_one(spelling) for spelling in document_names(name))

    def _remove_one(self, name: str) -> bool:
        try:
            self.filesystem.delete_file(self._path(name))
        except (FileNotFoundError, OSError):
            return False
        return True

    def names(self) -> tuple[str, ...]:
        """Every document under the directory, folders included."""
        selector = pyarrow.fs.FileSelector(self.directory, recursive=True, allow_not_found=True)
        prefix = self.directory.rstrip("/") + "/"
        found = []
        for info in self.filesystem.get_file_info(selector):
            if info.type != pyarrow.fs.FileType.File or not is_document(info.path):
                continue
            path = info.path
            found.append(path[len(prefix) :] if path.startswith(prefix) else path)
        return tuple(sorted(found))

    def read_many(self, prefix: str) -> dict[str, dict[str, Any]]:
        """Every document under `prefix`, one file open each.

        `_read_one` and not `read`: these names came off the directory, so each
        already spells the format it is in and probing the others is a failed
        open per document.
        """
        return {
            name: document
            for name in self.names()
            if name.startswith(prefix) and (document := self._read_one(name)) is not None
        }

    def write_all(self, documents: Mapping[str, Mapping[str, Any]]) -> None:
        """Replace every registry document, dropping identities no longer declared."""
        kept = {document_stem(name) for name in documents}
        stale = [name for name in self.names() if document_stem(name) not in kept]
        for name, document in documents.items():
            self.write(name, document)
        for name in stale:
            self.remove(name)

    def stamp(self, name: str) -> float:
        """When one document was last written, off the filesystem itself."""
        try:
            (info,) = self.filesystem.get_file_info([self._path(name)])
        except (OSError, pyarrow.ArrowException):
            return 0.0
        if info.type != pyarrow.fs.FileType.File or info.mtime is None:
            return 0.0
        return info.mtime.timestamp()

    def _path(self, name: str) -> str:
        return posixpath.join(self.directory, name)


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

    def read(self, name: str) -> dict[str, Any] | None:
        """One member, as the document it holds; None when it is not there."""
        held = self._members()
        spelling = next((one for one in document_names(name) if one in held), None)
        if spelling is None:
            return None
        try:
            with zipfile.ZipFile(self.archive) as opened:
                return document_of(opened.read(held[spelling]), spelling)
        except (OSError, ValueError, zipfile.BadZipFile):
            # A torn archive is a cold cache, not a dead registry.
            return None

    def write(self, name: str, payload: Mapping[str, Any]) -> None:
        """Put one member in, replacing what was there under any format."""
        stale = tuple(one for one in document_names(name) if one != name)
        self._rewrite({name: document_text(payload, name)}, drop=stale)

    def remove(self, name: str) -> bool:
        """Drop one member; False when the archive did not hold it."""
        held = self._members()
        stale = tuple(one for one in document_names(name) if one in held)
        if not stale:
            return False
        self._rewrite({}, drop=stale)
        return True

    def names(self) -> tuple[str, ...]:
        """Every member, under the name the layout addresses it by."""
        return tuple(sorted(self._members()))

    def read_many(self, prefix: str) -> dict[str, dict[str, Any]]:
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

    def stamp(self, name: str) -> float:
        """When the archive holding this member was last written.

        The archive's own time and not the member's: a zip stamps every member
        at the start of zip time here, deliberately, so that publishing the
        same dictionary twice is the same bytes. The file is what has an age.
        """
        if name not in self._members():
            return 0.0
        try:
            return pathlib.Path(self.archive).stat().st_mtime
        except OSError:
            return 0.0

    def write_all(self, documents: Mapping[str, Mapping[str, Any]]) -> None:
        """Replace the whole archive with `documents`, in one deterministic pass.

        Writing a member at a time rebuilds the zip once per member, which for
        a dictionary of two thousand identities is two thousand rebuilds. A
        store being filled says so and pays for one.
        """
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as fresh:
            for name in sorted(documents):
                fresh.writestr(archive_member(name), document_text(documents[name], name))
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

    def _rewrite(self, put: Mapping[str, str], drop: Sequence[str]) -> None:
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
    if is_document(folder) or folder in (FIELDS, COMPONENTS):
        return ""
    return f"{folder}/"


# -- the layout ---------------------------------------------------------------


def shard_name(tag: int) -> str:
    """The document holding one tag: `fields/000000.json` for tags 0 to 499.

    `tag // SHARD_SPAN`, zero-padded, and that is the whole mapping: no index
    in `versions.json`, no lookup table, no scan.
    """
    return f"{FIELDS}/{int(tag) // SHARD_SPAN:06d}{DOCUMENT_SUFFIX}"


@dataclasses.dataclass(eq=False)
class ShardedLayout:
    """A FIX dictionary as tag-range shards, cross-version records inside them.

    Each shard is `{"<tag>": {record}}` in numeric tag order, and is read and
    held whole -- a tag lookup parses the few hundred records that share its
    range, never the dictionary. Questions about a whole version read every
    shard, once, and hold those too.
    """

    documents: Documents

    # -- what belongs to no identity -----------------------------------------

    def versions(self) -> tuple[str, ...]:
        """The version list the store holds; empty when it holds none."""
        return tuple(str(version) for version in self._index().get("versions", ()))

    def store_versions(self, versions: Sequence[str]) -> None:
        """Keep the version list, so the front page is fetched once."""
        self._store_index({**self._index(), "versions": list(versions)})

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
        return self._shard(shard_name(int(tag))).get(int(tag))

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
        return {
            _record_key(key): refuse_record(Field.from_dict(record))
            for key, record in document.items()
        }

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
                document_stem(name[len(prefix) :]): ComponentRecord.from_dict(document)
                for name, document in sorted(readable.items())
            }
        return held

    def forget(self) -> None:
        """Drop the held records, so the next read sees what was just written."""
        self.__dict__.pop("_shards", None)
        self.__dict__.pop("_fields", None)
        self.__dict__.pop("_components", None)
        self.__dict__.pop("_torn", None)

    @property
    def torn(self) -> tuple[str, ...]:
        """Documents this store holds and cannot read; empty when it is sound.

        A torn write costs one shard here, which is worse than one field and
        far better than a whole version answering short in silence. The
        registry treats a torn store as one to write again.
        """
        self.field_records, self.component_records  # noqa: B018 - both are read once
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
            entry.into_component() for _, entry in sorted(held.items()) if entry.folded in wanted
        ]

    # -- writing -------------------------------------------------------------

    def store_field(self, record: Field) -> str:
        """Write one field record, and name the document it landed in."""
        name = field_document(record)
        shard = self._shard(name)
        shard[record.fix.key] = record
        self.documents.write(name, _shard_document(shard))
        self.__dict__.pop("_fields", None)
        return name

    def remove_field(self, key: int | str) -> bool:
        """Delete one field record by tag or folded name; False when absent."""
        name = shard_name(key) if isinstance(key, int) else NAMED_FILE
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
        self.documents.write(f"{COMPONENTS}/{entry.slug}{DOCUMENT_SUFFIX}", entry.into_dict())
        self.component_records[entry.slug] = entry

    def remove_component(self, slug: str) -> bool:
        """Delete one component identity; False when the store did not hold it."""
        self.component_records.pop(slug, None)
        return self.documents.remove(f"{COMPONENTS}/{slug}{DOCUMENT_SUFFIX}")

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
            key = _field_key(member)
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
            self.store_component(fold_component(self.component_records.get(slug), found, version))
        for slug, entry in list(self.component_records.items()):
            if entry.name in declared or version not in entry.versions:
                continue
            # The version declares components and not this one, so this
            # version's declaration of it is gone rather than merely unstated.
            remaining = tuple(one for one in entry.versions if one != version)
            if remaining:
                self.store_component(dataclasses.replace(entry, versions=remaining))
            else:
                self.remove_component(slug)
        self._store_carriage()

    def _store_carriage(self) -> None:
        """Tell every block which messages carry it, once the trees settled.

        The same derivation a bulk collapse makes, run where a scrape folds
        one version at a time -- otherwise a store built a version at a time
        would answer `msgtypes` differently from one built in a single pass.
        Only what changed is written, so this costs nothing on a version whose
        messages reach the same blocks.
        """
        held = self.component_records
        for slug, entry in _used_in(held, {one.folded: one for one in held.values()}).items():
            if entry is not held[slug]:
                self.store_component(entry)


def field_document(record: Field) -> str:
    """The document one field record belongs in: its shard, or `named.json`."""
    tag = record.fix.tag
    return shard_name(tag) if tag is not None else NAMED_FILE


def _record_key(stored: str) -> int | str:
    """One shard key read back: a tag where it is one, a folded name otherwise."""
    return int(stored) if str(stored).isdigit() else fold(stored)


def _field_key(member: Field) -> int | str:
    """What makes two readings of a field the same field: its tag, else its name.

    Its tag, when it has one: a version may rename a tag -- 64 is
    `FutSettDate` through 4.3 and `SettlDate` after -- and a store that keyed
    on the name would hold two records for one field, each half its history.
    """
    tag = member.fix.get("tag")
    return int(tag) if tag else fold(member.name)


def _shard_document(shard: Mapping[int | str, Field]) -> dict[str, Any]:
    """One shard as it is written: numeric tag order, then the names.

    Keyed by the tag, or by the *canonical* name for a field FIX never
    numbered -- the key is what a person reads first, and folding it there
    would spell `AMON.isincode` in a case nothing else in the document uses.
    """
    tags = sorted(key for key in shard if isinstance(key, int))
    names = sorted(key for key in shard if not isinstance(key, int))
    return {
        (str(key) if isinstance(key, int) else shard[key].fix.canonical): shard[key].into_dict()
        for key in (*tags, *names)
    }


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
            readings.setdefault(_field_key(member), []).append((version, member))

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
        if reading and reading != kept
    )
    return (
        Collapse(record.fix.canonical, part, kept_version, dropped, record.fix.tag, kept_source)
        if dropped
        else None
    )


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


def documents_of(
    versions: Sequence[str],
    field_records: Mapping[int | str, Field],
    component_records: Mapping[str, ComponentRecord],
    sessions: Mapping[str, Sequence[tuple[str, bool]]],
    declared: Iterable[str] = (),
    suffix: str = DOCUMENT_SUFFIX,
) -> dict[str, dict[str, Any]]:
    """A whole store as `{document name: document}`, ready to write.

    `declared` names the versions whose components have been read, however few
    each has: a version missing from it answers "nobody asked" rather than
    "this version declares none".

    `suffix` is what the documents are named -- and so what they are written
    in, since `document_text` reads the name.
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
        documents[f"{document_stem(VERSIONS_FILE)}{suffix}"] = {
            key: index[key] for key in sorted(index)
        }
    shards: dict[str, dict[int | str, Field]] = {}
    for record in field_records.values():
        name = document_stem(field_document(record)) + suffix
        shards.setdefault(name, {})[record.fix.key] = record
    for name, shard in shards.items():
        documents[name] = _shard_document(shard)
    for slug, entry in component_records.items():
        documents[f"{COMPONENTS}/{slug}{suffix}"] = entry.into_dict()
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
            archive.writestr(archive_member(name), document_text(documents[name], name))
    write_bytes(output.getvalue(), target)
    parsed = Url.from_string(os.fspath(target))
    return pathlib.Path(parsed.store_path) if parsed.scheme in LOCAL else parsed.into_string()


def slug_collisions(names: Iterable[str]) -> dict[str, list[str]]:
    """`{slug: the names that share it}` -- empty when every identity is its own file."""
    found: dict[str, list[str]] = {}
    for name in names:
        found.setdefault(slug_of(name), []).append(name)
    return {slug: shared for slug, shared in found.items() if len(shared) > 1}
