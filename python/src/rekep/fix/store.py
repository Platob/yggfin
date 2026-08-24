"""Where a FIX dictionary is kept, in either of the two layouts it is kept in.

Two layouts, one interface. The **versioned** one is a file per FIX version
holding every field that version declares; it is what every store written
before this module holds, including a user's own `~/.config/fix`. The
**exploded** one is a file per field or component *identity* under `fields/`
and `components/`; it is what the published dictionary and the wheel now ship,
because one identity per file makes "how does this differ across versions" a
single-file diff and adding a field one small reviewable edit.

Which one a store is is read off what it holds, so both keep working and
neither has to be declared. A cold store is written exploded.

Both spellings live on a directory or inside a zip -- the extension decides,
exactly as it did before -- and neither ever reaches the network.
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

from rekep.fields import Field
from rekep.filesystems import write_bytes
from rekep.fix.entries import ANY_VERSION, ComponentEntry, FieldEntry, slug_of, variant_of
from rekep.fix.quickfix import SpecComponent
from rekep.urls import LOCAL, Url

#: What the exploded layout calls its two folders. Named here because the
#: reader, the writer, the migration and the tests must all spell them alike.
FIELDS = "fields"
COMPONENTS = "components"

#: Where the exploded layout keeps what belongs to no single identity: the
#: version list, each version's session layer, and which versions have had
#: their components read at all.
VERSIONS_FILE = "versions.json"
SESSIONS = "sessions"
STORED = "stored"
DECLARED = "declared"

VERSIONED = "versioned"
EXPLODED = "exploded"
LAYOUTS: tuple[str, ...] = (VERSIONED, EXPLODED)


class Documents(Protocol):
    """Reading and writing named JSON documents, wherever they are kept.

    The whole of what a layout needs from a place. `FixRegistry` owns one of
    these; a directory and a zip are two implementations of it, and neither
    layout below knows which it has.
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

        The exploded layout reads a folder whole or not at all -- "the fields
        of 4.4" is a question about every identity -- and a place that can
        answer two thousand documents more cheaply together than one at a time
        says so here.
        """
        ...


def document_text(payload: Mapping[str, Any]) -> str:
    """One stored document's text. The one place the on-disk spelling is decided."""
    return json.dumps(payload, indent=1)


# -- the two places ----------------------------------------------------------


@dataclasses.dataclass(eq=False)
class DirectoryDocuments:
    """JSON under a directory, on any Arrow filesystem."""

    filesystem: pyarrow.fs.FileSystem
    directory: str

    def read(self, name: str) -> dict[str, Any] | None:
        """One document, or None for anything that cannot be read as one."""
        try:
            with self.filesystem.open_input_stream(self._path(name)) as stream:
                return json.loads(stream.read().decode("utf-8"))
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
            stream.write(document_text(payload).encode())
        self.filesystem.move(scratch, path)

    def remove(self, name: str) -> bool:
        """Delete one document; False when the store did not hold it."""
        try:
            self.filesystem.delete_file(self._path(name))
        except (FileNotFoundError, OSError):
            return False
        return True

    def names(self) -> tuple[str, ...]:
        """Every JSON document under the directory, folders included."""
        selector = pyarrow.fs.FileSelector(self.directory, recursive=True, allow_not_found=True)
        prefix = self.directory.rstrip("/") + "/"
        found = []
        for info in self.filesystem.get_file_info(selector):
            if info.type != pyarrow.fs.FileType.File or not info.path.endswith(".json"):
                continue
            path = info.path
            found.append(path[len(prefix) :] if path.startswith(prefix) else path)
        return tuple(sorted(found))

    def read_many(self, prefix: str) -> dict[str, dict[str, Any]]:
        """Every document under `prefix`, one file open each."""
        return {
            name: document
            for name in self.names()
            if name.startswith(prefix) and (document := self.read(name)) is not None
        }

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
    """JSON inside one zip, which is a store and not only a way to publish one.

    A `zip -r fix.zip fix/` archive prefixes every member with its folder, so
    what a name means is resolved against what the archive already holds rather
    than assumed to sit at the root.
    """

    archive: str | os.PathLike[str]
    #: Called after a write, to copy a localized archive back where it came from.
    synchronise: Any = None

    def read(self, name: str) -> dict[str, Any] | None:
        """One member, as the document it holds; None when it is not there."""
        member = self._members().get(name)
        if member is None:
            return None
        try:
            with zipfile.ZipFile(self.archive) as opened:
                return json.loads(opened.read(member).decode("utf-8"))
        except (OSError, ValueError, zipfile.BadZipFile):
            # A torn archive is a cold cache, not a dead registry.
            return None

    def write(self, name: str, payload: Mapping[str, Any]) -> None:
        """Put one member in, replacing what was there."""
        self._rewrite({name: document_text(payload)}, drop=())

    def remove(self, name: str) -> bool:
        """Drop one member; False when the archive did not hold it."""
        if name not in self._members():
            return False
        self._rewrite({}, drop=(name,))
        return True

    def names(self) -> tuple[str, ...]:
        """Every JSON member, under the name the layout addresses it by."""
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
                        found[name] = json.loads(opened.read(member).decode("utf-8"))
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
            if not member.endswith(".json"):
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
    never merely the first one's folder, which for an exploded store is
    `fields/` and made the next write land in `fields/fields/`.
    """
    leading = {name.split("/", 1)[0] for name in members if name.endswith(".json")}
    if len(leading) != 1:
        return ""
    (folder,) = leading
    if folder.endswith(".json") or folder in (FIELDS, COMPONENTS):
        return ""
    return f"{folder}/"


# -- the two layouts ---------------------------------------------------------


@dataclasses.dataclass(eq=False)
class VersionedLayout:
    """A file per FIX version, each listing every field that version declares.

    What every store written before the exploded layout holds. Read, never
    written to: a store in this shape keeps answering, and `migrate` is how it
    becomes the other one.
    """

    documents: Documents

    layout: str = VERSIONED

    def versions(self) -> tuple[str, ...]:
        """The version list the store holds; empty when it holds none."""
        stored = self.documents.read(VERSIONS_FILE)
        return tuple(str(version) for version in stored["versions"]) if stored else ()

    def store_versions(self, versions: Sequence[str]) -> None:
        """Keep the version list, so the front page is fetched once."""
        self.documents.write(VERSIONS_FILE, {"versions": list(versions)})

    def spellings(self) -> tuple[str, ...]:
        """Every version this store has fields for, spelled as it stored them."""
        return tuple(
            sorted(
                name[: -len(".json")]
                for name in self.documents.names()
                if "/" not in name and name != VERSIONS_FILE
            )
        )

    def fields(self, version: str) -> list[Field] | None:
        """One version's fields; None when the store does not hold that version."""
        stored = self.documents.read(f"{version}.json")
        if stored is None:
            return None
        return [Field.from_dict(member) for member in stored["fields"]]

    def components(self, version: str) -> list[SpecComponent] | None:
        """Stored component declarations; None means this store predates them."""
        stored = self.documents.read(f"{version}.json")
        if stored is None or "components" not in stored:
            return None
        return [SpecComponent.from_dict(member) for member in stored["components"]]

    def session(self, version: str) -> tuple[tuple[str, bool], ...]:
        """`((name, required), ...)`: the standard header, then the trailer."""
        stored = self.documents.read(f"{version}.json")
        if not stored:
            return ()
        return tuple((str(name), bool(required)) for name, required in stored.get("session", ()))

    def store(
        self,
        version: str,
        fields: Sequence[Field],
        session: Sequence[tuple[str, bool]] = (),
        components: Sequence[SpecComponent] | None = None,
        url: str = "",
    ) -> None:
        """Keep one version's fields and optional spec declarations."""
        payload: dict[str, Any] = {
            "version": version,
            "url": url,
            "fields": [member.into_dict() for member in fields],
        }
        if session:
            payload["session"] = [[name, required] for name, required in session]
        if components is not None:
            payload["components"] = [member.into_dict() for member in components]
        self.documents.write(f"{version}.json", payload)


@dataclasses.dataclass(eq=False)
class ExplodedLayout:
    """A file per field or component identity, under `fields/` and `components/`.

    Every read walks the whole of one folder, so it is walked once and held:
    "the fields of 4.4" is a question about every identity, not about one file,
    and answering it per call would read two thousand documents per version.
    """

    documents: Documents

    layout: str = EXPLODED

    def versions(self) -> tuple[str, ...]:
        """The version list the store holds; empty when it holds none."""
        stored = self._index()
        return tuple(str(version) for version in stored.get("versions", ()))

    def store_versions(self, versions: Sequence[str]) -> None:
        """Keep the version list, so the front page is fetched once."""
        self._store_index({**self._index(), "versions": list(versions)})

    def _index(self) -> dict[str, Any]:
        """`versions.json`: everything about a version that is not an identity."""
        return self.documents.read(VERSIONS_FILE) or {}

    def _store_index(self, payload: Mapping[str, Any]) -> None:
        self.documents.write(VERSIONS_FILE, {key: payload[key] for key in sorted(payload)})

    @property
    def field_entries(self) -> dict[str, FieldEntry]:
        """`{slug: entry}` for every field identity, read once and held."""
        held = self.__dict__.get("_fields")
        if held is None:
            held = self.__dict__["_fields"] = self._read(FIELDS, FieldEntry)
        return held

    @property
    def slugs(self) -> dict[tuple[str, Any], str]:
        """`{identity: the file it is stored in}`, so a rename can find it."""
        return {entry_identity(entry): slug for slug, entry in self.field_entries.items()}

    @property
    def component_entries(self) -> dict[str, ComponentEntry]:
        """`{slug: entry}` for every component identity, read once and held."""
        held = self.__dict__.get("_components")
        if held is None:
            held = self.__dict__["_components"] = self._read(COMPONENTS, ComponentEntry)
        return held

    def forget(self) -> None:
        """Drop the held entries, so the next read sees what was just written."""
        self.__dict__.pop("_fields", None)
        self.__dict__.pop("_components", None)
        self.__dict__.pop("_torn", None)

    def spellings(self) -> tuple[str, ...]:
        """Every version any identity is declared for, `sessions.json` included.

        The wildcard a namespaced field carries is not a version and never appears
        here: it means "whichever version this store already has".
        """
        found = {
            version
            for entry in (*self.field_entries.values(), *self.component_entries.values())
            for version in entry.versions
        }
        index = self._index()
        found.update(index.get(SESSIONS, {}))
        found.update(index.get(STORED, ()))
        found.update(index.get(DECLARED, ()))
        found.discard(ANY_VERSION)
        return tuple(sorted(found))

    def fields(self, version: str) -> list[Field] | None:
        """One version's fields in tag order; None when it declares none.

        Tag order because that is the order the versioned layout stored them
        in, and a migration that reordered a version's fields would be a
        migration nobody could check by comparing the two.
        """
        entries = self.field_entries
        if not entries:
            return None
        found = [
            member
            for entry in entries.values()
            if (member := entry.into_field(version)) is not None
        ]
        if found:
            return sorted(found, key=_field_order)
        # No identity declares this version. Whether that is "nobody has read
        # it" or "it has none of the fields this store keeps" -- which is what
        # a projection of two fields leaves FIXT1.1 as -- is what the index
        # remembers, and answering the second as the first would send an
        # offline registry to the network for a version it already holds.
        return [] if self.stored(version) else None

    def components(self, version: str) -> list[SpecComponent] | None:
        """One version's component declarations; None when the store has none.

        None and `[]` are different answers -- "nobody ever read this version's
        spec" against "its spec declares none" -- and telling them apart is
        what makes a stale artifact detectable instead of silently extracting
        nothing. The version list is what remembers which.
        """
        found = [
            (entry.order(version), declared)
            for entry in self.component_entries.values()
            if (declared := entry.into_component(version)) is not None
        ]
        if found:
            return [declared for _, declared in sorted(found, key=lambda pair: pair[0])]
        return [] if self.declared(version) else None

    def session(self, version: str) -> tuple[tuple[str, bool], ...]:
        """`((name, required), ...)`: the standard header, then the trailer."""
        stored = self._index().get(SESSIONS, {}).get(version, ())
        return tuple((str(name), bool(required)) for name, required in stored)

    def store_session(self, version: str, session: Sequence[tuple[str, bool]]) -> None:
        """Keep one version's session layer beside the identities."""
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
        declared = sorted({*index.get(DECLARED, ()), version})
        self._store_index({**index, DECLARED: declared})

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

    def store_field(self, entry: FieldEntry, slug: str = "") -> str:
        """Write one field identity, and name the file it landed in."""
        held = self.slugs.get(entry_identity(entry))
        chosen = slug or held or allocate_slug(entry, self._taken())
        if held is not None and held != chosen:
            # The identity moved -- a newer version renamed the tag -- so the
            # file it used to be in goes rather than shadowing the new one.
            self.remove_field(held)
        self.documents.write(f"{FIELDS}/{chosen}.json", entry.into_dict())
        self.field_entries[chosen] = entry
        return chosen

    def _taken(self) -> dict[str, tuple[str, Any]]:
        """`{slug: (name, identity)}` for every field this store already holds."""
        return {
            slug: (entry.name, entry_identity(entry)) for slug, entry in self.field_entries.items()
        }

    def store_component(self, entry: ComponentEntry) -> None:
        """Write one component identity, replacing what was under its slug."""
        self.documents.write(f"{COMPONENTS}/{entry.slug}.json", entry.into_dict())
        self.component_entries[entry.slug] = entry

    def remove_field(self, slug: str) -> bool:
        """Delete one field identity; False when the store did not hold it."""
        self.field_entries.pop(slug, None)
        return self.documents.remove(f"{FIELDS}/{slug}.json")

    def remove_component(self, slug: str) -> bool:
        """Delete one component identity; False when the store did not hold it."""
        self.component_entries.pop(slug, None)
        return self.documents.remove(f"{COMPONENTS}/{slug}.json")

    def store(
        self,
        version: str,
        fields: Sequence[Field],
        session: Sequence[tuple[str, bool]] = (),
        components: Sequence[SpecComponent] | None = None,
        url: str = "",
    ) -> None:
        """Fold one whole version into the identities already stored.

        A scrape still arrives one version at a time, so this is where the
        per-version shape meets the per-identity one: each field joins the
        entry that owns its name, replacing that version's variant and leaving
        every other version of it alone.
        """
        del url  # An identity is not stored per version, so it carries no URL.
        newest = self._is_newest(version)
        by_identity = {entry_identity(entry): entry for entry in self.field_entries.values()}
        named = set()
        for member in fields:
            identity = identity_of(member)
            held = by_identity.get(identity)
            entry = fold_field(held, member, version, newest=newest or held is None)
            self.store_field(entry)
            by_identity[identity] = entry
            named.add(identity)
        for identity, entry in list(by_identity.items()):
            if identity in named or version not in entry.variants:
                continue
            # This call is what the version declares, so a field it no longer
            # names has lost that version -- the same thing rewriting a version
            # file used to say by leaving the field out of it.
            self._narrowed(entry, version)
        if components is not None:
            declared = {found.name for found in components}
            for order, found in enumerate(components):
                slug = slug_of(found.name)
                self.store_component(
                    fold_component(self.component_entries.get(slug), found, version, order)
                )
            for slug, entry in list(self.component_entries.items()):
                if entry.name in declared or version not in entry.variants:
                    continue
                # The version declares components and not this one, so this
                # version's variant of it is gone rather than merely unstated.
                remaining = {
                    spelled: variant
                    for spelled, variant in entry.variants.items()
                    if spelled != version
                }
                if remaining:
                    self.store_component(dataclasses.replace(entry, variants=remaining))
                else:
                    self.remove_component(slug)
            self.store_declared(version)
        self.store_stored(version)
        self.store_session(version, session)

    def _narrowed(self, entry: FieldEntry, version: str) -> None:
        """Drop one version from an entry, and the entry when that was its last."""
        remaining = {
            spelled: variant for spelled, variant in entry.variants.items() if spelled != version
        }
        slug = self.slugs.get(entry_identity(entry))
        if not remaining:
            if slug is not None:
                self.remove_field(slug)
            return
        self.store_field(dataclasses.replace(entry, variants=remaining), slug or "")

    def _is_newest(self, version: str) -> bool:
        """Whether `version` outranks every version this store already holds.

        The store's own version list is newest first, so a version at or above
        the newest one already stored owns the identities it declares. A
        version the list has never heard of is not assumed to be newer.
        """
        listed = self.versions()
        if version not in listed:
            return not self.field_entries
        stored = {spelled for entry in self.field_entries.values() for spelled in entry.versions}
        rank = listed.index(version)
        return all(rank <= listed.index(other) for other in stored if other in listed)

    @property
    def torn(self) -> tuple[str, ...]:
        """Documents this store holds and cannot read; empty when it is sound.

        A torn write costs one identity here where it used to cost a whole
        version, which is better -- and would be worse if it went unnoticed,
        because the version still answers and answers short. The registry
        treats a torn store as one to write again.
        """
        self.field_entries, self.component_entries  # noqa: B018 - both are read once
        return tuple(sorted(self.__dict__.get("_torn", ())))

    def _read(self, folder: str, kind: Any) -> dict[str, Any]:
        prefix = f"{folder}/"
        readable = self.documents.read_many(prefix)
        torn = self.__dict__.setdefault("_torn", set())
        torn.update(name for name in self.documents.names() if name.startswith(prefix))
        torn.difference_update(readable)
        return {
            name[len(prefix) : -len(".json")]: kind.from_dict(document)
            for name, document in sorted(readable.items())
        }


def _field_order(member: Field) -> tuple[int, int, str]:
    """Tag order, with the fields FIX never numbered after them, by name."""
    tag = member.fix.get("tag")
    return (0, int(tag), "") if tag else (1, 0, member.name)


def entry_identity(entry: FieldEntry) -> tuple[str, Any]:
    """What makes two entries the same field: the tag, or the folded name."""
    return ("tag", int(entry.tag)) if entry.tag else ("name", entry.slug)


def identity_of(member: Field) -> tuple[str, Any]:
    """What makes two readings of a field the same field.

    Its tag, when it has one: a version may rename a tag -- 64 is
    `FutSettDate` through 4.3 and `SettlDate` after -- and a store that keyed
    on the name would hold two entries for one field, each half its history.
    A field FIX never numbered has only its name, folded as every name here is.
    """
    tag = member.fix.get("tag")
    return ("tag", int(tag)) if tag else ("name", slug_of(member.name))


def fold_field(held: FieldEntry | None, member: Field, version: str, newest: bool) -> FieldEntry:
    """One version's reading of a field, folded into the identity that owns it.

    `newest` says whether this reading is the newest one the entry will hold,
    which is what decides the canonical name and tag: everything older is a
    variant of them, and a rename -- tag 64 is `FutSettDate` through 4.3 and
    `SettlDate` after -- is a variant rather than a second entry.
    """
    if held is None:
        return FieldEntry.from_fields([member], [version])
    tag = member.fix.get("tag")
    name, number = (member.name, int(tag) if tag else None) if newest else (held.name, held.tag)
    return FieldEntry(
        name=name,
        tag=number,
        kind=held.kind,
        aliases=held.aliases,
        variants={
            **_restated(held.variants, held.name, held.tag, name, number),
            version: variant_of(member, name, number),
        },
        column=held.column or member.fix.get("column", ""),
    )


def _restated(
    variants: Mapping[str, Mapping[str, Any]],
    was: str,
    had: int | None,
    name: str,
    tag: int | None,
) -> dict[str, Mapping[str, Any]]:
    """Variants restated against a canonical name and tag that just moved.

    A variant states only what it does not share with the identity, so what it
    states depends on what the identity is. When a newer version renames a tag,
    every older variant has to start saying the name it used to share
    silently -- otherwise the rename reads as though every version had the new
    name.
    """
    if (was, had) == (name, tag):
        return dict(variants)
    restated: dict[str, Mapping[str, Any]] = {}
    for version, variant in variants.items():
        rewritten = {key: value for key, value in variant.items() if key not in ("name", "tag")}
        spelled = str(variant.get("name") or was)
        if spelled != name:
            rewritten["name"] = spelled
        own = variant.get("tag") or had
        if own is not None and int(own) != (tag or 0):
            rewritten["tag"] = int(own)
        restated[version] = rewritten
    return restated


def fold_component(
    held: ComponentEntry | None, declared: SpecComponent, version: str, order: int = 0
) -> ComponentEntry:
    """One version's component folded into the identity that owns it."""
    fresh = ComponentEntry.from_components([declared], [version], [order])
    if held is None:
        return fresh
    return dataclasses.replace(held, variants={**held.variants, **fresh.variants})


Layout = VersionedLayout | ExplodedLayout


def layout_of(documents: Documents, default: str = EXPLODED) -> Layout:
    """Which layout a store is in, read off what it holds.

    Off what it holds and not off a setting, for the same reason the archive
    is read off the extension: a store has to say what it is before anything
    asks it, and a copied-in dictionary carries no setting with it. A store
    holding neither shape is cold and is written in `default`.
    """
    names = documents.names()
    if any(name.startswith((f"{FIELDS}/", f"{COMPONENTS}/")) for name in names):
        return ExplodedLayout(documents)
    if any("/" not in name and name != VERSIONS_FILE for name in names):
        return VersionedLayout(documents)
    if default not in LAYOUTS:
        raise ValueError(f"unknown FIX registry layout {default!r}; one of {list(LAYOUTS)}")
    return ExplodedLayout(documents) if default == EXPLODED else VersionedLayout(documents)


# -- turning one into the other ----------------------------------------------


def explode(
    versions: Sequence[str],
    fields: Mapping[str, Sequence[Field]],
    components: Mapping[str, Sequence[SpecComponent]],
) -> tuple[dict[str, FieldEntry], dict[str, ComponentEntry]]:
    """Per-version declarations as per-identity entries, newest version first.

    `versions` runs newest first and decides which reading owns each identity:
    the newest version's name and tag are the canonical ones, and everything
    older is a variant of them. Two identities whose canonical names would be
    stored in one file are refused rather than one silently overwriting the
    other.
    """
    by_identity: dict[tuple[str, Any], FieldEntry] = {}
    for version in versions:
        for member in fields.get(version, ()):
            identity = identity_of(member)
            held = by_identity.get(identity)
            by_identity[identity] = fold_field(held, member, version, newest=held is None)
    field_entries: dict[str, FieldEntry] = {}
    taken: dict[str, tuple[str, Any]] = {}
    for entry in by_identity.values():
        slug = allocate_slug(entry, taken)
        if slug in field_entries:
            raise ValueError(
                f"FIX fields {field_entries[slug].name!r} and {entry.name!r} are both "
                f"stored as {FIELDS}/{slug}.json"
            )
        field_entries[slug] = entry
        taken[slug] = (entry.name, entry_identity(entry))

    component_entries: dict[str, ComponentEntry] = {}
    for version in versions:
        for order, declared in enumerate(components.get(version, ())):
            slug = slug_of(declared.name)
            component_entries[slug] = fold_component(
                component_entries.get(slug), declared, version, order
            )
    return field_entries, component_entries


def documents_of(
    versions: Sequence[str],
    field_entries: Mapping[str, FieldEntry],
    component_entries: Mapping[str, ComponentEntry],
    sessions: Mapping[str, Sequence[tuple[str, bool]]],
    declared: Iterable[str] = (),
) -> dict[str, dict[str, Any]]:
    """A whole exploded store as `{document name: document}`, ready to write.

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
    for slug, entry in field_entries.items():
        documents[f"{FIELDS}/{slug}.json"] = entry.into_dict()
    for slug, entry in component_entries.items():
        documents[f"{COMPONENTS}/{slug}.json"] = entry.into_dict()
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


def allocate_slug(entry: FieldEntry, taken: Mapping[str, tuple[str, Any]]) -> str:
    """The file name one field entry is stored under, given what is already stored.

    Its name, which is what makes `fields/party_role.json` readable. Two
    identities can still share a name -- two venue dialects each numbering
    their own `MsgType` -- and there the tag joins the file name, for both of
    them, so which file an identity lands in never depends on which was read
    first.
    """
    slug = entry.slug
    identity = entry_identity(entry)
    held = taken.get(slug)
    if held is None or held[1] == identity:
        return slug
    return _qualified(entry)


def _qualified(entry: FieldEntry) -> str:
    """A slug that carries the identity, for a name two fields both claim."""
    return f"{entry.slug}_{entry.tag}" if entry.tag else entry.slug


def slug_collisions(names: Iterable[str]) -> dict[str, list[str]]:
    """`{slug: the names that share it}` -- empty when every identity is its own file."""
    found: dict[str, list[str]] = {}
    for name in names:
        found.setdefault(slug_of(name), []).append(name)
    return {slug: shared for slug, shared in found.items() if len(shared) > 1}
