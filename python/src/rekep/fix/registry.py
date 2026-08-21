"""Every FIX field of every FIX version, scraped once and kept for offline use."""

from __future__ import annotations

import concurrent.futures
import dataclasses
import html
import json
import os
import pathlib
import re
import time
import urllib.error
import urllib.request
import zipfile
from functools import cached_property
from typing import Any, ClassVar

from rekep.convert import Convertible
from rekep.fields import Field
from rekep.fix.fields import fix_field

#: The dictionary that is scraped: OnixS publishes every FIX version as one
#: page per version listing the tags, and one page per field carrying the
#: name, datatype, description and enumerated values.
BASE_URL = "https://www.onixs.biz/fix-dictionary"

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

_TAGS = re.compile(r"<[^>]+>")
_SPACES = re.compile(r"\s+")


@dataclasses.dataclass(eq=False)
class FixRegistry(Convertible):
    """The OnixS FIX dictionary as `Field`s, one scrape then offline forever.

    `fields("4.4")` answers from `~/.config/fix/4.4.json` when it is there and
    scrapes the site into it when it is not -- the by-tag page for the tag
    list, then one page per field, concurrently, for the datatype, the
    description and the enumerated values. Every field comes back as this
    package's generic `Field`: the Arrow type follows the FIX datatype
    (`rekep.fix.fields`), the description is the column comment, and the FIX
    identity rides the `fix:` metadata prefix.

    `lookup` finds a field by tag or by name across versions, newest first;
    `search` matches name, tag or description case-insensitively and falls
    back to Levenshtein distance when nothing does.
    """

    #: Where the dictionary lives; override to scrape a mirror.
    base_url: str = BASE_URL

    #: Where scrapes persist: a directory of JSON, or a `.zip` of the same
    #: files. The extension is what says which -- like every other inference
    #: here -- so one path names either, and a dictionary that travels as one
    #: file and a dictionary that travels as a directory are the same
    #: dictionary. One member per version, `versions.json` beside them, all
    #: plain JSON.
    cache_dir: str | os.PathLike[str] = CACHE_DIRECTORY

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

    #: Sent with every request, so the traffic says what it is.
    user_agent: ClassVar[str] = "rekep-fix-registry (+https://github.com/Platob/yggfin)"

    def __post_init__(self) -> None:
        """Normalise the two locations once, so everything downstream agrees."""
        self.base_url = str(self.base_url).rstrip("/")
        self.cache_dir = pathlib.Path(self.cache_dir)

    # -- versions ------------------------------------------------------------

    @cached_property
    def versions(self) -> tuple[str, ...]:
        """Every FIX version the dictionary carries, newest first.

        Application versions descend (`5.0.SP2` down to `4.0`) and the
        transport (`FIXT1.1`) comes last: a lookup that walks versions in this
        order answers with the newest definition, which is the one that
        subsumes the others.

        The store is asked first, the site second, and the store again when
        the site cannot be had -- three steps that are the same whether the
        store is a directory or an archive.
        """
        stored = self._stored_versions()
        if stored:
            return stored
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
        """Every field of one FIX version, from the cache or from one scrape.

        The first call for a version is the expensive one -- one page per
        field -- and the last: the result lands in the cache file the next
        call answers from. `refresh=True` scrapes again over a stale cache.

        The version is resolved to its canonical spelling first (`fixt1.1`
        is `FIXT1.1` wherever either has been seen), so the cache file, the
        site's case-sensitive directory and the lookup indexes are always
        addressed the one way the version is spelled -- a refresh through a
        lowercased spelling would otherwise scrape a 404, or fork the cache
        into a second file and leave a stale index serving the old fields.
        """
        version = self._spelling(version)
        if not refresh:
            stored = self._stored_fields(version)
            if stored is not None:
                return stored
        fields = self._scrape_version(version)
        self._store_fields(version, fields)
        self._indexes.pop(version, None)
        return fields

    def _spelling(self, version: str) -> str:
        """The canonical spelling of `version`, and a refusal of non-names.

        Resolved case-blind against what is already known -- the fetched
        version list when there is one, else the cache files on disk -- and
        never by a network round trip a plain cache read did not need. The
        character check is what keeps `fields()` from being handed a *path*:
        the version lands in a cache file name, and `..` or a separator in it
        would read and write outside the cache directory.
        """
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
            by_tag, by_name = indexed
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

    def tags(self, version: str | None = None) -> dict[str, int]:
        """Every field name to its tag number, lowercased, newest version winning.

        The `names` mapping `rekep.fix.tag_arrow_array` resolves rendered
        keys through: build it once and hand it to every call, because it
        walks whole versions. Lowercased here so the lookup there is one
        dictionary probe, never a scan.

        A version named explicitly loads through `fields`, so a cache or
        network failure *raises* -- an empty mapping here would quietly
        un-resolve every rendered key downstream, which is the worst way to
        learn the cache is cold. The walk over all versions keeps skipping
        the ones that cannot be had, like `lookup` does.
        """
        mapping: dict[str, int] = {}
        if version is not None:
            (candidate,) = self._versions(version)
            members = self.fields(candidate)
            for member in members:
                mapping.setdefault(member.name.lower(), int(member.fix["tag"]))
            return mapping
        for candidate in self._versions(None):
            for member in self._members(candidate):
                mapping.setdefault(member.name.lower(), int(member.fix["tag"]))
        return mapping

    def search(
        self,
        text: int | str,
        version: str | None = None,
        *,
        limit: int = 10,
        fuzzy: bool = True,
    ) -> list[Field]:
        """Fields matching `text` by tag, name or description, best first.

        Case-insensitive throughout. Ranked: an exact tag or name first, then
        a name prefix, then a name substring, then a description substring --
        and within one rank, newer versions first. When nothing matches at
        all and `fuzzy` is on, the nearest names by Levenshtein distance come
        back instead, so `"Sied"` still finds `Side` -- the fallback only
        runs when the cheap passes found nothing, which keeps the common case
        one dictionary probe.
        """
        wanted = str(text).strip().lower()
        if not wanted:
            return []
        ranked: list[tuple[int, int, int, Field]] = []
        for order, candidate in enumerate(self._versions(version)):
            for member in self._members(candidate):
                rank = _rank(member, wanted)
                if rank is not None:
                    ranked.append((rank, order, int(member.fix["tag"]), member))
        if not ranked and fuzzy and not _is_tag(wanted):
            ceiling = max(2, len(wanted) // 3)
            for order, candidate in enumerate(self._versions(version)):
                for member in self._members(candidate):
                    distance = _levenshtein(wanted, member.name.lower(), ceiling)
                    if distance is not None:
                        ranked.append((100 + distance, order, int(member.fix["tag"]), member))
        ranked.sort(key=lambda entry: entry[:3])
        return [member for *_, member in ranked[:limit]]

    @cached_property
    def _indexes(self) -> dict[str, tuple[dict[int, Field], dict[str, Field]] | None]:
        return {}

    def _index(self, version: str) -> tuple[dict[int, Field], dict[str, Field]] | None:
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
                by_tag = {int(member.fix["tag"]): member for member in members}
                # First declaration wins on a duplicated name, matching
                # `tags()` -- so the tag a name resolves to and the field a
                # lookup returns can never disagree about each other.
                by_name: dict[str, Field] = {}
                for member in members:
                    by_name.setdefault(member.name.lower(), member)
                built = self._indexes[version] = (by_tag, by_name)
        return built

    def _members(self, version: str) -> list[Field]:
        """`fields(version)` for a walk over many versions: absent means empty."""
        indexed = self._index(version)
        if indexed is None:
            return []
        return list(indexed[0].values())

    # -- scraping ------------------------------------------------------------

    def _scrape_version(self, version: str) -> list[Field]:
        """One version, whole: the by-tag list, then every field page."""
        listed = self._scrape_tags(version)
        with concurrent.futures.ThreadPoolExecutor(self.max_workers) as pool:
            details = list(pool.map(lambda tag: self._scrape_field(version, tag), listed))
        fields = []
        for (tag, (name, note)), detail in zip(listed.items(), details, strict=True):
            built = fix_field(
                detail.get("name") or name,
                tag,
                detail.get("type"),
                description=detail.get("description"),
                version=version,
                values=detail.get("values"),
            )
            if note:
                built.fix["note"] = note
            used = detail.get("used_in")
            if used:
                built.fix["used_in"] = json.dumps(used, separators=(",", ":"))
            fields.append(built)
        return fields

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
    # Five methods, and they are the whole of where the dictionary is kept.
    # Everything above -- the scraping, the version rules, the ordering, the
    # searching -- is written against these and is the same wherever the
    # fields live: a directory of JSON, or a zip of the same files.

    def _stored_versions(self) -> tuple[str, ...]:
        """The version list this store already holds; empty when it holds none."""
        cached = self._read_cache("versions.json")
        return tuple(cached["versions"]) if cached else ()

    def _store_versions(self, versions: tuple[str, ...]) -> None:
        """Keep the version list, so the front page is fetched once."""
        self._write_cache("versions.json", {"versions": list(versions)})

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
        if self.archived:
            names = _archived_names(self.cache_dir)
        else:
            names = {path.name: path.name for path in pathlib.Path(self.cache_dir).glob("*.json")}
        return tuple(sorted(name[: -len(".json")] for name in names if name != "versions.json"))

    def _stored_fields(self, version: str) -> list[Field] | None:
        """One version's fields as this store holds them; None when it does not."""
        cached = self._read_cache(f"{version}.json")
        if cached is None:
            return None
        return [Field.from_dict(member) for member in cached["fields"]]

    def _store_fields(self, version: str, fields: list[Field]) -> None:
        """Keep one whole version, replacing whatever was there."""
        self._write_cache(
            f"{version}.json",
            {
                "version": version,
                "url": f"{self.base_url}/{version}/",
                "fields": [member.into_dict() for member in fields],
            },
        )

    # -- the cache files and the wire -----------------------------------------

    @property
    def archived(self) -> bool:
        """Whether this registry keeps its dictionary in a zip rather than a directory.

        Read off the extension, and only off the extension: a path that does
        not exist yet has to say what it will be before anything is written
        to it, and `data/fix.zip` says it.
        """
        return pathlib.Path(self.cache_dir).suffix.lower() == ".zip"

    def into_zip(self, target: str | os.PathLike[str]) -> pathlib.Path:
        """Write everything this registry holds into one archive, and name it.

        The counterpart of pointing a registry at a `.zip`: a directory that
        was scraped becomes one file to publish or copy, and reading it back
        is `FixRegistry(cache_dir=that_file)`.

        Deflated, because a FIX dictionary is text that repeats itself: the
        published one goes from 2.86 MB to 0.47 MB. At zlib's own level,
        which is what the measurement says to use: level 9 is 2% smaller for
        twice the time, and level 1 is 26% bigger
        (`benchmarks/bench_fix_registry.py`).
        """
        path = pathlib.Path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        scratch = path.with_name(f"{path.name}.tmp")
        with zipfile.ZipFile(scratch, "w", zipfile.ZIP_DEFLATED) as archive:
            listed = self._stored_versions()
            if listed:
                archive.writestr(_member("versions.json"), _document({"versions": list(listed)}))
            for version in self._stored_spellings():
                stored = self._read_cache(f"{version}.json")
                if stored is not None:
                    archive.writestr(_member(f"{version}.json"), _document(stored))
        scratch.replace(path)
        return path

    def _read_cache(self, name: str) -> dict[str, Any] | None:
        if self.archived:
            return _read_archived(self.cache_dir, name)
        path = pathlib.Path(self.cache_dir) / name
        try:
            return json.loads(path.read_text("utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            # A torn write or someone else's file: scrape over it rather than
            # refuse to run offline forever.
            return None

    def _write_cache(self, name: str, payload: dict[str, Any]) -> None:
        if self.archived:
            _write_archived(self.cache_dir, name, _document(payload))
            return
        directory = pathlib.Path(self.cache_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        # Written beside then renamed, so a reader never sees half a file and
        # a crash never costs the cache that was already there.
        scratch = path.with_suffix(".tmp")
        scratch.write_text(_document(payload), "utf-8")
        scratch.replace(path)

    def _fetch(self, url: str) -> str:
        """One page, as text, retried while the site says "later".

        A whole-version scrape is thousands of pages and the site paces it:
        `429 Too Many Requests` arrives partway through, and every page
        refused while it lasts used to become a field with no type and no
        description, cached with nothing to say it was ever refused. So a
        transient answer waits and is asked again -- `Retry-After` when the
        site sends one, a doubling pause when it does not -- and only the last
        attempt raises.
        """
        request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
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
        with urllib.request.urlopen(request, timeout=self.timeout) as response:  # noqa: S310
            return response.read().decode("utf-8", "replace")


# -- the store, as a directory or as a zip ------------------------------------


def _document(payload: dict[str, Any]) -> str:
    """One cache file's text. The one place the on-disk spelling is decided."""
    return json.dumps(payload, indent=1)


def _member(name: str) -> zipfile.ZipInfo:
    """One archive member, stamped at the start of zip time.

    A zip records a modification time per member, so an archive built twice
    from the same dictionary is two different files unless the stamp is
    fixed. This one is published in a repository, where "nothing changed"
    has to look like nothing changed.
    """
    entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    entry.compress_type = zipfile.ZIP_DEFLATED
    entry.external_attr = 0o644 << 16
    return entry


def _archived_names(archive: str | os.PathLike[str]) -> dict[str, str]:
    """`{file name: member name}` for the JSON in an archive, or nothing.

    Keyed by the file's own name so a zip made of a *folder* -- `zip -r
    fix.zip fix/`, which prefixes every member with `fix/` -- reads the same
    as one made of the files. The shallowest member wins where two share a
    name, because that is the one a person unzipping and looking would find.
    A file that is not a zip, or is not there, holds no versions rather than
    raising: the caller's next step is to scrape, exactly as for an empty
    directory.
    """
    try:
        with zipfile.ZipFile(archive) as opened:
            members = opened.namelist()
    except (OSError, zipfile.BadZipFile):
        return {}
    found: dict[str, str] = {}
    for member in sorted(members, key=lambda name: (name.count("/"), name)):
        name = member.rsplit("/", 1)[-1]
        if name.endswith(".json"):
            found.setdefault(name, member)
    return found


def _read_archived(archive: str | os.PathLike[str], name: str) -> dict[str, Any] | None:
    """One member of an archive, as the document it holds; None when absent."""
    member = _archived_names(archive).get(name)
    if member is None:
        return None
    try:
        with zipfile.ZipFile(archive) as opened:
            return json.loads(opened.read(member).decode("utf-8"))
    except (OSError, ValueError, zipfile.BadZipFile):
        # A torn archive is a cold cache, not a dead registry -- the same
        # reading a torn file gets.
        return None


def _write_archived(archive: str | os.PathLike[str], name: str, document: str) -> None:
    """Put one member into an archive, replacing what was there.

    Rewritten whole rather than appended to: a zip will happily hold two
    members of one name, and the reader that then picks between them is
    picking between a stale version and a fresh one. The archive is built
    beside and renamed over, so a reader never sees half of it -- the same
    rule the directory store writes by.
    """
    path = pathlib.Path(archive)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _archived_names(path)
    # A zip of a folder keeps its folder: a member written into one goes in
    # beside its neighbours rather than at the root, where nothing else is.
    prefix = ""
    for member in existing.values():
        if "/" in member:
            prefix = member.rsplit("/", 1)[0] + "/"
        break
    scratch = path.with_name(f"{path.name}.tmp")
    with zipfile.ZipFile(scratch, "w", zipfile.ZIP_DEFLATED) as fresh:
        try:
            with zipfile.ZipFile(path) as opened:
                for member in opened.infolist():
                    if member.filename != existing.get(name):
                        fresh.writestr(member, opened.read(member))
        except (OSError, zipfile.BadZipFile):
            # Nothing there, or nothing readable there. A torn archive is
            # written over rather than mourned -- the same reading the
            # directory store gives a torn file -- and what could not be read
            # was not being served anyway.
            pass
        fresh.writestr(_member(existing.get(name, f"{prefix}{name}")), document)
    scratch.replace(path)


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
    """A field page cut into its three parts: prose, values, messages.

    Cut back to front, each part running from its own marker to the next: the
    messages off the `Used In` heading, the enumeration off the `Valid values`
    line, and the prose off the `Description` heading. Cutting first is what
    keeps the parts out of each other -- MsgType lists its own messages *as
    values*, so a `msgType_` link is only a message when it is below `Used In`,
    and a `k = v` line of prose is only a value when it is below `Valid
    values`.

    A page that opens no `Description` is the older layout: its prose starts at
    the type line, and the first heading, list or table after it is the only
    end the page gives.
    """
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
