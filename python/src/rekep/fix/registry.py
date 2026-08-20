"""Every FIX field of every FIX version, scraped once and kept for offline use."""

from __future__ import annotations

import concurrent.futures
import dataclasses
import html
import json
import os
import pathlib
import re
import urllib.request
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

#: One enumerated value: `0 = Day (or session)` -- inside a list item.
_VALUE_ITEM = re.compile(r"<li[^>]*>\s*(.*?)\s*</li>", re.DOTALL)
_VALUE = re.compile(r"^\s*(\S+)\s*=\s*(.+?)\s*$", re.DOTALL)

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

    #: Where scrapes persist. One file per version, `versions.json` beside
    #: them, all plain JSON -- inspectable, diffable, copyable.
    cache_dir: str | os.PathLike[str] = CACHE_DIRECTORY

    #: Seconds one page fetch may take, and how many fetch at once. The site
    #: is a static dictionary; eight lanes drain a version in seconds without
    #: leaning on it.
    timeout: float = 30.0
    max_workers: int = 8

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
        """
        cached = self._read_cache("versions.json")
        if cached is not None:
            return tuple(cached["versions"])
        try:
            page = self._fetch(f"{self.base_url}.html")
        except OSError:
            # Offline before the index was ever cached: the versions that
            # *were* scraped are the ones this registry can honestly serve.
            stored = tuple(
                sorted(
                    (
                        path.stem
                        for path in pathlib.Path(self.cache_dir).glob("*.json")
                        if path.stem != "versions"
                    ),
                    key=_version_key,
                    reverse=True,
                )
            )
            if stored:
                return stored
            raise
        found = dict.fromkeys(_VERSION_LINK.findall(page))
        found.pop("latest", None)
        versions = tuple(sorted(found, key=_version_key, reverse=True))
        if not versions:
            raise ValueError(f"{self.base_url}.html lists no FIX versions; is the layout new?")
        self._write_cache("versions.json", {"versions": list(versions)})
        return versions

    def _versions(self, version: str | None) -> tuple[str, ...]:
        """The versions a call walks: all of them, or the one it named."""
        if version is None:
            return self.versions
        if version not in self.versions:
            raise KeyError(f"{version!r} is not a FIX version here; one of {self.versions}")
        return (version,)

    # -- fields --------------------------------------------------------------

    def fields(self, version: str, *, refresh: bool = False) -> list[Field]:
        """Every field of one FIX version, from the cache or from one scrape.

        The first call for a version is the expensive one -- one page per
        field -- and the last: the result lands in the cache file the next
        call answers from. `refresh=True` scrapes again over a stale cache.
        """
        name = f"{version}.json"
        if not refresh:
            cached = self._read_cache(name)
            if cached is not None:
                return [Field.from_dict(member) for member in cached["fields"]]
        fields = self._scrape_version(version)
        self._write_cache(
            name,
            {
                "version": version,
                "url": f"{self.base_url}/{version}/",
                "fields": [member.into_dict() for member in fields],
            },
        )
        self._indexes.pop(version, None)
        return fields

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
                by_name = {member.name.lower(): member for member in members}
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
        except OSError:
            return {}
        detail: dict[str, Any] = {}
        title = _TITLE.search(page)
        if title and title[2] == str(tag):
            detail["name"] = _text(title[1])
        typed = _TYPE.search(page)
        if typed:
            detail["type"] = typed[1]
        description = _description(page, typed.end() if typed else 0)
        if description:
            detail["description"] = description
        values = _values(page)
        if values:
            detail["values"] = values
        used = _used_in(page)
        if used:
            detail["used_in"] = used
        return detail

    # -- the cache and the wire ---------------------------------------------

    def _read_cache(self, name: str) -> dict[str, Any] | None:
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
        directory = pathlib.Path(self.cache_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        # Written beside then renamed, so a reader never sees half a file and
        # a crash never costs the cache that was already there.
        scratch = path.with_suffix(".tmp")
        scratch.write_text(json.dumps(payload, indent=1), "utf-8")
        scratch.replace(path)

    def _fetch(self, url: str) -> str:
        """One page, as text. The single place the network is touched."""
        request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        with urllib.request.urlopen(request, timeout=self.timeout) as response:  # noqa: S310
            return response.read().decode("utf-8", "replace")


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


def _description(page: str, start: int) -> str:
    """The prose after the type line, up to the next section of the page.

    The pages put the field's own paragraph right after `Type:` and before
    the value list or the "Used in" heading, so the description is the text
    between those markers -- whatever markup it wears in a given version.
    """
    window = page[start : start + 8000]
    for marker in ("Valid values", "Used in", "Used In", "<h3", "<ul", "<table"):
        cut = window.find(marker)
        if cut >= 0:
            window = window[:cut]
    return _text(window)


def _values(page: str) -> dict[str, str]:
    """The enumerated values a field page lists: `{"1": "Buy", ...}`."""
    found: dict[str, str] = {}
    for item in _VALUE_ITEM.findall(page):
        text = _text(item)
        value = _VALUE.match(text)
        if value:
            found.setdefault(value[1], value[2])
    return found


def _used_in(page: str) -> list[str]:
    """The messages a field page says carry it, names only."""
    names = []
    for match in re.finditer(r"<a[^>]+href=\"msgType_[^\"]+\"[^>]*>(.*?)</a>", page, re.DOTALL):
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
