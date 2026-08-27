"""One protocol-neutral shape for ordered structured key/value entries.

`Entry` is both halves of a field: the stored struct a row persists -- tag,
key, value, namespace, comp -- and the ready view the accessor matches
against -- name, index, lead and folded spellings, derived lazily from the
stored spelling. One shape for both, so nothing renders a key to text and
re-splits it on the way to an answer.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Annotated, Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, scalar

TAG: pyarrow.DataType = pyarrow.int32()
NAMESPACED_KEY = r"(?s)^(?:(?P<namespace>.*)\.)?(?P<key>[^.]*)$"
GROUP_ENTRY = r"\[[0-9]+\]$"
IS_TAG = r"^[0-9]{1,9}$"
_TAGGED_TERMINAL = r"^(?P<tag>[0-9]{1,9})(?:\[[0-9]+\])?$"

#: A rendered key cut into whatever stood in front, the name, and its entry
#: index: `NoPartyIDs[0].PartyID` is lead `NoPartyIDs[0]` name `PartyID`;
#: `TECH.CLIENTID` is lead `TECH` name `CLIENTID`; `Side[0]` is name `Side`
#: index `0`. Greedy lead, so the *last* dot is the cut -- the same rule the
#: parser and the transcription apply.
KEY_VIEW = re.compile(
    r"(?s)^(?:(?P<lead>.*)\.)?(?P<name>[^.\[\]]*)(?:\[(?P<index>[0-9]+)\])?$",
    re.ASCII,
)

#: A lead that names a repeating-group entry rather than a namespace: it ends
#: with an index. The one dotted lead a bare name still answers through --
#: `get("PartyID")` finds `NoPartyIDs[0].PartyID`, because the group is where
#: the field sits and not what it is, while `TECH.CLIENTID` stays out of reach
#: of `get("CLIENTID")` because a vendor namespace is part of the name.
ENTRY_LEAD = re.compile(r"\[[0-9]+\]$", re.ASCII)


def fold(name: str) -> str:
    """A name as it is matched: case, and nothing else.

    Separators are part of a name here. Dropping them made `PartyID` and
    `Part_yid` one key and, worse, silently merged two identities a store
    holds apart -- a match a registry cannot then tell from a real collision.
    A spelling that differs by more than case is an alias, which is a thing
    the store records.
    """
    return str(name).strip().lower()


@scalar(slots=True)
class Entry(Convertible, Mapping[str, Any]):
    """One ordered structured key/value entry."""

    # The read views derive once from the stored spelling and cache in
    # private slots: reader state, never columns.
    __views: tuple[str, int | None, str | None, bool] | None = None
    __folded: str | None = None
    __folded_lead: str | None = None

    tag: Annotated[int, Field(data_type=TAG)] = 0
    """Numeric identity written or resolved; zero while unresolved."""

    key: str = ""
    """Terminal spelling without a leading argument marker."""

    value: str = ""
    """Text after the first equals sign; an empty value is empty text."""

    namespace: str | None = None
    """Prefix outside an indexed container."""

    comp: str | None = None
    """Indexed container prefix."""

    def __post_init__(self) -> None:
        """Normalize direct construction exactly like stored entries."""
        spelling = str(self.key).removeprefix("#")
        if self.namespace is None and self.comp is None:
            inferred, spelling, namespace, comp = _key_parts(spelling)
            self.namespace = namespace
            self.comp = comp
        else:
            inferred = _terminal_tag(spelling)
            namespace = None if self.namespace is None else str(self.namespace)
            comp = None if self.comp is None else str(self.comp)
            self.namespace = None if namespace is None else namespace.removeprefix("#")
            self.comp = None if comp is None else comp.removeprefix("#")
        self.tag = int(self.tag or inferred)
        self.key = spelling
        self.value = "" if self.value is None else str(self.value)

    def __setattr__(self, name: str, value: Any) -> None:
        object.__setattr__(self, name, value)
        if name in ENTRY_PARTS:
            # A stored member changed: the cached views re-derive.
            object.__setattr__(self, "_Entry__views", None)
            object.__setattr__(self, "_Entry__folded", None)
            object.__setattr__(self, "_Entry__folded_lead", None)

    @classmethod
    def from_stored(cls, entry: Any) -> Entry:
        """Normalize a scalar, mapping, or pair into one entry."""
        if isinstance(entry, cls):
            if isinstance(entry.value, str):
                return entry
            # A ready view built beside typed columns: storage takes the
            # normalized text, never a Python object Arrow cannot hold.
            return cls(
                tag=entry.tag,
                key=entry.key,
                value=entry.value,
                namespace=entry.namespace,
                comp=entry.comp,
            )
        if isinstance(entry, Mapping):
            value = entry.get("value")
            return cls(
                tag=int(entry.get("tag") or 0),
                key=str(entry["key"]),
                value="" if value is None else str(value),
                namespace=entry.get("namespace"),
                comp=entry.get("comp"),
            )
        key, value = entry
        return cls(
            key=str(key),
            value="" if value is None else str(value),
        )

    @classmethod
    def from_pair(cls, key: Any, value: Any) -> Entry:
        """One `(key, value)` pair as an entry, the value kept as given."""
        built = cls(key=str(key))
        built.value = value
        return built

    @classmethod
    def of(
        cls,
        tag: int = 0,
        key: str = "",
        value: Any = "",
        namespace: str | None = None,
        comp: str | None = None,
    ) -> Entry:
        """A ready view: the spelling kept verbatim and the value kept typed.

        For readers constructing entries beside typed columns; a stored or
        parsed entry normalizes through `__init__` or `from_stored` instead.
        """
        built = cls.__new__(cls)
        built.tag = int(tag)
        built.key = key
        built.value = value
        built.namespace = namespace
        built.comp = comp
        built.__views = None
        built.__folded = None
        built.__folded_lead = None
        return built

    def __getitem__(self, name: str) -> Any:
        if name not in ENTRY_PARTS:
            raise KeyError(name)
        return getattr(self, name)

    def __iter__(self):
        return iter(ENTRY_PARTS)

    def __len__(self) -> int:
        return len(ENTRY_PARTS)

    # -- the read view -------------------------------------------------------

    @property
    def spelling(self) -> str:
        """The verbatim spelling this entry renders under."""
        lead = self.namespace or self.comp
        if lead:
            return f"{lead}.{self.key}"
        if "[" in self.key:
            return self.key
        if self.tag:
            return str(self.tag)
        return self.key

    def _view(self) -> tuple[str, int | None, str | None, bool]:
        """`(name, index, lead, entry lead)`: the stored split, held apart.

        The stored members are already the split, so they answer directly --
        `comp` asserts group-entry semantics whatever its spelling, and the
        key parts only an index away from the name. Only a spelling the
        stored split cannot re-render byte for byte -- a dotted key under an
        explicit lead, a zero-padded index, a double lead -- re-splits whole
        under `KEY_VIEW`, which is how the parser reads such a key off a
        wire pair.
        """
        found = self.__views
        if found is None:
            lead = self.comp if self.comp else self.namespace
            name, index = self.key, None
            match = KEY_VIEW.match(self.key)
            if match is not None and match.group("index") is not None:
                name, index = match.group("name"), int(match.group("index"))
            spelled = name if index is None else f"{name}[{index}]"
            joined = self.namespace or self.comp
            full = f"{joined}.{self.key}" if joined else self.key
            if (f"{lead}.{spelled}" if lead else spelled) == full:
                found = (name, index, lead, bool(self.comp))
            else:
                match = KEY_VIEW.match(full)
                if match is None:
                    found = (full, None, None, False)
                else:
                    split_lead, name, spelled_index = match.group("lead", "name", "index")
                    found = (
                        name or full,
                        None if spelled_index is None else int(spelled_index),
                        split_lead,
                        bool(split_lead) and ENTRY_LEAD.search(split_lead) is not None,
                    )
            self.__views = found
        return found

    @property
    def name(self) -> str:
        """Terminal spelling without its entry index."""
        return self._view()[0]

    @property
    def index(self) -> int | None:
        """Entry position the key names, where it names one."""
        return self._view()[1]

    @property
    def lead(self) -> str | None:
        """Whatever stood in front of the name: a namespace or a group entry."""
        return self._view()[2]

    @property
    def entry_lead(self) -> bool:
        """Whether a bare-name ask may reach through `lead`: true for a group
        entry (`NoPartyIDs[0]`), false for a namespace."""
        return self._view()[3]

    @property
    def folded(self) -> str:
        """`name` as `Resolved.matches` compares it: folded once per entry.

        Once and not once per compare, because reading several dozen fields
        off one row compares every entry against every ask.
        """
        found = self.__folded
        if found is None:
            found = self.__folded = fold(self.name)
        return found

    @property
    def folded_lead(self) -> str:
        """`lead` folded, empty where the entry carries none."""
        found = self.__folded_lead
        if found is None:
            found = self.__folded_lead = fold(self.lead or "")
        return found

    # -- whole columns -------------------------------------------------------

    @classmethod
    def looks_structured_arrow(cls, messages: Any) -> Any:
        """Which rows contain two delimiter-separated assignments."""
        from rekep.text.entries import looks_structured_arrow

        return looks_structured_arrow(messages)

    @classmethod
    def parse_arrow(cls, messages: Any) -> Any:
        """Split text into ordered entries without protocol interpretation."""
        from rekep.text.entries import parse_arrow

        return parse_arrow(messages)

    @classmethod
    def pop_arrow(
        cls,
        stored: Any,
        names: tuple[str, ...],
        *,
        case_sensitive: bool = True,
    ) -> tuple[Any, Any]:
        """Return the first named value per row and every other entry."""
        from rekep.text.entries import pop_arrow

        return pop_arrow(stored, names, case_sensitive=case_sensitive)

    @staticmethod
    def structure_arrow(keys: Any, values: Any) -> tuple[Any, ...]:
        """Split key spellings into the stable entry members."""
        compute = pyarrow.compute
        keys = compute.replace_substring_regex(keys, pattern=r"^#", replacement="")
        values = compute.fill_null(values, "")
        encoded = keys.dictionary_encode()
        spellings, indices = encoded.dictionary, encoded.indices
        parts = compute.extract_regex(spellings, NAMESPACED_KEY)
        lead = compute.struct_field(parts, "namespace")
        terminals = compute.fill_null(compute.struct_field(parts, "key"), spellings)
        tagged = compute.struct_field(compute.extract_regex(terminals, _TAGGED_TERMINAL), "tag")
        tags = compute.fill_null(tagged, "0").cast(TAG)
        led = compute.fill_null(compute.greater(compute.binary_length(lead), 0), False)
        indexed = compute.fill_null(compute.match_substring_regex(lead, GROUP_ENTRY), False)
        nothing = pyarrow.scalar(None, pyarrow.string())
        return (
            compute.take(tags, indices),
            compute.take(terminals, indices),
            values,
            compute.take(
                compute.if_else(compute.and_(led, compute.invert(indexed)), lead, nothing),
                indices,
            ),
            compute.take(compute.if_else(compute.and_(led, indexed), lead, nothing), indices),
        )


ENTRIES: pyarrow.DataType = pyarrow.list_(
    pyarrow.field("item", Entry.into_field().data_type, nullable=False)
)
ENTRY_PARTS: tuple[str, ...] = tuple(member.name for member in ENTRIES.value_type)


def _key_parts(spelling: str) -> tuple[int, str, str | None, str | None]:
    """A generic key split into identity, terminal spelling, and location."""
    lead, separator, terminal = spelling.rpartition(".")
    key = terminal if separator and terminal else spelling
    tail = lead.rsplit(".", 1)[-1]
    index = tail.rpartition("[")[2].removesuffix("]")
    inside = bool(lead) and tail.endswith("]") and index.isdigit()
    return (
        _terminal_tag(key),
        key,
        lead if lead and not inside else None,
        lead if inside else None,
    )


def _terminal_tag(spelling: str) -> int:
    """Numeric identity at the end of a plain or indexed key."""
    tag, bracket, index = spelling.partition("[")
    indexed = bool(bracket) and spelling.endswith("]") and index[:-1].isdigit()
    numeric = tag.isascii() and tag.isdigit() and len(tag) <= 9
    return int(tag) if numeric and (not bracket or indexed) else 0
