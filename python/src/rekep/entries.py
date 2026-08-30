"""One protocol-neutral shape for ordered structured key/value entries."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Annotated, Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, column_name, scalar

TAG: pyarrow.DataType = pyarrow.int32()
IS_TAG = r"^[0-9]{1,9}$"
_TAGGED_TERMINAL = r"^(?P<tag>[0-9]{1,9})(?:\[[0-9]+\])?$"
_GROUPED_KEY = r"(?s)^(?:(?P<comp>.*\[[0-9]+\])\.(?P<key>[^.]+)|(?P<plain>.*))$"

#: A rendered key cut into its lead, name and entry index.
KEY_VIEW = re.compile(
    r"(?s)^(?:(?P<lead>.*)\.)?(?P<name>[^.\[\]]*)(?:\[(?P<index>[0-9]+)\])?$",
    re.ASCII,
)

#: A group entry is a location; every other dotted lead is part of the key.
ENTRY_LEAD = re.compile(r"\[[0-9]+\]$", re.ASCII)


def fold(name: str) -> str:
    """A name as it is matched: lowercase letters and digits."""
    return column_name(name)


@scalar(slots=True)
class Entry(Convertible, Mapping[str, Any]):
    """One ordered structured key/value entry."""

    # The read views derive once from the stored spelling and cache in
    # private slots: reader state, never columns.
    __views: tuple[str, int | None, str | None, bool] | None = None
    __folded: str | None = None
    __folded_lead: str | None = None

    tag: Annotated[int, Field(dtype=TAG)] = 0
    """Numeric identity written or resolved; zero while unresolved."""

    key: str = ""
    """Whole field spelling outside an indexed container."""

    value: str = ""
    """Text after the first equals sign; an empty value is empty text."""

    comp: str | None = None
    """Indexed container prefix."""

    def __post_init__(self) -> None:
        """Normalize direct construction exactly like stored entries."""
        spelling = str(self.key).removeprefix("#")
        if self.comp is None:
            inferred, spelling, comp = _key_parts(spelling)
            self.comp = comp
        else:
            inferred = _terminal_tag(spelling)
            self.comp = str(self.comp).removeprefix("#")
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
                comp=entry.comp,
            )
        if isinstance(entry, Mapping):
            value = entry.get("value")
            return cls(
                tag=int(entry.get("tag") or 0),
                key=str(entry["key"]),
                value="" if value is None else str(value),
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
        """The spelling this entry renders under."""
        if self.comp:
            return f"{self.comp}.{self.key}"
        if "[" in self.key:
            return self.key
        if self.tag:
            return str(self.tag)
        return self.key

    def _view(self) -> tuple[str, int | None, str | None, bool]:
        """`(name, index, lead, entry lead)` matched from the stored key."""
        found = self.__views
        if found is None:
            full = f"{self.comp}.{self.key}" if self.comp else self.key
            match = KEY_VIEW.match(full)
            if match is None:
                found = (full, None, None, False)
            else:
                lead, name, spelled_index = match.group("lead", "name", "index")
                found = (
                    name or full,
                    None if spelled_index is None else int(spelled_index),
                    lead,
                    bool(lead) and ENTRY_LEAD.search(lead) is not None,
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
        """The dotted prefix before the terminal name."""
        return self._view()[2]

    @property
    def entry_lead(self) -> bool:
        """Whether a bare-name ask may reach through an indexed group lead."""
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
    def payload_arrow(cls, messages: Any) -> Any:
        """The entries of every row that carries a payload; the rest empty."""
        from rekep.text.entries import payload_arrow

        return payload_arrow(messages)

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
        parts = compute.extract_regex(spellings, _GROUPED_KEY)
        lead = compute.struct_field(parts, "comp")
        grouped = compute.fill_null(compute.greater(compute.binary_length(lead), 0), False)
        terminals = compute.coalesce(
            compute.if_else(
                grouped,
                compute.struct_field(parts, "key"),
                compute.struct_field(parts, "plain"),
            ),
            spellings,
        )
        lead = compute.if_else(grouped, lead, pyarrow.scalar(None, pyarrow.string()))
        tagged = compute.struct_field(compute.extract_regex(terminals, _TAGGED_TERMINAL), "tag")
        tags = compute.fill_null(tagged, "0").cast(TAG)
        return (
            compute.take(tags, indices),
            compute.take(terminals, indices),
            values,
            compute.take(lead, indices),
        )


ENTRIES: pyarrow.DataType = pyarrow.list_(
    pyarrow.field("item", Entry.into_field().dtype, nullable=False)
)
ENTRY_PARTS: tuple[str, ...] = tuple(member.name for member in ENTRIES.value_type)


def _key_parts(spelling: str) -> tuple[int, str, str | None]:
    """A generic key split into identity, spelling and indexed location."""
    lead, separator, terminal = spelling.rpartition(".")
    inside = bool(separator and terminal and ENTRY_LEAD.search(lead))
    key = terminal if inside else spelling
    return _terminal_tag(key), key, lead if inside else None


def _terminal_tag(spelling: str) -> int:
    """Numeric identity at the end of a plain or indexed key."""
    tag, bracket, index = spelling.partition("[")
    indexed = bool(bracket) and spelling.endswith("]") and index[:-1].isdigit()
    numeric = tag.isascii() and tag.isdigit() and len(tag) <= 9
    return int(tag) if numeric and (not bracket or indexed) else 0
