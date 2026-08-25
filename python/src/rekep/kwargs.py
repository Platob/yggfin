"""One protocol-neutral shape for ordered structured key/value arguments."""

from __future__ import annotations

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


@scalar(slots=True)
class Kwarg(Convertible, Mapping[str, Any]):
    """One ordered structured key/value argument."""

    tag: Annotated[int, Field(arrow_type=TAG)] = 0
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
        """Normalize direct construction exactly like stored arguments."""
        spelling = str(self.key).removeprefix("#")
        if self.namespace is None and self.comp is None:
            inferred, spelling, namespace, comp = _key_parts(spelling)
            self.namespace = namespace
            self.comp = comp
        else:
            inferred = _terminal_tag(spelling)
            self.namespace = None if self.namespace is None else str(self.namespace)
            self.comp = None if self.comp is None else str(self.comp)
        self.tag = int(self.tag or inferred)
        self.key = spelling
        self.value = "" if self.value is None else str(self.value)

    @classmethod
    def from_stored(cls, entry: Any) -> Kwarg:
        """Normalize a scalar, mapping, or pair into one argument."""
        if isinstance(entry, cls):
            return entry
        if isinstance(entry, Mapping):
            value = entry.get("value")
            spelling = str(entry["key"]).removeprefix("#")
            namespace = entry.get("namespace")
            comp = entry.get("comp")
            parsed = _key_parts(spelling) if namespace is None and comp is None else None
            stored_tag = entry.get("tag")
            inferred_tag = parsed[0] if parsed is not None else _terminal_tag(spelling)
            return cls(
                tag=int(stored_tag or inferred_tag),
                key=parsed[1] if parsed is not None else spelling,
                value="" if value is None else str(value),
                namespace=parsed[2] if parsed is not None else namespace,
                comp=parsed[3] if parsed is not None else comp,
            )
        key, value = entry
        tag, spelling, namespace, comp = _key_parts(str(key).removeprefix("#"))
        return cls(
            tag=tag,
            key=spelling,
            value="" if value is None else str(value),
            namespace=namespace,
            comp=comp,
        )

    def __getitem__(self, name: str) -> Any:
        if name not in KWARG_PARTS:
            raise KeyError(name)
        return getattr(self, name)

    def __iter__(self):
        return iter(KWARG_PARTS)

    def __len__(self) -> int:
        return len(KWARG_PARTS)

    @classmethod
    def looks_structured_arrow(cls, messages: Any) -> Any:
        """Which rows contain two delimiter-separated assignments."""
        from rekep.text.kwargs import looks_structured_arrow

        return looks_structured_arrow(messages)

    @classmethod
    def parse_arrow(cls, messages: Any) -> Any:
        """Split text into ordered arguments without protocol interpretation."""
        from rekep.text.kwargs import parse_arrow

        return parse_arrow(messages)

    @classmethod
    def pop_arrow(
        cls,
        stored: Any,
        names: tuple[str, ...],
        *,
        case_sensitive: bool = True,
    ) -> tuple[Any, Any]:
        """Return the first named value per row and every other argument."""
        from rekep.text.kwargs import pop_arrow

        return pop_arrow(stored, names, case_sensitive=case_sensitive)

    @staticmethod
    def structure_arrow(keys: Any, values: Any) -> tuple[Any, Any, Any, Any, Any]:
        """Split key spellings into the stable argument members."""
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


KWARGS: pyarrow.DataType = pyarrow.list_(
    pyarrow.field("item", Kwarg.into_field().arrow_type, nullable=False)
)
KWARG_PARTS: tuple[str, ...] = tuple(member.name for member in KWARGS.value_type)


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
