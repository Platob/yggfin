"""The base every stable code is built on: ASCII packed into one integer.

A code packs left-justified into a fixed width, padded with trailing NULs,
so the stored integer reads back as text and orders exactly as the text
does. A member may also declare a *rank*, and a vocabulary ranked in
hundred-wide bands answers `band` and the pushed code sets
`ranked_at_least` and `ranked_below`.
"""

from __future__ import annotations

import enum
import functools
import json
import re
from collections import OrderedDict
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Self

import pyarrow

#: A code an ASCII enum learnt at runtime is remembered so the next read of the
#: same value is the same member, bounded so a stream of junk cannot grow it
#: without limit.
_ASCII_REGISTERED_LIMIT = 4_096

#: Where a feed's own ranks begin: everything from here up belongs to whoever
#: runs it, so nothing this package declares may reach it.
PRIVATE_RANK = 9_000
#: The abbreviation the dictionary puts in brackets after a value's prose --
#: `Good Till Cancel (GTC)` -- which is where the short name this package uses
#: is spelled, and nowhere else in the record.
_BRACKETED = re.compile(r"\(([A-Za-z0-9 ]+)\)")

_ASCII_REGISTERED: dict[type[enum.IntEnum], OrderedDict[int, enum.IntEnum]] = {}
_ASCII_ALIASES: dict[type[enum.IntEnum], dict[str, str]] = {}


class Ascii32(enum.IntEnum):
    """A printable ASCII code packed big-endian into the `int32` it stores.

    The code sits left-justified, padded with trailing NULs to exactly four
    bytes, so the stored integer orders exactly as the text does and a raw
    column dump reads back as its spelling. The set is closed by default: a
    stored integer is a compiled code or it is `UNKNOWN`, which keeps a
    Python answer and a pushed code-set filter on the same rows. A
    vocabulary that learns codes at runtime (`Protocol`, `MIC`, `Currency`)
    opts in through `_registers_unknown`, and even there only an exact round
    trip of the stored bytes registers.
    """

    BYTE_WIDTH = enum.nonmember(4)

    #: How wide a rank band is, for the vocabularies that declare ranks in
    #: bands. A code that ranks itself has one band of its own.
    WIDTH = enum.nonmember(100)

    #: The FIX field whose enumerated values this vocabulary codes, where it
    #: codes one. The wire codes themselves are **not** declared here: they
    #: belong to that field, the dictionary states them, and a second copy on
    #: each member is a second thing to keep in step with a rescrape. A
    #: vocabulary that codes no single field -- an open one, or one whose
    #: meaning is spread across several tags -- names nothing.
    FIX_FIELD = enum.nonmember("")

    def __new__(cls, value: int | str, rank: int | None = None) -> Self:
        text = str(value).strip().upper() if isinstance(value, str) else ""
        packed = cls._pack(text) if text else int(value)
        member = int.__new__(cls, packed)
        member._value_ = packed
        member._code = text
        member._rank = packed if rank is None else rank
        return member

    # -- what a member says of itself ---------------------------------------

    @property
    def code(self) -> str:
        """Protocol spelling, or empty for `UNKNOWN`."""
        return self._code

    @property
    def rank(self) -> int:
        """Ordering rank: the packed code unless the member declares one."""
        return self._rank

    def into_str(self) -> str:
        """Return the ISO/FIX spelling."""
        return self.code

    def into_fix(self) -> str:
        """The wire spelling: the dictionary's code for this member, or none.

        An open vocabulary's members *are* wire values -- `USD`, `XPAR` --
        while a closed mnemonic set writes whatever code the field it codes
        gives it; a grouping marker no wire value means renders as nothing.
        """
        wired = type(self)._wire_codes().get(self, "")
        if wired:
            return wired
        return self.code if type(self)._registers_unknown() else ""

    def __str__(self) -> str:
        return self.code

    # -- parsers ------------------------------------------------------------

    @classmethod
    def from_str(cls, value: Any) -> Self:
        """Parse a spelling: a member name, an alias, or a code.

        An open vocabulary registers a valid code it had not seen; a closed
        one answers `UNKNOWN`. An integer is a stored code.
        """
        if isinstance(value, cls):
            return value
        if isinstance(value, int):
            return cls.from_int(value)
        return cls._from_text(str(value) if value is not None else "")

    @classmethod
    def from_int(cls, value: Any, default: Self | None = None) -> Self:
        """Decode a stored integer: a known code, or `UNKNOWN`.

        An open vocabulary reads a well-formed unknown code back as a newly
        registered member -- exactly the bytes stored, never a respelling.
        A closed one answers only on its compiled codes, so the scalar
        reader and a pushed code-set filter keep the same rows.
        """
        try:
            packed = int(value)
        except (TypeError, ValueError):
            return default if default is not None else cls.UNKNOWN
        known = cls._value2member_map_.get(packed)
        if known is None:
            known = _ASCII_REGISTERED.setdefault(cls, OrderedDict()).get(packed)
        if isinstance(known, cls):
            return known
        if not cls._registers_unknown():
            return default if default is not None else cls.UNKNOWN
        half = 1 << (8 * cls.BYTE_WIDTH - 1)
        if packed < -half or packed >= half:
            return default if default is not None else cls.UNKNOWN
        try:
            text = cls._decode(packed)
        except (OverflowError, UnicodeDecodeError):
            return default if default is not None else cls.UNKNOWN
        if not cls._valid(text) or cls._pack(text) != packed:
            return default if default is not None else cls.UNKNOWN
        return cls._register(packed, text)

    @classmethod
    def from_fix(cls, value: Any, default: Self | None = None) -> Self:
        """Parse a short protocol value.

        The exact wire code first, case-sensitively; where that misses, a
        word spelling of a *compiled* member answers, because bridges render
        `SIDE=buy` where the wire says `1`. A vocabulary with no declared
        wire codes speaks its own codes there, so its values parse as
        spellings.
        """
        raw = str(value).strip() if value is not None else ""
        known = cls._fix_codes().get(raw)
        if known is not None:
            return known
        if cls._fix_codes():
            from rekep.fields import encoded_key

            worded = cls.worded_codes().get(encoded_key(raw))
            if worded is not None:
                return worded
            return default if default is not None else cls.UNKNOWN
        parsed = cls.from_str(value)
        return default if parsed is cls.UNKNOWN and default is not None else parsed

    @classmethod
    def register(cls, value: str, *, aliases: Any = ()) -> Self:
        """Register one code and optional source aliases.

        Open vocabularies only. A value that does not spell a valid code is
        `UNKNOWN`, not an exception: the callers sit on data paths.
        """
        if not cls._registers_unknown():
            raise TypeError(f"{cls.__name__} is a closed set; its codes are compiled")
        text = cls._canonical(str(value) if value is not None else "")
        if not cls._valid(text):
            return cls.UNKNOWN
        packed = cls._pack(text)
        known = cls._value2member_map_.get(packed)
        if known is None:
            known = _ASCII_REGISTERED.setdefault(cls, OrderedDict()).get(packed)
        member = known if isinstance(known, cls) else cls._register(packed, text)
        if aliases:
            configured = _ASCII_ALIASES.setdefault(cls, {})
            configured.update({cls._normalise(alias): member.code for alias in aliases})
            cls._from_text.cache_clear()
        return member

    # -- lookups ------------------------------------------------------------

    @property
    def band(self) -> Self:
        """The band-floor member this code's rank sits in, or the code itself.

        A ranked vocabulary says what a detailed code broadly means --
        `FILLED` is a `DONE`. One that ranks each member by its own packed
        code declares no floors, so every code is its own band.
        """
        return type(self)._bands().get(self._rank // self.WIDTH * self.WIDTH, self)

    @classmethod
    @functools.cache
    def _bands(cls) -> Mapping[int, Self]:
        return MappingProxyType(
            {member._rank: member for member in cls if member._rank % cls.WIDTH == 0}
        )

    @classmethod
    def ranked_at_least(cls, floor: Self) -> tuple[int, ...]:
        """Stored codes ranked at or above `floor`, for a pushed scan filter."""
        return tuple(int(member) for member in cls if member._rank >= floor._rank)

    @classmethod
    def ranked_below(cls, floor: Self) -> tuple[int, ...]:
        """Stored codes ranked below `floor`, for a pushed scan filter."""
        return tuple(int(member) for member in cls if member._rank < floor._rank)

    @classmethod
    @functools.cache
    def worded_codes(cls) -> Mapping[str, Self]:
        """Wire-backed compiled members by normalized name and built-in alias.

        Only members the field this vocabulary codes gives a wire value, so an
        ordering marker no wire value can ever mean does not answer.
        """
        wired = cls._wire_codes()
        from rekep.fields import encoded_key

        found: dict[str, Self] = {
            encoded_key(name): member
            for name, member in cls.__members__.items()
            if member and member in wired
        }
        for alias, target in cls._built_in_aliases().items():
            member = cls.__members__.get(target)
            folded = encoded_key(alias)
            if folded and member and member in wired:
                found.setdefault(folded, member)
        return MappingProxyType(found)

    # -- the shape a column declares ----------------------------------------

    @classmethod
    @functools.cache
    def into_arrow_type(cls) -> pyarrow.DictionaryType:
        """This enum's Arrow type: a dictionary of its codes, indexed as wide
        as the packed value a column stores, so the index type is also the
        storage a builder declares."""
        index = pyarrow.int32() if cls.BYTE_WIDTH <= 4 else pyarrow.int64()
        return pyarrow.dictionary(index, pyarrow.utf8())

    @classmethod
    def into_arrow_array(cls, values: Any) -> pyarrow.DictionaryArray:
        """A stored code column rendered as this enum spelled out.

        Arrow indexes a dictionary by position, not by the stored value, so
        the codes resolve to members and the members to their spellings; an
        unknown code renders as null.
        """
        compute = pyarrow.compute
        column = values.combine_chunks() if isinstance(values, pyarrow.ChunkedArray) else values
        index = cls.into_arrow_type().index_type
        stored = column.cast(index, safe=False)
        spellings = [member.code for member in cls]
        codes = pyarrow.array([int(member) for member in cls], index)
        positions = compute.index_in(stored, value_set=codes).cast(index, safe=False)
        return pyarrow.DictionaryArray.from_arrays(
            positions, pyarrow.array(spellings, pyarrow.utf8())
        )

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        """Describe the portable storage encoding."""
        metadata = {
            "encoding": "ascii-big-endian",
            "byte_width": str(cls.BYTE_WIDTH),
            "padding": "nul-right",
        }
        aliases = {
            **{
                name: member.code
                for name, member in cls.__members__.items()
                if member.code and name != member.code
            },
            **cls._built_in_aliases(),
        }
        if aliases:
            metadata["aliases"] = json.dumps(aliases, separators=(",", ":"), sort_keys=True)
        wires = {code: member.code for code, member in cls._fix_codes().items()}
        if wires:
            metadata["fix_aliases"] = json.dumps(wires, separators=(",", ":"), sort_keys=True)
        return metadata

    # -- machinery ----------------------------------------------------------

    @classmethod
    @functools.lru_cache(maxsize=4_096)
    def _from_text(cls, raw: str) -> Self:
        text = cls._canonical(raw)
        if not cls._valid(text):
            return cls.UNKNOWN
        packed = cls._pack(text)
        known = cls._value2member_map_.get(packed)
        if known is None:
            known = _ASCII_REGISTERED.setdefault(cls, OrderedDict()).get(packed)
        if isinstance(known, cls):
            return known
        if not cls._registers_unknown():
            return cls.UNKNOWN
        return cls._register(packed, text)

    @classmethod
    def _register(cls, packed: int, text: str) -> Self:
        member = int.__new__(cls, packed)
        member._name_ = text
        member._value_ = packed
        member._code = text
        member._rank = packed
        registered = _ASCII_REGISTERED.setdefault(cls, OrderedDict())
        registered[packed] = member
        registered.move_to_end(packed)
        if len(registered) > _ASCII_REGISTERED_LIMIT:
            registered.popitem(last=False)
        return member

    @classmethod
    def _missing_(cls, value: Any) -> Self:
        return cls.from_str(value) if isinstance(value, str) else cls.from_int(value)

    @classmethod
    def _normalise(cls, raw: str) -> str:
        return raw.strip().upper()

    @classmethod
    def aliased_codes(cls) -> dict[str, str]:
        """Every alias this enum resolves, normalized spelling to code."""
        return {**cls._built_in_aliases(), **_ASCII_ALIASES.get(cls, {})}

    @classmethod
    def _canonical(cls, raw: str) -> str:
        text = cls._normalise(raw)
        named = cls.__members__.get(text)
        if named is not None:
            return named.code
        fixed = cls._fix_codes().get(text)
        if fixed is not None:
            return fixed.code
        named = cls._named(raw)
        if named is not None:
            return named.code
        from rekep.fields import encoded_key

        aliases = cls.aliased_codes()
        aliased = aliases.get(text)
        folded = encoded_key(raw)
        if aliased is None and folded:
            aliased = next(
                (target for alias, target in aliases.items() if encoded_key(alias) == folded),
                None,
            )
        aliased = aliased or text
        named = cls.__members__.get(aliased)
        if named is not None:
            return named.code
        fixed = cls._fix_codes().get(aliased)
        return fixed.code if fixed is not None else aliased

    @classmethod
    def _valid(cls, text: str) -> bool:
        try:
            raw = text.encode("ascii")
        except UnicodeEncodeError:
            return False
        return bool(text) and len(raw) <= cls.BYTE_WIDTH and all(32 <= byte <= 126 for byte in raw)

    @classmethod
    def _pack(cls, text: str) -> int:
        raw = text.encode("ascii").ljust(cls.BYTE_WIDTH, b"\0")
        return int.from_bytes(raw, "big", signed=True)

    @classmethod
    def _decode(cls, packed: int) -> str:
        width = cls.BYTE_WIDTH
        raw = (packed & ((1 << (8 * width)) - 1)).to_bytes(width, "big")
        text = raw.rstrip(b"\0")
        if b"\0" in text:
            raise UnicodeDecodeError("ascii", raw, 0, width, "embedded NUL")
        return text.decode("ascii")

    @classmethod
    def _registers_unknown(cls) -> bool:
        return False

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        return {}

    # -- the wire codes, which belong to the dictionary ----------------------
    #
    # A member used to carry its FIX code beside its spelling. That made the
    # enum a second copy of one field's enumerated values -- and the two could
    # disagree, silently, in the direction that mis-parses a message. The
    # codes are read from the dictionary now, matched to members by the
    # spellings the registry declares for each value, and cached: `from_fix` runs
    # once per row and must stay a dictionary lookup.

    @classmethod
    def _fix_declaration(cls) -> Any:
        """The registry field this vocabulary codes, or None where it codes none.

        Imported here rather than at module scope: `rekep.fields` reaches this
        module for `EventType` and `State`, so a top-level import of the
        registry would close a cycle. By the time anything asks for a wire
        code the package is loaded.
        """
        if not cls.FIX_FIELD:
            return None
        from rekep.fix.registry import FixRegistry

        return FixRegistry.from_builtin().scalar(cls.FIX_FIELD)

    @classmethod
    def _named(cls, spelled: str) -> Self | None:
        """The member one spelling names, by member name or built-in alias.

        Deliberately not `from_str`: that consults the wire codes, which is
        the thing this is being asked to build.
        """
        text = cls._normalise(spelled)
        member = cls.__members__.get(text)
        if member is not None:
            return member
        aliased = cls._built_in_aliases().get(text)
        if aliased:
            return cls.__members__.get(aliased)
        from rekep.fields import encoded_key

        folded = encoded_key(spelled)
        if not folded:
            return None
        for name, member in cls.__members__.items():
            if encoded_key(name) == folded:
                return member
        for alias, target in cls._built_in_aliases().items():
            if encoded_key(alias) == folded:
                return cls.__members__.get(target)
        return None

    @classmethod
    def _member_of(cls, value: Any) -> Self | None:
        """The member one enumerated value names, by every spelling it has.

        The leading source alias first, then its prose, then the abbreviation
        the prose puts in brackets -- `Immediate Or Cancel (IOC)`.
        """
        spellings: list[str] = [*value.aliases]
        if value.meaning:
            spellings.append(value.meaning)
            spellings.extend(_BRACKETED.findall(value.meaning))
        for spelled in spellings:
            found = cls._named(spelled)
            if found is not None:
                return found
        return None

    @classmethod
    @functools.cache
    def _fix_codes(cls) -> Mapping[str, Self]:
        """`{wire code: member}` for the field this vocabulary codes."""
        declared = cls._fix_declaration()
        if declared is None:
            return MappingProxyType({})
        found: dict[str, Self] = {}
        for value in declared.fix.enumerated:
            member = cls._member_of(value)
            if member is not None:
                found.setdefault(value.value, member)
        return MappingProxyType(found)

    @classmethod
    @functools.cache
    def _wire_codes(cls) -> Mapping[Self, str]:
        """`{member: wire code}` -- the inverse, for rendering a value out."""
        found: dict[Self, str] = {}
        for code, member in cls._fix_codes().items():
            found.setdefault(member, code)
        return MappingProxyType(found)


class Ascii64(Ascii32):
    """An ASCII code of up to eight bytes stored as one signed `int64`.

    `Ascii32` at twice the width, for codes that outgrow four characters.
    """

    BYTE_WIDTH = enum.nonmember(8)
