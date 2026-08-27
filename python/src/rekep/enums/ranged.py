"""Bases for the stable codes: banded integers, and ASCII packed into one.

Two mechanisms, and which one a code uses is a property of the code. A banded
integer orders its members and degrades an unknown value to its band; an
ASCII mnemonic packs a fixed width of characters -- NUL-padded on the right
-- into the integer the column stores, so the stored value is readable
without a lookup and exact under a pushed code-set filter.
"""

from __future__ import annotations

import enum
import functools
import json
from collections import OrderedDict
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Self

import pyarrow

#: A code an ASCII enum learnt at runtime is remembered so the next read of the
#: same value is the same member, bounded so a stream of junk cannot grow it
#: without limit.
_ASCII_REGISTERED_LIMIT = 4_096
_ASCII_REGISTERED: dict[type[enum.IntEnum], OrderedDict[int, enum.IntEnum]] = {}
_ASCII_ALIASES: dict[type[enum.IntEnum], dict[str, str]] = {}

#: The extension singleton of every ASCII enum that has asked for one, by the
#: enum's name -- what `__arrow_ext_deserialize__` hands back so a round trip
#: through IPC lands on the same instance.
_ASCII_TYPES: dict[str, AsciiType] = {}


class Ranged(enum.IntEnum):
    """Banded integer code carrying its FIX character."""

    WIDTH = enum.nonmember(100)
    PRIVATE = enum.nonmember(9000)

    def __new__(cls, value: int, fix_code: str = "") -> Self:
        member = int.__new__(cls, value)
        member._value_ = value
        member._fix_code = fix_code
        member._band = value // cls.WIDTH * cls.WIDTH
        return member

    @property
    def band(self) -> int:
        """Band floor used by range predicates."""
        return self._band

    @classmethod
    def band_of(cls, value: int) -> int:
        """Return a raw code's band floor."""
        return int(value) // cls.WIDTH * cls.WIDTH

    def into_fix(self) -> str:
        """Return the FIX character, or empty when none exists."""
        return self._fix_code

    @classmethod
    def from_code(cls, value: Any, default: Self | None = None) -> Self:
        """Read a code, degrading unknown members to their band."""
        try:
            return cls(int(value))
        except (ValueError, TypeError):
            pass
        try:
            return cls(cls.band_of(value))
        except (ValueError, TypeError):
            return default if default is not None else cls(0)

    @classmethod
    def from_fix(cls, code: Any, default: Self | None = None) -> Self:
        """Read a case-sensitive FIX character."""
        member = cls._fix_codes().get(str(code).strip() if code is not None else "")
        if member is not None:
            return member
        return default if default is not None else cls(0)

    @classmethod
    def _missing_(cls, value: Any) -> Self | None:
        if isinstance(value, str):
            return cls.__members__.get(value.strip().upper())
        return None

    @classmethod
    @functools.cache
    def _fix_codes(cls) -> dict[str, Self]:
        return {member._fix_code: member for member in cls if member._fix_code}


class AsciiType(pyarrow.ExtensionType):
    """One ASCII enum's Arrow type: its integer storage, named for the enum.

    A singleton per enum: `into_arrow_type` builds it once, registers it with
    Arrow under `rekep.ascii.<Name>`, and always answers with that same
    instance -- so two schemas naming one enum share one type, and an IPC
    round trip resolves back to it.
    """

    def __init__(self, storage: pyarrow.DataType, name: str) -> None:
        self._ascii_name = name
        super().__init__(storage, f"rekep.ascii.{name}")

    def __arrow_ext_serialize__(self) -> bytes:
        return self._ascii_name.encode("utf-8")

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pyarrow.DataType, serialized: bytes
    ) -> AsciiType:
        named = serialized.decode("utf-8")
        known = _ASCII_TYPES.get(named)
        return known if known is not None else cls(storage_type, named)


class AsciiInt32(enum.IntEnum):
    """A printable ASCII code packed big-endian into the `int32` it stores.

    The code is NUL-padded on the right to exactly four bytes, so every
    spelling has one stored value and a raw column dump reads back as text.
    The set is closed by default -- a stored integer either is a compiled
    code or it is `UNKNOWN`, keeping a Python answer and a pushed code-set
    filter on the same rows. A vocabulary that must learn codes at runtime
    (`MIC`, `Currency`) opts in through `_registers_unknown`, and even there
    only an exact round-trip of the stored bytes registers.
    """

    BYTE_WIDTH = enum.nonmember(4)

    def __new__(cls, value: int | str, fix_code: str = "", rank: int | None = None) -> Self:
        text = str(value).strip().upper() if isinstance(value, str) else ""
        packed = cls._pack(text) if text else int(value)
        member = int.__new__(cls, packed)
        member._value_ = packed
        member._code = text
        member._fix_code = fix_code
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
        """The wire spelling: a declared FIX code, or an open set's own code.

        An open vocabulary's members *are* wire values -- `USD`, `XPAR` --
        while a closed mnemonic set writes only the codes it declared;
        a grouping marker with no wire code renders as nothing.
        """
        if self._fix_code:
            return self._fix_code
        return self.code if type(self)._registers_unknown() else ""

    def __str__(self) -> str:
        return self.code

    # -- parsers ------------------------------------------------------------

    @classmethod
    def from_str(cls, value: Any) -> Self:
        """Parse a spelling: a member name, an alias, or a code.

        An open vocabulary registers a valid code it had not seen; a closed
        one answers `UNKNOWN` for anything not compiled.
        """
        if isinstance(value, cls):
            return value
        if isinstance(value, int):
            return cls.from_int(value)
        return cls._from_text(str(value) if value is not None else "")

    @classmethod
    def from_int(cls, value: Any, default: Self | None = None) -> Self:
        """Decode a stored integer exactly: a known code, or `UNKNOWN`.

        A compiled or already-registered code answers directly. An open
        vocabulary also reads a well-formed unknown code back as a newly
        registered member -- exactly the bytes stored, never a respelling --
        while a closed one answers only on its compiled codes, so the scalar
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

        The exact wire code first and case-sensitively. Where that misses, a
        word spelling of a *compiled* member answers -- bridges render
        `SIDE=buy` and `TIMEINFORCE=gtd` where the wire says `1` and `6` --
        and nothing here registers a new code: an unknown value is the
        default, not a member invented from wire noise.
        """
        raw = str(value).strip() if value is not None else ""
        known = cls._fix_codes().get(raw)
        if known is not None:
            return known
        if cls._fix_codes():
            worded = cls.worded_codes().get(cls._normalise(raw))
            if worded is not None:
                return worded
            return default if default is not None else cls.UNKNOWN
        parsed = cls.from_str(value)
        return default if parsed is cls.UNKNOWN and default is not None else parsed

    @classmethod
    def from_stored(cls, value: Any, default: Self | None = None) -> Self:
        """Read a stored id: today's packed code, or a previous release's ordinal.

        Ranks predate the mnemonic encoding as the stored values themselves,
        so a store written before a recode still resolves to the member its
        rank names. Anything else is `UNKNOWN` (or `default`).
        """
        member = cls.from_int(value, default=None)
        if member is not cls.UNKNOWN:
            return member
        try:
            packed = int(value)
        except (TypeError, ValueError):
            return default if default is not None else cls.UNKNOWN
        ranked = cls.ranked().get(packed)
        if ranked is not None:
            return ranked
        return default if default is not None else cls.UNKNOWN

    @classmethod
    def register(cls, value: str, *, aliases: Any = ()) -> Self:
        """Register one code and optional source aliases.

        Open vocabularies only: a closed set's codes are compiled, and asking
        it to learn one is a programming error rather than data. A value that
        does not spell a valid code is `UNKNOWN`, not an exception -- the
        callers sit on data paths.
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

    @classmethod
    @functools.cache
    def ranked(cls) -> Mapping[int, Self]:
        """Compiled members by declared rank -- the ids an ordinal release stored."""
        return MappingProxyType({member._rank: member for member in cls})

    @classmethod
    @functools.cache
    def worded_codes(cls) -> Mapping[str, Self]:
        """Wire-backed compiled members by normalized name and built-in alias.

        Only members that carry a FIX code: the wire spelling a code misses
        resolves here when a human wrote the meaning out, and to nothing
        otherwise. A member with no code -- `TimeInForce`'s ordering markers
        -- is not something a wire value can mean, so it never answers.
        """
        found: dict[str, Self] = {
            name: member for name, member in cls.__members__.items() if member and member._fix_code
        }
        for alias, target in cls._built_in_aliases().items():
            member = cls.__members__.get(target)
            if member and member._fix_code:
                found.setdefault(alias, member)
        return MappingProxyType(found)

    # -- the shape a column declares ----------------------------------------

    @classmethod
    def into_arrow_type(cls) -> AsciiType:
        """This enum's Arrow extension type, one instance per enum.

        Registered with Arrow on first ask, so a schema carrying it survives
        IPC; the underlying storage stays the plain integer column every
        engine reads.
        """
        found = _ASCII_TYPES.get(cls.__name__)
        if found is None:
            storage = pyarrow.int32() if cls.BYTE_WIDTH <= 4 else pyarrow.int64()
            found = AsciiType(storage, cls.__name__)
            _ASCII_TYPES[cls.__name__] = found
            try:
                pyarrow.register_extension_type(found)
            except pyarrow.lib.ArrowKeyError:
                pass
        return found

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
        wires = {member._fix_code: member.code for member in cls if member._fix_code}
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
        member._fix_code = ""
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
    def _canonical(cls, raw: str) -> str:
        text = cls._normalise(raw)
        named = cls.__members__.get(text)
        if named is not None:
            return named.code
        aliased = {**cls._built_in_aliases(), **_ASCII_ALIASES.get(cls, {})}.get(text, text)
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

    @classmethod
    @functools.cache
    def _fix_codes(cls) -> dict[str, Self]:
        return {member._fix_code: member for member in cls if member._fix_code}


class AsciiInt64(AsciiInt32):
    """An ASCII code of up to eight bytes stored as one signed `int64`.

    Exactly `AsciiInt32` with twice the width: same packing, same parsers,
    same closed-by-default registration -- for vocabularies whose codes
    outgrow four characters.
    """

    BYTE_WIDTH = enum.nonmember(8)
