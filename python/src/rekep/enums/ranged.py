"""Bases for the stable codes: banded integers, and ASCII packed into one.

Two mechanisms, and which one a code uses is a property of the code. A banded
integer orders its members and degrades an unknown value to its band; an ASCII
mnemonic packs up to four characters into the int the column stores, so the
stored value is readable without a lookup.
"""

from __future__ import annotations

import enum
import functools
import json
from collections import OrderedDict
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Self

#: A code an ASCII enum learnt at runtime is remembered so the next read of the
#: same value is the same member, bounded so a stream of junk cannot grow it
#: without limit.
_ASCII_REGISTERED_LIMIT = 4_096
_ASCII_REGISTERED: dict[type[enum.IntEnum], OrderedDict[int, enum.IntEnum]] = {}
_ASCII_ALIASES: dict[type[enum.IntEnum], dict[str, str]] = {}


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


class _AsciiInt32(enum.IntEnum):
    """A short ASCII code stored as its signed big-endian `int32`."""

    def __new__(cls, value: int | str, fix_code: str = "", rank: int | None = None) -> Self:
        text = str(value).strip().upper() if isinstance(value, str) else ""
        packed = cls._pack(text) if text else int(value)
        member = int.__new__(cls, packed)
        member._value_ = packed
        member._code = text
        member._fix_code = fix_code
        member._rank = packed if rank is None else rank
        return member

    @property
    def code(self) -> str:
        """Protocol spelling, or empty for `UNKNOWN`."""
        return self._code

    def into_str(self) -> str:
        """Return the ISO/FIX spelling."""
        return self.code

    def into_fix(self) -> str:
        """Return the short protocol spelling."""
        return self._fix_code or self.code

    def __str__(self) -> str:
        return self.code

    @classmethod
    def from_str(cls, value: Any) -> Self:
        """Parse and register a valid code not compiled into the enum."""
        if isinstance(value, cls):
            return value
        if isinstance(value, int):
            return cls.from_code(value)
        return cls._from_text(str(value) if value is not None else "")

    @classmethod
    @functools.lru_cache(maxsize=4_096)
    def _from_text(cls, raw: str) -> Self:
        text = cls._canonical(raw)
        if not cls._valid(text):
            return cls.UNKNOWN
        packed = cls._pack(text)
        registered = _ASCII_REGISTERED.setdefault(cls, OrderedDict())
        known = cls._value2member_map_.get(packed) or registered.get(packed)
        if known is not None:
            return known
        if not cls._registers_unknown():
            return cls.UNKNOWN
        member = int.__new__(cls, packed)
        member._name_ = text
        member._value_ = packed
        member._code = text
        member._fix_code = ""
        member._rank = packed
        registered[packed] = member
        registered.move_to_end(packed)
        if len(registered) > _ASCII_REGISTERED_LIMIT:
            registered.popitem(last=False)
        return member

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
    @functools.cache
    def worded_codes(cls) -> Mapping[str, Self]:
        """Compiled members by normalized name and built-in alias.

        Only compiled members: the wire spelling a code misses resolves here
        when a human wrote the meaning out, and to nothing otherwise.
        """
        found: dict[str, Self] = {
            name: member for name, member in cls.__members__.items() if member
        }
        for alias, target in cls._built_in_aliases().items():
            member = cls.__members__.get(target)
            if member:
                found.setdefault(alias, member)
        return MappingProxyType(found)

    @classmethod
    def from_code(cls, value: Any, default: Self | None = None) -> Self:
        """Decode a stored `int32`, returning `UNKNOWN` when invalid."""
        try:
            packed = int(value)
        except (TypeError, ValueError):
            return default if default is not None else cls.UNKNOWN
        registered = _ASCII_REGISTERED.setdefault(cls, OrderedDict())
        known = cls._value2member_map_.get(packed) or registered.get(packed)
        if known is not None:
            return known
        if packed < -(1 << 31) or packed >= 1 << 31:
            return default if default is not None else cls.UNKNOWN
        try:
            text = cls._decode(packed)
        except (OverflowError, UnicodeDecodeError):
            return default if default is not None else cls.UNKNOWN
        parsed = cls.from_str(text)
        return default if parsed is cls.UNKNOWN and default is not None else parsed

    @classmethod
    def _missing_(cls, value: Any) -> Self:
        return cls.from_str(value) if isinstance(value, str) else cls.from_code(value)

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
        return bool(text) and len(raw) <= 4 and all(32 <= byte <= 126 for byte in raw)

    @staticmethod
    def _pack(text: str) -> int:
        raw = text.encode("ascii")
        unsigned = int.from_bytes(raw, "big")
        return unsigned - (1 << 32) if unsigned >= 1 << 31 else unsigned

    @classmethod
    def _decode(cls, packed: int) -> str:
        unsigned = packed & 0xFFFFFFFF
        width = max(1, (unsigned.bit_length() + 7) // 8)
        return unsigned.to_bytes(width, "big").decode("ascii")

    @classmethod
    def _registers_unknown(cls) -> bool:
        return True

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        return {}

    @classmethod
    @functools.cache
    def _fix_codes(cls) -> dict[str, Self]:
        return {member._fix_code: member for member in cls if member._fix_code}

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        """Describe the portable storage encoding."""
        metadata = {"encoding": "ascii-big-endian", "max_bytes": "4"}
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
        return metadata


class _FixedAsciiInt32(_AsciiInt32):
    """A printable mnemonic padded with NUL to exactly four bytes."""

    @staticmethod
    def _pack(text: str) -> int:
        raw = text.encode("ascii").ljust(4, b"\0")
        return int.from_bytes(raw, "big", signed=True)

    @classmethod
    def _decode(cls, packed: int) -> str:
        raw = (packed & 0xFFFFFFFF).to_bytes(4, "big")
        text = raw.rstrip(b"\0")
        if b"\0" in text:
            raise UnicodeDecodeError("ascii", raw, 0, 4, "embedded NUL")
        return text.decode("ascii")

    @classmethod
    def _registers_unknown(cls) -> bool:
        return False

    def into_fix(self) -> str:
        """Return the protocol value, blank for grouping members."""
        return self._fix_code

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        metadata = {**super().schema_metadata(), "byte_width": "4", "padding": "nul-right"}
        metadata.pop("max_bytes", None)
        wires = {member._fix_code: member.code for member in cls if member._fix_code}
        if wires:
            metadata["fix_aliases"] = json.dumps(wires, separators=(",", ":"), sort_keys=True)
        return metadata
