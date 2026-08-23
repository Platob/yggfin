"""Base for stable banded integer codes."""

from __future__ import annotations

import enum
import functools
from typing import Any, Self


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
