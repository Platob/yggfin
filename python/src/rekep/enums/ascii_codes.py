"""The base every stable code is built on: ASCII packed into one integer.

A code packs a fixed width of characters -- right-justified, padded with
leading NULs -- into the integer the column stores, so the stored value is
readable without a lookup and exact under a pushed code-set filter. Order
is a separate fact: a member may declare a *rank*, and a vocabulary that
ranks in hundred-wide bands answers "what does this broadly mean" through
`band` and "which codes rank at least this" through `ranked_at_least`,
without the stored value having to be an ordinal.
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

#: Where a feed's own ranks begin: everything from here up belongs to whoever
#: runs it, so nothing this package declares may reach it.
PRIVATE_RANK = 9_000
_ASCII_REGISTERED: dict[type[enum.IntEnum], OrderedDict[int, enum.IntEnum]] = {}
_ASCII_ALIASES: dict[type[enum.IntEnum], dict[str, str]] = {}


class AsciiInt32(enum.IntEnum):
    """A printable ASCII code packed big-endian into the `int32` it stores.

    The code sits right-justified -- padded with leading NULs to exactly
    four bytes -- so every spelling has one stored value, a raw column dump
    reads back as text, and `EUR` stores as `\0EUR`: the plain integer of
    its own bytes, below every four-letter code.
    The set is closed by default -- a stored integer either is a compiled
    code or it is `UNKNOWN`, keeping a Python answer and a pushed code-set
    filter on the same rows. A vocabulary that must learn codes at runtime
    (`MIC`, `Currency`) opts in through `_registers_unknown`, and even there
    only an exact round-trip of the stored bytes registers.
    """

    BYTE_WIDTH = enum.nonmember(4)

    #: How wide a rank band is, for the vocabularies that declare ranks in
    #: bands. A code that ranks itself has one band of its own.
    WIDTH = enum.nonmember(100)

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
        one answers `UNKNOWN` for anything not compiled. An integer here is
        a stored id and reads through `from_stored`, so a value a previous
        release wrote still names its member.
        """
        if isinstance(value, cls):
            return value
        if isinstance(value, int):
            return cls.from_stored(value)
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
        and a closed set registers nothing: an unknown value is the default,
        not a member invented from wire noise. An open vocabulary with no
        declared wire codes speaks its own codes on the wire, so its values
        parse as spellings -- aliases resolve, and a well-formed unlisted
        code registers.
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
        """Read a stored id from any generation this vocabulary has written.

        A column outlives the release that filled it, so reading one means
        reading every id it may hold: today's packed code first, then the
        rank -- which is what an ordinal release stored -- and finally the
        bytes themselves, read as a code under any padding or width this
        family has used. A spelling a member has since been renamed away
        from resolves through its aliases, so `ORDR` still names the kind
        now spelled `ORDER`. Anything no generation ever wrote is `UNKNOWN`
        (or `default`).

        `from_int` stays the strict reader: it answers only on today's codes,
        which is what keeps a Python answer and a pushed code-set filter on
        the same rows.
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
        superseded = cls._from_superseded(packed)
        if superseded is not None:
            return superseded
        return default if default is not None else cls.UNKNOWN

    @classmethod
    def _from_superseded(cls, packed: int) -> Self | None:
        """The member an earlier packing of this code stored, if any.

        Every generation stored the same thing -- printable ASCII bytes of a
        code, padded with NULs -- and differed only in which side the
        padding sat on and how wide the integer was. So the bytes are read
        back at each width this family has used, from either end, and the
        text is resolved through the ordinary spelling path.

        Only bytes a generation would actually have written answer: packing
        canonicalizes a spelling before storing it, so anything that is not
        already canonical was never a stored id and is left unknown rather
        than respelled into a member.
        """
        for width in cls._stored_widths():
            if packed < -(1 << (8 * width - 1)) or packed >= 1 << (8 * width):
                continue
            raw = (packed & ((1 << (8 * width)) - 1)).to_bytes(width, "big")
            for text in (raw.lstrip(b"\0"), raw.rstrip(b"\0")):
                if not text or b"\0" in text:
                    continue
                try:
                    spelled = text.decode("ascii")
                except UnicodeDecodeError:
                    continue
                if spelled != cls._normalise(spelled):
                    continue
                found = cls._from_text(spelled)
                if found is not cls.UNKNOWN:
                    return found
        return None

    @classmethod
    def _stored_widths(cls) -> tuple[int, ...]:
        """Byte widths this vocabulary's stored ids have ever used, widest first."""
        return tuple(width for width in (8, 4) if width <= cls.BYTE_WIDTH)

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

    @property
    def band(self) -> Self:
        """The band-floor member this code's rank sits in, or the code itself.

        A vocabulary that declares ranks in hundred-wide bands says what a
        detailed code broadly means -- `FILLED` is a `DONE` -- without the
        stored value having to be an ordinal. One that ranks each member by
        its own packed code declares no bands, so every code is its own.
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
        """Stored codes ranked at or above `floor`, for a pushed scan filter.

        What replaces a range predicate over the stored value: the codes are
        mnemonics rather than ordinals, so a scan names the finite set it
        wants instead of a boundary.
        """
        return tuple(int(member) for member in cls if member._rank >= floor._rank)

    @classmethod
    def ranked_below(cls, floor: Self) -> tuple[int, ...]:
        """Stored codes ranked below `floor`, for a pushed scan filter."""
        return tuple(int(member) for member in cls if member._rank < floor._rank)

    @classmethod
    def ranked_between(cls, floor: Self, ceiling: Self) -> tuple[int, ...]:
        """Stored codes ranked in `[floor, ceiling)`, for a pushed scan filter."""
        return tuple(int(member) for member in cls if floor._rank <= member._rank < ceiling._rank)

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
    @functools.cache
    def into_arrow_type(cls) -> pyarrow.DictionaryType:
        """This enum's Arrow dictionary type, one instance per enum.

        A dictionary of the readable codes, indexed as wide as the packed
        value a column stores -- `int32` or `int64` by declared width -- so
        the index type is also the storage a builder declares for the column
        and `into_arrow_array` renders a stored column into this type.
        Nothing registers with Arrow: a dictionary type is a plain value
        type every engine already speaks.
        """
        index = pyarrow.int32() if cls.BYTE_WIDTH <= 4 else pyarrow.int64()
        return pyarrow.dictionary(index, pyarrow.utf8())

    @classmethod
    def into_arrow_array(cls, values: Any) -> pyarrow.DictionaryArray:
        """A stored code column rendered as this enum spelled out.

        Arrow indexes a dictionary by position, not by the value stored, so
        the packed codes are resolved to members and the members to their
        spellings; a code no generation of this vocabulary wrote renders as
        null rather than as a number nobody can read.
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
            "padding": "nul-left",
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
        """`Code(value)` reads a value, so it reads every generation of one."""
        return cls.from_str(value) if isinstance(value, str) else cls.from_stored(value)

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
        aliased = cls.aliased_codes().get(text, text)
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
        raw = text.encode("ascii").rjust(cls.BYTE_WIDTH, b"\0")
        return int.from_bytes(raw, "big", signed=True)

    @classmethod
    def _decode(cls, packed: int) -> str:
        width = cls.BYTE_WIDTH
        raw = (packed & ((1 << (8 * width)) - 1)).to_bytes(width, "big")
        text = raw.lstrip(b"\0")
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
