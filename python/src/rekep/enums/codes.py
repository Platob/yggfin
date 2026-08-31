"""Every stable protocol and market code, over the two bases.

One module rather than one per enum: they share nothing but their base, none
of them refers to another, and a reader looking for the spelling of a code
should not have to guess which file it is in.
"""

from __future__ import annotations

import enum
import functools
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Self

import pyarrow

from rekep.enums.ascii_codes import Ascii32, Ascii64

#: ISO 10962's category letter -- the first character of a `CFICode <461>` --
#: for each kind that has one.
#:
#: Not a FIX code, which is why it is a table and not a lookup: `CFICode` is a
#: six-character string and the dictionary enumerates no values for it at all,
#: so this classification is ISO 10962's and the registry cannot answer it.
#: FIX's own classification of the same instruments is `SecurityType <167>`,
#: read separately in `rekep.market.fix`.
_CFI_CATEGORIES: Mapping[str, str] = MappingProxyType(
    {
        "EQUITY": "E",
        "DEBT": "D",
        "FUND": "C",
        "CURRENCY": "T",
        "COMMODITY": "J",
        "INDEX": "M",
        "FUTURE": "F",
        "OPTION": "O",
        "SWAP": "S",
        "WARRANT": "R",
    }
)


class AssetKind(Ascii64):
    """Tradable asset kind, banded by settlement."""

    UNKNOWN = 0
    CASH = "CASH", 100
    EQUITY = "EQUITY", 110
    DEBT = "DEBT", 120
    FUND = "FUND", 130
    CURRENCY = "CURRENCY", 140
    COMMODITY = "COMMDTY", 150
    INDEX = "INDEX", 160
    DERIVATIVE = "DERIV", 200
    FUTURE = "FUTURE", 210
    OPTION = "OPTION", 220
    SWAP = "SWAP", 230
    WARRANT = "WARRANT", 240
    FORWARD = "FORWARD", 250
    STRUCTURED = "STRUCTD", 300
    SPREAD = "SPREAD", 310
    MULTILEG = "MULTILEG", 320
    BASKET = "BASKET", 330
    FINANCING = "FINANCE", 400
    REPO = "REPO", 410
    LOAN = "LOAN", 420

    @property
    def cfi_category(self) -> str:
        """ISO 10962's category letter for this kind, or empty where it has none."""
        return _CFI_CATEGORIES.get(self.name, "")

    @classmethod
    @functools.cache
    def _by_cfi(cls) -> Mapping[str, AssetKind]:
        return MappingProxyType({letter: cls[name] for name, letter in _CFI_CATEGORIES.items()})

    @classmethod
    def from_cfi(cls, category: Any) -> AssetKind:
        """The kind one ISO 10962 category letter names, or `UNKNOWN`.

        Spelled apart from `from_fix` because a CFI letter is not a FIX value:
        `E` is an equity to ISO 10962 and nothing at all to the dictionary.
        """
        letter = str(category or "").strip().upper()[:1]
        return cls._by_cfi().get(letter, cls.UNKNOWN)

    @property
    def is_derivative(self) -> bool:
        """Whether derivative-specific instrument fields apply."""
        return self.rank >= AssetKind.DERIVATIVE.rank


class EventType(Ascii64):
    """Event kind stored as an eight-byte ASCII mnemonic, banded by rank.

    Eight bytes buy explicit spellings -- `ORDER`, `QUOTE`, `EXECUTED` --
    where four forced abbreviations. The stored value is the readable
    mnemonic; the band order the row predicates reason over rides in each
    member's rank, so a kind question compares ranks and a storage scan
    filters on the finite code sets `ranked_at_least`/`ranked_below` spell.
    """

    UNKNOWN = 0
    MISC = "MISC", 10
    INTENT = "INTENT", 100
    ORDER = "ORDER", 110
    QUOTE = "QUOTE", 120
    FACT = "FACT", 200
    EXECUTION = "EXECUTED", 210
    INSTRUMENT = "INSTRMT", 220
    STATE = "STATE", 300
    BOOK = "BOOK", 320

    @property
    def is_snapshot(self) -> bool:
        """Whether the row is a state rather than an occurrence."""
        return self.band is EventType.STATE


class MarketKind(Ascii64):
    """Order pricing and execution semantics, in stable bands."""

    UNKNOWN = 0
    MARKET = "MARKET", 100
    MARKET_ORDER = "MKTORDER", 110
    MARKET_IF_TOUCHED = "MKTIFTCH", 120
    MARKET_TO_LIMIT = "MKTTOLMT", 130
    LIMIT = "LIMIT", 200
    LIMIT_ORDER = "LMTORDER", 210
    LIMIT_ON_CLOSE = "LMTCLOSE", 220
    LIMIT_OR_BETTER = "LMTBETTR", 230
    STOP = "STOP", 300
    STOP_ORDER = "STPORDER", 310
    STOP_LIMIT = "STOPLMT", 320
    PEGGED = "PEGGED", 400
    PEGGED_ORDER = "PEGORDER", 410
    PREVIOUSLY_QUOTED = "PREVQUOT", 420
    PREVIOUSLY_INDICATED = "PREVINDC", 430
    EXECUTION = "EXEC", 500
    ORDER_STATUS = "ORDSTAT", 510
    TRADE = "TRADE", 520
    TRADE_CORRECT = "TRDCORRC", 530
    TRADE_CANCEL = "TRDCANCL", 540
    LOCKED = "LOCKED", 550
    RELEASED = "RELEASED", 560
    CLEARING = "CLEARING", 600
    CLEARING_HOLD = "CLRHOLD", 610
    RELEASED_TO_CLEARING = "RELTOCLR", 620
    ACTIVATION = "ACTIVATN", 700
    TRIGGERED = "TRIGGERD", 710

    @classmethod
    def fix_mapping(cls) -> dict[int, dict[str, MarketKind]]:
        """Return FIX tag and wire spelling mappings."""
        return {
            tag: {code: cls(member) for code, member in values.items()}
            for tag, values in _MARKET_KIND_FIX.items()
        }

    @classmethod
    def from_fix(
        cls, code: Any, default: Self | None = None, *, tag: int | str | None = None
    ) -> Self:
        """Read a tag-scoped FIX value; ambiguous values are unknown."""
        spelling = str(code).strip() if code is not None else ""
        if tag is not None:
            try:
                member = _MARKET_KIND_FIX.get(int(tag), {}).get(spelling)
            except (TypeError, ValueError):
                member = None
            return cls(member) if member is not None else default or cls.UNKNOWN
        matches = {
            member for values in _MARKET_KIND_FIX.values() if (member := values.get(spelling))
        }
        return cls(matches.pop()) if len(matches) == 1 else default or cls.UNKNOWN

    def into_fix(self, tag: int | str | None = None) -> str:
        """Return the unique wire spelling under `tag`, when one exists."""
        if tag is not None:
            try:
                mapping = _MARKET_KIND_FIX.get(int(tag), {})
            except (TypeError, ValueError):
                return ""
            codes = {code for code, member in mapping.items() if member == self}
            return codes.pop() if len(codes) == 1 else ""
        codes = {
            code
            for values in _MARKET_KIND_FIX.values()
            for code, member in values.items()
            if member == self
            and len(
                {
                    candidate.get(code)
                    for candidate in _MARKET_KIND_FIX.values()
                    if code in candidate
                }
            )
            == 1
        }
        return codes.pop() if len(codes) == 1 else ""


_MARKET_KIND_FIX: dict[int, dict[str, int]] = {
    40: {
        "1": MarketKind.MARKET_ORDER,
        "5": MarketKind.MARKET_ORDER,
        "6": MarketKind.MARKET_ORDER,
        "A": MarketKind.MARKET_ORDER,
        "C": MarketKind.MARKET_ORDER,
        "G": MarketKind.MARKET_ORDER,
        "J": MarketKind.MARKET_IF_TOUCHED,
        "K": MarketKind.MARKET_TO_LIMIT,
        "T": MarketKind.MARKET_TO_LIMIT,
        "2": MarketKind.LIMIT_ORDER,
        "8": MarketKind.LIMIT_ORDER,
        "F": MarketKind.LIMIT_ORDER,
        "I": MarketKind.LIMIT_ORDER,
        "B": MarketKind.LIMIT_ON_CLOSE,
        "7": MarketKind.LIMIT_OR_BETTER,
        "3": MarketKind.STOP_ORDER,
        "R": MarketKind.STOP_ORDER,
        "4": MarketKind.STOP_LIMIT,
        "S": MarketKind.STOP_LIMIT,
        "9": MarketKind.PEGGED_ORDER,
        "L": MarketKind.PEGGED_ORDER,
        "M": MarketKind.PEGGED_ORDER,
        "P": MarketKind.PEGGED_ORDER,
        "D": MarketKind.PREVIOUSLY_QUOTED,
        "H": MarketKind.PREVIOUSLY_QUOTED,
        "Q": MarketKind.PREVIOUSLY_QUOTED,
        "E": MarketKind.PREVIOUSLY_INDICATED,
    },
    150: {
        "0": MarketKind.ORDER_STATUS,
        "3": MarketKind.ORDER_STATUS,
        "4": MarketKind.ORDER_STATUS,
        "5": MarketKind.ORDER_STATUS,
        "6": MarketKind.ORDER_STATUS,
        "7": MarketKind.ORDER_STATUS,
        "8": MarketKind.ORDER_STATUS,
        "9": MarketKind.ORDER_STATUS,
        "A": MarketKind.ORDER_STATUS,
        "B": MarketKind.ORDER_STATUS,
        "C": MarketKind.ORDER_STATUS,
        "D": MarketKind.ORDER_STATUS,
        "E": MarketKind.ORDER_STATUS,
        "F": MarketKind.TRADE,
        "G": MarketKind.TRADE_CORRECT,
        "H": MarketKind.TRADE_CANCEL,
        "I": MarketKind.ORDER_STATUS,
        "J": MarketKind.CLEARING_HOLD,
        "K": MarketKind.RELEASED_TO_CLEARING,
        "L": MarketKind.TRIGGERED,
        "M": MarketKind.LOCKED,
        "N": MarketKind.RELEASED,
    },
}


class OptionKind(Ascii64):
    """Option direction read from FIX `PutOrCall <201>`."""

    FIX_FIELD = enum.nonmember("PutOrCall")

    UNKNOWN = 0
    PUT = "PUT", 100
    CALL = "CALL", 200


class Protocol(Ascii64):
    """Protocol grammar and resolved version in one eight-byte ASCII name.

    Open, because the vocabulary belongs to the logs and not to this package:
    `rekep.fix.rules` ships the five below, and a desk whose rule names its
    own bridge stores that name as a code without a release here. FIX service
    packs use the compact `FIX5SP2` spelling so the exact version still fits.
    """

    #: The canonical shape, and so also what a stored code may read back as:
    #: `_canonical` upper-cases, so admitting a lower-case spelling here would
    #: let one name pack as two codes -- `from_str` folding to `FIX` while
    #: `from_int` registered `fix` beside it.
    _PATTERN = enum.nonmember(re.compile(r"^[A-Z0-9._-]{1,8}$"))
    _VERSIONED = enum.nonmember(
        re.compile(
            r"^(?P<family>FIXML|FXML|FIX|UL)[._-]?"
            r"(?P<major>[0-9]+)(?:\.(?P<minor>[0-9]+))?"
            r"(?:[._-]?SP(?P<servicepack>[0-9]+))?$"
        )
    )

    UNKNOWN = 0
    """No name resolved; a rule declaring one is a configuration error."""

    FIX = "FIX"
    """Numbered FIX tags alone."""

    FIXML = "FIXML"
    """Numbered tags and named keys together."""

    XML = "XML"
    """Structured XML events without a FIX application version."""

    UL = "UL"
    """Named keys alone."""

    MISC = "MISC"
    """Operational traffic whose vocabulary is known but carries no message."""

    OTHER = "OTHER"
    """The fall-through: a line no rule recognised."""

    @classmethod
    def _valid(cls, text: str) -> bool:
        return bool(cls._PATTERN.fullmatch(text))

    @classmethod
    def _canonical(cls, raw: str) -> str:
        """Fold dotted FIX spellings into the one persisted protocol token."""
        text = cls._normalise(raw)
        if text in {"FIXT.1.1", "FIXT1.1"}:
            return "FIXT1.1"
        matched = cls._VERSIONED.fullmatch(text)
        if matched is None:
            return super()._canonical(raw)
        family = "FIXML" if matched["family"] == "FXML" else matched["family"]
        minor = matched["minor"]
        service_pack = matched["servicepack"]
        if service_pack is not None:
            if minor not in {None, "0"}:
                return super()._canonical(raw)
            # A service pack belongs to FIX 5.0 when its compact persisted
            # spelling omits the minor number: `FIX5SP2` is `5.0.SP2`.
            minor = minor or "0"
            prefix = "FXML" if family == "FIXML" else family
            return f"{prefix}{matched['major']}SP{service_pack}"
        if minor is None:
            return super()._canonical(raw)
        return f"{family}{matched['major']}.{minor}"

    @property
    def family(self) -> Protocol:
        """The grammar rule that reads this versioned protocol."""
        code = self.code
        if code == "FIXT1.1":
            return type(self).FIX
        matched = type(self)._VERSIONED.fullmatch(code)
        if matched is None or (matched["minor"] is None and matched["servicepack"] is None):
            return self
        name = "FIXML" if matched["family"] == "FXML" else matched["family"]
        return type(self).__members__.get(name, self)

    @property
    def version(self) -> str | None:
        """The exact registry version encoded in this protocol, if any."""
        code = self.code
        if code == "FIXT1.1":
            return "FIXT1.1"
        matched = type(self)._VERSIONED.fullmatch(code)
        if matched is None:
            return None
        minor = matched["minor"]
        service_pack = matched["servicepack"]
        if service_pack is not None:
            return f"{matched['major']}.{minor or '0'}.SP{service_pack}"
        return None if minor is None else f"{matched['major']}.{minor}"

    @classmethod
    def with_version(cls, protocol: Any, version: str | None) -> Protocol:
        """Combine one grammar and registry version without losing either."""
        declared = cls.from_str(protocol)
        family = declared.family
        if version is None:
            return declared
        embedded = declared.version
        if embedded is not None and not embedded.startswith("FIXT"):
            return declared
        if family not in {cls.FIX, cls.FIXML, cls.UL}:
            return declared
        normalized = str(version).strip().upper()
        if normalized in {"FIXT.1.1", "FIXT1.1"}:
            return cls.from_str("FIXT1.1") if family is cls.FIX else family
        matched = re.fullmatch(
            r"(?:FIX[._-]?)?(?P<major>[0-9]+)\.(?P<minor>[0-9]+)"
            r"(?:[._-]?SP(?P<servicepack>[0-9]+))?",
            normalized,
        )
        if matched is None:
            return family
        service_pack = matched["servicepack"]
        if service_pack is None:
            spelling = f"{family.code}{matched['major']}.{matched['minor']}"
        else:
            prefix = "FXML" if family is cls.FIXML else family.code
            spelling = (
                f"{prefix}{matched['major']}SP{service_pack}"
                if matched["minor"] == "0"
                else f"{family.code}{matched['major']}.{matched['minor']}SP{service_pack}"
            )
        combined = cls.from_str(spelling)
        return family if combined is cls.UNKNOWN else combined

    @classmethod
    def into_family_arrow(cls, protocols: Any) -> pyarrow.Array:
        """Strip persisted FIX versions from a packed protocol column."""
        compute = pyarrow.compute
        column = (
            protocols.combine_chunks() if isinstance(protocols, pyarrow.ChunkedArray) else protocols
        )
        stored = column.cast(cls.into_arrow_type().index_type, safe=False)
        found = stored
        for code in compute.drop_null(compute.unique(stored)):
            family = cls.from_int(code.as_py()).family
            found = compute.if_else(compute.equal(stored, code), int(family), found)
        return found.cast(cls.into_arrow_type().index_type, safe=False)

    @classmethod
    def into_versions_arrow(cls, protocols: Any) -> pyarrow.Array:
        """Decode exact registry versions from packed protocol tokens."""
        compute = pyarrow.compute
        column = (
            protocols.combine_chunks() if isinstance(protocols, pyarrow.ChunkedArray) else protocols
        )
        stored = column.cast(cls.into_arrow_type().index_type, safe=False)
        found = pyarrow.nulls(len(stored), pyarrow.string())
        for code in compute.drop_null(compute.unique(stored)):
            version = cls.from_int(code.as_py()).version
            if version is not None:
                found = compute.if_else(compute.equal(stored, code), version, found)
        return found

    @classmethod
    def with_versions_arrow(cls, protocols: Any, versions: Any) -> pyarrow.Array:
        """Combine grammar and exact version columns through Arrow kernels."""
        compute = pyarrow.compute
        base = cls.into_family_arrow(protocols)
        values = (
            versions.combine_chunks() if isinstance(versions, pyarrow.ChunkedArray) else versions
        )
        values = values.cast(pyarrow.string(), safe=False)
        column = (
            protocols.combine_chunks() if isinstance(protocols, pyarrow.ChunkedArray) else protocols
        )
        found = column.cast(cls.into_arrow_type().index_type, safe=False)
        embedded = cls.into_versions_arrow(found)
        authoritative = compute.and_(
            compute.is_valid(embedded),
            compute.invert(compute.fill_null(compute.starts_with(embedded, "FIXT"), False)),
        )
        for code in compute.drop_null(compute.unique(base)):
            protocol = cls.from_int(code.as_py())
            selected = compute.equal(base, code)
            available = compute.filter(values, compute.fill_null(selected, False))
            for version in compute.drop_null(compute.unique(available)):
                combined = cls.with_version(protocol, version.as_py())
                where = compute.fill_null(
                    compute.and_(selected, compute.equal(values, version)), False
                )
                where = compute.and_(where, compute.invert(authoritative))
                found = compute.if_else(where, int(combined), found)
        return found.cast(cls.into_arrow_type().index_type, safe=False)

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        return {**super().schema_metadata(), "pattern": "[A-Z0-9._-]{1,8}"}


class State(Ascii64):
    """Event lifecycle, ordered by completion.

    Each code carries its rank as a two-digit prefix, so the stored value
    sorts exactly as the lifecycle does: `21NEW` before `41FILLED`, every
    live state below every terminal one. "Still live" and "finished" are
    rank questions, and a storage scan filters on the code sets
    `ranked_at_least` and `ranked_below` spell -- or on a
    range, which the ordering now makes honest.
    """

    #: The rank the terminal states begin at.
    TERMINAL = enum.nonmember(400)

    UNKNOWN = 0
    """Nothing has been stated."""
    PENDING = "10PENDNG", 100
    """Band floor: requested but not acknowledged."""
    PENDING_NEW = "11PNDNEW", 110
    """Awaiting first venue acknowledgement."""
    QUEUED = "12QUEUED", 120
    """Task accepted and waiting for execution."""
    OPEN = "20OPEN", 200
    """Band floor: live at the venue."""
    NEW = "21NEW", 210
    """Acknowledged and working."""
    ACCEPTED = "22ACCEPT", 220
    """Accepted but not yet working."""
    PENDING_REPLACE = "23PNDRPL", 230
    """Amendment pending while the original remains live."""
    PENDING_CANCEL = "24PNDCNL", 240
    """Cancellation pending while the order remains live."""
    SUSPENDED = "25SUSPND", 250
    """Held by the venue and resumable."""
    STOPPED = "26STOPPD", 260
    """Stopped at a price awaiting a trade."""
    RUNNING = "27RUNING", 270
    """Task execution is in progress."""
    PARTIAL = "30PARTL", 300
    """Band floor: live and partly complete."""
    PARTIALLY_FILLED = "31PRTFIL", 310
    """Some quantity traded; the rest remains live."""
    DONE = "40DONE", 400
    """Band floor and first terminal state."""
    FILLED = "41FILLED", 410
    """Every share traded."""
    DONE_FOR_DAY = "42DONEDY", 420
    """Over for the session."""
    CALCULATED = "43CALCD", 430
    """Priced and closed by the venue."""
    SUCCEEDED = "44SUCCED", 440
    """Task completed successfully."""
    CLOSED = "50CLOSED", 500
    """Band floor: over without completion."""
    CANCELLED = "51CANCLD", 510
    """Withdrawn before completion."""
    REPLACED = "52REPLCD", 520
    """Superseded by an amendment."""
    EXPIRED = "53EXPIRD", 530
    """Reached expiry while live."""
    INTERNAL_EXPIRED = "54INTEXP", 540
    """Expired locally after one day without a newer observation."""
    FAILED = "60FAILED", 600
    """Band floor: refused."""
    REJECTED = "61REJCTD", 610
    """Refused; reason fields explain why."""
    INTERNAL_REJECTED = "62INTREJ", 620
    """Refused by this pipeline before it could change market state."""

    @classmethod
    @functools.cache
    def fix_mapping(cls) -> Mapping[int, Mapping[str, State]]:
        """Tag-scoped lifecycle meanings written into a built FIX registry."""
        declared = {
            35: {
                "D": cls.PENDING_NEW,
                "F": cls.PENDING_CANCEL,
                "G": cls.PENDING_REPLACE,
                "9": cls.UNKNOWN,
            },
            39: {
                "0": cls.NEW,
                "1": cls.PARTIALLY_FILLED,
                "2": cls.FILLED,
                "3": cls.DONE_FOR_DAY,
                "4": cls.CANCELLED,
                "5": cls.REPLACED,
                "6": cls.PENDING_CANCEL,
                "7": cls.STOPPED,
                "8": cls.REJECTED,
                "9": cls.SUSPENDED,
                "A": cls.PENDING_NEW,
                "B": cls.CALCULATED,
                "C": cls.EXPIRED,
                "D": cls.ACCEPTED,
                "E": cls.PENDING_REPLACE,
            },
            150: {
                "0": cls.NEW,
                "1": cls.PARTIALLY_FILLED,
                "2": cls.FILLED,
                "3": cls.DONE_FOR_DAY,
                "4": cls.CANCELLED,
                "5": cls.REPLACED,
                "6": cls.PENDING_CANCEL,
                "7": cls.STOPPED,
                "8": cls.REJECTED,
                "9": cls.SUSPENDED,
                "A": cls.PENDING_NEW,
                "B": cls.CALCULATED,
                "C": cls.EXPIRED,
                "E": cls.PENDING_REPLACE,
                "F": cls.PARTIALLY_FILLED,
                "G": cls.REPLACED,
                "H": cls.CANCELLED,
            },
            279: {
                "0": cls.NEW,
                "1": cls.OPEN,
                "2": cls.CANCELLED,
                "3": cls.CANCELLED,
                "4": cls.CANCELLED,
                "5": cls.OPEN,
            },
            297: {
                "0": cls.ACCEPTED,
                "1": cls.CANCELLED,
                "2": cls.CANCELLED,
                "3": cls.CANCELLED,
                "4": cls.CANCELLED,
                "5": cls.REJECTED,
                "6": cls.CANCELLED,
                "7": cls.EXPIRED,
                "9": cls.REJECTED,
                "10": cls.PENDING,
                "11": cls.CANCELLED,
                "12": cls.OPEN,
                "13": cls.OPEN,
                "14": cls.CANCELLED,
                "15": cls.CANCELLED,
                "16": cls.OPEN,
                "17": cls.CANCELLED,
                "18": cls.OPEN,
                "19": cls.PENDING_CANCEL,
                "21": cls.FILLED,
                "22": cls.FILLED,
                "23": cls.EXPIRED,
            },
            694: {
                "1": cls.FILLED,
                "2": cls.OPEN,
                "3": cls.EXPIRED,
                "4": cls.OPEN,
                "5": cls.CANCELLED,
                "6": cls.CANCELLED,
                "7": cls.CANCELLED,
                "8": cls.CANCELLED,
                "9": cls.OPEN,
                "10": cls.OPEN,
                "11": cls.ACCEPTED,
                "12": cls.CANCELLED,
            },
        }
        return MappingProxyType({tag: MappingProxyType(values) for tag, values in declared.items()})

    @property
    def is_live(self) -> bool:
        """Whether the event is working at the venue."""
        return State.OPEN.rank <= self.rank < State.TERMINAL

    @property
    def is_terminal(self) -> bool:
        """Whether no further lifecycle transition is expected."""
        return self.rank >= State.TERMINAL

    @classmethod
    def live_codes(cls) -> tuple[int, ...]:
        """Stored codes of the live states, for a pushed scan filter."""
        return tuple(int(member) for member in cls if member.is_live)

    @classmethod
    def terminal_codes(cls) -> tuple[int, ...]:
        """Stored codes of the terminal states, for a pushed scan filter."""
        return tuple(int(member) for member in cls if member.is_terminal)


class Direction(Ascii32):
    """Message transport direction stored as a four-byte ASCII mnemonic."""

    UNKNOWN = 0
    SENT = "SENT"
    RECV = "RECV"


class MIC(Ascii32):
    """ISO 10383 code stored as four ASCII bytes in one `int32`."""

    _PATTERN = enum.nonmember(re.compile(r"^[A-Z0-9]{4}$"))

    UNKNOWN = 0
    """No valid market identifier was present."""

    XOFF = "XOFF"
    """Off-market transaction."""

    XXXX = "XXXX"
    """No market, including an unlisted instrument."""

    # The venues a capture keeps meeting, compiled rather than left to
    # registration. Which ones: the **operating** MIC of a primary listing
    # book, plus the listed-derivatives complexes those books clear through.
    # Operating and not segment, because `XPAR` is where a Euronext Paris
    # trade belongs whichever book inside it printed, and a segment list is
    # the half of ISO 10383 that churns.
    #
    # Compiling is not decoration. `into_arrow_array` renders the compiled
    # members and nulls everything else, so before this every real venue in a
    # `lastmkt` column read back as null while `from_int` named it -- the scalar
    # reader and the column renderer disagreeing on the same bytes. Only
    # compiled codes reach the `enum:values` a contract publishes, and only
    # they are safe from the eviction the learnt-code registry does.
    #
    # No stored value moves: a code packs from its own bytes, so a venue that
    # registered yesterday is the same integer compiled today. A venue missing
    # here costs one registration; a wrong code here is a stored value nobody
    # can correct, which is what keeps this list to venues, not vendors.

    # -- Europe
    XPAR = "XPAR"
    """Euronext Paris."""
    XAMS = "XAMS"
    """Euronext Amsterdam."""
    XBRU = "XBRU"
    """Euronext Brussels."""
    XLIS = "XLIS"
    """Euronext Lisbon."""
    XMIL = "XMIL"
    """Euronext Milan."""
    XDUB = "XDUB"
    """Euronext Dublin."""
    XOSL = "XOSL"
    """Euronext Oslo Bors."""
    XETR = "XETR"
    """Deutsche Boerse Xetra."""
    XFRA = "XFRA"
    """Frankfurt Stock Exchange."""
    XLON = "XLON"
    """London Stock Exchange."""
    XSWX = "XSWX"
    """SIX Swiss Exchange."""
    XMAD = "XMAD"
    """Bolsa de Madrid."""
    XSTO = "XSTO"
    """Nasdaq Stockholm."""
    XCSE = "XCSE"
    """Nasdaq Copenhagen."""
    XHEL = "XHEL"
    """Nasdaq Helsinki."""
    XWBO = "XWBO"
    """Wiener Boerse."""
    XEUR = "XEUR"
    """Eurex."""
    XLME = "XLME"
    """London Metal Exchange."""
    IFEU = "IFEU"
    """ICE Futures Europe."""

    # -- The Americas
    XNYS = "XNYS"
    """New York Stock Exchange."""
    XNAS = "XNAS"
    """Nasdaq."""
    ARCX = "ARCX"
    """NYSE Arca."""
    BATS = "BATS"
    """Cboe BZX."""
    XCBO = "XCBO"
    """Cboe Options Exchange."""
    IEXG = "IEXG"
    """Investors Exchange."""
    XCME = "XCME"
    """Chicago Mercantile Exchange."""
    XCBT = "XCBT"
    """Chicago Board of Trade."""
    XNYM = "XNYM"
    """New York Mercantile Exchange."""
    XCEC = "XCEC"
    """Commodity Exchange."""
    IFUS = "IFUS"
    """ICE Futures U.S."""
    XTSE = "XTSE"
    """Toronto Stock Exchange."""

    # -- Asia-Pacific, and the rest
    XTKS = "XTKS"
    """Tokyo Stock Exchange."""
    XHKG = "XHKG"
    """Hong Kong Exchanges."""
    XSES = "XSES"
    """Singapore Exchange."""
    XASX = "XASX"
    """Australian Securities Exchange."""
    XSHG = "XSHG"
    """Shanghai Stock Exchange."""
    XSHE = "XSHE"
    """Shenzhen Stock Exchange."""
    XKRX = "XKRX"
    """Korea Exchange."""
    XNSE = "XNSE"
    """National Stock Exchange of India."""
    XBOM = "XBOM"
    """BSE India."""
    XJSE = "XJSE"
    """Johannesburg Stock Exchange."""

    @classmethod
    def _valid(cls, text: str) -> bool:
        return bool(cls._PATTERN.fullmatch(text))

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        return {**super().schema_metadata(), "pattern": "[A-Z0-9]{4}"}

    @classmethod
    def arrow_from_strings(cls, *values: Any) -> Any:
        """Pack the first valid MIC across string columns with Arrow kernels."""
        if not values:
            raise ValueError("at least one MIC source column is required")
        import pyarrow
        import pyarrow.compute as compute

        alphabet = pyarrow.array(list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        encoded = []
        for value in values:
            text = compute.utf8_upper(compute.utf8_trim_whitespace(value.cast(pyarrow.string())))
            valid = compute.fill_null(
                compute.match_substring_regex(text, cls._PATTERN.pattern), False
            )
            packed = pyarrow.repeat(pyarrow.scalar(0, pyarrow.int32()), len(text))
            for index, multiplier in enumerate((1 << 24, 1 << 16, 1 << 8, 1)):
                character = compute.utf8_slice_codeunits(text, start=index, stop=index + 1)
                position = compute.index_in(character, value_set=alphabet)
                byte = compute.if_else(
                    compute.less(position, 10),
                    compute.add(position, 48),
                    compute.add(position, 55),
                ).cast(pyarrow.int32())
                packed = compute.add(packed, compute.multiply(byte, multiplier)).cast(
                    pyarrow.int32()
                )
            encoded.append(compute.if_else(valid, packed, pyarrow.scalar(None, pyarrow.int32())))
        return compute.coalesce(*encoded)


class Currency(Ascii32):
    """ISO 4217 alphabetic code stored as three ASCII letters.

    Packed like every other ASCII code -- NUL-padded to the storage width --
    so the fourth byte is simply zero. No decimal count rides in the value:
    a minor-unit convention is venue data, not part of the code.
    """

    _PATTERN = enum.nonmember(re.compile(r"^[A-Z]{3}$"))

    @classmethod
    def _valid(cls, text: str) -> bool:
        return bool(cls._PATTERN.fullmatch(text))

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        return {
            "$": "USD",
            "US$": "USD",
            "\u20ac": "EUR",
            "\u00a3": "GBP",
            "\u00a5": "JPY",
        }

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        return {**super().schema_metadata(), "pattern": "[A-Z]{3}"}

    UNKNOWN = 0
    """No currency was present."""

    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"
    CHF = "CHF"
    CAD = "CAD"
    AUD = "AUD"
    NZD = "NZD"
    CNY = "CNY"
    HKD = "HKD"
    SGD = "SGD"
    SEK = "SEK"
    NOK = "NOK"
    DKK = "DKK"
    PLN = "PLN"
    CZK = "CZK"
    HUF = "HUF"
    MXN = "MXN"
    BRL = "BRL"
    ZAR = "ZAR"
    INR = "INR"
    KRW = "KRW"
    TWD = "TWD"
    XAU = "XAU"
    XAG = "XAG"
    XPT = "XPT"
    XPD = "XPD"
    XTS = "XTS"
    XXX = "XXX"


class SecurityIDSource(Ascii32):
    """Which scheme a `SecurityID <48>` is issued under, as a four-byte code.

    Open, because a desk's own reference system is a scheme like any other and
    the dictionary cannot know it. The thirty-three the dictionary does
    enumerate are compiled under short codes: their spellings run to
    `FINANCIAL_INSTRUMENT_GLOBAL_IDENTIFIER` and a stored code is four bytes,
    so the name is the spelling and the code is what a column holds. The wire
    values stay the dictionary's -- `FIX_FIELD` names the field they are read
    from, so tag 22's codes are not written down a second time here.
    """

    FIX_FIELD = enum.nonmember("SecurityIDSource")

    UNKNOWN = 0
    """No scheme was stated, so the identifier beside it names its own."""

    CUSIP = "CUSP"
    SEDOL = "SEDL"
    QUIK = "QUIK"
    ISIN = "ISIN"
    """ISO 6166, and the one scheme this package asks about by name."""
    RIC = "RIC"
    ISO_CURRENCY = "CCY"
    ISO_COUNTRY = "CTRY"
    EXCHANGE_SYMBOL = "EXCH"
    CTA = "CTA"
    BLOOMBERG = "BBG"
    WERTPAPIER = "WKN"
    DUTCH = "DUTC"
    VALOREN = "VALO"
    SICOVAM = "SICO"
    BELGIAN = "BELG"
    COMMON = "COMN"
    CLEARING_HOUSE = "CLRH"
    ISDA_FPML_SPEC = "ISDA"
    OPRA = "OPRA"
    ISDA_FPML_URL = "FPML"
    LETTER_OF_CREDIT = "LOC"
    MARKETPLACE = "MKTP"
    MARKIT_RED_ENTITY = "RDEC"
    MARKIT_RED_PAIR = "RDPC"
    CFTC_COMMODITY = "CFTC"
    ISDA_COMMODITY = "ICRP"
    FIGI = "FIGI"
    LEI = "LEI"
    SYNTHETIC = "SYNT"
    FIDESSA = "FIDM"
    INDEX_NAME = "INDX"
    UNIFORM_SYMBOL = "UNIF"
    DIGITAL_TOKEN = "DTI"

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        """The dictionary's spelling for each scheme, so a value finds its member.

        `from_fix` matches a dictionary value to a member by the spellings that
        value declares, and those are the long names above the codes. Without
        this bridge every wire code would answer `UNKNOWN` -- and these are the
        only place the long spellings appear, because the codes are what a
        column stores.
        """
        return {
            "ISINNumber": "ISIN",
            "RICCode": "RIC",
            "ISOCurrencyCode": "ISO_CURRENCY",
            "ISOCountryCode": "ISO_COUNTRY",
            "ExchangeSymbol": "EXCHANGE_SYMBOL",
            "ConsolidatedTapeAssociation": "CTA",
            "BloombergSymbol": "BLOOMBERG",
            "Wertpapier": "WERTPAPIER",
            "Dutch": "DUTCH",
            "Valoren": "VALOREN",
            "Sicovam": "SICOVAM",
            "Belgian": "BELGIAN",
            "Common": "COMMON",
            "ClearingHouse": "CLEARING_HOUSE",
            "ISDAFpMLSpecification": "ISDA_FPML_SPEC",
            "OptionPriceReportingAuthority": "OPRA",
            "ISDAFpMLURL": "ISDA_FPML_URL",
            "LetterOfCredit": "LETTER_OF_CREDIT",
            "MarketplaceAssignedIdentifier": "MARKETPLACE",
            "MARKIT_RED_ENTITY_CLIP": "MARKIT_RED_ENTITY",
            "MARKIT_RED_PAIR_CLIP": "MARKIT_RED_PAIR",
            "CFTC_COMMODITY_CODE": "CFTC_COMMODITY",
            "ISDA_COMMODITY_REFERENCE_PRICE": "ISDA_COMMODITY",
            "FINANCIAL_INSTRUMENT_GLOBAL_IDENTIFIER": "FIGI",
            "LEGAL_ENTITY_IDENTIFIER": "LEI",
            "FIDESSA_INSTRUMENT_MNEMONIC": "FIDESSA",
            "DIGITAL_TOKEN_IDENTIFIER": "DIGITAL_TOKEN",
        }


class Side(Ascii32):
    """Direction stored as a four-byte ASCII mnemonic."""

    FIX_FIELD = enum.nonmember("Side")

    UNKNOWN = 0
    """No side stated."""
    BUY = "BUY"
    """Buying and book bid."""
    BID = "BUY"
    """Alias of `BUY`."""
    BUY_MINUS = "BYMN"
    """Buy not above the last differing price."""
    BORROW = "BORR"
    """Borrowing collateral."""
    SUBSCRIBE = "SUBS"
    """Subscribing to a fund."""
    SELL = "SELL"
    """Selling and book ask."""
    ASK = "SELL"
    """Alias of `SELL`."""
    SELL_PLUS = "SLPL"
    """Sell not below the last differing price."""
    SELL_SHORT = "SHRT"
    """Selling stock not held."""
    SELL_SHORT_EXEMPT = "SHEX"
    """Exempt short sale."""
    LEND = "LEND"
    """Lending collateral."""
    REDEEM = "REDM"
    """Redeeming a fund holding."""
    CROSS = "CROS"
    """Both sides are the same participant."""
    CROSS_SHORT = "CRSH"
    """Cross with a short sell leg."""
    CROSS_SHORT_EXEMPT = "CRSE"
    """Cross with an exempt short leg."""
    AS_DEFINED = "ASDF"
    """Direction defined by the multileg instrument."""
    OPPOSITE = "OPPO"
    """Opposite of the multileg definition."""
    UNDISCLOSED = "UNDS"
    """Direction withheld."""

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        return {"LONG": "BUY", "OFFER": "SELL", "SHORT": "SELL_SHORT"}

    @property
    def sign(self) -> int:
        """Return +1 buying, -1 selling or 0 otherwise."""
        if self in (Side.BUY, Side.BUY_MINUS, Side.BORROW, Side.SUBSCRIBE):
            return 1
        if self in (
            Side.SELL,
            Side.SELL_PLUS,
            Side.SELL_SHORT,
            Side.SELL_SHORT_EXEMPT,
            Side.LEND,
            Side.REDEEM,
        ):
            return -1
        return 0

    @property
    def opposite(self) -> Side:
        """Return the plain opposite; neutral sides return themselves."""
        if self.sign > 0:
            return Side.SELL
        if self.sign < 0:
            return Side.BUY
        return self


class TimeInForce(Ascii32):
    """Order lifetime stored as a ranked four-byte ASCII mnemonic."""

    FIX_FIELD = enum.nonmember("TimeInForce")

    UNKNOWN = 0, 0
    """Venue default."""
    IMMEDIATE = "IMMD", 100
    """Ordering marker for non-resting instructions."""
    IOC = "IOC", 110
    """Trade what can immediately and cancel the rest."""
    FOK = "FOK", 120
    """Trade all immediately or none."""
    SESSION = "SESS", 200
    """Ordering marker for session-valid instructions."""
    DAY = "DAY", 210
    """Good for the session."""
    AT_OPEN = "OPEN", 220
    """Opening auction only."""
    AT_CLOSE = "CLOS", 230
    """Closing auction only."""
    GTX = "GTX", 240
    """Good until crossing."""
    GOOD_THROUGH_CROSSING = "GTCR", 250
    """Valid through the next crossing phase."""
    AT_CROSSING = "ATCR", 260
    """Valid only during crossing."""
    GFA = "GFA", 270
    """Good for one auction."""
    RESTING = "REST", 300
    """Ordering marker for cross-session instructions."""
    GTC = "GTC", 310
    """Good until cancelled."""
    GTD = "GTD", 320
    """Good until `Event.expunix`."""
    GFT = "GFT", 330
    """Good for a duration resolved into `Event.expunix`."""
    GFM = "GFM", 340
    """Good for the current month."""

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        """Spellings this vocabulary answers to beside its own member names.

        The five the dictionary uses that this package abbreviates are here
        because a spelling is an enum's business; the wire code each one
        reaches is the dictionary's, and is read from it.
        """
        return {
            "IMMEDIATE_OR_CANCEL": "IOC",
            "FILL_OR_KILL": "FOK",
            "GOOD_TIL_CANCELLED": "GTC",
            "GOOD_TILL_CANCELLED": "GTC",
            "GOOD_TIL_DATE": "GTD",
            "GOOD_TILL_DATE": "GTD",
            "AT_THE_OPENING": "AT_OPEN",
            "AT_THE_CLOSE": "AT_CLOSE",
            "GOOD_FOR_AUCTION": "GFA",
            "GOOD_FOR_TIME": "GFT",
            "GOOD_FOR_MONTH": "GFM",
        }

    def _rank_of(self, other: Any) -> int | Any:
        return other._rank if isinstance(other, TimeInForce) else NotImplemented

    def __lt__(self, other: Any) -> bool:
        rank = self._rank_of(other)
        return int(self) < other if rank is NotImplemented else self._rank < rank

    def __le__(self, other: Any) -> bool:
        rank = self._rank_of(other)
        return int(self) <= other if rank is NotImplemented else self._rank <= rank

    def __gt__(self, other: Any) -> bool:
        rank = self._rank_of(other)
        return int(self) > other if rank is NotImplemented else self._rank > rank

    def __ge__(self, other: Any) -> bool:
        rank = self._rank_of(other)
        return int(self) >= other if rank is NotImplemented else self._rank >= rank

    @property
    def rests(self) -> bool:
        """Whether an unfilled order remains in the book."""
        return self >= TimeInForce.SESSION
