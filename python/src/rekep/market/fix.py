"""FIX messages as market events: what a venue said, as rows this package stores."""

from __future__ import annotations

import dataclasses
import datetime
import functools
import types
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, TypeVar

from rekep.convert import Convertible
from rekep.enums import (
    MIC,
    AssetKind,
    Currency,
    EventType,
    MarketKind,
    OptionKind,
    Side,
    State,
    TimeInForce,
)
from rekep.fields import StructField, encoded_key
from rekep.fix.access import FieldAccess
from rekep.fix.columns import IDENTIFIER_FIELDS, UNKNOWN_SCHEME, id_scheme
from rekep.fix.fields import EPOCH_ORDINAL, NANOS, SECONDS_A_DAY, unix_of
from rekep.fix.message import group_pairs, group_segment_pairs, indexed_group_pairs
from rekep.fix.registry import FixRegistry
from rekep.market.event import MarketEvent
from rekep.market.instrument import Instrument, Leg
from rekep.market.orders import Execution, Order, _quantity_transition
from rekep.market.ticker import SymbolTicker
from rekep.market.transacted import TRANSACTED, Transacted, resolve
from rekep.text.fixmsg import FixMsg

TMarketEvent = TypeVar("TMarketEvent", bound=MarketEvent)

#: Protocol controls read by translation but not necessarily stored as their
#: own model column. The bundled registry supplies their tags and datatypes.
CARRIED_FIELDS: tuple[str, ...] = (
    "BodyLength",
    "CheckSum",
    "MsgType",
    "SenderCompID",
    "TargetCompID",
    "SendingTime",
    "OrigSendingTime",
    "OrigTime",
    "PossDupFlag",
    "TransactTime",
    "TradeDate",
    "ExpireTime",
    "ExpireDate",
    "ExposureDuration",
    "ExposureDurationUnit",
    "OrdStatus",
    "OrdType",
    "ExecType",
    "ExecTransType",
    "CxlRejReason",
    "CxlRejResponseTo",
    "NoMDEntries",
    "MDEntryType",
    "MDEntryPx",
    "MDEntrySize",
    "MDEntryDate",
    "MDEntryTime",
    "MDEntryID",
    "MDUpdateAction",
    "NumberOfOrders",
    "TrdMatchID",
    "ExDestination",
    # The repeating groups an instrument is read out of. A group's own
    # `NumInGroup` count and the members that land somewhere other than a
    # column of their own -- an alternative identifier becomes a key of
    # `altids`, so its two tags are read and stored and are not extras.
    "NoSecurityAltID",
    "SecurityAltID",
    "SecurityAltIDSource",
    "NoLegs",
    # The group a multi-sided TradeCaptureReport splits by: each entry is one
    # Execution, so the count is read and is not an extra.
    "NoSides",
    # The older way to say when a contract expires, which `maturitydate` falls back
    # to. A venue that sends it usually sends no `MaturityDate <541>` at all.
    "MaturityMonthYear",
    "LegMaturityMonthYear",
    # Quotes, including the two default sizes used by mass-quote sets.
    "QuoteID",
    "QuoteReqID",
    "QuoteType",
    "QuoteStatus",
    "QuoteRejectReason",
    "QuoteRespType",
    "QuoteCancelType",
    "BidPx",
    "OfferPx",
    "BidSize",
    "OfferSize",
    "DefBidSize",
    "DefOfferSize",
    "ValidUntilTime",
    "NoQuoteSets",
    "NoQuoteEntries",
    "QuoteSetID",
    "QuoteEntryID",
)

#: Encoding fields, omitted from market metadata; BeginString remains evidence.
FRAMING_FIELDS = frozenset({"BodyLength", "CheckSum"})

#: Which repeating group each structured regulatory column is read out of,
#: for the translation layer -- which holds a message rather than the typed
#: columns the parse layer has.
_REGULATORY_GROUPS: Mapping[str, str] = types.MappingProxyType(
    {
        "trdregtimestamps": "NoTrdRegTimestamps",
        "sidetrdregts": "NoSideTrdRegTS",
    }
)

#: The regulatory groups' member fields, which say "this entry carries its
#: own clock" even where a count-free rendering omits the group's count.
_REGULATORY_MEMBERS: tuple[str, ...] = (
    "TrdRegTimestamp",
    "TrdRegTimestampType",
    "TrdRegTimestampOrigin",
    "SideTrdRegTimestamp",
    "SideTrdRegTimestampType",
    "SideTrdRegTimestampSrc",
)

#: MDEntryType <269> to the side of the book an entry belongs to. Everything
#: else it enumerates -- an index value, a settlement price, a session high,
#: an imbalance -- is a statistic about the market rather than an order in it,
#: and is not a market event this package stores.
ENTRY_SIDES: dict[str, Side] = {"0": Side.BID, "1": Side.ASK}

# These sets say which market shape the package implements, under the
# standard's own name for each message; the protocol dictionary does not
# duplicate them, it only says what a feed spells them as.
ENTRY_HANDLERS = frozenset({"marketdatasnapshotfullrefresh", "marketdataincrementalrefresh"})
ORDER_HANDLERS = frozenset(
    {"newordersingle", "ordercancelrequest", "ordercancelreplacerequest", "ordercancelreject"}
)
#: The reject is the one order message whose real state lives in `OrdStatus
#: <39>` -- it says where the order *stands* after the refusal -- so dispatch
#: reads it there rather than settling for the MsgType's asserted state.
CANCEL_REJECT_HANDLER = "ordercancelreject"
QUOTE_HANDLERS = frozenset(
    {"quotestatusreport", "quoteresponse", "quote", "quotecancel", "quoterequest"}
)
MASS_QUOTE_HANDLERS = frozenset({"massquote", "massquoteack"})
EXECUTION_REPORT_HANDLER = "executionreport"
#: The request decodes like the report it echoes -- but only when it carries
#: a trade: a plain query fabricates nothing.
EXECUTION_REQUEST_HANDLER = "tradecapturereportrequest"
EXECUTION_HANDLERS = frozenset({"tradecapturereport", EXECUTION_REQUEST_HANDLER})

#: Every message shape this package implements. The dispatch vocabulary,
#: which is what `MarketTags.handlers` asks the dictionary to spell.
HANDLERS: frozenset[str] = frozenset(
    {
        *ENTRY_HANDLERS,
        *ORDER_HANDLERS,
        *QUOTE_HANDLERS,
        *MASS_QUOTE_HANDLERS,
        *EXECUTION_HANDLERS,
        EXECUTION_REPORT_HANDLER,
    }
)

#: What says a report request actually carries a trade rather than criteria.
#: Fields both translation paths read, so the gate answers the same flat.
_TRADE_EVIDENCE_FIELDS: tuple[str, ...] = (
    "LastPx",
    "LastQty",
    "TradeID",
    "TrdMatchID",
    "ExecID",
    "ExecRefID",
)

#: Every field `FixEvents.instrument` reads, the two repeating groups included.
#: An entry of a refresh that names none of them is not describing another
#: instrument -- it is describing a level of the one the header named -- so it
#: takes the header's instrument whole rather than building a poorer copy.
INSTRUMENT_FIELDS: frozenset[str] = frozenset(
    {
        "Currency",
        "SecurityIDSource",
        "SecurityID",
        "Symbol",
        "ExDestination",
        "SecurityDesc",
        "SecurityType",
        "MaturityMonthYear",
        "PutOrCall",
        "StrikePrice",
        "SecurityExchange",
        "ContractMultiplier",
        "NoSecurityAltID",
        "CFICode",
        "MaturityDate",
        "NoLegs",
        "RoundLot",
        "MinPriceIncrement",
    }
)

INSTRUMENT_MESSAGE_FIELDS: frozenset[str] = INSTRUMENT_FIELDS | frozenset(
    {
        "BeginString",
        "MsgType",
        "NoMDEntries",
        "NoQuoteSets",
        "NoQuoteEntries",
        "QuoteSetID",
        "QuoteEntryID",
        *(field for rung in TRANSACTED for field in rung.fields),
    }
)

#: The MDEntryType <269> that is a trade rather than a resting interest.
ENTRY_TRADE = "2"

# Standardisation is intentionally many-to-one for order kinds and lifecycle
# states. Keep the wire spelling beside the standard code for an audit.
_STATE_FIELDS = ("OrdStatus", "ExecType", "MDUpdateAction", "QuoteStatus", "QuoteRespType")

#: Bridge shorthand observed on real UL rows for spellings the dictionary
#: writes longer -- `EXECTYPE=cancel` where its vocabulary says `canceled`.
#: Folded wherever the dictionary's own spellings are folded, never over an
#: explicit code.
_SPELLING_SHORTHAND: Mapping[str, str] = types.MappingProxyType({"cancel": "canceled"})


@functools.cache
def _tag_of(name: str) -> int:
    """One field's tag, from the dictionary. `MarketKind.fix_mapping()` is keyed
    by tag and the call site already names the field, so the number is spare."""
    return int(FixRegistry.from_builtin().scalar(name).fix.tag)


RAW_METADATA_FIELDS = frozenset(_STATE_FIELDS) | {
    "OrdType",
    "CxlQty",
    "ExpireTime",
    "ExpireDate",
    "ExposureDuration",
    "ExposureDurationUnit",
    "QuoteCancelType",
    "QuoteType",
    "QuoteRejectReason",
}

_EXPOSURE_UNIT_NS = types.MappingProxyType(
    {
        0: NANOS,
        1: 100_000_000,
        2: 10_000_000,
        3: 1_000_000,
        4: 1_000,
        5: 1,
        10: 60 * NANOS,
        11: 3_600 * NANOS,
        12: SECONDS_A_DAY * NANOS,
        13: 7 * SECONDS_A_DAY * NANOS,
    }
)


@dataclasses.dataclass(frozen=True)
class MarketTags:
    """One FIX dictionary as the market layer reads it: names as wire tags.

    Nothing here is a fact about a message, so every set is resolved once per
    `(registry, version)` and shared by every message that reads through it.
    Rebuilding them a message at a time -- a hundred and sixty names for the
    claimed set alone -- was a third of a conversion (benchmarks/bench_market.py).
    """

    access: FieldAccess
    version: str | None = None

    @classmethod
    def of(cls, registry: FixRegistry | None = None, version: str | None = None) -> MarketTags:
        """One shared reading per `(registry, version)`, memos and all."""
        selected = registry if registry is not None else FixRegistry.from_builtin()
        return cls._of(selected, version, selected.revision)

    @classmethod
    @functools.lru_cache(maxsize=64)
    def _of(cls, registry: FixRegistry, version: str | None, _revision: int) -> MarketTags:
        """One reading fixed to a registry generation."""
        return cls(FieldAccess(registry, version), version)

    @classmethod
    @functools.cache
    def standard(cls) -> Mapping[str, int]:
        """Tags selected by canonical name from the packaged registry."""
        registry = FixRegistry.from_builtin()
        found = {name: registry.scalar(name).fix.tag for name in CARRIED_FIELDS}
        for shape in (MarketEvent, Order, Execution, Instrument):
            cls._declared(shape.into_field(), found)
        return types.MappingProxyType(found)

    @classmethod
    def _declared(cls, struct: StructField, into: dict[str, int]) -> None:
        """Every `fix:` tag under `struct`, nested members included."""
        for member in struct.fields:
            tag = member.fix.tag
            name = member.fix.name
            if tag and name:
                into.setdefault(name, tag)
            if member.fields:
                cls._declared(member, into)

    @functools.cached_property
    def tags(self) -> Mapping[str, int]:
        """Market field names to tags, overridden by the selected version."""
        standard = self.standard()
        registry = self.access.registry
        if self.version is None or registry is None:
            return standard
        try:
            configured = registry.tags(self.version)
        except (KeyError, OSError, ValueError):
            return standard
        found = dict(standard)
        for name in tuple(found):
            if tag := configured.get(encoded_key(name)):
                found[name] = tag
        for name, tag in configured.items():
            found.setdefault(name, tag)
        return types.MappingProxyType(found)

    @property
    def registry(self) -> FixRegistry:
        """Registry owning this cached market reading."""
        return self.access.registry or FixRegistry.from_builtin()

    @functools.cached_property
    def _coded_states(self) -> Mapping[str, Mapping[str, State]]:
        """The state maps by wire code alone, for set derivations to reason over."""
        return types.MappingProxyType(
            {name: _registry_values(self.registry, "state_values", name) for name in _STATE_FIELDS}
        )

    @functools.cached_property
    def states(self) -> Mapping[str, Mapping[str, State]]:
        """Field-specific state maps, word spellings folded beside the codes."""
        return types.MappingProxyType(
            {
                name: types.MappingProxyType(_worded_values(self.registry, name, coded))
                for name, coded in self._coded_states.items()
            }
        )

    @functools.cached_property
    def ordered(self) -> Mapping[str, State]:
        """Order-bearing MsgTypes and the request state each asserts."""
        return _registry_values(self.registry, "state_values", "MsgType")

    @functools.cached_property
    def execution_states(self) -> Mapping[str, State]:
        """Trade-bearing ExecTypes as completed execution occurrences."""
        states = self._coded_states["ExecType"]
        builtin = FixRegistry.from_builtin().field("ExecType")
        configured = self.registry.field("ExecType")
        trade_codes = frozenset(
            code
            for entry in (builtin, configured)
            if entry is not None
            for spelling, code in entry.fix.encoded.items()
            if spelling.startswith("trade")
        )
        coded = {
            code: State.FILLED if state is State.PARTIALLY_FILLED else state
            for code, state in states.items()
            if state in (State.PARTIALLY_FILLED, State.FILLED) or code in trade_codes
        }
        return types.MappingProxyType(_worded_values(self.registry, "ExecType", coded))

    @functools.cached_property
    def order_kinds(self) -> Mapping[str, MarketKind]:
        """`OrdType <40>` codes and dictionary spellings, as the kind each means."""
        return types.MappingProxyType(
            _worded_values(self.registry, "OrdType", MarketKind.fix_mapping()[_tag_of("OrdType")])
        )

    @functools.cached_property
    def execution_kinds(self) -> Mapping[str, MarketKind]:
        """`ExecType <150>` codes and dictionary spellings, as the kind each means."""
        return types.MappingProxyType(
            _worded_values(self.registry, "ExecType", MarketKind.fix_mapping()[_tag_of("ExecType")])
        )

    @functools.cached_property
    def exec_type_fallbacks(self) -> Mapping[str, State]:
        """ExecType lifecycle fallbacks that describe the Order, not an Execution."""
        states = self._coded_states["ExecType"]
        execution_codes = self.execution_states
        coded = {
            code: state
            for code, state in states.items()
            if code not in execution_codes or state in (State.PARTIALLY_FILLED, State.FILLED)
        }
        return types.MappingProxyType(_worded_values(self.registry, "ExecType", coded))

    @functools.cached_property
    def handlers(self) -> Mapping[str, str]:
        """The MsgTypes this package implements, to the shape each dispatches as.

        Built by *encoding* each implemented name -- the one direction the
        registry keeps -- rather than by decoding every value it knows: the
        vocabulary below is sixteen names, and asking the dictionary what it
        spells them as answers for a venue's own MsgTypes too, which is the
        whole reason the dispatch is not a table of wire values.

        Walked in sorted order so a configured dictionary that spells two
        names alike resolves the same way on every run.
        """
        found: dict[str, str] = {}
        for source in dict.fromkeys((FixRegistry.from_builtin(), self.registry)):
            entry = source.field(35)
            if entry is None:
                continue
            for handler in sorted(HANDLERS):
                value = entry.fix.encode(handler)
                if value != handler:
                    found[value] = handler
        return types.MappingProxyType(found)

    @functools.cached_property
    def names_by_tag(self) -> Mapping[str, str]:
        """Back from the selected wire tag to the standard name that earned it."""
        tag_text = self.access.tag_text
        return types.MappingProxyType(
            {tag: name for name in self.standard() if (tag := tag_text(name)) != name}
        )

    @functools.cached_property
    def lookup_tags(self) -> Mapping[str, str]:
        """Canonical market field names to their resolved wire keys."""
        tag_text = self.access.tag_text
        return types.MappingProxyType({name: tag_text(name) for name in self.tags})

    @functools.cached_property
    def claimed(self) -> frozenset[str]:
        """Every tag a shape already stores, plus the two framing fields.

        What `FixEvents.extras` drops: a field with a column of its own is not
        metadata, and a length or a checksum is not data at all. The rendered
        identities FIX never numbered are claimed through
        `rendered_spellings`, because their pairs keep the source spelling.
        """
        tag_text = self.access.tag_text
        return frozenset(tag_text(name) for name in self.standard()) | frozenset(
            tag_text(name) for name in FRAMING_FIELDS
        )

    @classmethod
    @functools.cache
    def rendered(cls) -> frozenset[str]:
        """Stored fields FIX never numbered, spelled as the registry does.

        The tagless half of `standard()`: a shape member annotated with a
        namespace record carries the record's name and no tag.
        """
        found: set[str] = set()

        def visit(struct: StructField) -> None:
            for member in struct.fields:
                if member.fix.name and not member.fix.tag:
                    found.add(member.fix.name)
                if member.fields:
                    visit(member)

        for shape in (MarketEvent, Order, Execution, Instrument):
            visit(shape.into_field())
        return frozenset(found)

    @functools.cached_property
    def rendered_spellings(self) -> frozenset[str]:
        """Every spelling a stored rendered field answers to, folded.

        A tagged pair canonicalizes to its wire tag, so `claimed` matches it
        exactly; a namespace pair keeps the spelling the bridge wrote --
        `PARENTCLORDID` stays `PARENTCLORDID` -- so its claim has to cover
        the record's canonical name, its aliases, and any casing of either.
        """
        found: set[str] = set()
        for name in self.rendered():
            entry = None if self.access.registry is None else self.access.registry.resolve(name)
            spellings = entry.fix.spellings() if entry is not None else (name,)
            found.update(encoded_key(spelled) for spelled in spellings)
        return frozenset(found)

    @functools.cached_property
    def audited(self) -> frozenset[str]:
        """Tags standardised many-to-one, kept beside the code they became."""
        return frozenset(self.access.tag_text(name) for name in RAW_METADATA_FIELDS)


def market_tags(
    registry: FixRegistry | None = None, version: str | None = None
) -> Mapping[str, int]:
    """Market field names to tags, overridden by a selected registry version."""
    return MarketTags.of(registry, version).tags


def _registry_values(registry: FixRegistry, method: str, field: str) -> Mapping[str, State]:
    """Builtin lifecycle configuration with one registry's explicit overrides."""
    builtin = FixRegistry.from_builtin()
    defaults = getattr(builtin, method)(field)
    configured = getattr(registry, method)(field)
    if registry is builtin or not configured:
        return defaults
    return types.MappingProxyType({**defaults, **configured})


def _worded_values(registry: FixRegistry, field: str, coded: Mapping[str, Any]) -> dict[str, Any]:
    """`coded` plus the dictionary's word spellings for the same codes.

    Bridges render `ORDSTATUS=canceled` where the wire says `4`. The parse
    stage rewrites the spellings its version's dictionary lists, and this
    fold answers for a scalar-built row and for a spelling that reached a
    stored column unrewritten. Folded *after* any set is derived from the
    codes, so a word means exactly what its code means there -- never a code
    the derivation excluded. A case variant of the code itself is not a word
    and adds nothing: FIX codes are case-sensitive, so it stays out. An
    explicit code always wins a collision.
    """
    if not coded:
        return dict(coded)
    builtin = FixRegistry.from_builtin()
    spelled: dict[str, Any] = {}
    for source in dict.fromkeys((builtin, registry)):
        entry = source.field(field)
        if entry is None:
            continue
        for spelling, code in entry.fix.encoded.items():
            # One or two characters is a code respelled, not a word -- codes
            # are case-sensitive and the exact lookup owns them, on both the
            # scalar and the flat path alike.
            if len(spelling) <= 2 or spelling.casefold() == str(code).casefold():
                continue
            value = coded.get(code)
            if value is not None:
                spelled.setdefault(spelling, value)
    for shorthand, spelling in _SPELLING_SHORTHAND.items():
        value = spelled.get(spelling)
        if value is not None:
            spelled.setdefault(shorthand, value)
    return {**spelled, **coded}


def _coded(table: Mapping[str, Any], value: Any, default: Any) -> Any:
    """One coded or word-spelled wire value through a folded lookup table."""
    raw = str(value).strip() if value is not None else ""
    found = table.get(raw)
    if found is None:
        found = table.get(encoded_key(raw))
    return found if found is not None else default


#: What a probe of a message's fields answers for a key it does not carry --
#: distinct from a key it carries as null, which is a value.
_ABSENT = object()


def _empty_fixmsg() -> FixMsg:
    """An empty parsed message for the translator default."""
    return FixMsg()


@dataclasses.dataclass
class FixEvents(Convertible):
    """The market events one FIX message carries, in the order it carries them."""

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Conversions inferred for a FIX event translator."""
        return types.MappingProxyType({**super().into_redirects(), str: "text"})

    message: FixMsg = dataclasses.field(default_factory=_empty_fixmsg)
    """The parsed message being translated."""

    venue: str | None = None
    """Which feed this came off, when the reader knows and the message does not."""

    mic: MIC | None = None
    """ISO venue supplied by the parsed log, when it already resolved one."""

    recunix: int = 0
    """When the line was recorded, which is the reader's clock and not the venue's."""

    registry: FixRegistry | None = None
    """Optional dictionary overriding standard tags for this feed."""

    fix_version: str | None = None
    """Registry version; otherwise inferred from BeginString when possible."""

    def __post_init__(self) -> None:
        """Require the parsed message that owns FIX-to-market conversion."""
        if not isinstance(self.message, FixMsg):
            raise TypeError(f"message must be FixMsg, got {type(self.message).__name__}")
        # The message owns FIX parsing, so the translator's dictionary is
        # linked onto it privately: every message-level read resolves under
        # the one dictionary this translation selects.
        if self.registry is not None:
            self.message.link_registry(self.registry)

    # -- building -----------------------------------------------------------

    @classmethod
    def from_text(cls, text: str | bytes, **carried: Any) -> FixEvents:
        """Events out of one log line, however it spells its separator."""
        return cls(message=FixMsg.from_text(text), **carried)

    @classmethod
    def from_pairs(
        cls,
        pairs: Iterable[tuple[Any, Any]],
        names: Mapping[str, int | str] | None = None,
        **carried: Any,
    ) -> FixEvents:
        """Events out of named or numbered pairs; unknown names remain metadata."""
        # With no explicit mapping, keep names until the instance can infer the
        # message version and apply its registry. Resolving newest-first here
        # would silently use the wrong tag for an older/custom version.
        return cls(message=FixMsg.from_pairs(pairs, names), **carried)

    # -- reading ------------------------------------------------------------

    @functools.cached_property
    def version(self) -> str | None:
        """Configured version, or the application version inferred from the message."""
        if self.fix_version is not None:
            return self.fix_version
        return self.message.resolved_version(self.registry)

    @functools.cached_property
    def dictionary(self) -> MarketTags:
        """The dictionary reading this message resolves through."""
        return MarketTags.of(self.registry, self.version)

    @functools.cached_property
    def tags(self) -> Mapping[str, int]:
        """The field-name index selected for this message."""
        if self.version is None:
            return types.MappingProxyType({})
        return self.dictionary.tags

    @functools.cached_property
    def access(self) -> FieldAccess:
        """The one field accessor (fix/access.py), scoped to this message's dictionary."""
        return self.dictionary.access

    @functools.cached_property
    def source_pairs(self) -> list[tuple[str, str]]:
        """The source-spelled projection, retained for indexed rendered groups."""
        return self.message.into_fix_pairs()

    @functools.cached_property
    def pairs(self) -> list[tuple[str, str]]:
        """The message projected once through this translator's dictionary."""
        return self.message.into_fix_pairs(self.access)

    @functools.cached_property
    def _has_indexed_pairs(self) -> bool:
        """Whether a rendered group path survives only in source spelling."""
        return self.message.has_indexed_entries

    @functools.cached_property
    def by_tag(self) -> dict[str, Any]:
        """The message as one value per key, first occurrence winning.

        The accessor's prepared execution: each named key resolves once
        through the shared rule table, and every later read is a dict probe.
        """
        flat = self._flat_by_tag
        if flat is not None:
            return flat
        found: dict[str, Any] = {}
        for key, value in self.pairs:
            found.setdefault(key, value)
        return found

    @functools.cached_property
    def _flat_by_tag(self) -> dict[str, Any] | None:
        """Promoted columns and simple numeric residuals without a FIX round trip."""
        return self.message.into_first_values(self.access)

    @functools.cached_property
    def by_folded_tag(self) -> dict[str, Any]:
        """The same values under folded keys, so a name miss is still a lookup.

        Built once per message rather than walked per miss: a translation asks
        for several dozen fields a message does not carry. The fold is
        `encoded_key`, the same last-tier rule the accessor matches by.
        """
        found: dict[str, Any] = {}
        for key, value in self.by_tag.items():
            found.setdefault(encoded_key(key), value)
        return found

    def get(self, field: int | str) -> Any:
        """The message's first value for one canonical name or numeric tag.

        One probe of the tag index and, only where that says the message does
        not carry the key at all, one of the folded index -- a sentinel rather
        than a second `in`, because a field stored null is carried and a
        translation asks for twice as many fields as a message holds.
        """
        if type(field) is str:
            tag = self.dictionary.lookup_tags.get(field)
            if tag is None:
                tag = self.access.tag_text(field)
        else:
            tag = str(field)
        found = self.by_tag.get(tag, _ABSENT)
        if found is not _ABSENT:
            return found
        if self._flat_by_tag is not None:
            return None
        return self.by_folded_tag.get(encoded_key(field if type(field) is str else str(field)))

    def state_of(self, field: str, default: State = State.UNKNOWN) -> State:
        """Standard state for one field-specific FIX code or word spelling."""
        return _coded(self.dictionary.states.get(field, {}), self.get(field), default)

    def execution_state(self, default: State = State.UNKNOWN) -> State:
        """State of the completed execution carried by this ExecType."""
        return _coded(self.dictionary.execution_states, self.get("ExecType"), default)

    @functools.cached_property
    def _message_kind(self) -> str:
        """Declared MsgType, or the narrow field-based fallback."""
        return self.get("MsgType") or self._inferred()

    def __iter__(self) -> Iterator[MarketEvent]:
        """Every market event the message carries, in the order it carries them."""
        if self.version is None:
            return
        kind = self._message_kind
        handler = self.dictionary.handlers.get(kind)
        if handler in ENTRY_HANDLERS:
            yield from self._entries(kind)
        elif handler in MASS_QUOTE_HANDLERS:
            yield from self._mass_quotes(kind)
        elif handler in QUOTE_HANDLERS:
            yield from self._quotes(kind)
        elif handler == EXECUTION_REPORT_HANDLER:
            yield from self._reported()
        elif handler in EXECUTION_HANDLERS:
            yield from self._sides(requested=handler == EXECUTION_REQUEST_HANDLER)
        elif handler in ORDER_HANDLERS and kind in self.dictionary.ordered:
            seeded = self.dictionary.ordered[kind]
            if handler == CANCEL_REJECT_HANDLER:
                seeded = self.state_of("OrdStatus", seeded)
            yield self.into_order(seeded)

    def into_instruments(self) -> Iterator[Instrument]:
        """Distinct repeating-entry instruments, or the header fallback."""
        return (instrument for _, instrument in self.into_instrument_observations())

    def into_instrument_observations(self) -> Iterator[tuple[int, Instrument]]:
        """Distinct `(entry time, instrument)` facts without constructing events."""
        if self.version is None:
            return
        seen: dict[int, list[Instrument]] = {}
        for reader in self._instrument_readers():
            instrument = reader.instrument
            if not instrument.identities():
                continue
            versions = seen.setdefault(instrument.xhash, [])
            if instrument in versions:
                continue
            versions.append(instrument)
            yield reader.unix, instrument

    def _instrument_readers(self) -> Iterator[FixEvents]:
        """Entry projections when present, otherwise the message header."""
        handler = self.dictionary.handlers.get(self._message_kind)
        if handler in ENTRY_HANDLERS:
            entries = self._group_entries("NoMDEntries")
            for entry in entries:
                yield self._inside(entry)
            if not entries:
                yield self
        elif handler in MASS_QUOTE_HANDLERS:
            yield from self._quote_readers()
        else:
            yield self

    def _inferred(self) -> str:
        """What a message with no MsgType <35> is, from the fields it carries.

        The fields are the evidence, most specific first: a market-data entry
        type means a refresh, an `ExecType <150>` or an `ExecID <17>` means an
        execution report, and an order's own identifiers mean an order. An
        empty string for anything else, which dispatches to nothing.
        """
        get = self.get
        if get("MDEntryType") is not None:
            return "X"
        if any(get(field) is not None for field in ("BidPx", "OfferPx", "QuoteStatus")):
            return "S"
        if get("ExecType") is not None or get("ExecID") is not None or get("ExecRefID") is not None:
            return "8"
        if get("ClOrdID") is not None or get("OrdType") is not None or get("OrdStatus") is not None:
            return "D"
        return ""

    def _reported(self) -> Iterator[MarketEvent]:
        """An ExecutionReport <8>: the order's new state, and the fill if there was one."""
        order = self.into_order(self.state_of("OrdStatus"))
        yield order
        if self.execution_state() is not State.UNKNOWN:
            # Completed *from the order*, not from a previous report: the
            # running totals a venue leaves out of a fill -- how much is done
            # now, how much is left, what the average is -- are all statements
            # about the order this report is on, and the order row is the one
            # thing here that already holds them.
            yield self.into_execution(order)

    def _entries(self, kind: str) -> Iterator[MarketEvent]:
        """One market-data refresh, entry by entry."""
        for entry in self._group_entries("NoMDEntries"):
            inside = self._inside(entry)
            entry_type = inside.get("MDEntryType")
            if entry_type in ENTRY_SIDES:
                yield inside.into_entry_order(ENTRY_SIDES[entry_type], snapshot=kind == "W")
            elif entry_type == ENTRY_TRADE:
                yield inside.into_entry_execution()

    def _sides(self, requested: bool = False) -> Iterator[Execution]:
        """A TradeCaptureReport, one Execution per `NoSides <552>` entry.

        A multi-sided report carries `Side <54>`, `OrderID <37>`, `ClOrdID
        <11>` and `Account <1>` *inside* each side entry, so one flat read
        kept one side's identity and silently dropped the other's. Each side
        reads the report level plus its own entry -- never a sibling's: the
        whole-message first-occurrence view would hand side one's account to
        a side two that did not repeat it. The report-level facts --
        `TrdMatchID <880>`, `LastPx <31>`, `LastQty <32>` -- fall through
        where an entry is silent, the report's resolved clock keeps steering
        a side that carries no clock of its own, and a report with no side
        entries at all is one execution, read flat as before.
        """
        entries, report = self._side_entries()
        if not entries:
            # A report request with no side entries and no trade content is a
            # query, and a query fabricates no execution.
            if requested and not any(self.get(name) for name in _TRADE_EVIDENCE_FIELDS):
                return
            yield self.into_execution()
            return
        level: dict[str, Any] = {}
        for key, value in report:
            level.setdefault(key, value)
        clocks = self._clock_keys
        for entry in entries:
            inside = self._inside(entry, base=level)
            if not any(key in clocks for key, _ in inside.pairs):
                inside.__dict__["transacted"] = self.transacted
            yield inside.into_execution()

    def _side_entries(self) -> tuple[list[list[tuple[str, str]]], list[tuple[str, str]]]:
        """`(side entries, report-level pairs)`, split without collapsing repeats.

        Segments and not `group_pairs`: a side regularly nests a multi-entry
        `NoPartyIDs` group, whose repeated tags would end a first-repeat scan
        in the middle of side one and silently drop every side after it. A
        segment runs from one delimiter to the next, whatever repeats inside.
        """
        count_tag = self.access.tag_text("NoSides")
        if self._flat_by_tag is not None and count_tag not in self.by_tag:
            return [], []
        prefix, entries = group_segment_pairs(
            self.pairs, count_tag, self._side_delimiter, with_prefix=True
        )
        if entries or not self._has_indexed_pairs:
            return entries, prefix
        indexed = indexed_group_pairs(self.source_pairs, "NoSides")
        report = [pair for pair in self.pairs if "NOSIDES[" not in str(pair[0]).upper()]
        return indexed, report

    @functools.cached_property
    def _side_delimiter(self) -> str:
        """The tag that opens one side entry, off the selected declaration."""
        registry = self.registry
        found = (
            None
            if registry is None
            else registry.group_delimiters("TrdCapRptSideGrp", ("NoSides",), self.version)
        )
        return self.access.tag_text(found[0] if found else "Side")

    @functools.cached_property
    def _clock_keys(self) -> frozenset[str]:
        """Every key `TRANSACTED` reads, as this dictionary's wire tags.

        The regulatory groups count by their members and their counts: a side
        carrying its own `SideTrdRegTS` resolves its own instant, and only a
        side with no clock at all keeps the report's answer.
        """
        named = {field for rung in TRANSACTED for field in rung.fields}
        named.update(_REGULATORY_GROUPS.values())
        named.update(_REGULATORY_MEMBERS)
        return frozenset(self.access.tag_text(field) for field in named)

    def _inside(
        self, entry: list[tuple[str, str]], base: Mapping[str, Any] | None = None
    ) -> FixEvents:
        """A repeating-group entry completed by its message header.

        `base` narrows what falls through: the whole message's first
        occurrences by default, or only the report level for a group whose
        entries are peers that must not answer for each other.
        """
        inside = FixEvents(
            message=type(self.message).from_pairs(entry),
            venue=self.venue,
            mic=self.mic,
            recunix=self.recunix,
            registry=self.registry,
            fix_version=self.version,
        )
        inside.__dict__["version"] = self.version
        inside.__dict__["tags"] = self.tags
        inside.__dict__["access"] = self.access
        own = inside.by_tag
        if all(inside.get(field) is None for field in INSTRUMENT_FIELDS):
            inside.__dict__["instrument"] = self.instrument
        inside.__dict__["by_tag"] = {**(self.by_tag if base is None else base), **own}
        return inside

    def _mass_quotes(self, kind: str) -> Iterator[Order]:
        """A MassQuote <i> or its acknowledgement, one quote entry at a time."""
        for quote in self._quote_readers():
            yield from quote._quotes(kind)

    def _quote_readers(self) -> Iterator[FixEvents]:
        """MassQuote entries flattened through their enclosing quote set."""
        sets = group_segment_pairs(
            self.pairs,
            self.access.tag_text("NoQuoteSets"),
            self._quote_group_delimiters[0],
        )
        if sets:
            for quote_set in sets:
                prefix, entries = group_segment_pairs(
                    quote_set,
                    self.access.tag_text("NoQuoteEntries"),
                    self._quote_group_delimiters[1],
                    with_prefix=True,
                )
                for entry in entries:
                    yield self._inside([*prefix, *entry])
            return
        entries = self._group_entries("NoQuoteEntries")
        if not entries:
            yield self
            return
        for entry in entries:
            yield self._inside(entry)

    @functools.cached_property
    def _quote_group_delimiters(self) -> tuple[str, str]:
        """Outer and inner delimiters from the selected FIX declaration."""
        registry = self.registry
        found = (
            None
            if registry is None
            else registry.group_delimiters(
                "QuotSetGrp", ("NoQuoteSets", "NoQuoteEntries"), self.version
            )
        )
        outer, inner = found if found else ("QuoteSetID", "QuoteEntryID")
        return self.access.tag_text(outer), self.access.tag_text(inner)

    def _quotes(self, kind: str) -> Iterator[Order]:
        """One FIX quote as an indicative order for each represented side."""
        get = self.get
        state = self._quote_state(kind)
        sides = (
            (Side.BID, "BidPx", "BidSize", "DefBidSize"),
            (Side.ASK, "OfferPx", "OfferSize", "DefOfferSize"),
        )
        populated = [
            side
            for side in sides
            if get(side[1]) is not None or get(side[2]) is not None or get(side[3]) is not None
        ]
        # A status/response commonly carries only QuoteID. It updates the side
        # it names, or both lifecycles created by the original two-sided quote.
        if not populated and kind in {"AI", "AJ", "Z"} and (get("QuoteEntryID") or get("QuoteID")):
            named_side = Side.from_fix(get("Side"), Side.UNKNOWN)
            populated = [side for side in sides if side[0] is named_side] or list(sides)
        for side, px, qty, default_qty in populated:
            yield self.into_quote_order(
                side,
                state,
                px=_number(get(px)),
                qty=_number(get(qty) or get(default_qty)),
            )

    def _quote_state(self, kind: str) -> State:
        """The standard lifecycle meaning asserted by one quote message."""
        if kind == "Z":
            return State.CANCELLED
        if kind == "AI":
            return self.state_of("QuoteStatus")
        if kind == "AJ":
            return self.state_of("QuoteRespType")
        return self.state_of("QuoteStatus", State.OPEN)

    # -- converting ---------------------------------------------------------

    def into_order(self, state: State = State.UNKNOWN) -> Order:
        """The order this message is about, in the state the message puts it in."""
        get = self.get
        unix = self.unix
        timeinforce = TimeInForce.from_fix(get("TimeInForce"), TimeInForce.DAY)
        duration = _integer(get("ExposureDuration"))
        exec_type = get("ExecType")
        if state is State.UNKNOWN:
            state = _coded(self.dictionary.exec_type_fallbacks, exec_type, State.UNKNOWN)
        order_qty = _number(get("OrderQty"))
        cumulative = _number(get("CumQty"))
        leaves = _number(get("LeavesQty"))
        if state is State.REPLACED:
            state = (
                State.FILLED
                if leaves == 0 and order_qty is not None and cumulative == order_qty
                else State.PARTIALLY_FILLED
                if cumulative is not None and cumulative > 0
                else State.NEW
            )
        transition = _quantity_transition(
            state,
            execution_state=self.execution_state(),
            order_qty=order_qty,
            cum_qty=cumulative,
            leavesqty=leaves,
            last_qty=_number(get("LastQty")),
            cancel_qty=_number(get("CxlQty")),
        )
        return self._finish(
            Order(
                unix=unix,
                creaunix=unix,
                recunix=self.recunix or unix,
                expunix=self._expires(timeinforce, unix, duration),
                state=transition.state,
                side=Side.from_fix(get("Side"), Side.UNKNOWN),
                px=_number(get("Price")),
                qty=transition.current_qty,
                prevqty=transition.previous_qty,
                kind=_coded(self.dictionary.order_kinds, get("OrdType"), MarketKind.UNKNOWN),
                timeinforce=timeinforce,
                stoppx=_number(get("StopPx")),
                hiddenqty=_hidden_qty(transition.current_qty, _number(get("MaxFloor"))),
                orderid=get("OrderID"),
                clordid=get("ClOrdID"),
                origclordid=get("OrigClOrdID"),
                clordlinkid=get("ClOrdLinkID"),
                parentclordid=get("ParentClOrdID"),
                parentorderid=get("ParentOrderID"),
                cxlrejreason=_integer(get("CxlRejReason")),
                cxlrejresponseto=get("CxlRejResponseTo"),
                **self._shared(),
            )
        )

    def into_quote_order(
        self,
        side: Side,
        state: State,
        *,
        px: float | None,
        qty: float | None,
    ) -> Order:
        """One side of a FIX quote as an indicative limit order."""
        get = self.get
        unix = self.unix
        return self._finish(
            Order(
                unix=unix,
                creaunix=unix,
                recunix=self.recunix or unix,
                expunix=unix_value(get("ValidUntilTime")),
                state=state,
                side=side,
                px=px,
                qty=qty,
                kind=MarketKind.LIMIT_ORDER,
                indicative=True,
                orderid=get("QuoteEntryID") or get("QuoteID"),
                clordid=get("QuoteReqID"),
                **self._shared(),
            )
        )

    def into_execution(self, order: Order | None = None) -> Execution:
        """What traded, as the report says it. `px` is `LastPx <31>`, not `Price <44>`."""
        get = self.get
        unix = self.unix
        return self._finish(
            Execution(
                unix=unix,
                creaunix=unix,
                recunix=self.recunix or unix,
                state=self.execution_state(),
                kind=_coded(self.dictionary.execution_kinds, get("ExecType"), MarketKind.UNKNOWN),
                side=Side.from_fix(get("Side"), Side.UNKNOWN),
                px=_number(get("LastPx")),
                qty=_number(get("LastQty")),
                execid=get("ExecID"),
                execrefid=get("ExecRefID"),
                tradeid=get("TradeID") or get("TrdMatchID"),
                linkedhashes=[order.xhash] if order is not None and order.xhash else [],
                parenthash=[order.hash] if order is not None and order.hash else [],
                orderid=get("OrderID"),
                clordid=get("ClOrdID"),
                origclordid=get("OrigClOrdID"),
                cumqty=_number(get("CumQty")),
                leavesqty=_number(get("LeavesQty")),
                aggressorindicator=_flag(get("AggressorIndicator")),
                settldate=_date(get("SettlDate")),
                settltype=get("SettlType"),
                settlcurrency=get("SettlCurrency"),
                settlcurrfxratecalc=get("SettlCurrFxRateCalc"),
                **self._shared(),
            ),
            order,
        )

    def into_entry_order(self, side: Side, snapshot: bool = False) -> Order:
        """One market-data entry as the resting interest it describes.

        A price level with a size *is* an order, aggregated or not, and reading
        it as one is what lets a book be folded from a feed and from an order
        stream by the same code. `MDEntryID <278>` is the venue's own handle on
        that interest, so it is the lifecycle identity when there is one.
        """
        get = self.get
        unix = self.unix
        return self._finish(
            Order(
                unix=unix,
                creaunix=unix,
                recunix=self.recunix or unix,
                state=self.state_of("MDUpdateAction", State.NEW if snapshot else State.OPEN),
                side=side,
                px=_number(get("MDEntryPx")),
                qty=_number(get("MDEntrySize")),
                kind=MarketKind.LIMIT_ORDER,
                indicative=True,
                # An entry with no id of its own is a *level*, not an order, so
                # the price is what persists across its updates: that is what
                # `MDUpdateAction <279>` addresses when it says Change or Delete,
                # and it is what makes a level's own lifecycle findable.
                orderid=get("MDEntryID")
                or (f"{side.name}@{get('MDEntryPx')}" if get("MDEntryPx") else None),
                **self._shared(),
            )
        )

    def into_entry_execution(self) -> Execution:
        """One market-data entry of type Trade <2> as the execution it reports."""
        get = self.get
        unix = self.unix
        return self._finish(
            Execution(
                unix=unix,
                creaunix=unix,
                recunix=self.recunix or unix,
                state=State.FILLED,
                kind=MarketKind.TRADE,
                side=Side.from_fix(get("Side"), Side.UNKNOWN),
                px=_number(get("MDEntryPx")),
                qty=_number(get("MDEntrySize")),
                execid=get("MDEntryID"),
                tradeid=get("TradeID") or get("TrdMatchID"),
                **self._shared(),
            )
        )

    def _finish(self, event: TMarketEvent, previous: MarketEvent | None = None) -> TMarketEvent:
        """Attach transient reference data before deriving and identifying."""
        event.attach_instrument(self.instrument)
        finished = event.with_previous(previous)
        if finished is None:
            raise AssertionError("a newly translated event cannot be unchanged")
        return finished

    @functools.cached_property
    def instrument(self) -> Instrument:
        """What the message says is being traded, groups and all.

        Cached like `by_tag`, and for the same reason: a message is *one*
        instrument, and an `ExecutionReport` yields two events off it while a
        refresh yields one per entry. Reading eighteen tags and two repeating
        groups once per event was that work N times -- a fifth of the cost of
        reading a five-entry refresh.
        """
        get = self.get
        cfi = get("CFICode")
        securitytype = get("SecurityType")
        altids = {**self._identifier_altids, **self.into_alt_ids()}
        ticker = SymbolTicker.from_fixmsg(self.message)
        return Instrument(
            symbolticker=ticker.symbolticker,
            symbol=get("Symbol") or "",
            kind=_classified(cfi, securitytype),
            securityid=get("SecurityID"),
            securityidsource=get("SecurityIDSource"),
            altids=altids or None,
            securitytype=securitytype,
            cficode=cfi,
            securityexchange=get("SecurityExchange"),
            currency=_currency(get("Currency")),
            contractmultiplier=_number(get("ContractMultiplier")),
            minpriceincrement=_number(get("MinPriceIncrement")),
            roundlot=_number(get("RoundLot")),
            maturitydate=_date(get("MaturityDate")) or _month_year(get("MaturityMonthYear")),
            strikeprice=_number(get("StrikePrice")),
            putorcall=OptionKind.from_fix(get("PutOrCall"), OptionKind.UNKNOWN),
            securitydesc=get("SecurityDesc"),
            legs=self.into_legs() or None,
        )

    def into_alt_ids(self) -> dict[str, str]:
        """Every alternative identifier the message carried, by the scheme's name."""
        entries = self.message.component_records("SecurityAltID")
        if entries is None:
            entries = self._group("NoSecurityAltID")
        found: dict[str, str] = {}
        for entry in entries:
            named = entry.get("SecurityAltID")
            if not named:
                continue
            raw_scheme = entry.get("SecurityAltIDSource")
            # The dictionary's own name for the scheme, the raw value where it
            # declares none, and one name for "the message did not say".
            key = id_scheme(raw_scheme) or str(raw_scheme or "")
            found.setdefault(key or UNKNOWN_SCHEME, named)
        return found

    def into_legs(self) -> list[Leg]:
        """The legs of a multileg instrument, from the `NoLegs <555>` group.

        Every member is the instrument field with a `Leg` in front of it --
        `LegSymbol <600>` is `Symbol <55>` for the leg -- so the reading is the
        same one, against a different set of tags.
        """
        entries = self.message.component_records("Legs")
        if entries is None:
            entries = self._group("NoLegs")
        built = []
        for entry in entries:
            cfi, securitytype = entry.get("LegCFICode"), entry.get("LegSecurityType")
            ticker = SymbolTicker.from_entries(
                entry.items(), registry=self.message.registry, version=self.version
            )
            built.append(
                Leg(
                    symbolticker=ticker.symbolticker,
                    symbol=entry.get("LegSymbol") or "",
                    side=Side.from_fix(entry.get("LegSide"), Side.UNKNOWN),
                    ratio=_number(entry.get("LegRatioQty")),
                    kind=_classified(cfi, securitytype),
                    securityid=entry.get("LegSecurityID"),
                    securityidsource=entry.get("LegSecurityIDSource"),
                    cficode=cfi,
                    securitytype=securitytype,
                    securityexchange=entry.get("LegSecurityExchange"),
                    currency=_currency(entry.get("LegCurrency")),
                    contractmultiplier=_number(entry.get("LegContractMultiplier")),
                    maturitydate=_date(entry.get("LegMaturityDate"))
                    or _month_year(entry.get("LegMaturityMonthYear")),
                    strikeprice=_number(entry.get("LegStrikePrice")),
                    putorcall=OptionKind.from_fix(entry.get("LegPutOrCall"), OptionKind.UNKNOWN),
                )
            )
        return built

    def _group_entries(self, name: str) -> list[list[tuple[str, str]]]:
        """One repeating group under its configured count tag or rendered name."""
        count_tag = self.access.tag_text(name)
        if self._flat_by_tag is not None and count_tag not in self.by_tag:
            return []
        found = group_pairs(self.pairs, count_tag)
        if found or not self._has_indexed_pairs:
            return found
        return indexed_group_pairs(self.source_pairs, name)

    def _group(self, name: str) -> list[dict[str, str]]:
        """One repeating group's entries, each as first-value-by-tag."""
        found = []
        for entry in self._group_entries(name):
            resolved: dict[str, str] = {}
            for key, value in self.access.tagged_pairs(entry):
                resolved.setdefault(self.dictionary.names_by_tag.get(key, key), value)
            found.append(resolved)
        return found

    # -- what every event carries ------------------------------------------

    @property
    def unix(self) -> int:
        """When the transaction happened, by `TRANSACTED` -- not when it was sent."""
        return self.transacted.unix

    @functools.cached_property
    def transacted(self) -> Transacted:
        """The resolved transaction time and the rung of the chain that gave it.

        The chain itself is `rekep.market.transacted`, called from here and
        from the parse layer alike: which clock a row happened at is one
        answer, and two copies of it would be two answers that agreed until
        they did not.
        """
        recorded = unix_value(self.get("SendingTime")) or self.recunix or None
        return resolve(
            self._clock,
            self._stamps,
            eventtype=self._event_type,
            recorded=recorded,
            member=self._stamp_member,
        )

    def _clock(self, name: str, day: int | None = None) -> int | None:
        """One FIX clock this message carries, in epoch nanoseconds."""
        return unix_value(self.get(name), day=day)

    def _stamps(self, column: str) -> Sequence[Any]:
        """One regulatory group's entries, out of the message's own pairs.

        The translation layer holds a message rather than typed columns, so
        the entries are read back out of the group the wire carried -- under
        the count tag the dictionary gives it, which is what `_group` does for
        every other component here.
        """
        group = _REGULATORY_GROUPS.get(column)
        return self._group_entries(group) if group else []

    def _stamp_member(self, entry: Any, name: str) -> Any:
        """One member of a regulatory entry, however the wire keyed it.

        Through the one accessor, because that is the whole of what it is
        for: a group's members arrive keyed by tag on a wire message and by
        name on a rendered one, and the rung declares neither spelling in
        particular -- it declares the field.
        """
        return self.access.reading(entry, name).raw

    @property
    def _event_type(self) -> EventType | None:
        """What kind of event this reader is about to build, for the ranking.

        Registry MsgType metadata is the classification authority. It is read
        before building an event because the ranking decides that event's own
        `unix`.
        """
        registry = self.access.registry
        if registry is None:
            return None
        return registry.msg_type_event_types().get(self._message_kind)

    def _expires(self, timeinforce: TimeInForce, unix: int, duration: int | None) -> int | None:
        """Exact expiry, from UTC time first and a fixed GFT duration second."""
        explicit = unix_of(self.get("ExpireTime"))
        if explicit is not None:
            return explicit
        if timeinforce is not TimeInForce.GFT or duration is None or duration <= 0:
            return None
        raw_unit = self.get("ExposureDurationUnit")
        unit = 0 if raw_unit is None else _integer(raw_unit)
        factor = _EXPOSURE_UNIT_NS.get(unit)
        if factor is None:
            return None
        expires = unix + duration * factor
        return expires if -(1 << 63) <= expires < 1 << 63 else None

    @functools.cached_property
    def _shared_values(self) -> dict[str, Any]:
        """Shared envelope values that are immutable during translation."""
        instrument = self.instrument
        mic = self._mic()
        return {
            "altids": self._identifier_altids,
            "mic": mic,
            "reason": self._reason(),
            "instrumentxhash": instrument.xhash,
            "symbolticker": instrument.symbolticker,
            "pxunit": instrument.currency.into_str() if instrument.currency else "",
            "currency": instrument.currency,
        }

    @functools.cached_property
    def _identifier_altids(self) -> dict[str, str]:
        """Every readable identifier this message carries, in lookup order."""
        found: dict[str, str] = {}
        for stored, field, tag in IDENTIFIER_FIELDS:
            value = self.get(tag) if tag else None
            if value is None:
                value = self.get(field)
            if value is not None and str(value):
                found.setdefault(stored, str(value))
        return found

    def _reason(self) -> str | None:
        """The first structured reject/restatement reason, completed by FIX text.

        The code leads because it is typed and `Text <58>` is prose: a reject
        carrying both used to keep the prose alone, and the code -- the one
        part a filter can match -- was dropped with it. `CxlRejResponseTo
        <434>` says which request a reject answers, so it rides beside the
        reason rather than landing only in the untyped metadata map.
        """
        parts: list[str] = []
        for name in (
            "OrdRejReason",
            "CxlRejReason",
            "QuoteRejectReason",
            "ExecRestatementReason",
        ):
            value = self.get(name)
            if value is None:
                continue
            parts.append(self._coded_reason(name, value))
            break
        response_to = self.get("CxlRejResponseTo")
        if response_to is not None:
            parts.append(self._coded_reason("CxlRejResponseTo", response_to))
        if text := self.get("Text"):
            parts.append(str(text))
        return "; ".join(parts) if parts else None

    def _coded_reason(self, name: str, value: Any) -> str:
        """One coded field spelled with the meaning the dictionary gives it."""
        label = self.access.meaning(name, str(value))
        return f"{name}={value}: {label}" if label else f"{name}={value}"

    def _shared(self) -> dict[str, Any]:
        """The shared envelope with event-owned metadata."""
        return {
            **self._shared_values,
            "altids": dict(self._shared_values["altids"]),
            "metadata": dict(self.extras),
        }

    def _mic(self) -> MIC | None:
        """First valid ISO code: exchange fields, configured feed, then session peers."""
        for value in (
            self.get("LastMkt"),
            self.get("SecurityExchange"),
            self.get("ExDestination"),
            self.venue,
            self.mic,
            self.get("SenderCompID"),
            self.get("TargetCompID"),
        ):
            found = MIC.from_str(value)
            if found is not MIC.UNKNOWN:
                return found
        return None

    @functools.cached_property
    def extras(self) -> dict[str, str]:
        """Every field the shapes have no column for, under the key it arrived as."""
        claimed, audited = self.dictionary.claimed, self.dictionary.audited
        rendered = self.dictionary.rendered_spellings
        return {
            key: str(value)
            for key, value in self.by_tag.items()
            if (key not in claimed and encoded_key(key) not in rendered) or key in audited
        }


#: `SecurityType <167>` to what an instrument settles as, for the venues that
#: send no `CFICode <461>`. FIX enumerates a hundred and eighteen of these and
#: most of them are one kind of bond, so this maps the **bands** rather than
#: the list: a value not here classifies as nothing, which is what `UNKNOWN`
#: is for and better than a guess. Read off the dictionary in `data/fix.zip`,
#: and checked against it by `tests/market/test_fix.py`.
SECURITY_TYPES: dict[str, AssetKind] = {
    # Equity
    "CS": AssetKind.EQUITY,
    "PS": AssetKind.EQUITY,
    # Collective investment
    "MF": AssetKind.FUND,
    # Derivatives
    "FUT": AssetKind.FUTURE,
    "OPT": AssetKind.OPTION,
    "OOF": AssetKind.OPTION,
    "OOP": AssetKind.OPTION,
    "OOC": AssetKind.OPTION,
    "WAR": AssetKind.WARRANT,
    "MLEG": AssetKind.MULTILEG,
    # Swaps, which FIX spells one per underlying
    "CDS": AssetKind.SWAP,
    "IRS": AssetKind.SWAP,
    "FXSWAP": AssetKind.SWAP,
    # Currency
    "FXSPOT": AssetKind.CURRENCY,
    "FXFWD": AssetKind.FORWARD,
    "FXNDF": AssetKind.FORWARD,
    "FORWARD": AssetKind.FORWARD,
    "CASH": AssetKind.CURRENCY,
    # Financing
    "REPO": AssetKind.REPO,
    "BUYSELL": AssetKind.REPO,
    "SECLOAN": AssetKind.LOAN,
    "SECPLEDGE": AssetKind.LOAN,
    "TERM": AssetKind.LOAN,
    "RVLV": AssetKind.LOAN,
    "RVLVTRM": AssetKind.LOAN,
    "BRIDGE": AssetKind.LOAN,
    "SWING": AssetKind.LOAN,
    # Debt: the long tail, by what a reader would call it
    "CORP": AssetKind.DEBT,
    "CB": AssetKind.DEBT,
    "TBOND": AssetKind.DEBT,
    "TNOTE": AssetKind.DEBT,
    "TBILL": AssetKind.DEBT,
    "TIPS": AssetKind.DEBT,
    "MUNI": AssetKind.DEBT,
    "GO": AssetKind.DEBT,
    "REV": AssetKind.DEBT,
    "MTN": AssetKind.DEBT,
    "CP": AssetKind.DEBT,
    "CD": AssetKind.DEBT,
    "ABS": AssetKind.DEBT,
    "MBS": AssetKind.DEBT,
    "CMO": AssetKind.DEBT,
    "FRN": AssetKind.DEBT,
    "EUCORP": AssetKind.DEBT,
    "EUSOV": AssetKind.DEBT,
    "BRADY": AssetKind.DEBT,
}


def _classified(cfi: str | None, securitytype: str | None) -> AssetKind:
    """What an instrument settles as, from its CFI code or from FIX's own word.

    The CFI first, because ISO 10962's category letter *is* what `AssetKind` is
    coded on and it classifies exactly. `SecurityType <167>` after it, because
    a venue that sends no CFI very often sends `CS`, `FUT` or `OPT` instead --
    and a reading that stopped at the CFI left every one of those `UNKNOWN`.
    """
    if cfi:
        found = AssetKind.from_cfi(cfi[:1])
        if found is not AssetKind.UNKNOWN:
            return found
    if securitytype:
        return SECURITY_TYPES.get(securitytype.strip().upper(), AssetKind.UNKNOWN)
    return AssetKind.UNKNOWN


def _month_year(text: str | None) -> datetime.date | None:
    """`MaturityMonthYear <200>` as a date -- the first of the month it names."""
    if not text:
        return None
    trimmed = text.strip()
    if len(trimmed) < 6 or not trimmed[:6].isdigit():
        return None
    day = trimmed[6:8]
    try:
        return datetime.date(int(trimmed[:4]), int(trimmed[4:6]), int(day) if day.isdigit() else 1)
    except ValueError:
        return None


def unix_value(value: Any, day: int | None = None) -> int | None:
    """FIX text or a typed parsed-log clock as epoch nanoseconds."""
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.UTC).replace(tzinfo=None)
        date = (value.toordinal() - EPOCH_ORDINAL) * SECONDS_A_DAY * NANOS
        clock = (value.hour * 3600 + value.minute * 60 + value.second) * NANOS
        return date + clock + value.microsecond * 1_000
    if isinstance(value, datetime.date):
        return (value.toordinal() - EPOCH_ORDINAL) * SECONDS_A_DAY * NANOS
    if isinstance(value, datetime.time):
        base = 0 if day is None else day - day % (SECONDS_A_DAY * NANOS)
        return (
            base
            + (value.hour * 3600 + value.minute * 60 + value.second) * NANOS
            + value.microsecond * 1_000
        )
    return unix_of(value, day=day)


def _currency(text: str | None) -> Currency | None:
    """A present FIX currency as its packed code; absence stays null."""
    return None if text is None else Currency.from_fix(text)


def _number(text: str | None) -> float | None:
    """A FIX `Price`, `Qty` or `float` as a float; None for anything that is not.

    None rather than zero, and that is the whole point: a price the venue did
    not send and a price of zero are different facts, and a market order has
    no limit at all.
    """
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _hidden_qty(qty: float | None, displayed: float | None) -> float | None:
    """Reserve quantity implied by FIX MaxFloor; absent when it cannot be known."""
    if qty is None or displayed is None:
        return None
    return max(qty - displayed, 0.0)


def _integer(text: Any) -> int | None:
    """A FIX `int`, `SeqNum` or reject code as an int; None for anything else."""
    if text is None or text == "":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _flag(text: str | None) -> bool | None:
    """A FIX `Boolean`: `Y` or `N`, and None for a venue that sent neither."""
    if not text:
        return None
    first = text.strip()[:1].upper()
    return True if first == "Y" else False if first == "N" else None


def _date(text: str | None) -> datetime.date | None:
    """A FIX `LocalMktDate` or `UTCDateOnly` as a date."""
    stamped = unix_of(text)
    if stamped is None:
        return None
    return datetime.date.fromordinal(stamped // (SECONDS_A_DAY * NANOS) + EPOCH_ORDINAL)
