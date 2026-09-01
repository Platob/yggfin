"""FIX fields promoted from parsed pairs to typed FixMsg columns."""

from __future__ import annotations

import functools
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.entries import ENTRIES as ENTRIES
from rekep.entries import TAG as TAG
from rekep.entries import Entry as Entry
from rekep.enums import SecurityIDSource
from rekep.fields import Field, column_name
from rekep.fix.fields import UTC_DATATYPES, arrow_type_of, documented_utc
from rekep.fix.registry import FixRegistry

# Ordered by the log schema, using the registry's canonical names so no tag is
# declared a second time in code.
_SESSION_FIELDS: tuple[str, ...] = (
    "BeginString",
    "BodyLength",
    "MsgType",
    "CheckSum",
    "SenderCompID",
    "SenderSubID",
    "SenderLocationID",
    "TargetCompID",
    "TargetSubID",
    "TargetLocationID",
    "OnBehalfOfCompID",
    "OnBehalfOfSubID",
    "OnBehalfOfLocationID",
    "DeliverToCompID",
    "DeliverToSubID",
    "DeliverToLocationID",
    "MsgSeqNum",
    "LastMsgSeqNumProcessed",
    "PossDupFlag",
    "PossResend",
    "SendingTime",
    "OrigSendingTime",
    "OnBehalfOfSendingTime",
    "ApplVerID",
    "CstmApplVerID",
    "ApplExtID",
    "MessageEncoding",
    "XmlDataLen",
    "XmlData",
    "SecureDataLen",
    "SecureData",
    "SignatureLength",
    "Signature",
)

_COMMON_FIELDS: tuple[str, ...] = (
    "Symbol",
    "SecurityID",
    "SecurityIDSource",
    "SecurityType",
    "CFICode",
    "SecurityExchange",
    "Currency",
    # What a contract *is*, beside what it is called: the same instrument
    # facts a leg of a multileg already carries as struct members, so a
    # message-scoped instrument and a leg's read the same way. `Instrument`
    # declares every one of them, and reading them off a column is what keeps
    # it from walking `entries` again for facts this stage already resolved.
    "MaturityDate",
    "MaturityMonthYear",
    "StrikePrice",
    "PutOrCall",
    "ContractMultiplier",
    "MinPriceIncrement",
    "RoundLot",
    "SecurityDesc",
    "Account",
    "ClOrdID",
    "OrigClOrdID",
    "OrderID",
    "ExecID",
    "GlobalOrderId",
    "RootOrderId",
    "RootOriginatorOrderId",
    "OrderFlags",
    "OrderOriginatorId",
    "ConversationId",
    "BloombergCode",
    "Side",
    "OrdType",
    "TimeInForce",
    "OrdStatus",
    "ExecType",
    "OrderQty",
    "Price",
    "AvgPx",
    "CumQty",
    "LeavesQty",
    "LastPx",
    "LastQty",
    "GrossTradeAmt",
    "LastShares",
    "LastMkt",
    "MarketMarker",
    "Env",
    "SettlCurrency",
    "TransactTime",
    "OrigTime",
    "CreationTime",
    "ExpireTime",
    "Text",
)

_QUOTE_FIELDS: tuple[str, ...] = (
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

# These four delimit quote groups. On grouped rows they remain in `entries`
# even when also lifted, so a later market reader can reconstruct one-entry
# groups without reparsing the raw message.
_QUOTE_GROUP_COUNTS: tuple[str, ...] = ("NoQuoteSets", "NoQuoteEntries")
_QUOTE_GROUP_STRUCTURE: tuple[str, ...] = (
    *_QUOTE_GROUP_COUNTS,
    "QuoteSetID",
    "QuoteEntryID",
)

# Fields materialized inside the structured Parties component.
_PARTY_FIELDS: tuple[str, ...] = (
    "PartyID",
    "PartyIDSource",
    "PartyRole",
    "PartyRoleQualifier",
    "NoPartyIDs",
    "NoPartySubIDs",
    "PartySubID",
    "PartySubIDType",
)

# Fields materialized inside the two structured regulatory-clock components.
# Declared here so each has a `DECLARATIONS` entry to annotate its column with;
# none of them is a flat column of its own, because each belongs to an entry.
_STAMP_GROUP_FIELDS: tuple[str, ...] = (
    "TrdRegTimestamp",
    "TrdRegTimestampType",
    "TrdRegTimestampOrigin",
    "TrdRegTimestampManualIndicator",
    "DeskType",
    "DeskTypeSource",
    "DeskOrderHandlingInst",
    "InformationBarrierID",
    "NBBOEntryType",
    "NBBOPrice",
    "NBBOQty",
    "NBBOSource",
    "NoTrdRegTimestamps",
    "SideTrdRegTimestamp",
    "SideTrdRegTimestampType",
    "SideTrdRegTimestampSrc",
    "NoSideTrdRegTS",
)

# Fields materialized inside the two structured instrument components -- the
# alternative identifiers of `NoSecurityAltID <454>` and the legs of `NoLegs
# <555>`. Declared here so each has a `DECLARATIONS` entry to annotate its
# column with; none of them is a flat column of its own, because each belongs
# to an entry.
_INSTRUMENT_GROUP_FIELDS: tuple[str, ...] = (
    "SecurityAltID",
    "SecurityAltIDSource",
    "SymbolPositionNumber",
    "NoSecurityAltID",
    "LegSymbol",
    "LegSecurityID",
    "LegSecurityIDSource",
    "LegSecurityType",
    "LegCFICode",
    "LegSecurityExchange",
    "LegMaturityDate",
    "LegMaturityMonthYear",
    "LegStrikePrice",
    "LegPutOrCall",
    "LegContractMultiplier",
    "LegCurrency",
    "LegSide",
    "LegRatioQty",
    "NoLegs",
)

# FIX's documentation establishes UTC for these timestamps.
_STAMP_FIELDS: tuple[str, ...] = (
    "SendingTime",
    "OrigSendingTime",
    "OnBehalfOfSendingTime",
    "TransactTime",
    "OrigTime",
    "CreationTime",
    "ExpireTime",
    "ValidUntilTime",
)


def physical_type(member: Field) -> pyarrow.DataType | None:
    """FIX-backed column type at Iceberg width and its documented zone.

    Dates are timestamps too: midnight is a usable first value, while a
    `date32` column cannot retain a later feed rule that supplies a clock.
    """
    dtype = member.dtype
    datatype = member.fix.get("type", "").strip().lower()
    if dtype is None:
        inferred = arrow_type_of(datatype)
        if not pyarrow.types.is_temporal(inferred):
            return None
        dtype = inferred
    if not (pyarrow.types.is_date(dtype) or pyarrow.types.is_timestamp(dtype)):
        return dtype
    zoned = datatype in UTC_DATATYPES or documented_utc(member.description)
    return pyarrow.timestamp("us", tz="UTC" if zoned else None)


#: What a column lifted from a registry record carries, and nothing else.
#:
#: Which FIX field it is, what the dictionary calls it, and what type the
#: protocol gives it. Deliberately not everything the registry knows: the
#: versions a field was seen in, the messages that carry it, the sources that
#: answered, and the enumeration it declares are
#: the *registry's* bookkeeping. Copied onto every column of every table they
#: made a published contract a second, worse copy of the dictionary -- and
#: made recording one newly observed spelling a change to a published schema.
#:
#: Aliases are the exception: their declared order is the fallback order for
#: rendered bridge keys, so a column needs them when a registry is unavailable.
_COLUMN_METADATA: tuple[str, ...] = (
    "description",
    "fix:aliases",
    "fix:tag",
    "fix:tags",
    "fix:name",
    "fix:type",
)


def column_metadata(source: Mapping[str, str]) -> dict[str, str]:
    """`source` narrowed to what a column says about the field it reads."""
    return {key: value for key, value in source.items() if key in _COLUMN_METADATA}


def _declaration(member: Field) -> Field:
    """A registry field in the physical shape used by parsed logs.

    Named as a column is named -- folded -- with the dictionary's spelling in
    `fix:name`, which is the readable half the fold throws away.
    """
    metadata = column_metadata(member.metadata)
    metadata["fix:name"] = member.name
    dtype = physical_type(member)
    if dtype is None:  # pragma: no cover - generated registry invariant
        raise ValueError(f"FIX field {member.name!r} has no Arrow type")
    return Field(
        name=column_name(member.name),
        dtype=dtype,
        nullable=True,
        metadata=metadata,
    )


_REGISTRY = FixRegistry.from_builtin()
_MERGED_FIELDS = _REGISTRY.merged_fields()

# Source identifiers retained on parsed market rows, in lifecycle lookup
# order. Tags come from the registry so this declaration never respells them,
# and each is stored under its folded name -- the same name its column would
# carry, so an `altids` key and a column key are never two spellings of one
# field.
_IDENTIFIER_NAMES: tuple[str, ...] = (
    "GlobalOrderId",
    "RootOrderId",
    "RootOriginatorOrderId",
    "OrderID",
    "SecondaryOrderID",
    "OrigClOrdID",
    "ClOrdID",
    "SecondaryClOrdID",
    "ClOrdLinkID",
    "ExecID",
    "SecondaryExecID",
    "ExecRefID",
    "TradeID",
    "TrdMatchID",
    "QuoteEntryID",
    "QuoteID",
    "QuoteReqID",
    "QuoteSetID",
    "MDEntryID",
    "MDEntryRefID",
)


def _identifier_tag(name: str) -> int:
    """Registry tag, plus FIX's omitted `MDEntryRefID <280>` declaration."""
    member = _MERGED_FIELDS.get(name)
    if member is not None and (tag := member.fix.tag) is not None:
        return tag
    if name == "MDEntryRefID":
        return 280
    raise ValueError(f"FIX identifier {name!r} has no tag")


IDENTIFIER_FIELDS: tuple[tuple[str, str, int], ...] = tuple(
    (column_name(name), name, _identifier_tag(name)) for name in _IDENTIFIER_NAMES
)

_ORDER = (
    _SESSION_FIELDS
    + _COMMON_FIELDS
    + _QUOTE_FIELDS
    + _PARTY_FIELDS
    + _STAMP_GROUP_FIELDS
    + _INSTRUMENT_GROUP_FIELDS
)
_FIELDS = tuple(_REGISTRY.scalar(name) for name in _ORDER)
FIXMSG_FIELDS: Mapping[int, Field] = MappingProxyType(
    {member.fix.tag: member for member in _FIELDS}
)
if len(FIXMSG_FIELDS) != len(_FIELDS):  # pragma: no cover - packaged registry invariant
    raise ValueError("the bundled FIX fields do not have unique tags")
DECLARATIONS: Mapping[int, Field] = MappingProxyType(
    {tag: _declaration(member) for tag, member in FIXMSG_FIELDS.items()}
)
#: The same declarations under the FIX name each one carries, which is how a
#: model annotates its column: a member called `msgtype` says
#: `DECLARED["MsgType"]` and the tag stays in the dictionary, where it is
#: stated once. A name this registry has not got raises here, at import,
#: rather than annotating a column with the wrong field the way a mistyped tag
#: did. Keyed by the dictionary's spelling and not by the folded column name,
#: because the spelling is what a reader writing the annotation has.
DECLARED: Mapping[str, Field] = MappingProxyType(
    {member.fix.canonical: member for member in DECLARATIONS.values()}
)

_TAGS_BY_NAME = {member.name: member.fix.tag for member in _FIELDS}
STAMPS: frozenset[int] = frozenset(_TAGS_BY_NAME[name] for name in _STAMP_FIELDS)
SESSION: tuple[tuple[int, str], ...] = tuple(
    (tag, DECLARATIONS[tag].name) for name in _SESSION_FIELDS if (tag := _TAGS_BY_NAME[name])
)
COMMON: tuple[tuple[int, str], ...] = tuple(
    (tag, DECLARATIONS[tag].name) for name in _COMMON_FIELDS if (tag := _TAGS_BY_NAME[name])
)
QUOTE: tuple[tuple[int, str], ...] = tuple(
    (tag, DECLARATIONS[tag].name) for name in _QUOTE_FIELDS if (tag := _TAGS_BY_NAME[name])
)
FLAT: tuple[tuple[int, str], ...] = SESSION + COMMON + QUOTE
COLUMNS: Mapping[int, str] = MappingProxyType(dict(FLAT))
TYPES: Mapping[int, pyarrow.DataType] = MappingProxyType(
    {tag: DECLARATIONS[tag].dtype for tag in COLUMNS}
)
TAGS: pyarrow.Array = pyarrow.array(sorted(COLUMNS), pyarrow.int32())
QUOTE_GROUP_COUNTS: pyarrow.Array = pyarrow.array(
    [_TAGS_BY_NAME[name] for name in _QUOTE_GROUP_COUNTS], pyarrow.int32()
)
QUOTE_GROUP_STRUCTURE: pyarrow.Array = pyarrow.array(
    [_TAGS_BY_NAME[name] for name in _QUOTE_GROUP_STRUCTURE], pyarrow.int32()
)

# -- fields FIX never numbered ------------------------------------------------
#
# A bridge renders names the standard has no tag for -- `ISINCODE` beside the
# instrument block, a vendor's `TECH.CLIENTID`. They are declared in the same
# registry as every numbered tag, under `kind: namespace`, and the ones the
# parsed log gives a column of their own name it in `fix:column`.
#
# Derived, not written here: registering a newly observed namespaced field or a
# newly observed spelling of one is then a change to the registry, which has a
# schema, a collision check and a CLI -- rather than one more Python literal in
# this file, which had exactly one and could not have had a hundred.


def _named(entry: Field) -> tuple[str, ...]:
    """Every spelling a rendered key may carry for one namespaced field, whole."""
    aliased = [str(alias.get("name", "")) for alias in entry.fix.aliases if alias.get("name")]
    spellings = [entry.fix.name, *aliased]
    return tuple(dict.fromkeys(column_name(name) for name in spellings if name.strip()))


def _namespace_column(entry: Field) -> Field:
    """One namespaced field as the log column it is lifted into.

    The declared column is folded the way every other column is, and the name
    the registry knows it by is what the column says it is called -- so
    `ISINCODE` is stored as `isincode` and still reads back as `ISINCODE`.
    """
    built = Field(
        name=entry.fix.column,
        dtype=entry.dtype,
        nullable=True,
        metadata=column_metadata(entry.metadata),
    )
    built.fix.name = entry.fix.name
    return built


def namespace_columns(registry: FixRegistry) -> Mapping[str, Field]:
    """`{canonical name: the log column it is lifted into}` for one dictionary."""
    return MappingProxyType(
        {
            entry.fix.name: _namespace_column(entry)
            for entry in registry.merged_fields().values()
            if entry.fix.column
        }
    )


def named_columns(registry: FixRegistry) -> Mapping[str, Field]:
    """`{rendered spelling: the log column}` for one dictionary's namespaced fields.

    Whole names, and their last dotted segment beside them. Whole because a
    namespace is part of the name -- `TECH.CLIENTID` and a second
    vendor's `CLIENTID` are two fields, and matching on the tail alone would
    make them one. The tail as well because the same field is rendered both
    ways in one estate, and only where exactly one field claims it: a tail two
    fields would answer to is a guess, and is left out rather than guessed.
    """
    merged = registry.merged_fields()
    columns = namespace_columns(registry)
    # Canonical names claim their folds before aliases. Within the alias tier,
    # registry declaration order is priority and the first claimant wins.
    whole: dict[str, str] = {}
    for name in columns:
        whole.setdefault(column_name(merged[name].fix.canonical), name)
    for name in columns:
        for spelling in _named(merged[name])[1:]:
            whole.setdefault(spelling, name)
    tails: dict[str, set[str]] = {}
    for spelling, name in whole.items():
        tail = spelling.rsplit(".", 1)[-1]
        if tail != spelling:
            tails.setdefault(tail, set()).add(name)
    found = {spelling: columns[name] for spelling, name in whole.items()}
    for tail, owners in tails.items():
        if tail not in found and len(owners) == 1:
            found[tail] = columns[next(iter(owners))]
    return MappingProxyType(found)


# -- the schemes an identifier may be issued under ---------------------------
#
# `SecurityIDSource <22>` enumerates thirty-three of them and this package used
# to compile its own copy as an enum, banded by issuer. The dictionary already
# names every one, so the copy is gone and a scheme is stored under the name
# the dictionary gives it.

#: The scheme `Instrument.isincode` is read from, by the dictionary's own
#: symbol for it. One scheme is named here because "this instrument's ISIN" is
#: this package's question and not the dictionary's -- it enumerates the
#: schemes and ranks none -- while the wire value beside it is still read from
#: the dictionary rather than written down.
ISIN_SCHEME = "ISINNumber"

#: What an identifier carried under no stated scheme is filed under.
UNKNOWN_SCHEME = "UNKNOWN"


@functools.cache
def _schemes() -> Field:
    return _REGISTRY.scalar("SecurityIDSource")


def id_schemes() -> Mapping[str, str]:
    """`{wire value: the scheme it names}` for `SecurityIDSource <22>`."""
    return _schemes().fix.symbols


def id_scheme(value: Any) -> str:
    """The scheme a stored value names, by its wire code or by its own name.

    Empty where nothing names one, so a caller keeps whatever the message
    wrote rather than filing it under a scheme the dictionary never declared.
    """
    text = str(value or "").strip()
    if not text:
        return ""
    declared = _schemes().fix
    return declared.symbols.get(text) or declared.symbols.get(declared.encode(text), "")


def isin_identity(
    securityid: Any, securityidsource: Any, isincode: Any
) -> tuple[Any, SecurityIDSource | None, Any]:
    """The identifier pair and the flat ISIN, each filled from the other.

    An ISIN reaches a row two ways -- `SecurityID <48>` under the source tag 22
    calls ISIN, or a bridge's own rendered `ISINCODE` -- and which one a feed
    wrote is not a reader's business. Filled here rather than at each reader
    because the ticker is built from the pair: a row carrying only the second
    had no identifier at all, and fell back to its symbol.
    """
    scheme = SecurityIDSource.from_str(securityidsource) if securityidsource else None
    identifier = securityid or None
    isin = isincode or None
    if isin and (identifier is None or (scheme is None and identifier == isin)):
        # Or where the two agree and nothing named a scheme: an identifier that
        # *is* the row's ISIN is issued under ISIN, whichever field carried it.
        return isin, SecurityIDSource.ISIN, isin
    if identifier is not None and scheme is SecurityIDSource.ISIN:
        return identifier, scheme, isin or identifier
    return identifier, scheme, isin


NAMESPACE_FIELDS: Mapping[str, Field] = namespace_columns(_REGISTRY)
NAMESPACE_COLUMNS: Mapping[str, Field] = named_columns(_REGISTRY)

#: The ones the parsed log declares by name, kept as names so `FixMsg` can
#: annotate its columns with them.
#: `SecurityIDSource <22>` with the type left to the column that declares it.
#: The standard types the field `String`; this package reads its thirty-three
#: codes as one code, and `DECLARED` would hand back the standard's width.
SECURITY_ID_SOURCE: Field = _REGISTRY.scalar("SecurityIDSource", dtype=None)
SECURITY_ID_SOURCE.metadata = column_metadata(SECURITY_ID_SOURCE.metadata)
SECURITY_ID_SOURCE.fix.name = SECURITY_ID_SOURCE.fix.canonical

ISIN_CODE: Field = NAMESPACE_FIELDS["ISINCODE"]
PARENT_CL_ORD_ID: Field = NAMESPACE_FIELDS["ParentClOrdID"]
PARENT_ORDER_ID: Field = NAMESPACE_FIELDS["ParentOrderID"]
