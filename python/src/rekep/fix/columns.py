"""FIX fields promoted from parsed pairs to typed FixMsg columns."""

from __future__ import annotations

import json
from collections.abc import Mapping
from types import MappingProxyType

import pyarrow
import pyarrow.compute

from rekep.entries import ENTRIES as ENTRIES
from rekep.entries import TAG as TAG
from rekep.entries import Entry as Entry
from rekep.fields import Field
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
    "Account",
    "ClOrdID",
    "OrigClOrdID",
    "OrderID",
    "ExecID",
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
    "TransactTime",
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

# FIX's documentation establishes UTC for these four timestamps.
_STAMP_FIELDS: tuple[str, ...] = (
    "SendingTime",
    "OrigSendingTime",
    "OnBehalfOfSendingTime",
    "TransactTime",
    "ValidUntilTime",
)


def _physical_type(member: Field) -> pyarrow.DataType:
    """Registry type at Iceberg width, zoned only when FIX documents UTC."""
    data_type = member.data_type
    if data_type is None:  # pragma: no cover - generated registry invariant
        raise ValueError(f"FIX field {member.name!r} has no Arrow type")
    if not pyarrow.types.is_timestamp(data_type):
        return data_type
    datatype = member.fix.get("type", "").strip().lower()
    documented = (member.description or "").lower()
    timezone = "UTC" if datatype.startswith("utc") or "expressed in utc" in documented else None
    return pyarrow.timestamp("us", tz=timezone)


def _declaration(member: Field) -> Field:
    """A registry field in the physical shape used by parsed logs."""
    metadata = dict(member.metadata)
    metadata["fix:name"] = member.name
    return Field(
        name=member.name,
        data_type=_physical_type(member),
        nullable=True,
        metadata=metadata,
    )


_REGISTRY = FixRegistry.from_builtin()
_MERGED_FIELDS = _REGISTRY.merged_fields()

# Source identifiers retained on parsed market rows, in lifecycle lookup
# order. Tags come from the registry so this declaration never respells them.
_IDENTIFIER_NAMES: tuple[tuple[str, str], ...] = (
    ("order_id", "OrderID"),
    ("secondary_order_id", "SecondaryOrderID"),
    ("orig_cl_ord_id", "OrigClOrdID"),
    ("cl_ord_id", "ClOrdID"),
    ("secondary_cl_ord_id", "SecondaryClOrdID"),
    ("cl_ord_link_id", "ClOrdLinkID"),
    ("exec_id", "ExecID"),
    ("secondary_exec_id", "SecondaryExecID"),
    ("exec_ref_id", "ExecRefID"),
    ("trade_id", "TradeID"),
    ("trd_match_id", "TrdMatchID"),
    ("quote_entry_id", "QuoteEntryID"),
    ("quote_id", "QuoteID"),
    ("quote_req_id", "QuoteReqID"),
    ("quote_set_id", "QuoteSetID"),
    ("md_entry_id", "MDEntryID"),
    ("md_entry_ref_id", "MDEntryRefID"),
)


def _identifier_tag(name: str) -> int:
    """Registry tag, plus FIX's omitted `MDEntryRefID <280>` declaration."""
    member = _MERGED_FIELDS.get(name)
    if member is not None and (tag := member.fix.get("tag")):
        return int(tag)
    if name == "MDEntryRefID":
        return 280
    raise ValueError(f"FIX identifier {name!r} has no tag")


IDENTIFIER_FIELDS: tuple[tuple[str, str, int], ...] = tuple(
    (stored, name, _identifier_tag(name)) for stored, name in _IDENTIFIER_NAMES
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
    {int(member.fix["tag"]): member for member in _FIELDS}
)
if len(FIXMSG_FIELDS) != len(_FIELDS):  # pragma: no cover - packaged registry invariant
    raise ValueError("the bundled FIX fields do not have unique tags")
DECLARATIONS: Mapping[int, Field] = MappingProxyType(
    {tag: _declaration(member) for tag, member in FIXMSG_FIELDS.items()}
)

_TAGS_BY_NAME = {member.name: int(member.fix["tag"]) for member in _FIELDS}
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
    {tag: DECLARATIONS[tag].data_type for tag in COLUMNS}
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
    spellings = [entry.fix["name"], *_json_list(entry.fix.get("aliases"))]
    return tuple(dict.fromkeys(name.strip().lower() for name in spellings if name.strip()))


def _json_list(value: str | None) -> list[str]:
    """The alias names a merged declaration carries, provenance dropped."""
    try:
        decoded = json.loads(value or "[]")
    except ValueError:  # pragma: no cover - the registry writes these itself
        return []
    return [str(alias.get("name", "")) for alias in decoded if alias.get("name")]


#: What a lifted namespaced field carries into the log contract. Deliberately not
#: everything the registry knows: the aliases a name answers to and the
#: versions it was seen in are registry bookkeeping, and putting them here
#: would make recording one newly observed spelling a change to a published
#: schema.
_NAMESPACE_METADATA: tuple[str, ...] = ("description", "fix:name", "fix:type")


def _namespace_column(entry: Field) -> Field:
    """One namespaced field as the log column it is lifted into."""
    return Field(
        name=entry.fix["column"],
        data_type=entry.data_type,
        nullable=True,
        metadata={key: entry.metadata[key] for key in _NAMESPACE_METADATA if key in entry.metadata},
    )


def namespace_columns(registry: FixRegistry) -> Mapping[str, Field]:
    """`{canonical name: the log column it is lifted into}` for one dictionary."""
    return MappingProxyType(
        {
            entry.fix["name"]: _namespace_column(entry)
            for entry in registry.merged_fields().values()
            if entry.fix.get("column")
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
    whole = {spelling: name for name, _ in columns.items() for spelling in _named(merged[name])}
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


NAMESPACE_FIELDS: Mapping[str, Field] = namespace_columns(_REGISTRY)
NAMESPACE_COLUMNS: Mapping[str, Field] = named_columns(_REGISTRY)

#: The ones the parsed log declares by name, kept as names so `FixMsg` can
#: annotate its columns with them.
ISIN_CODE: Field = NAMESPACE_FIELDS["ISINCODE"]
PARENT_CL_ORD_ID: Field = NAMESPACE_FIELDS["ParentClOrdID"]
PARENT_ORDER_ID: Field = NAMESPACE_FIELDS["ParentOrderID"]
