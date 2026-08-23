"""FIX fields promoted from parsed pairs to typed log columns."""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType

import pyarrow

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

# These four delimit quote groups. On grouped rows they remain in `fix_tags`
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

# FIX's documentation establishes UTC for these four timestamps.
_STAMP_FIELDS: tuple[str, ...] = (
    "SendingTime",
    "OrigSendingTime",
    "OnBehalfOfSendingTime",
    "TransactTime",
    "ValidUntilTime",
)

# Public analytical names may clarify a protocol term while `fix:name` keeps
# its exact registry spelling. These overrides are part of the log contract.
_NAMES: Mapping[str, str] = MappingProxyType({"AvgPx": "vwap"})


def _snake(name: str) -> str:
    """FIX's canonical name as a public Python/Arrow name."""
    name = re.sub(r"IDs$", "Ids", name)
    words = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", words).lower()


def _physical_type(member: Field) -> pyarrow.DataType:
    """Registry type at Iceberg width, zoned only when FIX documents UTC."""
    arrow_type = member.arrow_type
    if arrow_type is None:  # pragma: no cover - generated registry invariant
        raise ValueError(f"FIX field {member.name!r} has no Arrow type")
    if not pyarrow.types.is_timestamp(arrow_type):
        return arrow_type
    datatype = member.fix.get("type", "").strip().lower()
    documented = (member.description or "").lower()
    timezone = "UTC" if datatype.startswith("utc") or "expressed in utc" in documented else None
    return pyarrow.timestamp("us", tz=timezone)


def _declaration(member: Field) -> Field:
    """A registry field in the physical shape used by parsed logs."""
    metadata = dict(member.metadata)
    metadata["fix:name"] = member.name
    return Field(
        name=_NAMES.get(member.name, _snake(member.name)),
        arrow_type=_physical_type(member),
        nullable=True,
        metadata=metadata,
    )


_REGISTRY = FixRegistry.from_builtin()
_ORDER = _SESSION_FIELDS + _COMMON_FIELDS + _QUOTE_FIELDS + _PARTY_FIELDS
_FIELDS = tuple(_REGISTRY.scalar(name) for name in _ORDER)
LOG_FIELDS: Mapping[int, Field] = MappingProxyType(
    {int(member.fix["tag"]): member for member in _FIELDS}
)
if len(LOG_FIELDS) != len(_FIELDS):  # pragma: no cover - packaged registry invariant
    raise ValueError("the bundled FIX fields do not have unique tags")
DECLARATIONS: Mapping[int, Field] = MappingProxyType(
    {tag: _declaration(member) for tag, member in LOG_FIELDS.items()}
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
    {tag: DECLARATIONS[tag].arrow_type for tag in COLUMNS}
)
TAGS: pyarrow.Array = pyarrow.array(sorted(COLUMNS), pyarrow.int32())
QUOTE_GROUP_COUNTS: pyarrow.Array = pyarrow.array(
    [_TAGS_BY_NAME[name] for name in _QUOTE_GROUP_COUNTS], pyarrow.int32()
)
QUOTE_GROUP_STRUCTURE: pyarrow.Array = pyarrow.array(
    [_TAGS_BY_NAME[name] for name in _QUOTE_GROUP_STRUCTURE], pyarrow.int32()
)

# A bridge-rendered instrument identifier rather than a standard FIX tag. Its
# source spelling stays in metadata now that the public column is snake_case.
ISIN_CODE = Field(
    name="isincode",
    arrow_type=pyarrow.string(),
    nullable=True,
    metadata={
        "description": "ISIN carried by the rendered ISINCODE field.",
        "fix:name": "ISINCODE",
        "fix:type": "String",
    },
)
NAMED: Mapping[str, Field] = MappingProxyType({"isincode": ISIN_CODE})
