"""What the published FIX registries hold, and the calls that rebuild them.

The published dictionary (`data/fix.zip`) is the whole standard plus rekep's
frozen vocabulary and is far too large to ship in a wheel;
`rekep/fix/registry.zip` is its standard-field projection plus that vocabulary.
The projection key list lives here as reviewable data and is checked against
what the package actually looks up.
"""

from __future__ import annotations

import os
import pathlib
import tempfile
from collections.abc import Mapping, Sequence
from types import MappingProxyType

from rekep.fix.registry import FixRegistry
from rekep.fix.rekep import register_rekep
from rekep.fix.store import ConflictReport

#: How many identities the published dictionary collapses, per part, today.
#: A record keeps one reading and the versions that declare it, so every
#: disagreement between sources or versions is a decision -- and 522 value
#: readings with different meanings are a list somebody
#: can read. A refresh that grows any of these introduced conflicts nobody
#: looked at, and fails rather than shipping them.
CONFLICT_BASELINE: Mapping[str, int] = MappingProxyType(
    {
        "values": 522,
        "aliases": 223,
        "added": 16,
        "type": 274,
        "name": 59,
        "note": 18,
        "members": 65,
        "encoded": 134,
    }
)

#: Session and application fields the parsed FixMsg lifts into its own columns.
#: `rekep.fix.columns` is the authority; a test holds this list to it.
FIXMSG_FIELDS: tuple[str, ...] = (
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
    "Symbol",
    "SecurityID",
    "SecurityIDSource",
    "SecurityType",
    "CFICode",
    "SecurityExchange",
    "Currency",
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
    "PartyID",
    "PartyIDSource",
    "PartyRole",
    "NoPartyIDs",
    "NoPartySubIDs",
    "PartySubID",
    "PartySubIDType",
    "TrdRegTimestamp",
    "TrdRegTimestampType",
    "TrdRegTimestampOrigin",
    "NoTrdRegTimestamps",
    "SideTrdRegTimestamp",
    "SideTrdRegTimestampType",
    "SideTrdRegTimestampSrc",
    "NoSideTrdRegTS",
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

#: Fields the market translation reads, whether or not it stores each as its
#: own column. `rekep.market.fix.market_tags` is the authority; the same test
#: holds this list to it. Kept here rather than imported because `fix` is
#: underneath `market` and must not depend on it.
MARKET_FIELDS: tuple[str, ...] = (
    "OrigTime",
    "TradeDate",
    "ExpireTime",
    "ExpireDate",
    "ExposureDuration",
    "ExposureDurationUnit",
    "ExecTransType",
    "CxlRejReason",
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
    "NoSides",
    "ClOrdLinkID",
    "CxlRejResponseTo",
    "SettlCurrFxRateCalc",
    "StopPx",
    "ExecRefID",
    "TradeID",
    "AggressorIndicator",
)

#: Fields no declaration here names but real bridge traffic carries, so a key
#: that resolves to one of them is a known field rather than an unknown name.
#: Each earns its place by having been counted in a capture, and a projection
#: that dropped one would move those counts back into "unknown".
BRIDGE_FIELDS: tuple[str, ...] = (
    "AdvId",
    "AdvRefID",
    "AdvSide",
    "AdvTransType",
    "BeginSeqNo",
    "ClientID",
    "Commission",
    "CommType",
    "ContraBroker",
    "EndSeqNo",
    "EventType",
    "ExecBroker",
    "ExecInst",
    "ExecRestatementReason",
    "FutSettDate",
    "HandlInst",
    "IOIID",
    "LastCapacity",
    "LastMkt",
    "MaxFloor",
    "MultiLegReportingType",
    "NoTradingSessions",
    "OrdRejReason",
    "OrderCapacity",
    "Rule80A",
    "SecondaryClOrdID",
    "SecondaryExecID",
    "SecondaryOrderID",
    "SettlCurrency",
    "SettlDate",
    "SettlType",
    "TradingSessionID",
    "TradingSessionSubID",
)

#: Fields FIX never numbered that the parsed log gives a column of its own.
#: Selected by name, because a name is all such a field has.
NAMESPACE_FIELDS: tuple[str, ...] = ("ISINCODE", "ParentClOrdID", "ParentOrderID")

#: Every standard key the packaged projection selects, in declaration order.
#: `register_rekep` adds the 27 package-owned identities after this selection.
PROJECTED: tuple[str, ...] = tuple(
    dict.fromkeys((*FIXMSG_FIELDS, *MARKET_FIELDS, *BRIDGE_FIELDS, *NAMESPACE_FIELDS))
)


def publish_builtin(
    source: str | os.PathLike[str],
    target: str | os.PathLike[str],
) -> pathlib.Path | str:
    """Rebuild the wheel's registry from the published dictionary, and name it."""
    registry = FixRegistry(cache_dir=source, offline=True)
    with tempfile.TemporaryDirectory(prefix="rekep-fix-projection-") as scratch:
        staged = registry.into_projection(pathlib.Path(scratch) / "registry.zip", PROJECTED)
        projected = register_rekep(FixRegistry(cache_dir=staged, offline=True))
        return projected.into_zip(target)


def publish_full(
    source: str | os.PathLike[str],
    target: str | os.PathLike[str],
) -> pathlib.Path | str:
    """Register rekep's vocabulary and publish the complete offline registry."""
    registry = register_rekep(FixRegistry(cache_dir=source, offline=True))
    return registry.into_zip(target)


def missing_from(registry: FixRegistry, keys: Sequence[str] = PROJECTED) -> list[str]:
    """Which `keys` a registry cannot answer, so a short artifact fails loudly."""
    return [key for key in keys if not registry.lookup(key)]


def beyond_baseline(report: ConflictReport) -> list[str]:
    """Which collapse counts a rebuild grew past `CONFLICT_BASELINE`, as lines."""
    return report.exceeds(CONFLICT_BASELINE)
