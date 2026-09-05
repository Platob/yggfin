"""The package-owned FIX fields, components, and persisted message types."""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING

import pyarrow

from rekep.entries import ENTRIES
from rekep.fields import Field
from rekep.fix.entries import ANY_VERSION, STANDARD, Alias, ComponentRecord, record_copy
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import block, field_member, reference_member

if TYPE_CHECKING:
    from collections.abc import Mapping

    from rekep.fix.registry import FixRegistry

# FIX reserves 5000-9999 for users and venues commonly occupy 20000, so 30000+ avoids both.
REKEP_TAG_OFFSET = 30000
#: The last FIX version that spelled tag 32 `LastShares`, which is what the
#: alias is sourced to: a spelling is attributed to where it was read.
LASTSHARES_VERSION = "4.2"
# `enums.ascii_codes.PRIVATE_RANK = 9000` is a rank band, not a FIX tag range.
# Package fields use ordinary FIX names. A custom tag is already a complete
# identity, so a namespace in its name would give one field two identities.

REKEP_FIELD_DECLARATIONS: tuple[tuple[str, str, str, str], ...] = (
    ("unix", "Unix", "int64", "Event time in whole nanoseconds since the epoch."),
    (
        "unixpartition",
        "UnixPartition",
        "int32",
        "Hour boundary of `unix` in whole epoch seconds.",
    ),
    (
        "eventtype",
        "MarketEventType",
        "int64",
        "Packed rekep event-kind code.",
    ),
    (
        "creaunix",
        "CreaUnix",
        "int64",
        "Creation time in whole nanoseconds since the epoch.",
    ),
    (
        "recunix",
        "RecUnix",
        "int64",
        "Recording time in whole nanoseconds since the epoch.",
    ),
    (
        "expunix",
        "ExpUnix",
        "int64",
        "Expiry time in whole nanoseconds since the epoch.",
    ),
    (
        "snapunix",
        "SnapUnix",
        "int64",
        "Source event time for a snapshot in whole epoch nanoseconds.",
    ),
    ("hash", "Hash", "String", "Time-anchored composition of `unix` and `vhash`."),
    ("vhash", "VHash", "int64", "Clock-free XXH3-64 value digest."),
    (
        "xhash",
        "XHash",
        "String",
        "Direct XXH3-128 digest of the UTF-8 `code`.",
    ),
    (
        "prevhash",
        "PrevHash",
        "String",
        "Hash of the preceding lifecycle version.",
    ),
    (
        "prevunix",
        "PrevUnix",
        "int64",
        "Event time of the preceding lifecycle version.",
    ),
    (
        "parenthash",
        "ParentHash",
        "String",
        "Ordered hashes of source events.",
    ),
    (
        "linkhashes",
        "LinkHashes",
        "MultipleValueString",
        "Ordered exact hashes of related events.",
    ),
    ("version", "Version", "int64", "Zero-based lifecycle version number."),
    ("state", "State", "int64", "Packed ranked lifecycle state."),
    ("code", "Code", "String", "Readable lifecycle identifier."),
    (
        "altids",
        "AltIDs",
        "String",
        "Other identifiers keyed by scheme or lifecycle field.",
    ),
    (
        "reason",
        "Reason",
        "String",
        "Reason the event was rejected or could not be interpreted.",
    ),
    (
        "symbolticker",
        "SymbolTicker",
        "String",
        "Canonical instrument spelling.",
    ),
    (
        "unmap",
        "Unmap",
        "String",
        "Payload entries the registry did not resolve; null when all resolved.",
    ),
    ("pxunit", "PxUnit", "String", "Unit in which `lastpx` is expressed."),
    ("qtyunit", "QtyUnit", "String", "Unit in which `lastqty` is expressed."),
    (
        "notional",
        "Notional",
        "double",
        "Producer-computed `lastpx * lastqty * multiplier`.",
    ),
    (
        "marketmarker",
        "MarketMarker",
        "Boolean",
        "Whether the source marks the row as market activity.",
    ),
    (
        "globalorderid",
        "GlobalOrderId",
        "String",
        "Order identifier shared across source systems.",
    ),
    (
        "creationtime",
        "CreationTime",
        "UTCTimestamp",
        "Lifecycle creation timestamp expressed in UTC.",
    ),
    ("env", "Env", "String", "Source environment name."),
    (
        "rootorderid",
        "RootOrderId",
        "String",
        "Identifier of the root order in the lifecycle.",
    ),
    (
        "rootoriginatororderid",
        "RootOriginatorOrderId",
        "String",
        "Originator identifier of the root order.",
    ),
    (
        "orderflags",
        "OrderFlags",
        "String",
        "Source flags attached to the order.",
    ),
    (
        "orderoriginatorid",
        "OrderOriginatorId",
        "String",
        "Identifier of the order's originating participant.",
    ),
    (
        "conversationid",
        "ConversationId",
        "String",
        "Identifier shared by messages in one conversation.",
    ),
    (
        "bloombergcode",
        "BloombergCode",
        "String",
        "Bloomberg identifier supplied by the source bridge.",
    ),
)

# Tags are written out because removing a field must not renumber every field
# after it. LastPx and LastQty use FIX's existing tags 31 and 32, leaving the
# retired package slots 30022, 30023 and 30028 empty -- 30028 held LastShares
# until it went back to being tag 32's own pre-4.3 spelling.
REKEP_TAGS: Mapping[str, int] = MappingProxyType(
    {
        "unix": 30000,
        "unixpartition": 30001,
        "eventtype": 30002,
        "creaunix": 30003,
        "recunix": 30004,
        "expunix": 30005,
        "snapunix": 30006,
        "hash": 30007,
        "vhash": 30008,
        "xhash": 30009,
        "prevhash": 30010,
        "prevunix": 30011,
        "parenthash": 30012,
        "linkhashes": 30013,
        "version": 30014,
        "state": 30015,
        "code": 30016,
        "altids": 30017,
        "reason": 30019,
        "symbolticker": 30020,
        "unmap": 30021,
        "pxunit": 30024,
        "qtyunit": 30025,
        "notional": 30026,
        "marketmarker": 30029,
        "globalorderid": 30030,
        "creationtime": 30031,
        "env": 30032,
        "rootorderid": 30033,
        "rootoriginatororderid": 30034,
        "orderflags": 30035,
        "orderoriginatorid": 30036,
        "conversationid": 30037,
        "bloombergcode": 30038,
    }
)

_REKEP_DTYPES: Mapping[str, pyarrow.DataType] = MappingProxyType(
    {
        "hash": pyarrow.binary(16),
        "xhash": pyarrow.binary(16),
        "prevhash": pyarrow.binary(16),
        "parenthash": pyarrow.list_(pyarrow.field("item", pyarrow.binary(16), nullable=False)),
        "linkhashes": pyarrow.list_(pyarrow.field("item", pyarrow.binary(16), nullable=False)),
        "altids": pyarrow.map_(
            pyarrow.string(), pyarrow.field("value", pyarrow.string(), nullable=False)
        ),
        "unmap": ENTRIES,
    }
)

REKEP_MSG_TYPES: tuple[tuple[str, str], ...] = (
    ("Message", "URM"),
    ("FixMsg", "URF"),
    ("Instrument", "URI"),
    ("Order", "URO"),
    ("Execution", "URE"),
    ("Book", "URB"),
)
REKEP_COMPONENT_NAMES: tuple[str, ...] = (
    "RekepHeader",
    "RekepMarket",
    *(f"REKEP.{contract}" for contract, _msg_type in REKEP_MSG_TYPES),
)

_HEADER_COLUMNS = (
    *(column for column, *_ in REKEP_FIELD_DECLARATIONS[:18]),
    "lastmkt",
    "reason",
)
_MARKET_COLUMNS = ("pxunit", "qtyunit", "notional")
_OPTIONAL_HEADER_COLUMNS = frozenset(
    {"expunix", "snapunix", "prevhash", "prevunix", "parenthash", "reason"}
)
_REQUIRED_MARKET_COLUMNS = frozenset({"pxunit", "qtyunit"})


def register_rekep(registry: FixRegistry) -> FixRegistry:
    """Ensure one registry holds rekep's wildcard-version FIX vocabulary."""
    records = dict(registry.field_records())
    lastqty = records.get("LastQty")
    if lastqty is not None and not any(
        alias.folded == "lastshares" for alias in lastqty.fix.named_aliases
    ):
        # `LastShares` is tag 32's own pre-4.3 name, so it is that identity
        # under an older spelling and not a field of its own.
        lastqty = record_copy(lastqty)
        lastqty.fix.named_aliases = (
            *lastqty.fix.named_aliases,
            Alias("LastShares", source=LASTSHARES_VERSION),
        )
        registry.add_fields((lastqty,))
        records["LastQty"] = lastqty
    lastmkt = records.get("LastMkt")
    expected_lastmkt = _lastmkt_field(lastmkt)
    if lastmkt is None:
        registry.add_fields((expected_lastmkt,))
    elif lastmkt.into_dict() != expected_lastmkt.into_dict():
        registry.add_fields((expected_lastmkt,))
    records["LastMkt"] = expected_lastmkt
    for name, tag, description in (
        ("SettlCurrency", 120, "Currency code of settlement denomination."),
        ("LegCurrency", 556, "Currency code in which the leg is priced."),
    ):
        held = records.get(name)
        expected = _currency_field(held, name, tag, description)
        if held is None:
            registry.add_fields((expected,))
        elif held.into_dict() != expected.into_dict():
            registry.add_fields((expected,))
        records[name] = expected
    tagged = {entry.fix.tag: entry for entry in records.values() if entry.fix.tag is not None}
    additions: list[Field] = []
    for declared in REKEP_FIELD_DECLARATIONS:
        expected = _field(declared)
        by_tag = tagged.get(expected.fix.tag)
        by_name = records.get(expected.fix.canonical)
        if by_tag is None and by_name is None:
            additions.append(expected)
            records[expected.fix.canonical] = expected
            tagged[expected.fix.tag] = expected
            continue
        if not _same_field(by_tag, expected) or not _same_field(by_name, expected):
            raise ValueError(
                f"rekep FIX field {expected.fix.canonical!r} does not own tag "
                f"{expected.fix.tag} and column {expected.fix.column!r}"
            )
    if additions:
        # Package fields share one shard. Reconcile them together so an archive
        # refresh rewrites that shard once and invalidates the registry once.
        registry.add_fields(additions, STANDARD)

    for expected in _component_records():
        held = registry.component_records().get(expected.name)
        if held is not None:
            if held.into_dict() != expected.into_dict():
                raise ValueError(f"rekep FIX component {expected.name!r} has another declaration")
            continue
        if expected.msg_type:
            claimed = registry.message_records().get(expected.msg_type)
            if claimed is not None:
                raise ValueError(f"rekep MsgType {expected.msg_type!r} is already {claimed.name!r}")
        registry.add_fields((expected.into_record(),))
    return registry


def rekep_is_registered(registry: FixRegistry) -> bool:
    """Whether every package-owned declaration is already exact."""
    records = registry.field_records()
    lastqty = records.get("LastQty")
    if lastqty is not None and not any(
        alias.folded == "lastshares" for alias in lastqty.fix.named_aliases
    ):
        return False
    lastmkt = records.get("LastMkt")
    if lastmkt is None or lastmkt.dtype != pyarrow.int32() or lastmkt.fix.tag != 30:
        return False
    for name, tag in (("SettlCurrency", 120), ("LegCurrency", 556)):
        currency = records.get(name)
        if currency is None or currency.dtype != pyarrow.int32() or currency.fix.tag != tag:
            return False
    tagged = {entry.fix.tag: entry for entry in records.values() if entry.fix.tag is not None}
    for declared in REKEP_FIELD_DECLARATIONS:
        expected = _field(declared)
        if not _same_field(tagged.get(expected.fix.tag), expected) or not _same_field(
            records.get(expected.fix.canonical), expected
        ):
            return False
    components = registry.component_records()
    for expected in _component_records():
        held = components.get(expected.name)
        if held is None or held.into_dict() != expected.into_dict():
            return False
    return True


def _field(declared: tuple[str, str, str, str]) -> Field:
    """One package-owned field record from its frozen declaration."""
    column, name, datatype, description = declared
    built = fix_field(
        name,
        REKEP_TAGS[column],
        datatype,
        description=description,
    )
    built.fix.versions = (ANY_VERSION,)
    built.fix.column = column
    if column in _REKEP_DTYPES:
        built = Field(
            name=built.name,
            dtype=_REKEP_DTYPES[column],
            nullable=built.nullable,
            metadata=built.metadata,
        )
    return built


def _lastmkt_field(held: Field | None) -> Field:
    """The standard LastMkt identity with the market model's packed MIC storage."""
    built = (
        record_copy(held)
        if held is not None
        else fix_field(
            "LastMkt",
            30,
            "Exchange",
            description="Market of the last fill or routed order.",
        )
    )
    built.dtype = pyarrow.int32()
    if held is None:
        built.fix.versions = (ANY_VERSION,)
    return built


def _currency_field(held: Field | None, name: str, tag: int, description: str) -> Field:
    """A standard FIX Currency identity with packed Currency storage."""
    built = (
        record_copy(held)
        if held is not None
        else fix_field(
            name,
            tag,
            "Currency",
            description=description,
        )
    )
    built.dtype = pyarrow.int32()
    built.fix.type = "Currency"
    if held is None:
        built.fix.versions = (ANY_VERSION,)
    return built


def _same_field(held: Field | None, expected: Field) -> bool:
    """Whether a stored identity is the exact rekep wire-to-column mapping."""
    return held is not None and held.into_dict() == expected.into_dict()


def _component_records() -> tuple[ComponentRecord, ...]:
    """The two reusable blocks followed by the six persisted contracts."""
    header = ComponentRecord(
        name="RekepHeader",
        versions=(ANY_VERSION,),
        declaration=block(
            "RekepHeader",
            tuple(
                field_member("LastMkt", 30)
                if column == "lastmkt"
                else _member(column, required=column not in _OPTIONAL_HEADER_COLUMNS)
                for column in _HEADER_COLUMNS
            ),
        ),
    )
    market = ComponentRecord(
        name="RekepMarket",
        versions=(ANY_VERSION,),
        declaration=block(
            "RekepMarket",
            (
                field_member("LastPx", 31),
                field_member("LastQty", 32),
                *(
                    _member(column, required=column in _REQUIRED_MARKET_COLUMNS)
                    for column in _MARKET_COLUMNS
                ),
            ),
        ),
    )
    messages = tuple(
        ComponentRecord(
            name=f"REKEP.{name}",
            versions=(ANY_VERSION,),
            declaration=block(f"REKEP.{name}", _message_members(name), msg_type),
        )
        for name, msg_type in REKEP_MSG_TYPES
    )
    return (header, market, *messages)


def _member(column: str, *, required: bool = False) -> Field:
    """One custom tagged field in a component declaration."""
    name = next(
        name
        for declared, name, _type, _description in REKEP_FIELD_DECLARATIONS
        if declared == column
    )
    return field_member(name, REKEP_TAGS[column], required=required)


def _message_members(name: str) -> tuple[Field, ...]:
    """The package components one persisted contract carries."""
    members = [reference_member("RekepHeader", required=True)]
    if name == "FixMsg":
        members.extend((_member("symbolticker", required=True), _member("unmap")))
    elif name == "Instrument":
        members.append(_member("symbolticker", required=True))
    elif name in {"Order", "Execution", "Book"}:
        members.extend(
            (
                _member("symbolticker", required=True),
                reference_member("RekepMarket", required=True),
            )
        )
    return tuple(members)
