"""The package-owned FIX fields, components, and persisted message types."""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING

import pyarrow

from rekep.entries import ENTRIES
from rekep.fields import Field
from rekep.fix.entries import ANY_VERSION, ComponentRecord
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import block, field_member, reference_member

if TYPE_CHECKING:
    from collections.abc import Mapping

    from rekep.fix.registry import FixRegistry

# FIX reserves 5000-9999 for users and venues commonly occupy 20000, so 30000+ avoids both.
REKEP_TAG_OFFSET = 30000
# `enums.ascii_codes.PRIVATE_RANK = 9000` is a rank band, not a FIX tag range.
# Canonical wire names use `REKEP.` so standard identities keep their names;
# `fix.column` holds the folded persisted spelling.

REKEP_FIELD_DECLARATIONS: tuple[tuple[str, str, str, str, str], ...] = (
    ("unix", "Unix", "int64", "Unix", "Event time in whole nanoseconds since the epoch."),
    (
        "unixpartition",
        "UnixPartition",
        "int32",
        "UnixPartition",
        "Hour boundary of `unix` in whole epoch seconds.",
    ),
    ("eventtype", "EventType", "int64", "EventType", "Packed rekep event-kind code."),
    (
        "creaunix",
        "CreaUnix",
        "int64",
        "CreaUnix",
        "Creation time in whole nanoseconds since the epoch.",
    ),
    (
        "recunix",
        "RecUnix",
        "int64",
        "RecUnix",
        "Recording time in whole nanoseconds since the epoch.",
    ),
    (
        "expunix",
        "ExpUnix",
        "int64",
        "ExpUnix",
        "Expiry time in whole nanoseconds since the epoch.",
    ),
    (
        "snapunix",
        "SnapUnix",
        "int64",
        "SnapUnix",
        "Source event time for a snapshot in whole epoch nanoseconds.",
    ),
    ("hash", "Hash", "String", "Hash", "Time-anchored composition of `unix` and `vhash`."),
    ("vhash", "VHash", "int64", "ValueHash", "Clock-free XXH3-64 value digest."),
    ("xhash", "XHash", "int64", "Xhash", "Stable lifecycle identity."),
    (
        "prevhash",
        "PrevHash",
        "String",
        "PrevHash",
        "Hash of the preceding lifecycle version.",
    ),
    (
        "prevunix",
        "PrevUnix",
        "int64",
        "PrevUnix",
        "Event time of the preceding lifecycle version.",
    ),
    (
        "parenthash",
        "ParentHash",
        "String",
        "ParentHash",
        "Ordered hashes of source events.",
    ),
    (
        "linkedhashes",
        "LinkedHashes",
        "MultipleValueString",
        "LinkedHashes",
        "Ordered exact hashes of related event versions.",
    ),
    ("version", "Version", "int64", "Version", "Zero-based lifecycle version number."),
    ("state", "State", "int64", "State", "Packed ranked lifecycle state."),
    ("code", "Code", "String", "Code", "Readable lifecycle identifier."),
    (
        "altids",
        "AltIDs",
        "String",
        "AltIDs",
        "Other identifiers keyed by scheme or lifecycle field.",
    ),
    ("mic", "MIC", "int32", "MIC", "ISO 10383 market identifier code."),
    (
        "reason",
        "Reason",
        "String",
        "Reason",
        "Reason the event was rejected or could not be interpreted.",
    ),
    (
        "symbolticker",
        "SymbolTicker",
        "String",
        "SymbolTicker",
        "Canonical instrument spelling.",
    ),
    (
        "unmap",
        "Unmap",
        "String",
        "Unmapped",
        "Payload entries the registry did not resolve; null when all resolved.",
    ),
    ("px", "Px", "double", "Px", "Price in `pxunit`."),
    ("qty", "Qty", "double", "Qty", "Quantity in `qtyunit`."),
    ("pxunit", "PxUnit", "String", "PxUnit", "Unit in which `px` is expressed."),
    ("qtyunit", "QtyUnit", "String", "QtyUnit", "Unit in which `qty` is expressed."),
    (
        "notional",
        "Notional",
        "double",
        "Notional",
        "Producer-computed `px * qty * multiplier`.",
    ),
)

REKEP_TAGS: Mapping[str, int] = MappingProxyType(
    {
        column: REKEP_TAG_OFFSET + ordinal
        for ordinal, (column, _name, _type, _display, _description) in enumerate(
            REKEP_FIELD_DECLARATIONS
        )
    }
)

_REKEP_DTYPES: Mapping[str, pyarrow.DataType] = MappingProxyType(
    {
        "hash": pyarrow.binary(16),
        "prevhash": pyarrow.binary(16),
        "parenthash": pyarrow.list_(pyarrow.field("item", pyarrow.binary(16), nullable=False)),
        "linkedhashes": pyarrow.list_(pyarrow.field("item", pyarrow.binary(16), nullable=False)),
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

_HEADER_COLUMNS = tuple(column for column, *_ in REKEP_FIELD_DECLARATIONS[:20])
_MARKET_COLUMNS = ("px", "qty", "pxunit", "qtyunit", "notional")
_OPTIONAL_HEADER_COLUMNS = frozenset(
    {"expunix", "snapunix", "prevhash", "prevunix", "parenthash", "mic", "reason"}
)
_REQUIRED_MARKET_COLUMNS = frozenset({"pxunit", "qtyunit"})


def register_rekep(registry: FixRegistry) -> FixRegistry:
    """Ensure one registry holds rekep's wildcard-version FIX vocabulary."""
    records = dict(registry.field_records())
    tagged = {entry.fix.tag: entry for entry in records.values() if entry.fix.tag is not None}
    for ordinal, declared in enumerate(REKEP_FIELD_DECLARATIONS):
        expected = _field(ordinal, declared)
        by_tag = tagged.get(expected.fix.tag)
        by_name = records.get(expected.fix.canonical)
        if by_tag is None and by_name is None:
            registry.add_field(expected)
            records[expected.fix.canonical] = expected
            tagged[expected.fix.tag] = expected
            continue
        if not _same_field(by_tag, expected) or not _same_field(by_name, expected):
            raise ValueError(
                f"rekep FIX field {expected.fix.canonical!r} does not own tag "
                f"{expected.fix.tag} and column {expected.fix.column!r}"
            )

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
        registry.add_component(expected)
    return registry


def rekep_is_registered(registry: FixRegistry) -> bool:
    """Whether every package-owned declaration is already exact."""
    records = registry.field_records()
    tagged = {entry.fix.tag: entry for entry in records.values() if entry.fix.tag is not None}
    for ordinal, declared in enumerate(REKEP_FIELD_DECLARATIONS):
        expected = _field(ordinal, declared)
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


def _field(ordinal: int, declared: tuple[str, str, str, str, str]) -> Field:
    """One package-owned field record from its frozen ordinal."""
    column, name, datatype, display, description = declared
    built = fix_field(
        f"REKEP.{name}",
        REKEP_TAG_OFFSET + ordinal,
        datatype,
        description=description,
    )
    built.fix.versions = (ANY_VERSION,)
    built.fix.column = column
    built.fix.display = display
    if column in _REKEP_DTYPES:
        built = Field(
            name=built.name,
            dtype=_REKEP_DTYPES[column],
            nullable=built.nullable,
            metadata=built.metadata,
        )
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
                _member(column, required=column not in _OPTIONAL_HEADER_COLUMNS)
                for column in _HEADER_COLUMNS
            ),
        ),
    )
    market = ComponentRecord(
        name="RekepMarket",
        versions=(ANY_VERSION,),
        declaration=block(
            "RekepMarket",
            tuple(
                _member(column, required=column in _REQUIRED_MARKET_COLUMNS)
                for column in _MARKET_COLUMNS
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
    ordinal = next(
        index
        for index, (declared, _name, _type, _display, _description) in enumerate(
            REKEP_FIELD_DECLARATIONS
        )
        if declared == column
    )
    name = REKEP_FIELD_DECLARATIONS[ordinal][1]
    return field_member(f"REKEP.{name}", REKEP_TAG_OFFSET + ordinal, required=required)


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
