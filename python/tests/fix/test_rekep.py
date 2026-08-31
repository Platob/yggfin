"""The package-owned FIX vocabulary and its frozen wire identities."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fields import Field
from rekep.fix import ANY_VERSION, Alias, fix_field
from rekep.fix.quickfix import members_of
from rekep.fix.registry import FixRegistry, builtin_projection
from rekep.fix.rekep import (
    REKEP_COMPONENT_NAMES,
    REKEP_MSG_TYPES,
    REKEP_TAG_OFFSET,
    REKEP_TAGS,
    register_rekep,
    rekep_is_registered,
)
from rekep.market import Event, MarketEvent
from rekep.text import FixMsg

EXPECTED_FIELDS: tuple[tuple[str, str, int], ...] = (
    ("unix", "Unix", 30000),
    ("unixpartition", "UnixPartition", 30001),
    ("eventtype", "MarketEventType", 30002),
    ("creaunix", "CreaUnix", 30003),
    ("recunix", "RecUnix", 30004),
    ("expunix", "ExpUnix", 30005),
    ("snapunix", "SnapUnix", 30006),
    ("hash", "Hash", 30007),
    ("vhash", "VHash", 30008),
    ("xhash", "XHash", 30009),
    ("prevhash", "PrevHash", 30010),
    ("prevunix", "PrevUnix", 30011),
    ("parenthash", "ParentHash", 30012),
    ("linkhashes", "LinkHashes", 30013),
    ("version", "Version", 30014),
    ("state", "State", 30015),
    ("code", "Code", 30016),
    ("altids", "AltIDs", 30017),
    ("reason", "Reason", 30019),
    ("symbolticker", "SymbolTicker", 30020),
    ("unmap", "Unmap", 30021),
    ("pxunit", "PxUnit", 30024),
    ("qtyunit", "QtyUnit", 30025),
    ("notional", "Notional", 30026),
    ("codesource", "CodeSource", 30027),
    ("lastshares", "LastShares", 30028),
    ("marketmarker", "MarketMarker", 30029),
    ("globalorderid", "GlobalOrderId", 30030),
    ("creationtime", "CreationTime", 30031),
    ("env", "Env", 30032),
    ("rootorderid", "RootOrderId", 30033),
    ("rootoriginatororderid", "RootOriginatorOrderId", 30034),
    ("orderflags", "OrderFlags", 30035),
    ("orderoriginatorid", "OrderOriginatorId", 30036),
    ("conversationid", "ConversationId", 30037),
    ("bloombergcode", "BloombergCode", 30038),
)

HEADER_FIELDS = (
    *EXPECTED_FIELDS[:18],
    ("lastmkt", "LastMkt", 30),
    EXPECTED_FIELDS[18],
    EXPECTED_FIELDS[24],
)
MARKET_FIELDS = (
    ("lastpx", "LastPx", 31),
    ("lastqty", "LastQty", 32),
    *EXPECTED_FIELDS[21:24],
)

EXPECTED_MESSAGES: tuple[tuple[str, str], ...] = (
    ("Message", "URM"),
    ("FixMsg", "URF"),
    ("Instrument", "URI"),
    ("Order", "URO"),
    ("Execution", "URE"),
    ("Book", "URB"),
)


def test_rekep_field_tags_are_frozen_and_round_trip_through_the_registry() -> None:
    registry = FixRegistry.from_builtin()
    assert REKEP_TAG_OFFSET == 30000
    assert tuple(REKEP_TAGS.items()) == tuple(
        (column, tag) for column, _name, tag in EXPECTED_FIELDS
    )

    for column, canonical, tag in EXPECTED_FIELDS:
        by_name = registry.resolve(canonical)
        by_tag = registry.field(tag)
        assert by_name is not None
        assert by_tag is not None
        assert (by_name.fix.canonical, by_name.fix.tag, by_name.fix.column) == (
            canonical,
            tag,
            column,
        )
        assert (by_tag.fix.canonical, by_tag.fix.tag, by_tag.fix.column) == (
            canonical,
            tag,
            column,
        )
        assert by_tag.fix.canonical
        assert by_tag.description.endswith(".")

    assert registry.resolve("EventType").fix.tag == 865
    assert registry.resolve("MarketEventType").fix.tag == 30002
    assert registry.resolve("REKEP.Unix") is None
    assert registry.resolve("LinkXHashes") is None
    assert registry.resolve("unix").fix.tag == 30000
    assert registry.field(30022) is None
    assert registry.field(30023) is None
    assert registry.resolve("Price").fix.tag == 44
    assert registry.resolve("LastQty").fix.tag == 32
    assert registry.resolve("LastShares").fix.tag == 30028
    assert all(
        alias.folded != "lastshares" for alias in registry.resolve("LastQty").fix.named_aliases
    )
    lastmkt = registry.resolve("LastMkt")
    assert lastmkt is not None
    assert (lastmkt.fix.tag, lastmkt.fix.type, lastmkt.dtype) == (
        30,
        "Exchange",
        pyarrow.int32(),
    )
    settlcurrency = registry.resolve("SettlCurrency")
    assert settlcurrency is not None
    assert (settlcurrency.fix.tag, settlcurrency.fix.type, settlcurrency.dtype) == (
        120,
        "Currency",
        pyarrow.int32(),
    )
    legcurrency = registry.resolve("LegCurrency")
    assert legcurrency is not None
    assert (legcurrency.fix.tag, legcurrency.fix.type, legcurrency.dtype) == (
        556,
        "Currency",
        pyarrow.int32(),
    )
    assert registry.resolve("MIC") is None
    assert registry.field(30018) is None
    assert registry.resolve("XHash").dtype == pyarrow.binary(16)
    assert registry.resolve("LinkHashes").dtype == pyarrow.list_(
        pyarrow.field("item", pyarrow.binary(16), nullable=False)
    )
    assert {
        column: registry.resolve(canonical).dtype
        for column, canonical, _tag in EXPECTED_FIELDS[-11:]
    } == {
        "lastshares": pyarrow.float64(),
        "marketmarker": pyarrow.bool_(),
        "globalorderid": pyarrow.string(),
        "creationtime": pyarrow.timestamp("us", tz="UTC"),
        "env": pyarrow.string(),
        "rootorderid": pyarrow.string(),
        "rootoriginatororderid": pyarrow.string(),
        "orderflags": pyarrow.string(),
        "orderoriginatorid": pyarrow.string(),
        "conversationid": pyarrow.string(),
        "bloombergcode": pyarrow.string(),
    }


def test_rekep_components_and_contract_msg_types_are_frozen() -> None:
    registry = FixRegistry.from_builtin()
    assert REKEP_MSG_TYPES == EXPECTED_MESSAGES
    msg_type_field = registry.field(35)
    assert msg_type_field is not None
    standard_codes = {value.value for value in msg_type_field.fix.enumerated}
    package_codes = {msg_type for _contract, msg_type in EXPECTED_MESSAGES}
    assert all(code.startswith("U") for code in package_codes)
    assert package_codes.isdisjoint({*standard_codes, "UL"})
    assert REKEP_COMPONENT_NAMES == (
        "RekepHeader",
        "RekepMarket",
        "REKEP.Message",
        "REKEP.FixMsg",
        "REKEP.Instrument",
        "REKEP.Order",
        "REKEP.Execution",
        "REKEP.Book",
    )

    components = registry.component_records()
    messages = registry.message_records()
    assert [member.name for member in members_of(components["RekepHeader"].declaration)] == [
        canonical for _column, canonical, _tag in HEADER_FIELDS
    ]
    assert [member.name for member in members_of(components["RekepMarket"].declaration)] == [
        canonical for _column, canonical, _tag in MARKET_FIELDS
    ]
    assert {msg_type: messages[msg_type].name for _contract, msg_type in EXPECTED_MESSAGES} == {
        msg_type: f"REKEP.{contract}" for contract, msg_type in EXPECTED_MESSAGES
    }
    for contract, msg_type in EXPECTED_MESSAGES:
        record = messages[msg_type]
        assert registry.merged_component(msg_type) is record
        assert registry.merged_component(f"REKEP.{contract}") is record

    expected_members = {
        "REKEP.Message": ("RekepHeader",),
        "REKEP.FixMsg": ("RekepHeader", "SymbolTicker", "Unmap"),
        "REKEP.Instrument": ("RekepHeader", "SymbolTicker"),
        "REKEP.Order": ("RekepHeader", "SymbolTicker", "RekepMarket"),
        "REKEP.Execution": ("RekepHeader", "SymbolTicker", "RekepMarket"),
        "REKEP.Book": ("RekepHeader", "SymbolTicker", "RekepMarket"),
    }
    assert {
        name: tuple(member.name for member in members_of(components[name].declaration))
        for name in expected_members
    } == expected_members


def test_rekep_component_projection_matches_the_persisted_event_fields() -> None:
    registry = FixRegistry.from_builtin()
    expected = {
        "RekepHeader": (Event, HEADER_FIELDS),
        "RekepMarket": (MarketEvent, MARKET_FIELDS),
    }

    for component, (shape, declarations) in expected.items():
        projected = registry.component_field(component, "4.4")
        assert projected is not None
        contract = shape.into_field()
        assert [(member.name, member.dtype, member.nullable) for member in projected.fields] == [
            (
                column,
                contract.field(column).dtype,
                contract.field(column).nullable,
            )
            for column, _canonical, _tag in declarations
        ]


def test_registering_rekep_twice_does_not_mutate_the_store(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "registry")
    register_rekep(registry)
    revision = registry.revision
    register_rekep(registry)
    assert registry.revision == revision


def test_registration_retires_lastqtys_legacy_lastshares_alias(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "registry")
    lastqty = fix_field("LastQty", 32, "Qty")
    lastqty.fix.versions = (ANY_VERSION,)
    lastqty.fix.named_aliases = (Alias("LastShares", source="4.0"),)
    registry.add_field(lastqty)

    register_rekep(registry)

    assert registry.resolve("LastQty").fix.tag == 32
    assert registry.resolve("LastShares").fix.tag == 30028
    assert registry.alias_conflicts() == {}


def test_registration_refuses_an_extra_alias(tmp_path: Path) -> None:
    registry = register_rekep(FixRegistry(cache_dir=tmp_path / "registry"))
    registry.alias_field("Unix", "PackageUnix")

    assert not rekep_is_registered(registry)
    with pytest.raises(ValueError, match="does not own tag"):
        register_rekep(registry)


def test_registration_refuses_changed_versions(tmp_path: Path) -> None:
    registry = register_rekep(FixRegistry(cache_dir=tmp_path / "registry"))
    stored = registry.field(REKEP_TAG_OFFSET)
    assert stored is not None
    changed = Field.from_dict(stored.into_dict())
    changed.fix.versions = ("4.4",)
    registry.update_field(changed)

    assert not rekep_is_registered(registry)
    with pytest.raises(ValueError, match="does not own tag"):
        register_rekep(registry)


def test_the_builtin_registry_is_read_without_rewriting_its_archive() -> None:
    archive = Path(builtin_projection())
    before = archive.read_bytes(), archive.stat().st_mtime_ns
    FixMsg.into_registry.cache_clear()
    try:
        FixRegistry.set_builtin(None)
        FixRegistry.from_builtin()
        assert (archive.read_bytes(), archive.stat().st_mtime_ns) == before
    finally:
        FixMsg.into_registry.cache_clear()
