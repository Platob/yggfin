"""The package-owned FIX vocabulary and its frozen wire identities."""

from __future__ import annotations

from pathlib import Path

import pytest

from rekep.fields import Field
from rekep.fix.quickfix import members_of
from rekep.fix.registry import FixRegistry, builtin_projection
from rekep.fix.rekep import (
    REKEP_COMPONENT_NAMES,
    REKEP_FIELD_DECLARATIONS,
    REKEP_MSG_TYPES,
    REKEP_TAG_OFFSET,
    REKEP_TAGS,
    register_rekep,
    rekep_is_registered,
)
from rekep.market import Event, MarketEvent
from rekep.text import FixMsg

EXPECTED_FIELDS: tuple[tuple[str, str, int], ...] = (
    ("unix", "REKEP.Unix", 30000),
    ("unixpartition", "REKEP.UnixPartition", 30001),
    ("eventtype", "REKEP.EventType", 30002),
    ("creaunix", "REKEP.CreaUnix", 30003),
    ("recunix", "REKEP.RecUnix", 30004),
    ("expunix", "REKEP.ExpUnix", 30005),
    ("snapunix", "REKEP.SnapUnix", 30006),
    ("hash", "REKEP.Hash", 30007),
    ("vhash", "REKEP.VHash", 30008),
    ("xhash", "REKEP.XHash", 30009),
    ("prevhash", "REKEP.PrevHash", 30010),
    ("prevunix", "REKEP.PrevUnix", 30011),
    ("parenthash", "REKEP.ParentHash", 30012),
    ("linkedhashes", "REKEP.LinkedHashes", 30013),
    ("version", "REKEP.Version", 30014),
    ("state", "REKEP.State", 30015),
    ("code", "REKEP.Code", 30016),
    ("altids", "REKEP.AltIDs", 30017),
    ("mic", "REKEP.MIC", 30018),
    ("reason", "REKEP.Reason", 30019),
    ("symbolticker", "REKEP.SymbolTicker", 30020),
    ("unmap", "REKEP.Unmap", 30021),
    ("px", "REKEP.Px", 30022),
    ("qty", "REKEP.Qty", 30023),
    ("pxunit", "REKEP.PxUnit", 30024),
    ("qtyunit", "REKEP.QtyUnit", 30025),
    ("notional", "REKEP.Notional", 30026),
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
        assert by_tag.fix.display
        assert by_tag.description.endswith(".")

    assert registry.resolve("EventType").fix.tag == 865
    assert registry.resolve("REKEP.EventType").fix.tag == 30002
    assert registry.resolve("unix") is None


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
        canonical for _column, canonical, _tag in EXPECTED_FIELDS[:20]
    ]
    assert [member.name for member in members_of(components["RekepMarket"].declaration)] == [
        canonical for _column, canonical, _tag in EXPECTED_FIELDS[22:]
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
        "REKEP.FixMsg": ("RekepHeader", "REKEP.SymbolTicker", "REKEP.Unmap"),
        "REKEP.Instrument": ("RekepHeader", "REKEP.SymbolTicker"),
        "REKEP.Order": ("RekepHeader", "REKEP.SymbolTicker", "RekepMarket"),
        "REKEP.Execution": ("RekepHeader", "REKEP.SymbolTicker", "RekepMarket"),
        "REKEP.Book": ("RekepHeader", "REKEP.SymbolTicker", "RekepMarket"),
    }
    assert {
        name: tuple(member.name for member in members_of(components[name].declaration))
        for name in expected_members
    } == expected_members


def test_rekep_component_projection_matches_the_persisted_event_fields() -> None:
    registry = FixRegistry.from_builtin()
    displays = {
        column: display
        for column, _name, _datatype, display, _description in REKEP_FIELD_DECLARATIONS
    }
    expected = {
        "RekepHeader": (Event, EXPECTED_FIELDS[:20]),
        "RekepMarket": (MarketEvent, EXPECTED_FIELDS[22:]),
    }

    for component, (shape, declarations) in expected.items():
        projected = registry.component_field(component, "4.4")
        assert projected is not None
        contract = shape.into_field()
        assert [
            (member.name, member.dtype, member.nullable, member.fix.display)
            for member in projected.fields
        ] == [
            (
                column,
                contract.field(column).dtype,
                contract.field(column).nullable,
                displays[column],
            )
            for column, _canonical, _tag in declarations
        ]


def test_registering_rekep_twice_does_not_mutate_the_store(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "registry", offline=True)
    register_rekep(registry)
    revision = registry.revision
    register_rekep(registry)
    assert registry.revision == revision


def test_registration_refuses_an_extra_alias(tmp_path: Path) -> None:
    registry = register_rekep(FixRegistry(cache_dir=tmp_path / "registry", offline=True))
    registry.alias_field("REKEP.Unix", "PackageUnix")

    assert not rekep_is_registered(registry)
    with pytest.raises(ValueError, match="does not own tag"):
        register_rekep(registry)


def test_registration_refuses_changed_versions(tmp_path: Path) -> None:
    registry = register_rekep(FixRegistry(cache_dir=tmp_path / "registry", offline=True))
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
        FixRegistry.from_builtin.cache_clear()
        FixRegistry.from_builtin()
        assert (archive.read_bytes(), archive.stat().st_mtime_ns) == before
    finally:
        FixMsg.into_registry.cache_clear()
