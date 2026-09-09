"""The published table contracts match their owning declarations."""

import json
from pathlib import Path

from rekep import Message
from rekep.fields import Field
from rekep.fix import fix_message_field
from rekep.iceberg import (
    CONTRACT_KEYS,
    derived_keys,
    iceberg_contract,
    iceberg_contract_field,
    metrics_for,
    partition_keys,
    primary_keys,
)

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"
CONTRACT = SCHEMAS / "rekep" / "message.json"
FIX_CONTRACT = SCHEMAS / "rekep" / "fix-message.json"


def load_contract() -> Field:
    """Read the raw message contract as the struct it declares."""
    return iceberg_contract_field(CONTRACT.read_text(encoding="utf-8"), "Message")


def load_fix_contract() -> Field:
    """Read the FIX contract as the struct it declares."""
    return iceberg_contract_field(FIX_CONTRACT.read_text(encoding="utf-8"), "FixMsg")


def test_only_the_runtime_table_shapes_are_published() -> None:
    contracts = sorted(
        path for suffix in ("*.yaml", "*.yml", "*.json") for path in SCHEMAS.rglob(suffix)
    )
    assert contracts == sorted([CONTRACT, FIX_CONTRACT])


def test_a_contract_is_the_three_things_iceberg_stores() -> None:
    """The document is read here as JSON, not as an object that hides its shape."""
    document = json.loads(CONTRACT.read_text(encoding="utf-8"))

    assert list(document) == list(CONTRACT_KEYS) == ["schema", "partition-spec", "sort-order"]
    assert [member["name"] for member in document["schema"]["fields"]] == [
        member.name for member in Message.field()
    ]
    assert document["schema"]["identifier-field-ids"] == [1, 2]
    assert document["partition-spec"]["fields"] == [
        {"source-id": 4, "field-id": 1000, "transform": "hour", "name": "timepartition_hour"}
    ]
    # Order 0 is what a created table records; a bare `SortOrder()` is order 1.
    assert document["sort-order"] == {"order-id": 0, "fields": []}


def test_contract_round_trip_keeps_shape_and_identity() -> None:
    contract = load_contract()
    document = CONTRACT.read_text(encoding="utf-8")

    assert document == f"{iceberg_contract(Message.field())}\n"
    # A contract read back and republished is the same bytes, so anything the
    # reader drops shows up here as a diff rather than as a silent loss.
    assert document == f"{iceberg_contract(contract)}\n"
    assert Field.from_arrow(contract.into_arrow()) == contract


def test_contract_matches_the_message_declaration() -> None:
    published = load_contract()
    declared = Message.field()

    # `check_metadata=False`: an Iceberg schema carries no Arrow metadata, so
    # the published shape loses `digest:*` and `partition:sources` and gains an
    # `iceberg:field_id` on every column. Types, names and nullability agree.
    assert published.into_arrow_schema().equals(declared.into_arrow_schema())
    assert primary_keys(published) == primary_keys(declared) == ["url", "rownum"]
    assert partition_keys(published) == partition_keys(declared) == {"timepartition": "hour"}
    assert metrics_for(published) == metrics_for(declared)


def test_a_contract_does_not_carry_what_only_arrow_metadata_states() -> None:
    """The cost of the format, as an assertion rather than as prose.

    Each of these is asserted against the runtime declaration instead:
    `test_message.py` owns the digest and derived-partition contract, and
    `test_the_fix_declaration_keeps_its_registry_metadata` below owns the tags.
    """
    published = load_contract()

    assert derived_keys(Message.field()) == {"timepartition": ("timestamp",)}
    assert derived_keys(published) == {}
    assert [member.name for member in Message.field() if member.digest.is_holder()] == ["bodyhash"]
    assert [member.name for member in published if member.digest.is_holder()] == []
    assert published.into_arrow_schema().field("bodyhash").metadata == {
        b"description": b"XXH3-128 digest of the exact body bytes, filled during field apply.",
        b"iceberg:field_id": b"11",
    }


def test_fix_contract_is_a_table_contract_for_iceberg_simulation() -> None:
    document = FIX_CONTRACT.read_text(encoding="utf-8")
    fixed = load_fix_contract()

    assert document == f"{iceberg_contract(fix_message_field())}\n"
    assert document == f"{iceberg_contract(fixed)}\n"
    assert len(fixed) == 111
    assert primary_keys(fixed) == ["url", "rownum"]
    assert partition_keys(fixed) == {"timepartition": "hour"}
    schema = fixed.into_arrow_schema()
    # Columns use the dictionary's folded names; their tags are metadata, so
    # the published contract states the name and the registry states the tag.
    assert "35" not in schema.names
    assert "msgtype" in schema.names
    assert schema.names[-2:] == ["nofixentries", "nounmappedfixentries"]
    # Every timestamp is microseconds, which is what Iceberg v2 stores
    # without a precision shim -- the codec's market clock included.
    assert schema.field("sendingtime").type.unit == "us"
    assert schema.field("timestamp").type.unit == "us"
    assert [member.name for member in fixed if not member.nullable] == [
        "url",
        "rownum",
        "body",
        "beginstring",
        "msghash",
        "timestamp",
        "unixpartition",
    ]


def test_the_fix_declaration_keeps_its_registry_metadata() -> None:
    """What the FIX contract stopped publishing, still owned by the runtime."""
    schema = fix_message_field().into_arrow_schema()

    assert schema.field("msgtype").metadata[b"fix:tag"] == b"35"
    assert load_fix_contract().into_arrow_schema().field("msgtype").metadata == {
        b"description": schema.field("msgtype").metadata[b"description"],
        b"iceberg:field_id": b"13",
    }


def test_raw_message_contract_keeps_source_keys() -> None:
    message = load_contract()
    assert primary_keys(message) == ["url", "rownum"]
    assert partition_keys(message) == {"timepartition": "hour"}
    assert [member.name for member in message][4:10] == [
        "threadId",
        "sessionUid",
        "msgCtxId",
        "seqNum",
        "plugin",
        "level",
    ]
    assert [int(member.iceberg["field_id"]) for member in message] == list(range(1, 13))
