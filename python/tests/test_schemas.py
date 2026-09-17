"""The published table contracts match their owning declarations."""

import json
from pathlib import Path

import pyarrow
from yggdryl.fix import fix_schema, fix_schema_carrying

from rekep import Message
from rekep.fields import Field
from rekep.fix import (
    FIXMSG,
    PAYLOAD,
    fix_carrier,
    fix_message_field,
    fix_parse_field,
    fix_registry,
)
from rekep.iceberg import (
    CONTRACT_KEYS,
    derived_keys,
    iceberg_contract,
    iceberg_contract_field,
    metrics_for,
    partition_keys,
    primary_keys,
    sort_keys,
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
        member.name for member in Message.into_field()
    ]
    # One row per body, whatever session carried it: `bodyhash` is the digest
    # of the bytes and the whole key.
    assert document["schema"]["identifier-field-ids"] == [11]
    assert document["partition-spec"]["fields"] == [
        {"source-id": 4, "field-id": 1000, "transform": "hour", "name": "timepartition_hour"}
    ]
    # Order 0 is what a created table records; a bare `SortOrder()` is order 1.
    assert document["sort-order"] == {"order-id": 0, "fields": []}


def test_contract_round_trip_keeps_shape_and_identity() -> None:
    contract = load_contract()
    document = CONTRACT.read_text(encoding="utf-8")

    assert document == f"{iceberg_contract(Message.into_field())}\n"
    # A contract read back and republished is the same bytes, so anything the
    # reader drops shows up here as a diff rather than as a silent loss.
    assert document == f"{iceberg_contract(contract)}\n"
    assert Field.from_arrow(contract.into_arrow()) == contract


def test_contract_matches_the_message_declaration() -> None:
    published = load_contract()
    declared = Message.into_field()

    # `check_metadata=False`: an Iceberg schema carries no Arrow metadata, so
    # the published shape loses `digest:*` and `partition:sources` and gains an
    # `iceberg:field_id` on every column. Types, names and nullability agree.
    assert published.into_arrow_schema().equals(declared.into_arrow_schema())
    assert primary_keys(published) == primary_keys(declared) == ["bodyhash"]
    assert partition_keys(published) == partition_keys(declared) == {"timepartition": "hour"}
    assert metrics_for(published) == metrics_for(declared)


def test_a_contract_does_not_carry_what_only_arrow_metadata_states() -> None:
    """The cost of the format, as an assertion rather than as prose.

    Each of these is asserted against the runtime declaration instead:
    `test_message.py` owns the digest and derived-partition contract, and
    `test_the_fix_declaration_keeps_its_registry_metadata` below owns the tags.
    """
    published = load_contract()

    assert derived_keys(Message.into_field()) == {"timepartition": ("timestamp",)}
    assert derived_keys(published) == {}
    assert [member.name for member in Message.into_field() if member.digest.is_holder()] == [
        "bodyhash"
    ]
    assert [member.name for member in published if member.digest.is_holder()] == []
    assert published.into_arrow_schema().field("bodyhash").metadata[b"iceberg:field_id"] == b"11"
    # A column's description survives as Iceberg's `doc`; the struct's own does
    # not, and neither do the `python:*` keys naming the class that declared it.
    assert Message.into_field().metadata["description"].startswith("One ULBridge text line")
    assert Message.into_field().metadata["python:qualname"] == "Message"
    assert dict(published.metadata) == {}


def test_the_fix_contract_is_what_the_current_dictionary_answers() -> None:
    """Drift fails here rather than surfacing in a table.

    The document is generated output -- `rekep fields dump --pyclass
    rekep.fix:fix_message_field` writes it -- so this compares the committed
    bytes with what the installed yggdryl produces today, column for column,
    and a dictionary that moved under it is a failing test rather than a
    schema evolution nobody asked for.
    """
    document = FIX_CONTRACT.read_text(encoding="utf-8")
    fixed = load_fix_contract()

    assert document == f"{iceberg_contract(fix_message_field())}\n"
    assert document == f"{iceberg_contract(fixed)}\n"
    # And the row behind it is yggdryl's, field for field, with the capture's
    # own columns in front through the one supported seam.
    declared = fix_schema_carrying(fix_carrier(), fix_schema(fix_registry(), FIXMSG))
    assert [member.name for member in fix_parse_field()] == [member.name for member in declared]
    # The stored row is that row minus the payload the parse read it out of:
    # `logs.messages` holds those bytes and `bodyhash` is the handle into them.
    assert [member.name for member in fixed] == [
        member.name for member in declared if member.name != PAYLOAD
    ]
    assert len(fixed) == 129 == len(fix_schema(fix_registry(), FIXMSG)) + 6
    assert len(fix_parse_field()) == 130 == len(fixed) + 1


def test_the_stored_row_does_not_repeat_the_bytes_it_was_read_from() -> None:
    """`fix.messages` references the text; `logs.messages` holds it."""
    stored = fix_message_field().into_arrow_schema()

    assert PAYLOAD == "body"
    assert PAYLOAD not in stored.names
    assert PAYLOAD in fix_parse_field().into_arrow_schema().names, "the parse still reads it"
    assert PAYLOAD in Message.into_field().into_arrow_schema().names, "and the log still holds it"
    # The handle stays, non-null, and is the key of the table that has the bytes.
    assert stored.field("bodyhash").type == pyarrow.binary(16)
    assert stored.field("bodyhash").nullable is False
    assert primary_keys(Message.into_field()) == ["bodyhash"]
    # And it is a carried value here, not a digest this shape computes: a
    # holder whose source column the table does not store refuses the apply
    # outright, on the write and on every read back through the declaration.
    assert Message.into_field()["bodyhash"].digest.is_holder()
    assert not fix_message_field()["bodyhash"].digest.is_holder()
    assert fix_message_field()["bodyhash"].digest.sources is None


def test_the_fix_table_is_laid_out_by_the_event_and_keyed_by_its_identity() -> None:
    fixed = load_fix_contract()
    document = json.loads(FIX_CONTRACT.read_text(encoding="utf-8"))

    # One message logged at three hops is one event with one identity, so the
    # key is that identity and not where the line was read from.
    assert primary_keys(fixed) == ["curruuid"]
    assert partition_keys(fixed) == {"unix": "hour"}
    assert list(sort_keys(fixed)) == ["unix", "seqnum", "curruuid"]
    assert document["partition-spec"]["fields"] == [
        {"source-id": 7, "field-id": 1000, "transform": "hour", "name": "unix_hour"}
    ]
    assert [field["direction"] for field in document["sort-order"]["fields"]] == ["asc"] * 3

    schema = fixed.into_arrow_schema()
    # Columns use the dictionary's folded names; their tags are metadata, so
    # the published contract states the name and the registry states the tag.
    assert "35" not in schema.names
    assert "msgtype" in schema.names
    # The capture's own columns lead, the crate's clocks open the dictionary's
    # half, and the arrival record closes the row under the counter that
    # counts it.
    assert schema.names[:7] == [
        "rownum",
        "timestamp",
        "timepartition",
        "threadId",
        "level",
        "bodyhash",
        "unix",
    ]
    assert schema.names[-3:] == ["metadata", "nofixentries", "fixentries"]
    # Every timestamp is microseconds, which is what Iceberg v2 stores
    # without a precision shim -- the event's own clock included.
    assert schema.field("unix").type.unit == "us"
    assert schema.field("timestamp").type.unit == "us"
    # What every message settles, and nothing more: a read is not a snapshot,
    # and a capture `timestamp` is context.
    assert [member.name for member in fixed if not member.nullable] == [
        "rownum",
        "bodyhash",
        "unix",
        "creatunix",
        "curruuid",
        "crossuuid",
        "hashcode",
        "crosshashcode",
        "beginstring",
    ]
    assert fixed["snapunix"].nullable


def test_the_fix_declaration_keeps_its_registry_metadata() -> None:
    """What the FIX contract stopped publishing, still owned by the runtime."""
    schema = fix_message_field().into_arrow_schema()

    assert schema.field("msgtype").metadata[b"fix:tag"] == b"35"
    assert schema.field("curruuid").metadata[b"fix:tag"] == b"65039"
    assert schema.field("px").metadata[b"fix:tag"] == b"65043"
    assert load_fix_contract().into_arrow_schema().field("msgtype").metadata == {
        b"description": schema.field("msgtype").metadata[b"description"],
        b"iceberg:field_id": b"29",
    }


def test_raw_message_contract_keeps_the_captures_the_bridge_names() -> None:
    message = load_contract()
    assert primary_keys(message) == ["bodyhash"]
    assert partition_keys(message) == {"timepartition": "hour"}
    assert [member.name for member in message][4:10] == [
        "threadId",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "pluginid",
        "level",
    ]
    assert [int(member.iceberg["field_id"]) for member in message] == list(range(1, 13))
