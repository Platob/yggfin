"""The published table contracts match their owning declarations."""

import json
from pathlib import Path

import pyarrow
import pytest
from yggdryl.fix import fix_schema

from rekep import Message
from rekep.fields import Field
from rekep.fix import (
    FIXMSG,
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
from rekep.market import book_field, market_event_field

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"
CONTRACT = SCHEMAS / "rekep" / "message.json"
FIX_CONTRACT = SCHEMAS / "rekep" / "fixmsg.json"
MARKET_CONTRACTS = {
    SCHEMAS / "rekep" / "book.json": book_field,
    SCHEMAS / "rekep" / "marketevent.json": market_event_field,
}


def load_contract() -> Field:
    """Read the `Message` contract as the struct it declares."""
    return iceberg_contract_field(CONTRACT.read_text(encoding="utf-8"), "Message")


def load_fix_contract() -> Field:
    """Read the FIX contract as the struct it declares."""
    return iceberg_contract_field(FIX_CONTRACT.read_text(encoding="utf-8"), "FixMsg")


def test_only_the_runtime_table_shapes_are_published() -> None:
    contracts = sorted(
        path for suffix in ("*.yaml", "*.yml", "*.json") for path in SCHEMAS.rglob(suffix)
    )
    assert contracts == sorted([CONTRACT, FIX_CONTRACT, *MARKET_CONTRACTS])


@pytest.mark.parametrize(("path", "factory"), MARKET_CONTRACTS.items())
def test_market_contracts_are_native_storage_projections(path, factory):
    assert path.read_text(encoding="utf-8") == f"{iceberg_contract(factory())}\n"


def test_a_contract_is_the_three_things_iceberg_stores() -> None:
    """The document is read here as JSON, not as an object that hides its shape."""
    document = json.loads(CONTRACT.read_text(encoding="utf-8"))

    assert list(document) == list(CONTRACT_KEYS) == ["schema", "partition-spec", "sort-order"]
    assert [member["name"] for member in document["schema"]["fields"]] == [
        member.name for member in Message.into_field()
    ]
    # The line identity is the whole key; the exact-content code remains an
    # ordinary column the native read states beside it.
    assert document["schema"]["identifier-field-ids"] == [2]
    assert document["schema"]["fields"][1]["name"] == "curruuid"
    assert document["schema"]["fields"][1]["type"] == "fixed[16]"
    assert document["schema"]["fields"][1]["required"] is True, "the read states it on every row"
    # The hour of the event itself, exactly as both FIX tables are laid out.
    assert document["partition-spec"]["fields"] == [
        {"source-id": 1, "field-id": 1000, "transform": "hour", "name": "currunix_hour"}
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
    # the published shape loses `DIGEST:*` and `PARTITION:sources` and gains an
    # `ICEBERG:field_id` on every column. Types, names and nullability agree.
    assert published.into_arrow_schema().equals(declared.into_arrow_schema())
    assert primary_keys(published) == primary_keys(declared) == ["curruuid"]
    assert partition_keys(published) == partition_keys(declared) == {"currunix": "hour"}
    assert metrics_for(published) == metrics_for(declared)


def test_a_contract_does_not_carry_what_only_arrow_metadata_states() -> None:
    """The cost of the format, as an assertion rather than as prose.

    Each of these is asserted against the runtime declaration instead, and
    `test_the_fix_declaration_keeps_its_registry_metadata` below owns the tags.
    """
    published = load_contract()

    # Nothing is derived on either side: the table is laid out by the event
    # the row already carries, so no column is computed from another.
    assert derived_keys(Message.into_field()) == derived_keys(published) == {}
    # Nothing here computes a digest: both identity and content code arrive
    # from the native read, so neither shape holds a derived digest.
    assert [member.name for member in Message.into_field() if member.digest.is_holder()] == []
    assert [member.name for member in published if member.digest.is_holder()] == []
    assert published.into_arrow_schema().field("currhashcode").metadata[b"ICEBERG:field_id"] == (
        b"3"
    )
    # A column's description survives as Iceberg's `doc`; the struct's own does
    # not, and neither do the `python:*` keys naming the class that declared it.
    assert Message.into_field().metadata["description"].startswith("One ULBridge text line")
    assert Message.into_field().metadata["PYTHON:qualname"] == "Message"
    assert dict(published.metadata) == {}


def test_the_fix_contract_is_what_the_current_dictionary_answers() -> None:
    """Drift fails here rather than surfacing in a table.

    The document is generated output -- `iceberg_contract(fix_message_field())`
    and a closing newline -- so this compares the committed bytes with what
    the installed yggdryl produces today, column for column, and a dictionary
    that moved under it is a failing test rather than a schema evolution
    nobody asked for. One document for two tables, because `fix.raw` and
    `fix.refined` are one shape.
    """
    document = FIX_CONTRACT.read_text(encoding="utf-8")
    fixed = load_fix_contract()

    assert document == f"{iceberg_contract(fix_message_field())}\n"
    assert document == f"{iceberg_contract(fixed)}\n"
    # Both parse and storage use the native fixed row, field for field.
    declared = fix_schema(fix_registry(), FIXMSG)
    assert [member.name for member in fix_parse_field()] == [member.name for member in declared]
    assert [member.name for member in fixed] == [member.name for member in declared]
    assert len(fixed) == len(fix_parse_field()) == len(declared) == 128


def test_the_stored_row_holds_none_of_the_text_it_was_read_from() -> None:
    """A FIX row carries native event facts only; `logs.messages` owns what a
    line printed and the two captures no field takes, and `srcuuids` joins an
    event back to those lines. Where a line was read from is its `crosscode`
    and its row number its `seqnum` -- the event columns, which a FIX row
    holds under the same names meaning the message's chain and its step."""
    stored = fix_message_field().into_arrow_schema()
    parsed = fix_parse_field().into_arrow_schema()
    logged = Message.into_field().into_arrow_schema()

    for column in ("msgthreadid", "loglevel", "body"):
        assert column not in stored.names, column
        assert column not in parsed.names, column
    for column in ("crosscode", "seqnum"):
        assert column in stored.names and column in logged.names, column
    # The bracket's own facts are native fields a text line fills, so they are
    # on both shapes under one spelling and nothing translates between them.
    for column in ("msgsessionid", "msgctxid", "msgseqnum", "msgpluginid"):
        assert column in stored.names and column in logged.names, column
    assert "body" in logged.names
    assert stored.field("srcuuids").type.field(0).type == pyarrow.binary(16)
    assert logged.field("curruuid").type == pyarrow.binary(16)
    # A text row and a FIX row each declare only their own identity; the
    # content code remains an ordinary quality/audit column on both shapes.
    assert primary_keys(Message.into_field()) == ["curruuid"]
    assert not [member.name for member in fix_message_field() if member.digest.is_holder()], (
        "no column here is a digest this shape computes"
    )


def test_the_fix_tables_are_laid_out_by_the_event_and_keyed_by_its_identity() -> None:
    fixed = load_fix_contract()
    document = json.loads(FIX_CONTRACT.read_text(encoding="utf-8"))

    # One message logged at three hops is one event with one identity, so the
    # key is that identity and not where the line was read from. The
    # partition is the event's own instant and nothing beside it: a key is
    # scoped to its partition, and the carried capture hour must not be one.
    assert primary_keys(fixed) == ["curruuid"]
    assert partition_keys(fixed) == {"currunix": "hour"}
    assert list(sort_keys(fixed)) == ["currunix", "seqnum", "curruuid"]
    assert document["partition-spec"]["fields"] == [
        {"source-id": 1, "field-id": 1000, "transform": "hour", "name": "currunix_hour"}
    ]
    assert document["schema"]["identifier-field-ids"] == [17]
    assert [field["direction"] for field in document["sort-order"]["fields"]] == ["asc"] * 3

    schema = fixed.into_arrow_schema()
    # Columns use the dictionary's folded names; their tags are metadata, so
    # the published contract states the name and the registry states the tag.
    assert "35" not in schema.names
    assert "msgtype" in schema.names
    # The crate clocks open the native row and the residual arrival record
    # closes it under its count.
    assert schema.names[0] == "currunix"
    assert schema.names[-3:] == ["metadata", "nofixentries", "fixentries"]
    # Every timestamp is microseconds, which is what Iceberg v2 stores
    # without a precision shim -- the event's own clock included.
    assert schema.field("currunix").type.unit == "us"
    # What every message settles, and nothing more: a read is not a snapshot,
    # and the clock a line was printed at is context.
    assert [member.name for member in fixed if not member.nullable] == [
        "currunix",
        "creaunix",
        "curruuid",
        "crossuuid",
        "currhashcode",
        "crosshashcode",
        "beginstring",
    ]
    assert fixed["snapunix"].nullable
    assert fixed["seqnum"].nullable, "empty on every `fix.raw` row"


def test_the_fix_declaration_keeps_its_registry_metadata() -> None:
    """What the FIX contract stopped publishing, still owned by the runtime."""
    schema = fix_message_field().into_arrow_schema()

    assert schema.field("msgtype").metadata[b"FIX:tag"] == b"35"
    assert schema.field("curruuid").metadata[b"FIX:tag"] == b"65039"
    assert schema.field("state").metadata[b"FIX:tag"] == b"65052"
    assert schema.field("exprtime").metadata[b"FIX:tag"] == b"65053"
    assert load_fix_contract().into_arrow_schema().field("msgtype").metadata == {
        b"description": schema.field("msgtype").metadata[b"description"],
        b"ICEBERG:field_id": b"28",
    }


def test_the_message_contract_keeps_the_captures_the_bridge_names() -> None:
    message = load_contract()
    assert primary_keys(message) == ["curruuid"]
    assert partition_keys(message) == {"currunix": "hour"}
    assert [member.name for member in message][6:] == [
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    ]
    assert [int(member.iceberg["field_id"]) for member in message] == list(range(1, 13))
    # The event the read settles opens the row, exactly as it opens a FIX one.
    assert [member.name for member in message][:3] == ["currunix", "curruuid", "currhashcode"]
