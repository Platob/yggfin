"""The raw Message contract matches its owning declaration."""

from pathlib import Path

from yggdryl import Field

from rekep import Message
from rekep.iceberg import derived_keys, partition_keys, primary_keys

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"
CONTRACT = SCHEMAS / "rekep" / "message.json"
FIX_CONTRACT = SCHEMAS / "rekep" / "fix-message.json"


def load_contract() -> Field:
    """Read the native Yggdryl field contract."""
    return Field.from_json(CONTRACT.read_text(encoding="utf-8"))


def test_only_the_runtime_table_shapes_are_published() -> None:
    contracts = sorted(
        path for suffix in ("*.yaml", "*.yml", "*.json") for path in SCHEMAS.rglob(suffix)
    )
    assert contracts == sorted([CONTRACT, FIX_CONTRACT])


def test_contract_round_trip_keeps_shape_and_identity() -> None:
    contract = load_contract()
    assert CONTRACT.read_text(encoding="utf-8") == f"{contract.into_json(indent=2)}\n"
    assert Field.from_json(contract.into_json()) == contract
    assert Field.from_arrow(contract.into_arrow()) == contract


def test_contract_matches_the_message_declaration() -> None:
    published = load_contract()
    declared = Message.field()
    assert published == declared
    assert published.into_arrow_schema().equals(declared.into_arrow_schema(), check_metadata=True)
    assert primary_keys(published) == primary_keys(declared)
    assert partition_keys(published) == partition_keys(declared)


def test_fix_contract_is_a_native_field_snapshot_for_iceberg_simulation() -> None:
    document = FIX_CONTRACT.read_text(encoding="utf-8")
    fixed = Field.from_json(document)

    assert document == f"{fixed.into_json(indent=2)}\n"
    assert len(fixed) == 101
    assert primary_keys(fixed) == ["url", "rownum"]
    assert partition_keys(fixed) == {"timepartition": "hour"}
    # Every timestamp is microseconds, which is what Iceberg v2 stores
    # without a precision shim -- the derived market clock included.
    assert fixed.into_arrow_schema().field("52").type.unit == "us"
    assert fixed.into_arrow_schema().field("30004").type.unit == "us"


def test_raw_message_contract_keeps_source_keys() -> None:
    message = load_contract()
    assert primary_keys(message) == ["url", "rownum"]
    assert partition_keys(message) == {"timepartition": "hour"}
    assert derived_keys(message) == {"timepartition": ("timestamp",)}
    assert [member.name for member in message if member.digest.is_holder()] == ["msghash"]
    assert message["msghash"].digest.sources == ["body"]
    assert all(member.partition.transform is None for member in message)
