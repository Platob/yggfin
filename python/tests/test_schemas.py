"""The raw Message contract matches its owning declaration."""

from pathlib import Path

from yggdryl import Field

from rekep import Message
from rekep.iceberg import partition_keys, primary_keys

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"
CONTRACT = SCHEMAS / "rekep" / "message.yaml"


def load_contract() -> Field:
    """Read the native Yggdryl field contract."""
    return Field.from_yaml(CONTRACT.read_text(encoding="utf-8"))


def test_only_the_message_output_is_published() -> None:
    contracts = sorted(
        path for suffix in ("*.yaml", "*.yml", "*.json") for path in SCHEMAS.rglob(suffix)
    )
    assert contracts == [CONTRACT]


def test_contract_round_trip_keeps_shape_and_identity() -> None:
    contract = load_contract()
    assert CONTRACT.read_text(encoding="utf-8") == contract.into_yaml()
    assert Field.from_dict(contract.into_dict()) == contract
    assert Field.from_arrow(contract.into_arrow()) == contract


def test_contract_matches_the_message_declaration() -> None:
    published = load_contract()
    declared = Message.field()
    assert published == declared
    assert published.into_arrow_schema().equals(declared.into_arrow_schema(), check_metadata=True)
    assert primary_keys(published) == primary_keys(declared)
    assert partition_keys(published) == partition_keys(declared)


def test_raw_message_contract_keeps_source_keys() -> None:
    message = load_contract()
    assert primary_keys(message) == ["sourceurl", "sourcerownum"]
    assert partition_keys(message) == {}
