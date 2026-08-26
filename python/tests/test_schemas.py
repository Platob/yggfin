"""The six persisted contracts must match their owning declarations."""

from pathlib import Path

import pytest

from rekep import Field, FixMsg, Message
from rekep.market import Book, Execution, Instrument, Order

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"
CONTRACTS = sorted(
    path for suffix in ("*.yaml", "*.yml", "*.json") for path in SCHEMAS.rglob(suffix)
)
PUBLISHED = {
    "fixmsg.yaml": FixMsg,
    "message.yaml": Message,
    "instrument.yaml": Instrument,
    "book.yaml": Book,
    "order.yaml": Order,
    "execution.yaml": Execution,
}


def test_only_pipeline_outputs_are_published() -> None:
    assert len(CONTRACTS) == 6
    assert {path.name for path in CONTRACTS} == set(PUBLISHED)


@pytest.mark.parametrize("path", CONTRACTS, ids=lambda path: path.name)
def test_contract_round_trip_keeps_shape_and_identity(path: Path) -> None:
    contract = Field.from_(str(path))
    assert path.read_bytes() == contract.into_yaml()
    assert Field.from_dict(contract.into_dict()) == contract
    assert Field.from_arrow_schema(contract.into_arrow_schema()) == contract
    assert contract.metadata["version"] == "1"


@pytest.mark.parametrize("name,shape", sorted(PUBLISHED.items()))
def test_contract_matches_its_declaration(name: str, shape: type) -> None:
    published = Field.from_yaml(str(SCHEMAS / "rekep" / name))
    declared = shape.into_field()
    assert published == declared
    assert published.into_arrow_schema().equals(declared.into_arrow_schema(), check_metadata=True)
    assert published.primary_keys() == declared.primary_keys()
    assert published.partition_keys() == declared.partition_keys()


@pytest.mark.parametrize("name", ["message.yaml", "fixmsg.yaml"])
def test_message_contracts_keep_time_keys(name: str) -> None:
    message = Field.from_yaml(str(SCHEMAS / "rekep" / name))
    assert message.primary_keys() == ["unix", "hash"]
    assert message.partition_keys() == {"unix_partition": "identity"}


def test_market_contract_keeps_protocol_metadata() -> None:
    order = Field.from_yaml(str(SCHEMAS / "rekep" / "order.yaml"))
    assert order.field("tif").fix["tag"] == "59"
    assert order.field("px").fix["name"] == "Price"
    assert order.field("side").fix["tag"] == "54"
    assert "fix:tag" not in order.field("code").metadata, "a lifecycle is not a FIX field"
    assert "instrument" not in order.names


def test_published_contracts_have_no_nested_table_keys() -> None:
    for name in PUBLISHED:
        contract = Field.from_yaml(str(SCHEMAS / "rekep" / name))
        for member in contract.fields:
            for inner in member.fields:
                assert not inner.is_primary_key, f"{name}: {member.name}.{inner.name}"
                assert not inner.is_partition_key, f"{name}: {member.name}.{inner.name}"
