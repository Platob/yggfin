"""The six persisted contracts must match their owning declarations."""

from pathlib import Path

import pyarrow
import pytest

from rekep import Field, FixMsg, FixRegistry, Message
from rekep.fields import column_name
from rekep.market import Book, Execution, InstUpdate, Order

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"
CONTRACTS = sorted(
    path for suffix in ("*.yaml", "*.yml", "*.json") for path in SCHEMAS.rglob(suffix)
)
#: What each contract's stored shape is on, so a bump is a deliberate edit
#: here and not a number that drifted with a declaration. All six moved to 2
#: together, because every container now writes one `fields` block where a
#: list wrote `item` and a map wrote `key`/`value`: the same Arrow schema and
#: the same `Field`, written a different way, which is exactly what a stored
#: shape version is for.
VERSIONS = dict.fromkeys(
    ("fixmsg.yaml", "message.yaml", "instrument.yaml", "book.yaml", "order.yaml", "execution.yaml"),
    "2",
)

PUBLISHED = {
    "fixmsg.yaml": FixMsg,
    "message.yaml": Message,
    "instrument.yaml": InstUpdate,
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
    assert contract.metadata["version"] == VERSIONS[path.name], "a stored shape changed"


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
    assert message.partition_keys() == {"unixpartition": "identity"}


def test_market_contract_keeps_protocol_metadata() -> None:
    order = Field.from_yaml(str(SCHEMAS / "rekep" / "order.yaml"))
    assert order.field("timeinforce").fix["tag"] == "59"
    assert order.field("lastpx").fix["name"] == "Price"
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


# -- names ------------------------------------------------------------------
#
# A column name is folded, and the fold is also how a spelling is matched --
# so these hold both halves of one rule, on the shapes that are published.

#: What Arrow calls the parts of a container when nobody named them. They are
#: not columns and nothing looks them up, so they are exempt from the rule.
_CONTAINER_PARTS = frozenset({"item", "key", "value"})


def _columns(field: Field) -> list[Field]:
    """Every member of a contract, nested ones included, container parts aside."""
    found = []
    for member in field.fields:
        if member.name not in _CONTAINER_PARTS:
            found.append(member)
        found.extend(_columns(member))
    return found


def test_fix_backed_persisted_dates_are_microsecond_timestamps() -> None:
    found = [
        member
        for shape in PUBLISHED.values()
        for member in _columns(shape.into_field())
        if member.fix.get("tag") and pyarrow.types.is_temporal(member.dtype)
    ]
    assert found
    assert all(not pyarrow.types.is_date(member.dtype) for member in found)
    assert all(
        not pyarrow.types.is_timestamp(member.dtype) or member.dtype.unit == "us"
        for member in found
    )


@pytest.mark.parametrize("path", CONTRACTS, ids=lambda path: path.name)
def test_every_column_is_folded(path: Path) -> None:
    """The name a contract stores is the name a fold produces: lowercase, and
    nothing that is not a letter or a digit. One name, so a reader who has the
    column has the attribute and the document key too."""
    for member in _columns(Field.from_(str(path))):
        assert member.name == column_name(member.name), f"{path.name}: {member.name}"


@pytest.mark.parametrize("path", CONTRACTS, ids=lambda path: path.name)
def test_no_column_repeats_its_fix_name_as_display(path: Path) -> None:
    for member in _columns(Field.from_(str(path))):
        assert "fix:display" not in member.metadata, f"{path.name}: {member.name}"


@pytest.mark.parametrize("path", CONTRACTS, ids=lambda path: path.name)
def test_a_folded_column_matches_the_registry_by_its_own_name(path: Path) -> None:
    """A column that reads a FIX field resolves in the registry spelled as the
    column spells it -- which is the point of matching case-insensitively, and
    of there being no snake-cased spelling to strip first."""
    registry = FixRegistry.from_builtin()
    for member in _columns(Field.from_(str(path))):
        tag = member.fix.tag
        if not tag or member.fix.name is None:
            continue
        found = registry.field(column_name(member.fix.name))
        assert found is not None, f"{path.name}: {member.name} names no registry field"
        assert found.fix.tag == tag, f"{path.name}: {member.name}"


def test_a_registry_lookup_ignores_case_and_not_the_letters() -> None:
    """One field however a feed spells it, and no fold across two fields."""
    registry = FixRegistry.from_builtin()
    for spelling in ("MsgType", "msgtype", "MSGTYPE", "mSgTyPe"):
        found = registry.field(spelling)
        assert found is not None and found.fix.tag == 35, spelling
    assert FixMsg.into_field().field("MsgType") is FixMsg.into_field().field("msgtype")
