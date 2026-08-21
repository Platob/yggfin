"""The contracts under `schemas/` are checked here, so they cannot drift.

A contract nobody verifies is a comment. Two properties are worth the check:
what a file says is what it loads back as (otherwise a consumer and a producer
read the same document as different types), and a contract that was dumped
from a declaration still matches it (otherwise a column exists in Python and
not in the agreement).
"""

from pathlib import Path

import pyarrow
import pytest

from rekep import Field, Log

#: The contract directory is at the repo root, beside `python/` -- it is
#: published to whoever exchanges data with this repo, not shipped in the wheel.
SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"

CONTRACTS = sorted(
    path for suffix in ("*.yaml", "*.yml", "*.json") for path in SCHEMAS.rglob(suffix)
)

#: Pinned so an empty or moved directory fails here rather than passing every
#: test below by iterating over nothing.
EXPECTED_CONTRACTS = 3


def test_the_directory_holds_the_contracts_the_tests_assume() -> None:
    assert len(CONTRACTS) == EXPECTED_CONTRACTS
    assert {path.name for path in CONTRACTS} == {"log.yaml", "quote.yaml", "venue.json"}


@pytest.mark.parametrize("path", CONTRACTS, ids=lambda path: path.name)
def test_a_contract_loads_as_the_type_it_names(path: Path) -> None:
    """Parse, dump, parse: a document that survives that is one, unambiguously."""
    contract = Field.from_(str(path))
    assert Field.from_dict(contract.into_dict()) == contract
    assert contract.into_arrow_schema()


@pytest.mark.parametrize("path", CONTRACTS, ids=lambda path: path.name)
def test_a_contract_says_who_it_is(path: Path) -> None:
    """Identity rides in the metadata, so a schema still names its contract."""
    contract = Field.from_(str(path))
    assert contract.name
    assert contract.metadata["namespace"]
    assert contract.metadata["description"]
    schema = contract.into_arrow_schema()
    assert Field.from_arrow_schema(schema) == contract


def test_the_log_contract_is_the_declaration() -> None:
    """`schemas/rekep/log.yaml` is `Log.FIELD` published; a drift fails here.

    Regenerate it with:
        uv run python -c "from rekep import Log; Log.FIELD.into_yaml('../schemas/rekep/log.yaml')"
    """
    published = Field.from_yaml(str(SCHEMAS / "rekep" / "log.yaml"))
    assert published == Log.FIELD
    assert published.into_arrow_schema().equals(Log.FIELD.into_arrow_schema(), check_metadata=True)
    assert published.primary_keys() == ["recorded_at_unix", "hash64"]
    assert published.partition_keys() == {"recorded_at_date": "identity"}


def test_the_quote_contract_carries_every_nested_kind() -> None:
    """The example is also the format reference, so it has to exercise it."""
    quote = Field.from_yaml(str(SCHEMAS / "trading" / "quote.yaml"))
    types = {member.name: member.arrow_type for member in quote.fields}
    assert types["venue"] == pyarrow.struct(
        [
            pyarrow.field("mic", pyarrow.string(), nullable=False),
            pyarrow.field("country", pyarrow.string()),
        ]
    )
    assert pyarrow.types.is_list(types["legs"])
    assert pyarrow.types.is_struct(types["legs"].value_type)
    assert pyarrow.types.is_map(types["tags"])
    assert types["top_of_book"] == pyarrow.list_(pyarrow.field("item", pyarrow.float64()), 2), (
        "a fixed_size_list keeps the width the contract states"
    )
    assert types["history"] == pyarrow.large_list(pyarrow.field("item", pyarrow.int64()))
    assert types["amount"] == pyarrow.decimal128(38, 9)
    assert types["checksum"] == pyarrow.binary(16)
    assert types["received_at"] == pyarrow.timestamp("us", tz="UTC")


def test_a_contract_is_a_target_shape_for_real_data() -> None:
    """What a contract is *for*: a nearly-right batch is cast onto it."""
    venue = Field.from_json(str(SCHEMAS / "trading" / "venue.json"))
    batch = pyarrow.RecordBatch.from_pydict(
        # The wrong order, a narrower type, and no `country` at all.
        {"name": ["Euronext Paris"], "mic": ["XPAR"]},
        schema=pyarrow.schema([("name", pyarrow.large_string()), ("mic", pyarrow.string())]),
    )
    cast = venue.cast_arrow(batch)
    assert cast.schema.names == ["mic", "name", "country", "sessions"]
    assert cast.column("country").to_pylist() == [None]
    assert cast.schema.field("name").type == pyarrow.string()


def test_a_contract_refuses_data_that_cannot_satisfy_it() -> None:
    """A missing NOT NULL column is named by its path, not filled with nulls."""
    venue = Field.from_json(str(SCHEMAS / "trading" / "venue.json"))
    batch = pyarrow.RecordBatch.from_pydict({"mic": ["XPAR"]})
    with pytest.raises(ValueError, match="Venue.name"):
        venue.cast_arrow(batch)
