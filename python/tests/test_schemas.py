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
from rekep.market import Book, BookSide, Execution, Instrument, Order, Reference

#: The contract directory is at the repo root, beside `python/` -- it is
#: published to whoever exchanges data with this repo, not shipped in the wheel.
SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"

CONTRACTS = sorted(
    path for suffix in ("*.yaml", "*.yml", "*.json") for path in SCHEMAS.rglob(suffix)
)

#: Pinned so an empty or moved directory fails here rather than passing every
#: test below by iterating over nothing.
EXPECTED_CONTRACTS = 9

#: The market shapes are tables, so each is published; `Event`, `MarketEvent`,
#: `Level` and `LevelUpdate` are not, so they are not -- an abstract base is
#: nothing two sides exchange, and a level travels inside the side that holds it.
PUBLISHED = {
    "instrument.yaml": Instrument,
    "reference.yaml": Reference,
    "order.yaml": Order,
    "execution.yaml": Execution,
    "bookside.yaml": BookSide,
    "book.yaml": Book,
}


def test_the_directory_holds_the_contracts_the_tests_assume() -> None:
    assert len(CONTRACTS) == EXPECTED_CONTRACTS
    assert {path.name for path in CONTRACTS} == {
        "log.yaml",
        "quote.yaml",
        "venue.json",
        *PUBLISHED,
    }


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
    assert published.primary_keys() == ["unix", "hash"]
    assert published.partition_keys() == {"unix_hour": "identity"}


@pytest.mark.parametrize(
    "name,shape", sorted(PUBLISHED.items()), ids=lambda value: getattr(value, "__name__", value)
)
def test_a_market_contract_is_the_declaration(name: str, shape: type) -> None:
    """A column added in Python and not published here fails the build.

    Regenerate them all from `python/` with:
        uv run python -c "import rekep.market as m; \
[getattr(m, n).FIELD.into_yaml(f'../schemas/rekep/{n.lower()}.yaml') \
 for n in ('Instrument', 'Order', 'Execution', 'BookSide', 'Book')]"
    """
    published = Field.from_yaml(str(SCHEMAS / "rekep" / name))
    assert published == shape.FIELD
    assert published.into_arrow_schema().equals(
        shape.FIELD.into_arrow_schema(), check_metadata=True
    )
    assert published.primary_keys() == shape.FIELD.primary_keys()
    assert published.partition_keys() == shape.FIELD.partition_keys()


def test_a_market_contract_publishes_the_fix_tags_a_consumer_needs() -> None:
    """The `fix:` keys are half of what makes the contract readable without our code."""
    order = Field.from_yaml(str(SCHEMAS / "rekep" / "order.yaml"))
    assert order.field("tif").fix["tag"] == "59"
    assert order.field("px").fix["name"] == "Price"
    assert order.field("instrument").field("symbol").fix["tag"] == "55"


def test_a_published_market_contract_declares_no_nested_key() -> None:
    """A key nothing reads would tell a consumer a nested column identifies a row."""
    for name in PUBLISHED:
        contract = Field.from_yaml(str(SCHEMAS / "rekep" / name))
        for member in contract.fields:
            for inner in member.fields:
                assert not inner.is_primary_key, f"{name}: {member.name}.{inner.name}"
                assert not inner.is_partition_key, f"{name}: {member.name}.{inner.name}"


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


def test_a_published_contract_can_carry_its_iceberg_ids() -> None:
    """A table identifies a column by id, so a contract for one says which."""
    venue = Field.from_json(str(SCHEMAS / "trading" / "venue.json"))
    assert [member.field_id for member in venue.fields] == [1, 2, 3, 4]
    assert venue.field("sessions").item.field("label").field_id == 6
    assert venue.field("mic").metadata["iceberg:field_id"] == "1"
    # And they are what Iceberg gets back, rather than a fresh numbering.
    schema = venue.into_iceberg_schema()
    assert [(field.field_id, field.name) for field in schema.fields] == [
        (1, "mic"),
        (2, "name"),
        (3, "country"),
        (4, "sessions"),
    ]


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
