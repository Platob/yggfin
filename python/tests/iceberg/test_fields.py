"""Projecting a field onto Iceberg, and reading one back."""

import datetime
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, Field, StructField, field
from rekep.iceberg import iceberg_field, iceberg_partition_spec, iceberg_schema, metrics_for
from rekep.iceberg.fields import (
    COLUMN_METRICS,
    DEFAULT_INFERRED,
    INFERRED_METRICS,
    MAX_INFERRED,
    _leaves,
)


@field
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, Field.partition_key()]
    """Trading day."""

    venue: str | None = None
    """Where it traded, when known."""


@pytest.fixture
def schema() -> object:
    return Quote.FIELD.into_iceberg_schema()


# -- the schema -------------------------------------------------------------


def test_columns_and_order(schema: object) -> None:
    assert [f.name for f in schema.fields] == ["symbol", "day", "venue"]
    assert [f.field_id for f in schema.fields] == [1, 2, 3], "ids numbered from one"


def test_nullability_becomes_requiredness(schema: object) -> None:
    assert schema.find_field("symbol").required
    assert not schema.find_field("venue").required


def test_descriptions_become_docs(schema: object) -> None:
    assert schema.find_field("symbol").doc == "Instrument."
    assert schema.find_field("day").doc == "Trading day."


def test_the_primary_key_becomes_the_identifier_fields(schema: object) -> None:
    assert schema.identifier_field_ids == [schema.find_field("symbol").field_id]


def test_a_field_without_a_key_declares_no_identifier() -> None:
    @field
    class Loose(Convertible):
        symbol: str

    assert Loose.FIELD.into_iceberg_schema().identifier_field_ids == []


def test_ids_match_what_pyiceberg_would_assign_from_the_same_arrow_schema() -> None:
    """The projection is pyiceberg's own, so the two cannot drift apart."""
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    from pyiceberg.schema import assign_fresh_schema_ids

    arrow = Quote.FIELD.into_arrow_schema()
    fresh = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(arrow))
    ours = Quote.FIELD.into_iceberg_schema()
    assert {f.name: f.field_id for f in ours.fields} == {f.name: f.field_id for f in fresh.fields}


def test_one_field_projects_on_its_own() -> None:
    built = iceberg_field(Quote.FIELD.field("symbol"), field_id=7)
    assert (built.name, built.field_id, built.required) == ("symbol", 7, True)
    assert built.doc == "Instrument."


def test_a_nested_field_projects_too() -> None:
    @field
    class Book(Convertible):
        """A book."""

        venue: Quote
        legs: list[int]

    schema = Book.FIELD.into_iceberg_schema()
    assert str(schema.find_field("venue").field_type).startswith("struct")
    assert schema.find_field("legs").field_type.element_type.__class__.__name__ == "LongType"


# -- the partition spec -----------------------------------------------------


def test_the_partition_spec_follows_the_declaration(schema: object) -> None:
    spec = Quote.FIELD.into_iceberg_partition_spec(schema)
    (partition,) = spec.fields
    assert partition.name == "day", "an identity partition keeps the column name"
    assert partition.source_id == schema.find_field("day").field_id
    assert partition.field_id == 1000, "Iceberg numbers partition fields from 1000"


def test_a_transform_is_parsed_as_iceberg_spells_it() -> None:
    @field
    class Bucketed(Convertible):
        symbol: Annotated[str, Field.partition_key("bucket[16]")]
        stamp: Annotated[datetime.datetime, Field.partition_key("day")]

    spec = Bucketed.FIELD.into_iceberg_partition_spec()
    assert [str(f.transform) for f in spec.fields] == ["bucket[16]", "day"]
    assert [f.name for f in spec.fields] == ["symbol_bucket", "stamp_day"], (
        "the width is in the spec already, and a partition name is a directory name"
    )
    for partition in spec.fields:
        assert "[" not in partition.name and "]" not in partition.name


def test_nothing_declared_is_an_unpartitioned_spec() -> None:
    @field
    class Flat(Convertible):
        symbol: str

    assert Flat.FIELD.into_iceberg_partition_spec().fields == ()


# -- reading one back -------------------------------------------------------


def test_a_schema_comes_back_as_a_struct_field(schema: object) -> None:
    built = StructField.from_iceberg_schema(schema, "Quote")
    assert built.names == ["symbol", "day", "venue"]
    assert built.primary_keys() == ["symbol"], "the identifier fields come back as the key"
    assert built.field("symbol").description == "Instrument."
    assert not built.field("symbol").nullable
    assert built.field("venue").nullable


def test_the_spec_comes_back_as_partition_keys(schema: object) -> None:
    spec = Quote.FIELD.into_iceberg_partition_spec(schema)
    built = StructField.from_iceberg_schema(schema, "Quote", spec)
    assert built.partition_keys() == {"day": "identity"}


def test_the_widths_are_the_stores_own(schema: object) -> None:
    """A field that renamed pyiceberg's widths would make every read convert."""
    built = StructField.from_iceberg_schema(schema)
    assert built.field("symbol").arrow_type == pyarrow.large_string()


def test_the_round_trip_keeps_names_types_and_keys(schema: object) -> None:
    back = StructField.from_iceberg_schema(schema, "Quote").into_iceberg_schema()
    assert [(f.name, str(f.field_type), f.required, f.doc) for f in back.fields] == [
        (f.name, str(f.field_type), f.required, f.doc) for f in schema.fields
    ]
    assert back.identifier_field_ids == schema.identifier_field_ids


def test_the_module_functions_take_a_field_directly() -> None:
    """The methods on a field are the front door; these are what they call."""
    assert [f.name for f in iceberg_schema(Quote.FIELD).fields] == Quote.FIELD.names
    assert [f.name for f in iceberg_partition_spec(Quote.FIELD).fields] == ["day"]


# -- column ids --------------------------------------------------------------


def test_a_schema_read_back_carries_its_column_ids() -> None:
    """Iceberg identifies a column by id, so the id is part of what it is."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, NestedField, StringType, StructType

    schema = Schema(
        NestedField(5, "mic", StringType(), required=True, doc="ISO 10383."),
        NestedField(
            9,
            "venue",
            StructType(NestedField(12, "size", IntegerType(), required=False)),
            required=False,
        ),
        identifier_field_ids=[5],
    )
    field = StructField.from_iceberg_schema(schema, "Venue")
    assert [(member.name, member.field_id) for member in field.fields] == [("mic", 5), ("venue", 9)]
    assert field.field("venue").field("size").field_id == 12
    assert field.field("mic").is_primary_key is True


def test_declared_ids_are_kept_rather_than_renumbered() -> None:
    """A round trip through a contract file is an identity, not a rename."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType

    schema = Schema(NestedField(5, "mic", StringType(), required=True))
    published = Field.from_yaml(StructField.from_iceberg_schema(schema, "Venue").into_yaml())
    assert published.field("mic").field_id == 5
    assert [(f.field_id, f.name) for f in published.into_iceberg_schema().fields] == [(5, "mic")]


def test_a_declaration_with_no_ids_is_numbered_fresh() -> None:
    """The user should not have to know the protocol to hand over a shape."""
    plain = StructField.from_arrow_schema(
        pyarrow.schema([("mic", pyarrow.string()), ("size", pyarrow.int32())]), "Venue"
    )
    assert plain.field("mic").field_id is None
    assert [(f.field_id, f.name) for f in plain.into_iceberg_schema().fields] == [
        (1, "mic"),
        (2, "size"),
    ]


def test_ids_ride_under_the_protocol_prefix() -> None:
    """`iceberg:field_id` beside the other Iceberg keys; parquet's is the bridge."""
    from rekep.fields import FIELD_ID
    from rekep.iceberg.fields import PARQUET_FIELD_ID

    field = Field(name="mic", arrow_type=pyarrow.string())
    field.field_id = 7
    assert field.metadata[FIELD_ID] == "7"
    assert FIELD_ID == "iceberg:field_id"
    assert field.into_dict()["metadata"] == {FIELD_ID: "7"}
    assert PARQUET_FIELD_ID == b"PARQUET:field_id", "what parquet files carry, not what we write"


# -- metrics ----------------------------------------------------------------


@field
class Wide(Convertible):
    """A shape with more leaves than Iceberg infers bounds for."""

    day: Annotated[datetime.date, Field.partition_key()]
    """Trading day."""

    note: Annotated[str, Field.primary_key()]
    """Free text, keyed on."""


def _widened(leaves: int) -> StructField:
    """`Wide` grown to `leaves` members, the two declared ones included."""
    source = Wide.FIELD
    grown = pyarrow.struct(
        list(source.arrow_fields)
        + [pyarrow.field(f"pad{index}", pyarrow.int64()) for index in range(leaves - 2)]
    )
    return StructField(name=source.name, arrow_type=grown, metadata=source.metadata)


def test_the_keys_a_reader_filters_on_are_declared_by_name() -> None:
    declared = metrics_for(Quote.FIELD)
    assert declared == {
        "write.metadata.metrics.column.day": "full",
        "write.metadata.metrics.column.symbol": "truncate(16)",
    }, "partition and primary keys, each at a width worth storing"


def test_a_shape_inside_the_budget_does_not_restate_it() -> None:
    assert INFERRED_METRICS not in metrics_for(Quote.FIELD)
    assert INFERRED_METRICS not in metrics_for(_widened(DEFAULT_INFERRED))


def test_a_shape_past_the_budget_raises_it_to_its_own_width() -> None:
    declared = metrics_for(_widened(DEFAULT_INFERRED + 1))
    assert declared[INFERRED_METRICS] == str(DEFAULT_INFERRED + 1)


def test_the_budget_is_raised_no_further_than_the_ceiling() -> None:
    declared = metrics_for(_widened(MAX_INFERRED + 10))
    assert declared[INFERRED_METRICS] == str(MAX_INFERRED), "bounds cost bytes per manifest entry"


def test_a_nested_member_is_counted_leaf_by_leaf() -> None:
    inner = pyarrow.struct([("a", pyarrow.int64()), ("b", pyarrow.int64())])
    grown = pyarrow.struct([("scalar", pyarrow.int64()), ("nested", inner)])
    assert _leaves(grown) == ["scalar", "nested.a", "nested.b"]


def test_a_repeated_member_spends_the_budget_it_cannot_use() -> None:
    listed = pyarrow.list_(pyarrow.struct([("a", pyarrow.int64()), ("b", pyarrow.int64())]))
    mapped = pyarrow.map_(pyarrow.string(), pyarrow.string())
    grown = pyarrow.struct([("legs", listed), ("ids", mapped)])
    assert _leaves(grown) == ["legs.item.a", "legs.item.b", "ids.key", "ids.value"]


def test_the_property_names_are_the_ones_iceberg_reads() -> None:
    from pyiceberg.table import TableProperties

    assert COLUMN_METRICS == TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX
    assert INFERRED_METRICS == "write.metadata.metrics.max-inferred-column-defaults"
