"""Projecting a field onto Iceberg, and reading one back."""

import datetime
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, Field, scalar
from rekep.fields import (
    arrow_type,
    field_names,
    field_of,
    fields,
    partition_key,
    primary_key,
    replace_field,
)
from rekep.iceberg import (
    iceberg_field,
    iceberg_partition_spec,
    iceberg_schema,
    iceberg_struct_field,
    metrics_for,
    partition_keys,
    primary_keys,
)
from rekep.iceberg.fields import (
    COLUMN_METRICS,
    DEFAULT_INFERRED,
    INFERRED_METRICS,
    MAX_INFERRED,
    _leaves,
)


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, partition_key()]
    """Trading day."""

    venue: str | None = None
    """Where it traded, when known."""


@pytest.fixture
def schema() -> object:
    return iceberg_schema(Quote.field())


# -- one field on its own ---------------------------------------------------


def test_one_field_projects_on_its_own_and_numbers_from_the_id_given() -> None:
    """`iceberg_field` is the documented single-column form.

    It is on the site and it was in no test: the two lines that import the
    projection and call it were the only lines of `field.py` the whole suite
    never reached.
    """
    projected = iceberg_field(Quote.field().field("symbol"), field_id=7)
    assert projected.name == "symbol"
    assert projected.field_id == 7
    assert projected.required is True
    assert projected.doc == "Instrument."


def test_one_field_projects_the_same_way_the_whole_schema_does(schema: object) -> None:
    """Two ways to the same NestedField, which is what makes the short one safe."""
    whole = schema.find_field("venue")
    alone = iceberg_field(Quote.field().field("venue"), field_id=whole.field_id)
    assert alone.name == whole.name
    assert alone.field_type == whole.field_type
    assert alone.required == whole.required


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
    @scalar
    class Loose(Convertible):
        symbol: str

    assert iceberg_schema(Loose.field()).identifier_field_ids == []


def test_false_primary_key_metadata_is_not_an_identifier() -> None:
    field = Field.from_arrow_schema(
        pyarrow.schema(
            [
                pyarrow.field(
                    "value",
                    pyarrow.int64(),
                    nullable=False,
                    metadata={b"iceberg:primary_key": b"false"},
                )
            ]
        ),
        "Row",
    )
    assert primary_keys(field) == []


def test_nullable_primary_key_metadata_is_rejected() -> None:
    field = Field.from_arrow_schema(
        pyarrow.schema(
            [
                pyarrow.field(
                    "value",
                    pyarrow.int64(),
                    nullable=True,
                    metadata={b"iceberg:primary_key": b"true"},
                )
            ]
        ),
        "Row",
    )
    with pytest.raises(ValueError, match="primary key.*nullable"):
        primary_keys(field)


def test_ids_match_what_pyiceberg_would_assign_from_the_same_arrow_schema() -> None:
    """The projection is pyiceberg's own, so the two cannot drift apart."""
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    from pyiceberg.schema import assign_fresh_schema_ids

    arrow = Quote.field().into_arrow_schema()
    fresh = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(arrow))
    ours = iceberg_schema(Quote.field())
    assert {f.name: f.field_id for f in ours.fields} == {f.name: f.field_id for f in fresh.fields}


def test_one_field_projects_on_its_own() -> None:
    built = iceberg_field(Quote.field().field("symbol"), field_id=7)
    assert (built.name, built.field_id, built.required) == ("symbol", 7, True)
    assert built.doc == "Instrument."


def test_a_nested_field_projects_too() -> None:
    @scalar
    class Book(Convertible):
        """A book."""

        venue: Quote
        legs: list[int]

    schema = iceberg_schema(Book.field())
    assert str(schema.find_field("venue").field_type).startswith("struct")
    assert schema.find_field("legs").field_type.element_type.__class__.__name__ == "LongType"


# -- the partition spec -----------------------------------------------------


def test_the_partition_spec_follows_the_declaration(schema: object) -> None:
    spec = iceberg_partition_spec(Quote.field(), schema)
    (partition,) = spec.fields
    assert partition.name == "day", "an identity partition keeps the column name"
    assert partition.source_id == schema.find_field("day").field_id
    assert partition.field_id == 1000, "Iceberg numbers partition fields from 1000"


def test_a_transform_is_parsed_as_iceberg_spells_it() -> None:
    @scalar
    class Bucketed(Convertible):
        symbol: Annotated[str, partition_key("bucket[16]")]
        stamp: Annotated[datetime.datetime, partition_key("day")]

    spec = iceberg_partition_spec(Bucketed.field())
    assert [str(f.transform) for f in spec.fields] == ["bucket[16]", "day"]
    assert [f.name for f in spec.fields] == ["symbol_bucket", "stamp_day"], (
        "the width is in the spec already, and a partition name is a directory name"
    )
    for partition in spec.fields:
        assert "[" not in partition.name and "]" not in partition.name


def test_nothing_declared_is_an_unpartitioned_spec() -> None:
    @scalar
    class Flat(Convertible):
        symbol: str

    assert iceberg_partition_spec(Flat.field()).fields == ()


# -- reading one back -------------------------------------------------------


def test_a_schema_comes_back_as_a_struct_field(schema: object) -> None:
    built = iceberg_struct_field(schema, "Quote")
    assert field_names(built) == ["symbol", "day", "venue"]
    assert primary_keys(built) == ["symbol"], "the identifier fields come back as the key"
    assert built.field("symbol").metadata["description"] == "Instrument."
    assert not built.field("symbol").nullable
    assert built.field("venue").nullable


def test_the_spec_comes_back_as_partition_keys(schema: object) -> None:
    spec = iceberg_partition_spec(Quote.field(), schema)
    built = iceberg_struct_field(schema, "Quote", spec)
    assert partition_keys(built) == {"day": "identity"}


def test_the_widths_are_arrow_s_narrow_ones(schema: object) -> None:
    """`schema_to_pyarrow` answers `large_string` for every Iceberg string and
    takes no argument that says otherwise, so a shape read back from a store
    described columns wider than whatever wrote them. It is narrowed at that
    seam, and the round trip below shows nothing is lost by it: Iceberg has one
    `string`, and both Arrow widths are it.

    The conversion this used to avoid does not show above host noise --
    measured over 400,000 rows, interleaved in one process.
    """
    built = iceberg_struct_field(schema)
    assert arrow_type(built.field("symbol")) == pyarrow.string()


def test_the_round_trip_keeps_names_types_and_keys(schema: object) -> None:
    back = iceberg_schema(iceberg_struct_field(schema, "Quote"))
    assert [(f.name, str(f.field_type), f.required, f.doc) for f in back.fields] == [
        (f.name, str(f.field_type), f.required, f.doc) for f in schema.fields
    ]
    assert back.identifier_field_ids == schema.identifier_field_ids


def test_the_module_functions_take_a_field_directly() -> None:
    assert [f.name for f in iceberg_schema(Quote.field()).fields] == field_names(Quote.field())
    assert [f.name for f in iceberg_partition_spec(Quote.field()).fields] == ["day"]


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
    field = iceberg_struct_field(schema, "Venue")
    assert [(member.name, int(member.iceberg["field_id"])) for member in fields(field)] == [
        ("mic", 5),
        ("venue", 9),
    ]
    assert int(field.field("venue").field("size").iceberg["field_id"]) == 12
    assert field.field("mic").iceberg["primary_key"] == "true"


def test_declared_ids_are_kept_rather_than_renumbered() -> None:
    """A round trip through a contract file is an identity, not a rename."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType

    schema = Schema(NestedField(5, "mic", StringType(), required=True))
    published = Field.from_yaml(iceberg_struct_field(schema, "Venue").into_yaml())
    assert int(published.field("mic").iceberg["field_id"]) == 5
    assert [(f.field_id, f.name) for f in iceberg_schema(published).fields] == [(5, "mic")]


def test_a_declaration_with_no_ids_is_numbered_fresh() -> None:
    """The user should not have to know the protocol to hand over a shape."""
    plain = field_of(
        pyarrow.schema([("mic", pyarrow.string()), ("size", pyarrow.int32())]), "Venue"
    )
    assert plain.field("mic").iceberg.get("field_id") is None
    assert [(f.field_id, f.name) for f in iceberg_schema(plain).fields] == [
        (1, "mic"),
        (2, "size"),
    ]


def test_declared_ids_are_preserved_while_missing_ids_are_assigned() -> None:
    arrow = pyarrow.schema(
        [
            pyarrow.field("mic", pyarrow.string(), metadata={"iceberg:field_id": "5"}),
            pyarrow.field("size", pyarrow.int32()),
        ]
    )
    mixed = field_of(arrow, "Venue")

    assert [(field.field_id, field.name) for field in iceberg_schema(mixed).fields] == [
        (5, "mic"),
        (6, "size"),
    ]


def test_nested_declared_ids_are_preserved_in_sibling_first_order() -> None:
    child = pyarrow.field("size", pyarrow.int32(), metadata={"iceberg:field_id": "12"})
    arrow = pyarrow.schema(
        [
            pyarrow.field(
                "venue",
                pyarrow.struct([child, pyarrow.field("rank", pyarrow.int32())]),
                metadata={"iceberg:field_id": "9"},
            ),
            pyarrow.field("mic", pyarrow.string()),
        ]
    )
    schema = iceberg_schema(field_of(arrow, "Venue"))

    assert schema.find_field("venue").field_id == 9
    assert schema.find_field("mic").field_id == 13
    assert schema.find_field("venue.size").field_id == 12
    assert schema.find_field("venue.rank").field_id == 14


def test_partial_list_and_map_ids_are_preserved() -> None:
    item = pyarrow.field(
        "element",
        pyarrow.struct(
            [
                pyarrow.field("x", pyarrow.int64(), metadata={"iceberg:field_id": "7"}),
                pyarrow.field("y", pyarrow.string()),
            ]
        ),
        metadata={"iceberg:field_id": "8"},
    )
    key = pyarrow.field(
        "key", pyarrow.string(), nullable=False, metadata={"iceberg:field_id": "10"}
    )
    arrow = pyarrow.schema(
        [
            pyarrow.field("items", pyarrow.list_(item), metadata={"iceberg:field_id": "5"}),
            pyarrow.field(
                "mapping",
                pyarrow.map_(key, pyarrow.field("value", pyarrow.int32())),
                metadata={"iceberg:field_id": "9"},
            ),
        ]
    )
    schema = iceberg_schema(field_of(arrow, "Nested"))
    listed = schema.find_type("items")
    mapped = schema.find_type("mapping")

    assert listed.element_id == 8
    assert [(field.field_id, field.name) for field in listed.element_type.fields] == [
        (7, "x"),
        (11, "y"),
    ]
    assert (mapped.key_id, mapped.value_id) == (10, 12)


def test_repeated_declared_ids_are_rejected() -> None:
    arrow = pyarrow.schema(
        [
            pyarrow.field("mic", pyarrow.string(), metadata={"iceberg:field_id": "5"}),
            pyarrow.field("size", pyarrow.int32(), metadata={"iceberg:field_id": "5"}),
        ]
    )

    with pytest.raises(ValueError, match="field id 5.*mic.*size"):
        iceberg_schema(field_of(arrow, "Venue"))


def test_an_invalid_declared_id_is_rejected() -> None:
    arrow = pyarrow.schema(
        [pyarrow.field("mic", pyarrow.string(), metadata={"iceberg:field_id": ""})]
    )

    with pytest.raises(ValueError, match="invalid Iceberg field id"):
        iceberg_schema(field_of(arrow, "Venue"))


def test_ids_ride_under_the_protocol_prefix() -> None:
    """`iceberg:field_id` beside the other Iceberg keys; parquet's is the bridge."""
    from rekep.fields import FIELD_ID
    from rekep.iceberg.fields import PARQUET_FIELD_ID

    field = Field(name="mic", dtype=pyarrow.string())
    field.iceberg["field_id"] = "7"
    assert field.metadata[FIELD_ID] == "7"
    assert FIELD_ID == "iceberg:field_id"
    assert field.into_dict()["metadata"][FIELD_ID] == "7"
    assert PARQUET_FIELD_ID == b"PARQUET:field_id", "what parquet files carry, not what we write"


# -- metrics ----------------------------------------------------------------


@scalar
class Wide(Convertible):
    """A shape with more leaves than Iceberg infers bounds for."""

    day: Annotated[datetime.date, partition_key()]
    """Trading day."""

    note: Annotated[str, primary_key()]
    """Free text, keyed on."""


def _widened(leaves: int) -> Field:
    """`Wide` grown to `leaves` members, the two declared ones included."""
    source = Wide.field()
    grown = pyarrow.struct(
        [member.into_arrow() for member in fields(source)]
        + [pyarrow.field(f"pad{index}", pyarrow.int64()) for index in range(leaves - 2)]
    )
    return replace_field(source, dtype=grown)


def test_the_keys_a_reader_filters_on_are_declared_by_name() -> None:
    declared = metrics_for(Quote.field())
    assert declared == {
        "write.metadata.metrics.column.day": "full",
        "write.metadata.metrics.column.symbol": "truncate(16)",
    }, "partition and primary keys, each at a width worth storing"


def test_a_shape_inside_the_budget_does_not_restate_it() -> None:
    assert INFERRED_METRICS not in metrics_for(Quote.field())
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
