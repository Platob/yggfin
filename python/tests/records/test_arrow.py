import datetime
import decimal
import enum
import pathlib
import textwrap
import uuid
from typing import Annotated

import pyarrow
import pytest

from rekep import Arrow, ArrowFieldBuilder, Record, record


class Side(enum.StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class Tier(enum.IntEnum):
    RETAIL = 1
    INSTITUTIONAL = 2


@record
class Venue(Record):
    """A trading venue.

    Attributes:
        mic: ISO 10383 market identifier.
        timeout: Seconds before giving up on a quote,
            wrapped onto a second line.
    """

    mic: str
    timeout: float | None = None


@record
class Book(Record):
    """A book of orders.

    Second paragraph, which is not part of the summary.

    :param name: Human name of the book.
    :param size: Lots on the book.
    """

    name: str
    opened: datetime.date
    side: Side
    size: Annotated[int, Arrow(type=pyarrow.int32(), metadata={"unit": "lots"})]
    venues: list[Venue]
    limits: dict[str, int]
    root: pathlib.Path | None = None


@record
class Instrument(Record):
    """A tradable instrument."""

    isin: str
    """ISO 6166 identifier."""

    lot: int
    """Minimum tradable quantity,
    wrapped onto a second line.
    """

    currency: str = "EUR"
    # a plain comment is not a description

    tick: float = 0.01
    """Smallest price increment."""


@record
class Node(Record):
    """A record that refers to itself."""

    name: str
    child: "Node | None" = None


@pytest.fixture
def schema() -> pyarrow.Schema:
    return Book.into_arrow_schema()


# -- nullability ------------------------------------------------------------


def test_plain_annotations_are_non_nullable(schema: pyarrow.Schema) -> None:
    assert not schema.field("name").nullable
    assert not schema.field("opened").nullable
    assert not schema.field("venues").nullable


def test_optional_annotations_are_nullable(schema: pyarrow.Schema) -> None:
    assert schema.field("root").nullable
    assert pyarrow.struct(ArrowFieldBuilder().fields(Venue)).field("timeout").nullable


def test_nullability_can_be_overridden() -> None:
    @record
    class Forced(Record):
        loose: Annotated[str, Arrow(nullable=True)]
        tight: Annotated[str | None, Arrow(nullable=False)]

    fields = ArrowFieldBuilder().fields(Forced)
    assert fields[0].nullable
    assert not fields[1].nullable


def test_item_nullability_survives_into_the_list() -> None:
    @record
    class Holder(Record):
        loose: list[str | None]
        tight: list[str]

    fields = {f.name: f for f in ArrowFieldBuilder().fields(Holder)}
    assert fields["loose"].type.field(0).nullable
    assert not fields["tight"].type.field(0).nullable


# -- type inference ---------------------------------------------------------


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (bool, pyarrow.bool_()),
        (int, pyarrow.int64()),
        (float, pyarrow.float64()),
        (str, pyarrow.string()),
        (bytes, pyarrow.binary()),
        (datetime.datetime, pyarrow.timestamp("us")),
        (datetime.date, pyarrow.date32()),
        (datetime.time, pyarrow.time64("us")),
        (datetime.timedelta, pyarrow.duration("us")),
        (decimal.Decimal, pyarrow.decimal128(38, 9)),
        (uuid.UUID, pyarrow.string()),
        (pathlib.Path, pyarrow.string()),
        (Side, pyarrow.string()),
        (Tier, pyarrow.int64()),
    ],
)
def test_scalars(annotation: type, expected: pyarrow.DataType) -> None:
    assert ArrowFieldBuilder().data_type(annotation) == expected


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (list[int], pyarrow.list_(pyarrow.field("item", pyarrow.int64(), nullable=False))),
        (set[str], pyarrow.list_(pyarrow.field("item", pyarrow.string(), nullable=False))),
        (tuple[int, ...], pyarrow.list_(pyarrow.field("item", pyarrow.int64(), nullable=False))),
        (
            dict[str, int],
            pyarrow.map_(pyarrow.string(), pyarrow.field("value", pyarrow.int64(), nullable=False)),
        ),
    ],
)
def test_collections(annotation: type, expected: pyarrow.DataType) -> None:
    assert ArrowFieldBuilder().data_type(annotation) == expected


def test_fixed_tuple_becomes_a_positional_struct() -> None:
    struct = ArrowFieldBuilder().data_type(tuple[int, str])
    assert struct.num_fields == 2
    assert [struct.field(i).name for i in range(2)] == ["f0", "f1"]
    assert struct.field(1).type == pyarrow.string()


def test_nested_record_becomes_a_struct(schema: pyarrow.Schema) -> None:
    item = schema.field("venues").type.field(0)
    assert pyarrow.types.is_struct(item.type)
    assert [item.type.field(i).name for i in range(item.type.num_fields)] == ["mic", "timeout"]


def test_into_arrow_type_is_a_struct() -> None:
    assert Venue.into_arrow_type() == pyarrow.struct(ArrowFieldBuilder().fields(Venue))


def test_into_arrow_field_names_itself_by_default() -> None:
    assert Venue.into_arrow_field().name == "Venue"
    assert Venue.into_arrow_field("venue").name == "venue"
    assert not Venue.into_arrow_field().nullable
    assert Venue.into_arrow_field(nullable=True).nullable


# -- annotation overrides ---------------------------------------------------


def test_annotated_overrides_the_inferred_type(schema: pyarrow.Schema) -> None:
    assert schema.field("size").type == pyarrow.int32()


def test_annotated_metadata_is_attached(schema: pyarrow.Schema) -> None:
    assert schema.field("size").metadata[b"unit"] == b"lots"


@pytest.mark.parametrize(
    ("extra", "check"),
    [
        (pyarrow.int16(), lambda f: f.type == pyarrow.int16()),
        ({"unit": "bps"}, lambda f: f.metadata[b"unit"] == b"bps"),
        ("a bare string is a description", lambda f: b"description" in f.metadata),
    ],
)
def test_annotated_shorthands(extra: object, check) -> None:
    @record
    class Short(Record):
        value: Annotated[int, extra]

    assert check(ArrowFieldBuilder().fields(Short)[0])


# -- docstrings -------------------------------------------------------------


def test_google_style_attributes_become_descriptions() -> None:
    fields = {f.name: f for f in ArrowFieldBuilder().fields(Venue)}
    assert fields["mic"].metadata[b"description"] == b"ISO 10383 market identifier."


def test_wrapped_description_lines_are_joined() -> None:
    fields = {f.name: f for f in ArrowFieldBuilder().fields(Venue)}
    assert fields["timeout"].metadata[b"description"] == (
        b"Seconds before giving up on a quote, wrapped onto a second line."
    )


def test_sphinx_style_params_become_descriptions(schema: pyarrow.Schema) -> None:
    assert schema.field("name").metadata[b"description"] == b"Human name of the book."


def test_class_summary_becomes_schema_metadata(schema: pyarrow.Schema) -> None:
    assert schema.metadata[b"description"] == b"A book of orders."


def test_undescribed_fields_carry_only_their_id(schema: pyarrow.Schema) -> None:
    assert set(schema.field("opened").metadata) == {b"PARQUET:field_id"}


def test_explicit_description_beats_the_docstring() -> None:
    @record
    class Described(Record):
        """One field.

        Attributes:
            value: From the docstring.
        """

        value: Annotated[int, Arrow(description="From the annotation.")]

    field = ArrowFieldBuilder().fields(Described)[0]
    assert field.metadata[b"description"] == b"From the annotation."


def test_descriptions_are_inherited_from_a_base_docstring() -> None:
    @record
    class Extended(Venue):
        """A venue with a desk.

        Attributes:
            desk: Owning desk.
        """

        desk: str = "default"

    fields = {f.name: f for f in ArrowFieldBuilder().fields(Extended)}
    assert fields["mic"].metadata[b"description"] == b"ISO 10383 market identifier."
    assert fields["desk"].metadata[b"description"] == b"Owning desk."


# -- refusals ---------------------------------------------------------------


def test_a_self_referential_record_is_refused_not_chased() -> None:
    with pytest.raises(TypeError, match="no recursive types"):
        Node.into_arrow_schema()


def test_a_non_optional_union_is_refused() -> None:
    @record
    class Ambiguous(Record):
        value: int | str

    with pytest.raises(TypeError, match="union"):
        Ambiguous.into_arrow_schema()


def test_an_unknown_leaf_is_refused_with_a_way_out() -> None:
    class Opaque:
        pass

    @record
    class Holder(Record):
        value: Opaque

    with pytest.raises(TypeError, match="Arrow"):
        Holder.into_arrow_schema()


def test_a_non_dataclass_is_refused() -> None:
    class Loose(Record):
        pass

    with pytest.raises(TypeError, match="must be a dataclass"):
        Loose.into_arrow_schema()


# -- dispatch ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("requested", "stem"),
    [
        (pyarrow.Schema, "arrow_schema"),
        (pyarrow.Field, "arrow_field"),
        (pyarrow.DataType, "arrow_type"),
        (pyarrow.StructType, "arrow_type"),
    ],
)
def test_into_redirects_on_the_requested_arrow_type(requested: type, stem: str) -> None:
    assert Venue.redirect_of(requested) == stem


def test_into_schema_via_dispatch(schema: pyarrow.Schema) -> None:
    book = Book(
        name="b",
        opened=datetime.date(2026, 1, 1),
        side=Side.BUY,
        size=1,
        venues=[],
        limits={},
    )
    assert book.into_(pyarrow.Schema) == schema


# -- extension --------------------------------------------------------------


def test_a_builder_can_be_taught_a_new_leaf() -> None:
    class Opaque:
        pass

    class WiderBuilder(ArrowFieldBuilder):
        SCALARS = {**ArrowFieldBuilder.SCALARS, Opaque: pyarrow.binary()}

    @record
    class Holder(Record):
        ARROW_BUILDER = WiderBuilder
        value: Opaque

    assert Holder.into_arrow_schema().field("value").type == pyarrow.binary()


# -- attribute docstrings ---------------------------------------------------


def test_a_literal_under_a_field_becomes_its_description() -> None:
    fields = {f.name: f for f in ArrowFieldBuilder().fields(Instrument)}
    assert fields["isin"].metadata[b"description"] == b"ISO 6166 identifier."
    assert fields["tick"].metadata[b"description"] == b"Smallest price increment."


def test_a_wrapped_attribute_docstring_is_folded() -> None:
    fields = {f.name: f for f in ArrowFieldBuilder().fields(Instrument)}
    assert fields["lot"].metadata[b"description"] == (
        b"Minimum tradable quantity, wrapped onto a second line."
    )


def test_a_comment_is_not_a_description() -> None:
    fields = {f.name: f for f in ArrowFieldBuilder().fields(Instrument)}
    assert fields["currency"].metadata is None


def test_an_attribute_docstring_beats_the_class_docstring() -> None:
    @record
    class Both(Record):
        """Two sources.

        Attributes:
            value: From the class docstring.
        """

        value: int
        """From under the field."""

    field = ArrowFieldBuilder().fields(Both)[0]
    assert field.metadata[b"description"] == b"From under the field."


def test_an_annotation_still_beats_an_attribute_docstring() -> None:
    @record
    class Both(Record):
        value: Annotated[int, Arrow(description="From the annotation.")]
        """From under the field."""

    field = ArrowFieldBuilder().fields(Both)[0]
    assert field.metadata[b"description"] == b"From the annotation."


def test_attribute_docstrings_are_inherited() -> None:
    @record
    class Listed(Instrument):
        """An instrument with a venue."""

        mic: str = "XPAR"
        """Where it lists."""

    fields = {f.name: f for f in ArrowFieldBuilder().fields(Listed)}
    assert fields["isin"].metadata[b"description"] == b"ISO 6166 identifier."
    assert fields["mic"].metadata[b"description"] == b"Where it lists."


def test_a_record_without_readable_source_still_projects() -> None:
    """`exec` leaves nothing for `inspect.getsource` to find; that is not fatal."""
    source = textwrap.dedent(
        """
        @record
        class Generated(Record):
            value: int
        """
    )
    namespace: dict[str, object] = {"Record": Record, "record": record}
    exec(source, namespace)  # noqa: S102

    schema = namespace["Generated"].into_arrow_schema()
    assert schema.names == ["value"]
    assert set(schema.field("value").metadata) == {b"PARQUET:field_id"}


# -- caching ----------------------------------------------------------------


def test_projections_are_cached_per_class() -> None:
    assert Venue.into_arrow_schema() is Venue.into_arrow_schema()
    assert Venue.into_arrow_type() is Venue.into_arrow_type()
    assert Venue.into_arrow_field() is Venue.into_arrow_field()


def test_the_cache_keys_on_the_arguments() -> None:
    assert Venue.into_arrow_field("a") is not Venue.into_arrow_field("b")
    assert Venue.into_arrow_field(nullable=True) is not Venue.into_arrow_field(nullable=False)
    assert Venue.into_arrow_field("a") is Venue.into_arrow_field("a")


def test_the_cache_keys_on_the_class() -> None:
    """A subclass adds fields, so it must not read the base's cached entry."""

    @record
    class Extended(Venue):
        desk: str = "default"

    assert Extended.into_arrow_field() is not Venue.into_arrow_field()
    assert Extended.into_arrow_schema().names == ["mic", "timeout", "desk"]
    assert Venue.into_arrow_schema().names == ["mic", "timeout"]


def test_a_builder_override_is_not_served_from_a_sibling_cache() -> None:
    class Opaque:
        pass

    class WiderBuilder(ArrowFieldBuilder):
        SCALARS = {**ArrowFieldBuilder.SCALARS, Opaque: pyarrow.large_binary()}

    @record
    class Narrow(Record):
        value: str

    @record
    class Wide(Record):
        ARROW_BUILDER = WiderBuilder
        value: Opaque

    assert Narrow.into_arrow_schema().field("value").type == pyarrow.string()
    assert Wide.into_arrow_schema().field("value").type == pyarrow.large_binary()


# -- protocol metadata ------------------------------------------------------


def test_iceberg_mapping_lands_under_prefixed_keys() -> None:
    @record
    class Partitioned(Record):
        day: Annotated[str, Arrow(iceberg={"write-order": "asc", "sort-order": "1"})]

    metadata = ArrowFieldBuilder().fields(Partitioned)[0].metadata
    assert metadata[b"iceberg:write-order"] == b"asc"
    assert metadata[b"iceberg:sort-order"] == b"1"


def test_iceberg_mapping_merges_and_coexists() -> None:
    @record
    class Mixed(Record):
        day: Annotated[str, Arrow(iceberg={"sort-order": "1"}), {"unit": "day"}, "The day."]

    metadata = ArrowFieldBuilder().fields(Mixed)[0].metadata
    assert metadata[b"iceberg:sort-order"] == b"1"
    assert metadata[b"unit"] == b"day"
    assert metadata[b"description"] == b"The day."


# -- field ids --------------------------------------------------------------


def test_field_ids_are_stamped_in_iceberg_order(schema: pyarrow.Schema) -> None:
    """Siblings first, then each subtree, exactly as pyiceberg assigns fresh ids."""
    ids = {f.name: int(f.metadata[b"PARQUET:field_id"]) for f in schema}
    assert ids == {
        "name": 1,
        "opened": 2,
        "side": 3,
        "size": 4,
        "venues": 5,
        "limits": 6,
        "root": 7,
    }

    venues = schema.field("venues").type
    item = venues.field(0)
    assert int(item.metadata[b"PARQUET:field_id"]) == 8
    struct = item.type
    nested = {
        struct.field(i).name: int(struct.field(i).metadata[b"PARQUET:field_id"])
        for i in range(struct.num_fields)
    }
    assert nested == {"mic": 9, "timeout": 10}

    limits = schema.field("limits").type
    assert int(limits.key_field.metadata[b"PARQUET:field_id"]) == 11
    assert int(limits.item_field.metadata[b"PARQUET:field_id"]) == 12


def test_field_ids_match_what_pyiceberg_would_assign(schema: pyarrow.Schema) -> None:
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    from pyiceberg.schema import assign_fresh_schema_ids

    fresh = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(schema))
    stamped = Book.into_iceberg_schema()
    assert {f.name: f.field_id for f in stamped.fields} == {
        f.name: f.field_id for f in fresh.fields
    }


def test_a_builder_can_switch_ids_off() -> None:
    class NoIds(ArrowFieldBuilder):
        FIELD_IDS = False

    @record
    class Bare(Record):
        ARROW_BUILDER = NoIds
        value: int

    assert Bare.into_arrow_schema().field("value").metadata is None
    assert [f.field_id for f in Bare.into_iceberg_schema().fields] == [1], "fresh fallback"
