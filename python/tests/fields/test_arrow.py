"""Projecting a class's type hints onto Arrow."""

import datetime
import decimal
import enum
import functools
import pathlib
import textwrap
import uuid
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, Field, FieldBuilder, scalar


class Side(enum.StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class Tier(enum.IntEnum):
    RETAIL = 1
    INSTITUTIONAL = 2


@scalar
class Venue(Convertible):
    """A trading venue.

    Attributes:
        mic: ISO 10383 market identifier.
        timeout: Seconds before giving up on a quote,
            wrapped onto a second line.
    """

    mic: str
    timeout: float | None = None


@scalar
class Book(Convertible):
    """A book of orders.

    Second paragraph, which is not part of the summary.

    :param name: Human name of the book.
    :param size: Lots on the book.
    """

    name: str
    opened: datetime.date
    side: Side
    size: Annotated[int, Field(dtype=pyarrow.int32(), metadata={"unit": "lots"})]
    venues: list[Venue]
    limits: dict[str, int]
    root: pathlib.Path | None = None


@scalar
class Instrument(Convertible):
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


@scalar
class Node(Convertible):
    """A class that refers to itself."""

    name: str
    child: "Node | None" = None


@pytest.fixture
def schema() -> pyarrow.Schema:
    return Book.into_field().into_arrow_schema()


def members(cls: type) -> dict[str, Field]:
    return {member.name: member for member in FieldBuilder().fields(cls)}


# -- nullability ------------------------------------------------------------


def test_plain_annotations_are_non_nullable(schema: pyarrow.Schema) -> None:
    assert not schema.field("name").nullable
    assert not schema.field("opened").nullable
    assert not schema.field("venues").nullable


def test_optional_annotations_are_nullable(schema: pyarrow.Schema) -> None:
    assert schema.field("root").nullable
    assert members(Venue)["timeout"].nullable


def test_nullability_can_be_declared() -> None:
    @scalar
    class Forced(Convertible):
        loose: Annotated[str, Field(nullable=True)]
        tight: Annotated[str | None, Field(nullable=False)]

    declared = members(Forced)
    assert declared["loose"].nullable
    assert not declared["tight"].nullable


def test_item_nullability_survives_into_the_list() -> None:
    @scalar
    class Holder(Convertible):
        loose: list[str | None]
        tight: list[str]

    declared = members(Holder)
    assert declared["loose"].dtype.field(0).nullable
    assert not declared["tight"].dtype.field(0).nullable


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
    assert FieldBuilder().arrow_type(annotation) == expected


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
    assert FieldBuilder().arrow_type(annotation) == expected


def test_fixed_tuple_becomes_a_positional_struct() -> None:
    struct = FieldBuilder().arrow_type(tuple[int, str])
    assert struct.num_fields == 2
    assert [struct.field(i).name for i in range(2)] == ["f0", "f1"]
    assert struct.field(1).type == pyarrow.string()


def test_a_nested_class_becomes_a_struct(schema: pyarrow.Schema) -> None:
    item = schema.field("venues").type.field(0)
    assert pyarrow.types.is_struct(item.type)
    assert [item.type.field(i).name for i in range(item.type.num_fields)] == ["mic", "timeout"]


def test_the_class_field_is_a_struct_named_after_the_class() -> None:
    assert Venue.into_field().name == "Venue"
    assert pyarrow.types.is_struct(Venue.into_field().dtype)
    assert not Venue.into_field().nullable
    assert [member.name for member in Venue.into_field().fields] == ["mic", "timeout"]


def test_a_member_is_reachable_by_name() -> None:
    assert Venue.into_field().field("mic").dtype == pyarrow.string()
    with pytest.raises(KeyError, match="no member"):
        Venue.into_field().field("absent")


# -- what an annotation declares --------------------------------------------


def test_a_declared_type_wins_over_the_inferred_one(schema: pyarrow.Schema) -> None:
    assert schema.field("size").type == pyarrow.int32()


def test_declared_metadata_is_attached(schema: pyarrow.Schema) -> None:
    assert schema.field("size").metadata[b"unit"] == b"lots"


@pytest.mark.parametrize(
    ("extra", "check"),
    [
        (pyarrow.int16(), lambda f: f.dtype == pyarrow.int16()),
        ({"unit": "bps"}, lambda f: f.metadata["unit"] == "bps"),
        ("a bare string is a description", lambda f: f.description),
    ],
)
def test_annotated_shorthands(extra: object, check) -> None:
    @scalar
    class Short(Convertible):
        value: Annotated[int, extra]

    assert check(members(Short)["value"])


def test_declarations_merge_left_to_right() -> None:
    @scalar
    class Mixed(Convertible):
        day: Annotated[str, {"unit": "day"}, "The day.", pyarrow.large_string()]

    declared = members(Mixed)["day"]
    assert declared.dtype == pyarrow.large_string()
    assert declared.metadata["unit"] == "day"
    assert declared.description == "The day."


# -- docstrings -------------------------------------------------------------


def test_google_style_attributes_become_descriptions() -> None:
    assert members(Venue)["mic"].description == "ISO 10383 market identifier."


def test_wrapped_description_lines_are_joined() -> None:
    assert members(Venue)["timeout"].description == (
        "Seconds before giving up on a quote, wrapped onto a second line."
    )


def test_sphinx_style_params_become_descriptions(schema: pyarrow.Schema) -> None:
    assert schema.field("name").metadata[b"description"] == b"Human name of the book."


def test_class_summary_becomes_schema_metadata(schema: pyarrow.Schema) -> None:
    assert schema.metadata[b"description"] == b"A book of orders."
    assert schema.metadata[b"name"] == b"Book"


def test_an_undescribed_field_carries_no_description(schema: pyarrow.Schema) -> None:
    assert b"description" not in schema.field("opened").metadata


def test_an_explicit_description_beats_the_docstring() -> None:
    @scalar
    class Described(Convertible):
        """One field.

        Attributes:
            value: From the docstring.
        """

        value: Annotated[int, "From the annotation."]

    assert members(Described)["value"].description == "From the annotation."


def test_descriptions_are_inherited_from_a_base_docstring() -> None:
    @scalar
    class Extended(Venue):
        """A venue with a desk.

        Attributes:
            desk: Owning desk.
        """

        desk: str = "default"

    declared = members(Extended)
    assert declared["mic"].description == "ISO 10383 market identifier."
    assert declared["desk"].description == "Owning desk."


def test_a_literal_under_a_field_becomes_its_description() -> None:
    declared = members(Instrument)
    assert declared["isin"].description == "ISO 6166 identifier."
    assert declared["tick"].description == "Smallest price increment."


def test_a_wrapped_attribute_docstring_is_folded() -> None:
    assert members(Instrument)["lot"].description == (
        "Minimum tradable quantity, wrapped onto a second line."
    )


def test_a_comment_is_not_a_description() -> None:
    assert members(Instrument)["currency"].metadata == {"fix:display": "Currency"}


def test_an_attribute_docstring_beats_the_class_docstring() -> None:
    @scalar
    class Both(Convertible):
        """Two sources.

        Attributes:
            value: From the class docstring.
        """

        value: int
        """From under the field."""

    assert members(Both)["value"].description == "From under the field."


def test_an_annotation_still_beats_an_attribute_docstring() -> None:
    @scalar
    class Both(Convertible):
        value: Annotated[int, "From the annotation."]
        """From under the field."""

    assert members(Both)["value"].description == "From the annotation."


def test_attribute_docstrings_are_inherited() -> None:
    @scalar
    class Listed(Instrument):
        """An instrument with a venue."""

        mic: str = "XPAR"
        """Where it lists."""

    declared = members(Listed)
    assert declared["isin"].description == "ISO 6166 identifier."
    assert declared["mic"].description == "Where it lists."


def test_a_class_without_readable_source_still_projects() -> None:
    """`exec` leaves nothing for `inspect.getsource` to find; that is not fatal."""
    source = textwrap.dedent(
        """
        @scalar
        class Generated(Convertible):
            value: int
        """
    )
    namespace: dict[str, object] = {"Convertible": Convertible, "scalar": scalar}
    exec(source, namespace)  # noqa: S102

    schema = namespace["Generated"].into_field().into_arrow_schema()
    assert schema.names == ["value"]
    assert schema.field("value").metadata == {b"fix:display": b"Value"}


# -- refusals ---------------------------------------------------------------


def test_a_self_referential_class_builds_one_level_then_binary() -> None:
    """Arrow has no recursive types: one nested level keeps the shape
    readable, and whatever sits below it defaults to a binary leaf."""
    child = Node.into_field().into_arrow_schema().field("child")
    assert child.type.field("child").type == pyarrow.binary()


def test_a_non_optional_union_is_refused() -> None:
    @scalar
    class Ambiguous(Convertible):
        value: int | str

    with pytest.raises(TypeError, match="union"):
        Ambiguous.into_field().into_arrow_schema()


def test_an_unknown_leaf_is_refused_with_a_way_out() -> None:
    class Opaque:
        pass

    @scalar
    class Holder(Convertible):
        value: Opaque

    with pytest.raises(TypeError, match=r"Field\(dtype="):
        Holder.into_field().into_arrow_schema()


def test_a_non_dataclass_is_refused() -> None:
    class Loose:
        pass

    with pytest.raises(TypeError, match="must be a dataclass"):
        Field.from_dataclass(Loose)


# -- extension --------------------------------------------------------------


def test_a_builder_can_be_taught_a_new_leaf() -> None:
    class Opaque:
        pass

    class WiderBuilder(FieldBuilder):
        @classmethod
        @functools.cache
        def into_scalars(cls):
            return {**super().into_scalars(), Opaque: pyarrow.binary()}

    @scalar
    class Holder(Convertible):
        @classmethod
        @functools.cache
        def into_field_builder(cls):
            return WiderBuilder

        value: Opaque

    assert Holder.into_field().into_arrow_schema().field("value").type == pyarrow.binary()
