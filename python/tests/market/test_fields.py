"""The three rules the market builder adds, each checked where it could be skipped."""

from __future__ import annotations

import dataclasses
import uuid
from typing import Annotated, ClassVar

import pyarrow
import pytest

from rekep.fields import DESCRIPTION, PARTITION_KEY, PRIMARY_KEY, Field, FieldBuilder, field
from rekep.market import Book, BookSide, Instrument, MarketEvent, Order
from rekep.market.enums import Side, State
from rekep.market.fields import MarketFieldBuilder, fix_tag, unkeyed

SHAPES = (Instrument, MarketEvent, Order, BookSide, Book)

#: Every list flavour, because a walk that spelled them all `list` would narrow
#: a 64-bit offset and drop a width without saying so.
FLAVOURS = (
    pyarrow.list_,
    pyarrow.large_list,
    pyarrow.list_view,
    pyarrow.large_list_view,
)


def metadata_in(arrow_type: pyarrow.DataType) -> list[dict[bytes, bytes]]:
    """Every nested field's metadata, at any depth.

    Walked rather than read off `str(type)`: Arrow does not print field
    metadata, so a test that searched the text for `iceberg:primary_key` would
    pass whether the key was stripped or not.
    """
    found = []
    for index in range(arrow_type.num_fields):
        member = arrow_type.field(index)
        found.append(dict(member.metadata or {}))
        found += metadata_in(member.type)
    return found


def keys_in(arrow_type: pyarrow.DataType) -> set[bytes]:
    """Every metadata key anywhere inside `arrow_type`."""
    return {key for metadata in metadata_in(arrow_type) for key in metadata}


@field
class Keyed:
    """A shape whose members are keys, for nesting inside another."""

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    identifier: Annotated[uuid.UUID, Field.primary_key()]
    """Its own key."""

    day: Annotated[str, Field.partition_key()]
    """Its own partition."""


def test_an_identifier_is_sixteen_fixed_bytes_and_not_text() -> None:
    """The base builder spells a UUID as a string; here it is the column width."""
    assert MarketFieldBuilder().data_type(uuid.UUID) == pyarrow.binary(16)
    assert FieldBuilder().data_type(uuid.UUID) == pyarrow.string()


def test_a_ranged_code_is_int32_and_not_int64() -> None:
    """The base builder takes the width of a Python int, which is twice what is needed."""
    assert MarketFieldBuilder().data_type(State) == pyarrow.int32()
    assert MarketFieldBuilder().data_type(Side) == pyarrow.int32()
    assert FieldBuilder().data_type(State) == pyarrow.int64()


def test_every_ranged_column_of_every_shape_is_int32() -> None:
    """The rule is worth nothing if one shape quietly bypasses the builder."""
    for shape in SHAPES:
        for member in shape.FIELD.fields:
            if member.name in ("state", "prev_state", "side", "kind", "tif", "option_kind"):
                assert member.arrow_type == pyarrow.int32(), f"{shape.__name__}.{member.name}"


def test_a_nested_shape_keeps_its_documentation_and_loses_its_keys() -> None:
    """A key is a table's; nothing reads a nested one, so publishing one would lie."""
    nested = MarketEvent.FIELD.field("instrument")
    assert Instrument.FIELD.primary_keys() == ["xh128"], "the table itself is still keyed"
    assert not nested.field("xh128").is_primary_key
    assert nested.field("symbol").description == Instrument.FIELD.field("symbol").description


def test_a_key_two_levels_down_is_stripped_too() -> None:
    """`Book` nests a `BookSide` which nests an `Instrument`: the walk must reach it."""
    deep = Book.FIELD.field("bid").field("instrument")
    assert deep.field("xh128").name == "xh128", "the member is still there"
    assert not deep.field("xh128").is_primary_key
    # Inside the nested side, and everything under it -- not the book's own
    # top level, which is a table and keeps its keys.
    inside = keys_in(Book.FIELD.field("bid").arrow_type)
    assert PRIMARY_KEY.encode() not in inside and PARTITION_KEY.encode() not in inside
    assert DESCRIPTION.encode() in inside, "and the comments are all still there"
    assert {PRIMARY_KEY.encode(), PARTITION_KEY.encode()} <= keys_in(Book.FIELD.arrow_type)


def test_the_shape_that_owns_the_table_keeps_its_own_keys() -> None:
    """Stripping the nested ones must not strip the top level with them."""
    for shape in SHAPES:
        assert shape.FIELD.primary_keys(), shape.__name__
    assert Book.FIELD.primary_keys() == ["unix", "h128"]
    assert Book.FIELD.partition_keys() == {"date": "identity"}


@pytest.mark.parametrize("flavour", FLAVOURS, ids=lambda build: build.__name__)
def test_a_key_inside_every_list_flavour_is_stripped_and_the_flavour_kept(flavour) -> None:
    """Crossing every branch of the walk, which is where a kind check goes wrong."""
    inside = flavour(Keyed.FIELD.arrow_type)
    stripped = unkeyed(inside)
    assert type(stripped) is type(inside)
    assert str(stripped).split("<", 1)[0] == str(inside).split("<", 1)[0]
    assert keys_in(inside) == {PRIMARY_KEY.encode(), PARTITION_KEY.encode(), DESCRIPTION.encode()}
    assert keys_in(stripped) == {DESCRIPTION.encode()}, "the comments must survive the strip"


def test_a_fixed_size_list_keeps_the_width_that_is_part_of_its_type() -> None:
    inside = pyarrow.list_(pyarrow.field("item", Keyed.FIELD.arrow_type), 3)
    stripped = unkeyed(inside)
    assert pyarrow.types.is_fixed_size_list(stripped) and stripped.list_size == 3
    assert keys_in(stripped) == {DESCRIPTION.encode()}


def test_a_key_inside_a_map_is_stripped_on_both_halves() -> None:
    inside = pyarrow.map_(pyarrow.string(), Keyed.FIELD.arrow_type, keys_sorted=True)
    stripped = unkeyed(inside)
    assert pyarrow.types.is_map(stripped) and stripped.keys_sorted
    assert keys_in(stripped) == {DESCRIPTION.encode()}


def test_a_leaf_comes_back_untouched() -> None:
    assert unkeyed(pyarrow.int32()) == pyarrow.int32()


def test_a_fix_tag_lands_where_the_protocol_reads_it() -> None:
    declared = fix_tag("Price", 44)
    assert declared.fix["name"] == "Price"
    assert declared.fix["tag"] == "44"
    assert declared.metadata == {"fix:name": "Price", "fix:tag": "44"}


def test_a_fix_tag_merges_with_the_other_declarations_on_a_member() -> None:
    """Both `Annotated` extras have to survive, or one silently wins."""
    built = Field.from_annotation(
        "unix", Annotated[int, Field.primary_key(), fix_tag("Symbol", 55)]
    )
    assert built.is_primary_key and built.fix["tag"] == "55"


def test_the_builder_is_wired_onto_every_shape() -> None:
    """A shape that forgot `FIELD_BUILDER` would project UUIDs as strings and pass."""
    for shape in SHAPES:
        assert getattr(shape, "FIELD_BUILDER", None) is MarketFieldBuilder, shape.__name__


def test_the_builder_is_not_a_dataclass_member() -> None:
    """A `ClassVar` must not become a column, or every shape grows one."""
    for shape in SHAPES:
        assert "FIELD_BUILDER" not in {f.name for f in dataclasses.fields(shape)}
        assert "FIELD_BUILDER" not in shape.FIELD.names
