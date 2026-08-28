"""The reverse projection: a class rebuilt from a field."""

import dataclasses
import datetime
import decimal
import functools
import typing
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, Field, scalar
from rekep.fields import ClassBuilder


@scalar
class Venue(Convertible):
    """A trading venue."""

    mic: str
    """ISO 10383 market identifier."""

    timeout: float | None = None


@scalar
class Book(Convertible):
    """A book of orders."""

    name: str
    """Human name of the book."""

    size: Annotated[int, Field(dtype=pyarrow.int32(), metadata={"unit": "lots"})]
    price: decimal.Decimal
    opened: datetime.datetime
    venues: list[Venue]
    limits: dict[str, int | None]
    root: str | None = None


@pytest.fixture
def rebuilt() -> type:
    return Book.into_field().into_dataclass()


# -- the round trip ---------------------------------------------------------


def test_the_rebuilt_class_projects_to_the_same_schema(rebuilt: type) -> None:
    assert rebuilt.into_field().into_arrow_schema().equals(Book.into_field().into_arrow_schema())


def test_the_rebuilt_class_is_a_dataclass(rebuilt: type) -> None:
    assert dataclasses.is_dataclass(rebuilt)
    assert [
        f.name for f in dataclasses.fields(rebuilt)
    ] == Book.into_field().into_arrow_schema().names


def test_the_rebuilt_class_takes_back_its_identity(rebuilt: type) -> None:
    assert rebuilt.__name__ == "Book"
    assert rebuilt.__module__ == Book.__module__
    assert rebuilt.__doc__ == "A book of orders."


def test_an_explicit_name_wins() -> None:
    assert Book.into_field().into_dataclass("Ledger").__name__ == "Ledger"


def test_an_anonymous_struct_falls_back_to_a_name() -> None:
    anonymous = Field.from_arrow_type(pyarrow.struct([("a", pyarrow.int64())]))
    assert anonymous.into_dataclass().__name__ == "ArrowFields"


def test_the_rebuilt_class_is_keyword_only(rebuilt: type) -> None:
    """Arrow field order owes nothing to Python's defaults-last rule."""
    with pytest.raises(TypeError):
        rebuilt("positional")


def test_exact_types_are_carried_not_re_inferred(rebuilt: type) -> None:
    assert rebuilt.into_field().field("size").dtype == pyarrow.int32()
    assert rebuilt.into_field().field("price").dtype == pyarrow.decimal128(38, 9)


def test_metadata_and_descriptions_survive(rebuilt: type) -> None:
    assert rebuilt.into_field().field("size").metadata["unit"] == "lots"
    assert rebuilt.into_field().field("name").description == "Human name of the book."


def test_a_nested_structs_own_descriptions_survive(rebuilt: type) -> None:
    item = rebuilt.into_field().field("venues").dtype.field(0)
    assert item.type.field(0).metadata[b"description"] == b"ISO 10383 market identifier."


def test_nullable_members_default_to_none(rebuilt: type) -> None:
    instance = rebuilt(
        name="eu",
        size=1,
        price=decimal.Decimal("1.5"),
        opened=datetime.datetime(2026, 8, 14, 0, 5),  # noqa: DTZ001
        venues=[],
        limits={},
    )
    assert instance.root is None


def test_a_rebuilt_instance_serialises_itself(rebuilt: type) -> None:
    """`Convertible` is the default base, so a clone is a document too."""
    instance = rebuilt(
        name="eu",
        size=1,
        price=decimal.Decimal("1.5"),
        opened=datetime.datetime(2026, 8, 14, 0, 5),  # noqa: DTZ001
        venues=[],
        limits={},
    )
    assert rebuilt.from_json(instance.into_json()) == instance


# -- a schema from outside --------------------------------------------------


def test_a_foreign_schema_becomes_a_class() -> None:
    """A parquet footer knows nothing of this package; it still projects."""
    schema = pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int32(), nullable=False),
            pyarrow.field("tags", pyarrow.list_(pyarrow.string())),
            pyarrow.field("book", pyarrow.struct([("bid", pyarrow.float64())])),
        ]
    )
    built = Field.from_arrow_schema(schema, "Quote").into_dataclass()
    assert built.__name__ == "Quote"
    assert (
        built.into_field()
        .into_arrow_schema()
        .equals(pyarrow.schema(list(schema), metadata={"name": "Quote"}))
    )


def test_schema_metadata_survives_the_reverse_projection() -> None:
    schema = pyarrow.schema(
        [("id", pyarrow.int64())],
        metadata={"name": "Quote", "namespace": "feeds", "version": "2", "owner": "md"},
    )

    built = Field.from_arrow_schema(schema).into_dataclass()

    assert built.into_field_metadata() == {"version": "2", "owner": "md"}
    with pytest.raises(TypeError):
        built.into_field_metadata()["version"] = "3"
    assert built.into_field().into_arrow_schema().equals(schema)


def test_an_empty_schema_metadata_set_masks_the_bases_metadata() -> None:
    class Versioned:
        @classmethod
        @functools.cache
        def into_field_metadata(cls):
            return {"version": "2"}

    schema = pyarrow.schema([("id", pyarrow.int64())], metadata={"name": "Quote"})
    built = ClassBuilder().dataclass(Field.from_arrow_schema(schema), base=Versioned)

    assert built.into_field_metadata() == {}
    assert built.into_field().into_arrow_schema().equals(schema)


def test_a_nested_struct_becomes_a_nested_class() -> None:
    schema = pyarrow.schema(
        [pyarrow.field("order_book", pyarrow.struct([("bid", pyarrow.int64())]))]
    )
    built = Field.from_arrow_schema(schema).into_dataclass("Quote")
    nested = built.__annotations__["order_book"]
    assert nested.__args__[0].__name__ == "OrderBook", "a field name becomes a class name"


def test_a_builder_can_be_pointed_at_another_base() -> None:
    class Base:
        pass

    class BasedBuilder(ClassBuilder):
        @classmethod
        @functools.cache
        def into_base(cls):
            return Base

    built = BasedBuilder().dataclass(
        Field.from_arrow_schema(pyarrow.schema([("a", pyarrow.int64())]))
    )
    assert issubclass(built, Base)


def test_a_list_entry_class_is_named_after_the_list() -> None:
    """Two groups in one struct must not both be classes called `Item`."""
    schema = pyarrow.schema(
        [
            pyarrow.field(
                "no_party_ids", pyarrow.list_(pyarrow.struct([("id", pyarrow.string())]))
            ),
            pyarrow.field("no_legs", pyarrow.list_(pyarrow.struct([("symbol", pyarrow.string())]))),
        ]
    )
    built = Field.from_arrow_schema(schema).into_dataclass("Order")
    entries = [
        typing.get_args(typing.get_args(typing.get_args(annotation)[0])[0])[0]
        for annotation in built.__annotations__.values()
    ]

    assert [entry.__name__ for entry in entries] == ["NoPartyIds", "NoLegs"]
    assert (
        built.into_field()
        .into_arrow_schema()
        .equals(pyarrow.schema(list(schema), metadata={"name": "Order"}))
    )


def test_a_column_python_cannot_spell_becomes_an_attribute_that_projects_back() -> None:
    """FIX tag 236 is `Yield`, and `yield` is a statement, not an attribute."""
    schema = pyarrow.schema([("yield", pyarrow.float64()), ("symbol", pyarrow.string())])

    built = Field.from_arrow_schema(schema).into_dataclass("Bond")

    assert list(built.__annotations__) == ["yield_", "symbol"]
    assert (
        built.into_field()
        .into_arrow_schema()
        .equals(pyarrow.schema(list(schema), metadata={"name": "Bond"}))
    )
    assert built.into_field_columns() == {"yield_": "yield"}, "the class says what it renamed"
    assert [member.name for member in built.into_field().fields] == ["yield", "symbol"]
