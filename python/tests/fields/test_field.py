"""`Field` itself, and the decorator that turns a class into one."""

import dataclasses
import datetime
import decimal
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, Field, ListField, MapField, StructField, field


@field
class Venue(Convertible):
    """A trading venue."""

    mic: str
    """ISO 10383 market identifier."""

    timeout: float | None = None


@field
class Book(Convertible):
    """A book of orders."""

    name: str
    size: Annotated[int, Field(arrow_type=pyarrow.int32(), metadata={"unit": "lots"})]
    price: decimal.Decimal
    opened: datetime.datetime
    venues: list[Venue]
    limits: dict[str, int | None]
    root: str | None = None


# -- the decorator ----------------------------------------------------------


def test_field_makes_a_dataclass() -> None:
    assert dataclasses.is_dataclass(Venue)
    assert [f.name for f in dataclasses.fields(Venue)] == ["mic", "timeout"]


def test_double_underscore_annotations_are_not_fields() -> None:
    @field
    class Cached(Convertible):
        mic: str
        __cache: dict = {}

    assert [f.name for f in dataclasses.fields(Cached)] == ["mic"]
    assert Cached.FIELD.into_arrow_schema().names == ["mic"]
    assert Cached(mic="XPAR").into_dict() == {"mic": "XPAR"}


def test_a_hidden_annotation_keeps_its_value_as_a_class_attribute() -> None:
    @field
    class Cached(Convertible):
        mic: str
        __cache: dict = {}

    assert Cached._Cached__cache == {}


def test_a_mutable_default_would_have_broken_a_plain_dataclass() -> None:
    """`__`-hiding happens before dataclass runs, so `{}` never reaches it."""
    with pytest.raises(ValueError, match="mutable default"):

        @dataclasses.dataclass
        class Plain:
            cache: dict = {}


def test_field_forwards_dataclass_keywords() -> None:
    @field(frozen=True, order=True)
    class Frozen(Convertible):
        mic: str

    assert Frozen(mic="A") < Frozen(mic="B")
    with pytest.raises(dataclasses.FrozenInstanceError):
        Frozen(mic="A").mic = "B"


def test_field_works_bare_and_called() -> None:
    @field
    class Bare(Convertible):
        mic: str

    @field()
    class Called(Convertible):
        mic: str

    assert dataclasses.is_dataclass(Bare)
    assert dataclasses.is_dataclass(Called)


# -- the projection is built once, per class --------------------------------


def test_the_projection_is_built_once() -> None:
    """The walk over hints, docstrings and nested classes is not per call."""
    assert Venue.FIELD is Venue.FIELD
    assert isinstance(Venue.__dict__["FIELD"], Field), "the descriptor stepped aside"


def test_an_instance_sees_the_same_field() -> None:
    assert Venue(mic="XPAR").FIELD is Venue.FIELD


def test_a_subclass_gets_its_own_projection_not_its_bases() -> None:
    @field
    class Extended(Venue):
        desk: str = "default"

    assert Extended.FIELD is not Venue.FIELD
    assert Extended.FIELD.into_arrow_schema().names == ["mic", "timeout", "desk"]
    assert Venue.FIELD.into_arrow_schema().names == ["mic", "timeout"]


# -- what a field holds -----------------------------------------------------


def test_a_field_holds_a_name_a_type_and_metadata() -> None:
    member = Venue.FIELD.field("mic")
    assert member.name == "mic"
    assert member.arrow_type == pyarrow.string()
    assert member.metadata == {"description": "ISO 10383 market identifier."}
    assert member.description == "ISO 10383 market identifier."


def test_metadata_is_always_a_dict_of_text() -> None:
    """Arrow would coerce on the way out; doing it here keeps two spellings equal."""
    assert Field().metadata == {}
    assert Field(metadata={"id": 1}).metadata == {"id": "1"}
    assert Field(metadata=None) == Field(metadata={})


def test_an_unstated_nullability_reads_as_not_null() -> None:
    assert Field(name="x", arrow_type=pyarrow.string()).nullable is None
    assert not Field(name="x", arrow_type=pyarrow.string()).into_arrow_field().nullable


def test_merge_lets_the_later_declaration_win() -> None:
    merged = Field(arrow_type=pyarrow.int64(), metadata={"unit": "lots", "a": "1"}).merge(
        Field(arrow_type=pyarrow.int32(), metadata={"unit": "bps"})
    )
    assert merged.arrow_type == pyarrow.int32()
    assert merged.metadata == {"unit": "bps", "a": "1"}


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        (pyarrow.int16(), Field(arrow_type=pyarrow.int16())),
        ({"unit": "bps"}, Field(metadata={"unit": "bps"})),
        ("The day.", Field(metadata={"description": "The day."})),
        (object(), Field()),
    ],
)
def test_of_reads_one_annotated_argument(extra: object, expected: Field) -> None:
    assert Field.of(extra) == expected


def test_a_field_without_a_type_cannot_convert() -> None:
    with pytest.raises(TypeError, match="no Arrow type"):
        Field(name="x").into_arrow_field()
    with pytest.raises(TypeError, match="no Arrow type"):
        Field(name="x").into_arrow_type()


# -- arrow, both ways -------------------------------------------------------


def test_a_class_projects_to_a_schema_of_its_members() -> None:
    schema = Book.FIELD.into_arrow_schema()
    assert schema.names == ["name", "size", "price", "opened", "venues", "limits", "root"]
    assert schema.metadata[b"name"] == b"Book"
    assert schema.metadata[b"namespace"] == Book.__module__.encode()


def test_a_scalar_field_is_a_one_column_schema() -> None:
    schema = Field(name="x", arrow_type=pyarrow.int64()).into_arrow_schema()
    assert schema.names == ["x"]


def test_a_schema_round_trips_back_into_the_same_field() -> None:
    assert Field.from_arrow_schema(Book.FIELD.into_arrow_schema()) == Book.FIELD


def test_an_arrow_field_round_trips() -> None:
    original = Book.FIELD.field("size").into_arrow_field()
    assert Field.from_arrow_field(original).into_arrow_field().equals(original)


def test_from_arrow_type_names_a_bare_type() -> None:
    built = Field.from_arrow_type(pyarrow.int32(), "size")
    assert (built.name, built.arrow_type, built.nullable) == ("size", pyarrow.int32(), False)


@pytest.mark.parametrize(
    ("requested", "stem"),
    [
        (pyarrow.Schema, "arrow_schema"),
        (pyarrow.Field, "arrow_field"),
        (pyarrow.DataType, "arrow_type"),
        (pyarrow.StructType, "arrow_type"),
        (dict, "dict"),
    ],
)
def test_into_redirects_on_the_requested_arrow_type(requested: type, stem: str) -> None:
    assert Field.redirect_of(requested) == stem


def test_into_the_requested_type() -> None:
    assert Book.FIELD.into_(pyarrow.Schema).equals(Book.FIELD.into_arrow_schema())
    assert Book.FIELD.into_(pyarrow.DataType) == Book.FIELD.arrow_type


# -- describing a field -----------------------------------------------------


def test_a_dump_nests_rather_than_flattening() -> None:
    dumped = Book.FIELD.into_dict()
    assert dumped["name"] == "Book"
    assert dumped["type"] == "struct"
    assert dumped["description"] == "A book of orders."
    assert dumped["metadata"] == {"namespace": Book.__module__}

    by_name = {member["name"]: member for member in dumped["fields"]}
    assert by_name["size"] == {"name": "size", "type": "int32", "metadata": {"unit": "lots"}}
    assert by_name["root"]["nullable"] is True
    assert by_name["venues"]["item"]["type"] == "struct", "a list shows its item"
    assert by_name["limits"]["key"]["type"] == "string", "a map shows both halves"
    assert by_name["limits"]["value"]["nullable"] is True


def test_a_dump_round_trips_through_plain_containers() -> None:
    assert Field.from_dict(Book.FIELD.into_dict()) == Book.FIELD


@pytest.mark.parametrize(
    "arrow_type",
    [
        pyarrow.decimal128(38, 9),
        pyarrow.decimal256(50, 3),
        pyarrow.timestamp("us", tz="Europe/Paris"),
        pyarrow.timestamp("ns"),
        pyarrow.duration("us"),
        pyarrow.date32(),
        pyarrow.time64("us"),
        pyarrow.large_binary(),
        pyarrow.list_(pyarrow.field("item", pyarrow.string(), nullable=False)),
    ],
)
def test_every_type_survives_a_dump(arrow_type: pyarrow.DataType) -> None:
    """The spellings Arrow has no alias for are rebuilt, not lost."""
    original = Field(name="value", arrow_type=arrow_type, nullable=False)
    assert Field.from_dict(original.into_dict()).arrow_type == arrow_type


def test_a_field_serialises_itself(tmp_path) -> None:
    """A `Field` is a `Convertible` dataclass, so the declaration is a document."""
    path = tmp_path / "book.json"
    Book.FIELD.into_json(path)
    assert Field.from_json(path) == Book.FIELD
    assert Field.from_yaml(Book.FIELD.into_yaml()) == Book.FIELD


# -- the type picks the class -----------------------------------------------


@pytest.mark.parametrize(
    ("arrow_type", "expected"),
    [
        (None, Field),
        (pyarrow.int64(), Field),
        (pyarrow.struct([("a", pyarrow.int64())]), StructField),
        (pyarrow.list_(pyarrow.int64()), ListField),
        (pyarrow.large_list(pyarrow.int64()), ListField),
        (pyarrow.map_(pyarrow.string(), pyarrow.int64()), MapField),
    ],
)
def test_the_arrow_type_picks_the_subclass(arrow_type: object, expected: type) -> None:
    assert type(Field(name="x", arrow_type=arrow_type)) is expected


def test_every_way_of_building_one_lands_on_the_same_class() -> None:
    struct = pyarrow.struct([("a", pyarrow.int64())])
    assert isinstance(Field.from_arrow_type(struct, "x"), StructField)
    assert isinstance(Field.from_arrow_field(pyarrow.field("x", struct)), StructField)
    assert isinstance(
        Field.from_arrow_schema(pyarrow.schema([("a", pyarrow.int64())])), StructField
    )
    assert isinstance(Field.from_dict(Book.FIELD.into_dict()), StructField)
    assert isinstance(Field.of(pyarrow.list_(pyarrow.int64())), ListField)
    assert isinstance(Book.FIELD.field("venues"), ListField)
    assert isinstance(Book.FIELD.field("limits"), MapField)


def test_asking_for_a_subclass_is_honoured() -> None:
    """The redirect is for `Field(...)`; a subclass named outright is built."""
    assert type(StructField(name="x", arrow_type=pyarrow.struct([]))) is StructField


def test_a_container_reaches_what_is_inside_it() -> None:
    assert Book.FIELD.field("venues").item.field("mic").arrow_type == pyarrow.string()
    limits = Book.FIELD.field("limits")
    assert limits.key.arrow_type == pyarrow.string()
    assert limits.value.arrow_type == pyarrow.int64()
    assert limits.value.nullable, "`int | None` values stay nullable through the map"


def test_a_leaf_has_no_fields() -> None:
    assert Field(name="x", arrow_type=pyarrow.int64()).fields == ()


# -- keys and partitions ----------------------------------------------------


def test_a_key_is_declared_and_read_back() -> None:
    @field
    class Quote(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        day: Annotated[datetime.date, Field.partition_key("day")]
        size: int

    assert Quote.FIELD.primary_keys() == ["symbol"]
    assert Quote.FIELD.partition_keys() == {"day": "day"}
    assert Quote.FIELD.field("symbol").is_primary_key
    assert Quote.FIELD.field("day").partition_transform == "day"
    assert not Quote.FIELD.field("size").is_primary_key


def test_an_identity_partition_says_so() -> None:
    @field
    class Quote(Convertible):
        day: Annotated[datetime.date, Field.partition_key()]

    assert Quote.FIELD.partition_keys() == {"day": "identity"}


def test_a_nullable_key_is_refused_at_declaration() -> None:
    with pytest.raises(TypeError, match="primary key and cannot be nullable"):

        @field
        class Loose(Convertible):
            symbol: Annotated[str | None, Field.primary_key()] = None

        Loose.FIELD.into_arrow_schema()


def test_a_nullable_key_is_refused_by_the_setter() -> None:
    built = Field(name="symbol", arrow_type=pyarrow.string(), nullable=True)
    with pytest.raises(TypeError, match="primary key and cannot be nullable"):
        built.is_primary_key = True


def test_setting_a_key_reaches_the_struct_it_came_from() -> None:
    """A member is a view of its container, not a copy of it."""
    required = pyarrow.field("symbol", pyarrow.string(), nullable=False)
    built = Field.from_arrow_schema(pyarrow.schema([required]), "Quote")
    built.field("symbol").is_primary_key = True
    assert built.primary_keys() == ["symbol"]
    assert built.into_arrow_schema().field("symbol").metadata[b"iceberg:primary_key"] == b"true"


def test_setting_something_deep_reaches_the_root() -> None:
    built = Field.from_arrow_schema(
        pyarrow.schema([("venue", pyarrow.struct([("mic", pyarrow.string())]))]), "Quote"
    )
    built.field("venue").field("mic").description = "Where it lists."
    nested = built.into_arrow_schema().field("venue").type.field(0)
    assert nested.metadata[b"description"] == b"Where it lists."


def test_a_partition_can_be_taken_back_off() -> None:
    built = Field(name="day", arrow_type=pyarrow.date32())
    built.is_partition_key = "day"
    assert built.is_partition_key and built.partition_transform == "day"
    built.is_partition_key = False
    assert not built.is_partition_key and built.partition_transform == ""


def test_a_changed_declaration_drops_what_was_derived_from_it() -> None:
    built = Field.from_arrow_schema(pyarrow.schema([("symbol", pyarrow.string())]), "Quote")
    before = built.into_arrow_schema()
    built.field("symbol").arrow_type = pyarrow.large_string()
    assert built.into_arrow_schema() is not before
    assert built.into_arrow_schema().field("symbol").type == pyarrow.large_string()
