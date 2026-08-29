"""`Field` itself, and the decorator that turns a class into one."""

import dataclasses
import datetime
import decimal
import functools
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, Field, ListField, MapField, StructField, scalar
from rekep.fields import (
    FixedSizeListField,
    LargeListField,
    LargeListViewField,
    ListViewField,
    column_name,
    column_names,
)
from rekep.fields.field import arrow_type_for


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
    size: Annotated[int, Field(dtype=pyarrow.int32(), metadata={"unit": "lots"})]
    price: decimal.Decimal
    opened: datetime.datetime
    venues: list[Venue]
    limits: dict[str, int | None]
    root: str | None = None


# -- the decorator ----------------------------------------------------------


def test_scalar_and_arrow_column_name_folds_are_exact_twins() -> None:
    values = pyarrow.chunked_array([["Msg_Type", "Straße"], [None, "Orig-Cl Ord_ID"]])
    expected = [column_name(value) if value is not None else None for value in values.to_pylist()]
    assert column_names(values).to_pylist() == expected


def test_field_makes_a_dataclass() -> None:
    assert dataclasses.is_dataclass(Venue)
    assert [f.name for f in dataclasses.fields(Venue)] == ["mic", "timeout"]


def test_double_underscore_annotations_are_not_fields() -> None:
    @scalar
    class Cached(Convertible):
        mic: str
        __cache: dict = {}

    assert [f.name for f in dataclasses.fields(Cached)] == ["mic"]
    assert Cached.into_field().into_arrow_schema().names == ["mic"]
    assert Cached(mic="XPAR").into_dict() == {"mic": "XPAR"}


def test_a_hidden_annotation_keeps_its_value_as_a_class_attribute() -> None:
    @scalar
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
    @scalar(frozen=True, order=True)
    class Frozen(Convertible):
        mic: str

    assert Frozen(mic="A") < Frozen(mic="B")
    with pytest.raises(dataclasses.FrozenInstanceError):
        Frozen(mic="A").mic = "B"


def test_field_works_bare_and_called() -> None:
    @scalar
    class Bare(Convertible):
        mic: str

    @scalar()
    class Called(Convertible):
        mic: str

    assert dataclasses.is_dataclass(Bare)
    assert dataclasses.is_dataclass(Called)


# -- the projection is built once, per class --------------------------------


def test_the_projection_is_built_once() -> None:
    """The walk over hints, docstrings and nested classes is not per call."""
    assert Venue.into_field() is Venue.into_field()
    assert "FIELD" not in Venue.__dict__
    assert isinstance(Venue.__dict__["into_field"], classmethod)


def test_a_scalar_projection_can_override_its_outer_name() -> None:
    declared = Venue.into_field()
    named = Venue.into_field("logs.venues")

    assert named is Venue.into_field("logs.venues")
    assert named == Venue.into_field(name="logs.venues")
    assert isinstance(named, StructField) and type(named) is type(declared)
    assert named.name == "logs.venues"
    assert named.dtype == declared.dtype and named.metadata == declared.metadata
    assert declared.name == "Venue", "naming a table does not mutate the cached contract"


def test_with_name_copies_a_field_and_generic_class_conversion_honours_it() -> None:
    declared = Venue.into_field()
    named = declared.with_name("market.venues")

    assert named is not declared and type(named) is type(declared)
    assert named.name == Field.from_(Venue, "market.venues").name == "market.venues"
    assert declared.name == "Venue"


def test_an_instance_sees_the_same_field() -> None:
    assert Venue(mic="XPAR").into_field() is Venue.into_field()


def test_a_subclass_gets_its_own_projection_not_its_bases() -> None:
    @scalar
    class Extended(Venue):
        desk: str = "default"

    assert Extended.into_field() is not Venue.into_field()
    assert Extended.into_field().into_arrow_schema().names == ["mic", "timeout", "desk"]
    assert Venue.into_field().into_arrow_schema().names == ["mic", "timeout"]


# -- what a field holds -----------------------------------------------------


def test_a_field_holds_a_name_a_type_and_metadata() -> None:
    member = Venue.into_field().field("mic")
    assert member.name == "mic"
    assert member.dtype == pyarrow.string()
    assert member.metadata == {
        "description": "ISO 10383 market identifier.",
        "fix:display": "MIC",
    }
    assert member.description == "ISO 10383 market identifier."


def test_metadata_is_always_a_dict_of_text() -> None:
    """Arrow would coerce on the way out; doing it here keeps two spellings equal."""
    assert Field().metadata == {}
    assert Field(metadata={"id": 1}).metadata == {"id": "1"}
    assert Field(metadata=None) == Field(metadata={})


def test_an_unstated_nullability_reads_as_not_null() -> None:
    assert Field(name="x", dtype=pyarrow.string()).nullable is None
    assert not Field(name="x", dtype=pyarrow.string()).into_arrow_field().nullable


def test_merge_lets_the_later_declaration_win() -> None:
    merged = Field(dtype=pyarrow.int64(), metadata={"unit": "lots", "a": "1"}).merge(
        Field(dtype=pyarrow.int32(), metadata={"unit": "bps"})
    )
    assert merged.dtype == pyarrow.int32()
    assert merged.metadata == {"unit": "bps", "a": "1"}


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        (pyarrow.int16(), Field(dtype=pyarrow.int16())),
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
    schema = Book.into_field().into_arrow_schema()
    assert schema.names == ["name", "size", "price", "opened", "venues", "limits", "root"]
    assert schema.metadata[b"name"] == b"Book"
    assert schema.metadata[b"namespace"] == Book.__module__.encode()


def test_a_class_can_publish_contract_metadata() -> None:
    @scalar
    class Versioned:
        @classmethod
        @functools.cache
        def into_field_metadata(cls):
            return {"version": "2", "owner": "market-data"}

        value: int

    assert Versioned.into_field().metadata["version"] == "2"
    assert Versioned.into_field().into_arrow_schema().metadata[b"owner"] == b"market-data"


def test_a_scalar_field_is_a_one_column_schema() -> None:
    schema = Field(name="x", dtype=pyarrow.int64()).into_arrow_schema()
    assert schema.names == ["x"]


def test_a_schema_round_trips_back_into_the_same_field() -> None:
    assert Field.from_arrow_schema(Book.into_field().into_arrow_schema()) == Book.into_field()


def test_an_arrow_field_round_trips() -> None:
    original = Book.into_field().field("size").into_arrow_field()
    assert Field.from_arrow_field(original).into_arrow_field().equals(original)


def test_from_arrow_type_names_a_bare_type() -> None:
    built = Field.from_arrow_type(pyarrow.int32(), "size")
    assert (built.name, built.dtype, built.nullable) == ("size", pyarrow.int32(), False)


@pytest.mark.parametrize(
    "source",
    [
        Book.into_field(),
        Book.into_field().into_arrow_schema(),
        Book.into_field().into_arrow_field(),
        Book,
        Book.into_field().into_dict(),
    ],
)
def test_from_takes_every_spelling_of_one_shape(source: object) -> None:
    """One reading of "a shape" for every call site that takes one."""
    assert Field.from_(source) == Book.into_field()


def test_from_reads_a_plain_dataclass_and_a_bare_type() -> None:
    @dataclasses.dataclass
    class Plain:
        mic: str

    assert Field.from_(Plain).names == ["mic"]
    assert Field.from_(pyarrow.int32(), "size").dtype == pyarrow.int32()


def test_from_refuses_what_names_no_shape() -> None:
    with pytest.raises(TypeError, match="does not name a shape"):
        Field.from_class(object())


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
    assert Book.into_field().into_(pyarrow.Schema).equals(Book.into_field().into_arrow_schema())
    assert Book.into_field().into_(pyarrow.DataType) == Book.into_field().dtype


# -- describing a field -----------------------------------------------------


def test_a_dump_nests_rather_than_flattening() -> None:
    dumped = Book.into_field().into_dict()
    assert dumped["name"] == "Book"
    assert dumped["type"] == "struct"
    assert dumped["description"] == "A book of orders."
    assert dumped["metadata"] == {"namespace": Book.__module__}

    by_name = {member["name"]: member for member in dumped["fields"]}
    assert by_name["size"] == {
        "name": "size",
        "type": "int32",
        "metadata": {"unit": "lots"},
        "fix": {"display": "Size"},
    }
    assert by_name["root"]["nullable"] is True
    assert by_name["venues"]["item"]["type"] == "struct", "a list shows its item"
    assert by_name["limits"]["key"]["type"] == "string", "a map shows both halves"
    assert by_name["limits"]["value"]["nullable"] is True


def test_a_dump_promotes_protocol_metadata_to_named_maps() -> None:
    original = Field(
        name="side",
        dtype=pyarrow.int32(),
        nullable=False,
        metadata={
            "fix:tag": "54",
            "fix:type": "char",
            "enum:name": "Side",
            "enum:values": '{"1":"BUY","2":"SELL"}',
            "vendor:wire": "x",
            "unit": "code",
        },
    )

    dumped = original.into_dict()

    assert dumped["fix"] == {"tag": "54", "type": "char"}
    assert dumped["enum"] == {"name": "Side", "values": '{"1":"BUY","2":"SELL"}'}
    assert dumped["vendor"] == {"wire": "x"}, "custom protocols use the same document shape"
    assert dumped["metadata"] == {"unit": "code"}
    assert Field.from_dict(dumped) == original


def test_a_protocol_map_and_legacy_metadata_cannot_disagree() -> None:
    with pytest.raises(ValueError, match="fix:tag"):
        Field.from_dict(
            {
                "name": "side",
                "type": "int32",
                "metadata": {"fix:tag": "54"},
                "fix": {"tag": "55"},
            }
        )


def test_yaml_keeps_scalar_metadata_and_protocol_maps_inline() -> None:
    original = Field(
        name="side",
        dtype=pyarrow.int32(),
        nullable=False,
        metadata={"unit": "code", "iceberg:primary_key": "true", "enum:name": "Side"},
    )

    rendered = original.into_yaml().decode().splitlines()

    assert "metadata: {unit: code}" in rendered
    assert "iceberg: {primary_key: 'true'}" in rendered
    assert "enum: {name: Side}" in rendered
    assert Field.from_yaml(original.into_yaml()) == original


def test_a_dump_round_trips_through_plain_containers() -> None:
    assert Field.from_dict(Book.into_field().into_dict()) == Book.into_field()


@pytest.mark.parametrize(
    "dtype",
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
        # Every list flavour, because a dump that named them all "list"
        # narrowed the offsets of one and dropped the width of another --
        # and both of those still cast, so nothing downstream raised.
        pyarrow.large_list(pyarrow.int32()),
        pyarrow.list_view(pyarrow.int32()),
        pyarrow.large_list_view(pyarrow.int32()),
        pyarrow.list_(pyarrow.int32(), 3),
        pyarrow.large_list(pyarrow.struct([("bid", pyarrow.float64())])),
        pyarrow.map_(pyarrow.string(), pyarrow.list_(pyarrow.int64())),
    ],
)
def test_every_type_survives_a_dump(dtype: pyarrow.DataType) -> None:
    """The spellings Arrow has no alias for are rebuilt, not lost."""
    original = Field(name="value", dtype=dtype, nullable=False)
    assert Field.from_dict(original.into_dict()).dtype == dtype


@pytest.mark.parametrize(
    ("dtype", "named"),
    [
        (pyarrow.list_(pyarrow.int32()), "list"),
        (pyarrow.large_list(pyarrow.int32()), "large_list"),
        (pyarrow.list_view(pyarrow.int32()), "list_view"),
        (pyarrow.large_list_view(pyarrow.int32()), "large_list_view"),
        (pyarrow.list_(pyarrow.int32(), 3), "fixed_size_list"),
    ],
)
def test_a_dump_names_the_list_flavour(dtype: pyarrow.DataType, named: str) -> None:
    """A contract file says which flavour it is, so a reader rebuilds that one."""
    assert Field(name="value", dtype=dtype).into_dict()["type"] == named


def test_a_map_dumps_whether_its_keys_are_sorted() -> None:
    """Arrow compares two maps that disagree on it as different types."""
    sorted_keys = pyarrow.map_(pyarrow.string(), pyarrow.int32(), keys_sorted=True)
    dumped = Field(name="value", dtype=sorted_keys).into_dict()
    assert dumped["keys_sorted"] is True
    assert Field.from_dict(dumped).dtype == sorted_keys
    plain = Field(name="value", dtype=pyarrow.map_(pyarrow.string(), pyarrow.int32()))
    assert "keys_sorted" not in plain.into_dict()


def test_a_fixed_width_binary_survives_the_spelling_arrow_prints() -> None:
    """`fixed_size_binary[16]` is what `str(type)` writes and what Arrow cannot read back."""
    assert arrow_type_for("fixed_size_binary[16]") == pyarrow.binary(16)
    original = Field(name="uuid", dtype=pyarrow.binary(16))
    assert Field.from_dict(original.into_dict()).dtype == pyarrow.binary(16)


@pytest.mark.parametrize("size", [-1, 2.7, True, "3", None])
def test_a_fixed_size_list_width_is_checked_not_coerced(size: object) -> None:
    """`int(size)` took all of these, and a negative width built a plain list."""
    dumped = {"name": "value", "type": "fixed_size_list", "item": {"type": "int32"}}
    with pytest.raises(ValueError, match="list_size"):
        Field.from_dict({**dumped, "list_size": size} if size is not None else dumped)


@pytest.mark.parametrize(
    ("written", "sorted_keys"), [(True, True), (False, False), ("true", True), ("false", False)]
)
def test_keys_sorted_is_read_strictly(written: object, sorted_keys: bool) -> None:
    """`bool("false")` is True, and `keys_sorted` is part of the map's type."""
    dumped = {
        "name": "value",
        "type": "map",
        "keys_sorted": written,
        "key": {"type": "string"},
        "value": {"type": "int32"},
    }
    assert Field.from_dict(dumped).dtype.keys_sorted is sorted_keys


def test_a_flag_that_is_neither_true_nor_false_is_refused() -> None:
    with pytest.raises(ValueError, match="keys_sorted"):
        Field.from_dict(
            {
                "name": "value",
                "type": "map",
                "keys_sorted": "no",
                "key": {"type": "string"},
                "value": {"type": "int32"},
            }
        )


def test_a_fixed_width_binary_is_refused_where_it_is_misspelled() -> None:
    """`fixed_size_binary[16)` is not a spelling Arrow ever writes."""
    assert arrow_type_for("fixed_size_binary(16)") == pyarrow.binary(16)
    with pytest.raises(ValueError, match="No type alias"):
        arrow_type_for("fixed_size_binary[16)")


def test_a_field_with_no_type_is_refused_by_name() -> None:
    """A hand-written contract that forgot one gets told which field it was."""
    with pytest.raises(ValueError, match="'venue' has no type"):
        Field.from_dict({"name": "venue"})


def test_a_fixed_size_list_dumps_the_width_it_needs_back() -> None:
    dumped = Field(name="value", dtype=pyarrow.list_(pyarrow.int32(), 3)).into_dict()
    assert dumped["list_size"] == 3
    del dumped["list_size"]
    with pytest.raises(ValueError, match="list_size"):
        Field.from_dict(dumped)


def test_a_field_serialises_itself(tmp_path) -> None:
    """A `Field` is a `Convertible` dataclass, so the declaration is a document."""
    path = tmp_path / "book.json"
    Book.into_field().into_json(path)
    assert Field.from_json(path) == Book.into_field()
    assert Field.from_yaml(Book.into_field().into_yaml()) == Book.into_field()


# -- the type picks the class -----------------------------------------------


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (None, Field),
        (pyarrow.int64(), Field),
        (pyarrow.struct([("a", pyarrow.int64())]), StructField),
        (pyarrow.list_(pyarrow.int64()), ListField),
        (pyarrow.large_list(pyarrow.int64()), LargeListField),
        (pyarrow.list_view(pyarrow.int64()), ListViewField),
        (pyarrow.large_list_view(pyarrow.int64()), LargeListViewField),
        (pyarrow.list_(pyarrow.int64(), 2), FixedSizeListField),
        (pyarrow.map_(pyarrow.string(), pyarrow.int64()), MapField),
    ],
)
def test_the_arrow_type_picks_the_subclass(dtype: object, expected: type) -> None:
    assert type(Field(name="x", dtype=dtype)) is expected


def test_every_way_of_building_one_lands_on_the_same_class() -> None:
    struct = pyarrow.struct([("a", pyarrow.int64())])
    assert isinstance(Field.from_arrow_type(struct, "x"), StructField)
    assert isinstance(Field.from_arrow_field(pyarrow.field("x", struct)), StructField)
    assert isinstance(
        Field.from_arrow_schema(pyarrow.schema([("a", pyarrow.int64())])), StructField
    )
    assert isinstance(Field.from_dict(Book.into_field().into_dict()), StructField)
    assert isinstance(Field.of(pyarrow.list_(pyarrow.int64())), ListField)
    assert isinstance(Book.into_field().field("venues"), ListField)
    assert isinstance(Book.into_field().field("limits"), MapField)


def test_asking_for_a_subclass_is_honoured() -> None:
    """The redirect is for `Field(...)`; a subclass named outright is built."""
    assert type(StructField(name="x", dtype=pyarrow.struct([]))) is StructField


def test_a_container_reaches_what_is_inside_it() -> None:
    assert Book.into_field().field("venues").item.field("mic").dtype == pyarrow.string()
    limits = Book.into_field().field("limits")
    assert limits.key.dtype == pyarrow.string()
    assert limits.value.dtype == pyarrow.int64()
    assert limits.value.nullable, "`int | None` values stay nullable through the map"


def test_a_leaf_has_no_fields() -> None:
    assert Field(name="x", dtype=pyarrow.int64()).fields == ()


# -- keys and partitions ----------------------------------------------------


def test_a_key_is_declared_and_read_back() -> None:
    @scalar
    class Quote(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        day: Annotated[datetime.date, Field.partition_key("day")]
        size: int

    assert Quote.into_field().primary_keys() == ["symbol"]
    assert Quote.into_field().partition_keys() == {"day": "day"}
    assert Quote.into_field().field("symbol").is_primary_key
    assert Quote.into_field().field("day").partition_transform == "day"
    assert not Quote.into_field().field("size").is_primary_key


def test_an_identity_partition_says_so() -> None:
    @scalar
    class Quote(Convertible):
        day: Annotated[datetime.date, Field.partition_key()]

    assert Quote.into_field().partition_keys() == {"day": "identity"}


def test_a_nullable_key_is_refused_at_declaration() -> None:
    with pytest.raises(TypeError, match="primary key and cannot be nullable"):

        @scalar
        class Loose(Convertible):
            symbol: Annotated[str | None, Field.primary_key()] = None

        Loose.into_field().into_arrow_schema()


def test_a_nullable_key_is_refused_by_the_setter() -> None:
    built = Field(name="symbol", dtype=pyarrow.string(), nullable=True)
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
    built = Field(name="day", dtype=pyarrow.date32())
    built.is_partition_key = "day"
    assert built.is_partition_key and built.partition_transform == "day"
    built.is_partition_key = False
    assert not built.is_partition_key and built.partition_transform == ""


def test_a_sort_order_is_declared_in_declaration_order() -> None:
    """The struct already has an order, so the sort order does not need a position."""

    @scalar
    class Quote(Convertible):
        day: Annotated[datetime.date, Field.sort_key()]
        symbol: str
        size: Annotated[int, Field.sort_key("desc")]

    assert Quote.into_field().sort_keys() == {"day": "asc", "size": "desc"}
    assert Quote.into_field().field("day").is_sort_key
    assert not Quote.into_field().field("symbol").is_sort_key
    assert Quote.into_field().field("symbol").sort_direction == ""


def test_a_column_can_be_partitioned_on_and_sorted_on_at_once() -> None:
    """They answer different questions -- which file, and where inside it."""

    @scalar
    class Quote(Convertible):
        day: Annotated[datetime.date, Field.partition_key("day"), Field.sort_key()]

    declared = Quote.into_field().field("day")
    assert declared.partition_transform == "day" and declared.sort_direction == "asc"


def test_a_sort_key_can_be_taken_back_off() -> None:
    built = Field(name="unix", dtype=pyarrow.int64())
    built.is_sort_key = "desc"
    assert built.is_sort_key and built.sort_direction == "desc"
    built.is_sort_key = False
    assert not built.is_sort_key and built.sort_direction == ""


def test_a_sort_key_survives_the_round_trip_through_arrow() -> None:
    """It is metadata on the column, so it travels wherever the schema does."""

    @scalar
    class Quote(Convertible):
        unix: Annotated[int, Field.sort_key()]

    schema = Quote.into_field().into_arrow_schema()
    assert schema.field("unix").metadata[b"iceberg:sort_key"] == b"asc"
    assert Field.from_arrow_schema(schema).sort_keys() == {"unix": "asc"}


def test_an_exact_external_sort_priority_survives_the_arrow_round_trip() -> None:
    built = Field.from_arrow_schema(
        pyarrow.schema(
            [
                pyarrow.field("at", pyarrow.int64(), metadata={b"iceberg:sort_key": b"asc"}),
                pyarrow.field("seq", pyarrow.int64(), metadata={b"iceberg:sort_key": b"desc"}),
            ],
            metadata={b"iceberg:sort_order": b'[["seq","desc"],["at","asc"]]'},
        ),
        "Ticked",
    )

    assert built.sort_keys() == {"seq": "desc", "at": "asc"}
    assert Field.from_arrow_schema(built.into_arrow_schema()).sort_keys() == {
        "seq": "desc",
        "at": "asc",
    }

    built.field("seq").description = "Sequence."
    assert built.sort_keys() == {"seq": "desc", "at": "asc"}
    built.field("seq").is_sort_key = "asc"
    assert built.sort_keys() == {"at": "asc", "seq": "asc"}


def test_a_changed_declaration_drops_what_was_derived_from_it() -> None:
    built = Field.from_arrow_schema(pyarrow.schema([("symbol", pyarrow.string())]), "Quote")
    before = built.into_arrow_schema()
    built.field("symbol").dtype = pyarrow.large_string()
    assert built.into_arrow_schema() is not before
    assert built.into_arrow_schema().field("symbol").type == pyarrow.large_string()


def test_every_list_flavour_keeps_its_own_shape() -> None:
    """A member set on a large list must not come back a plain one."""
    for dtype in (
        pyarrow.list_(pyarrow.int64()),
        pyarrow.large_list(pyarrow.int64()),
        pyarrow.list_view(pyarrow.int64()),
        pyarrow.large_list_view(pyarrow.int64()),
        pyarrow.list_(pyarrow.int64(), 3),
    ):
        built = Field(name="values", dtype=dtype)
        built.item.description = "One value."
        assert built.dtype.id == dtype.id, str(dtype)
        assert built.item.description == "One value."


def test_a_fixed_size_list_says_how_wide_it_is() -> None:
    assert Field(name="x", dtype=pyarrow.list_(pyarrow.int64(), 3)).list_size == 3
