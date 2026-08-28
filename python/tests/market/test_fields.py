"""The three rules the market builder adds, each checked where it could be skipped."""

from __future__ import annotations

import dataclasses
import enum
import json
import uuid
from typing import Annotated, get_type_hints

import pyarrow
import pytest

from rekep.enums import MarketKind, Side, State, TimeInForce
from rekep.fields import (
    DESCRIPTION,
    PARTITION_KEY,
    PRIMARY_KEY,
    Field,
    FieldBuilder,
    scalar,
)
from rekep.market import (
    HASH,
    Book,
    Event,
    Execution,
    Instrument,
    MarketEvent,
    Order,
)
from rekep.market.fields import (
    MarketConvertible,
    MarketFieldBuilder,
    Newtype,
    describe_enum,
    dictionary_arrow,
    enum_of,
    fix_tag,
    single_member,
    unkeyed,
)

SHAPES = (Instrument, MarketEvent, Order, Book)

#: Every list flavour, because a walk that spelled them all `list` would narrow
#: a 64-bit offset and drop a width without saying so.
FLAVOURS = (
    pyarrow.list_,
    pyarrow.large_list,
    pyarrow.list_view,
    pyarrow.large_list_view,
)


def metadata_in(dtype: pyarrow.DataType) -> list[dict[bytes, bytes]]:
    """Every nested field's metadata, at any depth.

    Walked rather than read off `str(type)`: Arrow does not print field
    metadata, so a test that searched the text for `iceberg:primary_key` would
    pass whether the key was stripped or not.
    """
    found = []
    for index in range(dtype.num_fields):
        member = dtype.field(index)
        found.append(dict(member.metadata or {}))
        found += metadata_in(member.type)
    return found


def keys_in(dtype: pyarrow.DataType) -> set[bytes]:
    """Every metadata key anywhere inside `dtype`."""
    return {key for metadata in metadata_in(dtype) for key in metadata}


@scalar
class Keyed(MarketConvertible):
    """A shape whose members are keys, for nesting inside another."""

    identifier: Annotated[uuid.UUID, Field.primary_key()]
    """Its own key."""

    day: Annotated[str, Field.partition_key()]
    """Its own partition."""


def test_an_identifier_is_sixteen_fixed_bytes() -> None:
    """One width for every identity, narrow or wide: a version hash carries an
    instant over a 64-bit digest and no longer fits a `long`."""
    for shape in SHAPES:
        for name in ("hash", "xhash"):
            if name in shape.into_field().names:
                assert shape.into_field().field(name).dtype == HASH, shape.__name__


def test_a_market_code_column_is_as_wide_as_its_code_declares() -> None:
    """The base builder takes the width of a Python int, which says nothing about
    the code; the market builder takes the width the code itself packs into."""
    assert MarketFieldBuilder().arrow_type(State) == pyarrow.int64()
    assert MarketFieldBuilder().arrow_type(Side) == pyarrow.int32()
    assert FieldBuilder().arrow_type(State) == pyarrow.int64()


def test_every_market_code_column_of_every_shape_matches_its_enum() -> None:
    """The rule is worth nothing if one shape quietly bypasses the builder."""
    for shape in SHAPES:
        hints = get_type_hints(shape, include_extras=True)
        for member in shape.into_field().fields:
            declared = enum_of(hints.get(member.name))
            if declared is None:
                continue
            assert member.dtype == declared.into_arrow_type().index_type, (
                f"{shape.__name__}.{member.name}"
            )


def test_the_instrument_is_one_flat_event_contract() -> None:
    assert Instrument.into_field().primary_keys() == ["unix", "hash"]
    assert Instrument.into_field().partition_keys() == {"unix_partition": "identity"}
    for shape in (Book, Order, MarketEvent):
        assert "instrument" not in shape.into_field().names


def test_the_shape_that_owns_the_table_keeps_its_own_keys() -> None:
    """Stripping the nested ones must not strip the top level with them."""
    for shape in SHAPES:
        assert shape.into_field().primary_keys(), shape.__name__
    assert Book.into_field().primary_keys() == ["unix", "hash"]
    assert Book.into_field().partition_keys() == {"unix_partition": "identity"}
    assert Book.into_field().sort_keys() == {"unix": "asc"}


@pytest.mark.parametrize("flavour", FLAVOURS, ids=lambda build: build.__name__)
def test_a_key_inside_every_list_flavour_is_stripped_and_the_flavour_kept(flavour) -> None:
    """Crossing every branch of the walk, which is where a kind check goes wrong."""
    inside = flavour(Keyed.into_field().dtype)
    stripped = unkeyed(inside)
    assert type(stripped) is type(inside)
    assert str(stripped).split("<", 1)[0] == str(inside).split("<", 1)[0]
    assert keys_in(inside) == {PRIMARY_KEY.encode(), PARTITION_KEY.encode(), DESCRIPTION.encode()}
    assert keys_in(stripped) == {DESCRIPTION.encode()}, "the comments must survive the strip"


def test_a_fixed_size_list_keeps_the_width_that_is_part_of_its_type() -> None:
    inside = pyarrow.list_(pyarrow.field("item", Keyed.into_field().dtype), 3)
    stripped = unkeyed(inside)
    assert pyarrow.types.is_fixed_size_list(stripped) and stripped.list_size == 3
    assert keys_in(stripped) == {DESCRIPTION.encode()}


def test_a_key_inside_a_map_is_stripped_on_both_halves() -> None:
    inside = pyarrow.map_(pyarrow.string(), Keyed.into_field().dtype, keys_sorted=True)
    stripped = unkeyed(inside)
    assert pyarrow.types.is_map(stripped) and stripped.keys_sorted
    assert keys_in(stripped) == {DESCRIPTION.encode()}


def test_a_leaf_comes_back_untouched() -> None:
    assert unkeyed(pyarrow.int32()) == pyarrow.int32()


def test_a_fix_tag_lands_where_the_protocol_reads_it() -> None:
    declared = fix_tag("Price")
    assert declared.fix["name"] == "Price"
    assert declared.fix["tag"] == "44"
    assert declared.fix["type"] == "Price"
    assert json.loads(declared.fix["versions"])[0] == "5.0.SP2"
    assert declared.description


def test_a_fix_tag_merges_with_the_other_declarations_on_a_member() -> None:
    """Both `Annotated` extras have to survive, or one silently wins."""
    built = Field.from_annotation("unix", Annotated[int, Field.primary_key(), fix_tag("Symbol")])
    assert built.is_primary_key and built.fix["tag"] == "55"


def test_the_builder_is_wired_onto_every_shape() -> None:
    """A shape that forgot the hook would project UUIDs as strings and pass."""
    for shape in SHAPES:
        assert shape.into_field_builder() is MarketFieldBuilder, shape.__name__


def test_the_builder_is_not_a_dataclass_member() -> None:
    """The projection hook must not become a column, or every shape grows one."""
    for shape in SHAPES:
        assert "into_field_builder" not in {f.name for f in dataclasses.fields(shape)}
        assert "into_field_builder" not in shape.into_field().names


def test_contract_metadata_cannot_be_changed_through_the_hook() -> None:
    with pytest.raises(TypeError):
        Event.into_field_metadata()["version"] = "2"


# -- what an enum means, in the schema ---------------------------------------


def test_every_enum_column_says_what_its_codes_mean() -> None:
    """A consumer that never imports this package has to read a bare stored code.

    The metadata is what turns a raw `77280626623812` back into `FILLED`.
    """
    for shape in SHAPES:
        for member in shape.into_field().fields:
            declared = enum_of(get_type_hints(shape, include_extras=True).get(member.name))
            if declared is None:
                continue
            keys = member.protocol("enum")
            assert keys["name"] == declared.__name__, member.name
            assert json.loads(keys["values"]) == {
                str(item.value): item.name for item in declared
            }, member.name


def test_enum_key_and_value_types_are_explicit() -> None:
    """`class K(str, Enum)` and `class K(IntEnum)` both subclass something."""
    numbers = enum.Enum("Numbers", {"A": 1, "B": 2})
    words = enum.Enum("Words", {"A": "a", "B": "b"})
    mixed = enum.Enum("Mixed", {"A": 1, "B": "b"})
    for declared, expected in ((numbers, "int32"), (words, "utf8"), (mixed, "mixed")):
        built = Field(name="k")
        describe_enum(built, declared)
        metadata = built.protocol("enum")
        assert metadata["key_type"] == expected, declared.__name__
        assert metadata["value_type"] == "utf8", declared.__name__

    currency = Instrument.into_field().field("currency").protocol("enum")
    assert currency["key_type"] == "int32"
    assert currency["value_type"] == "utf8"
    assert currency["encoding"] == "ascii-big-endian"
    assert currency["byte_width"] == "4"
    assert currency["padding"] == "nul-right"
    assert currency["pattern"] == "[A-Z]{3}"
    assert "dynamic" not in currency

    for declared in (Side, TimeInForce):
        metadata = describe_enum_metadata(declared)
        assert metadata["encoding"] == "ascii-big-endian"
        assert metadata["byte_width"] == "4"
        assert metadata["padding"] == "nul-right"
    assert json.loads(describe_enum_metadata(Side)["fix_aliases"])["1"] == "BUY"
    assert json.loads(describe_enum_metadata(TimeInForce)["fix_aliases"])["3"] == "IOC"
    aliases = json.loads(describe_enum_metadata(Side)["aliases"])
    assert aliases["BID"] == "BUY" and aliases["ASK"] == "SELL"


def describe_enum_metadata(declared: type) -> dict[str, str]:
    built = Field(name="code")
    describe_enum(built, declared)
    built.protocol("enum").update(declared.schema_metadata())
    return built.protocol("enum")


def test_a_column_that_is_not_an_enum_says_nothing_about_one() -> None:
    assert "name" not in Order.into_field().field("px").protocol("enum")
    assert "name" not in Order.into_field().field("code").protocol("enum")


def test_an_enum_behind_an_optional_is_still_found() -> None:
    """`Kind | None` is where an enum most often hides."""
    assert enum_of(Annotated[State | None, fix_tag("OrdStatus")]) is State
    assert enum_of(State) is State
    assert enum_of(str) is None


def test_the_published_contract_carries_the_member_table() -> None:
    """Which is the whole point: the file is what a consumer reads, not the code."""
    keys = Order.into_field().into_dict()
    state = next(member for member in keys["fields"] if member["name"] == "state")
    assert json.loads(state["enum"]["values"])[str(int(State.FILLED))] == "FILLED"
    assert state["type"] == "int64", "as wide as the code it stores"


def test_market_kind_metadata_keeps_each_tags_original_wire_mapping() -> None:
    for shape in (Order, Execution):
        mapping = json.loads(shape.into_field().field("kind").protocol("enum")["fix_values"])
        assert mapping["40"]["J"] == int(MarketKind.MARKET_IF_TOUCHED)
        assert mapping["150"]["J"] == int(MarketKind.CLEARING_HOLD)


# -- a shape of one member is that member ------------------------------------


@scalar
class Ticker(MarketConvertible):
    """One symbol, and nothing else."""

    symbol: str = ""
    """The symbol."""


@scalar
class Pair(MarketConvertible):
    """Two members, so still a struct."""

    left: str = ""
    """One."""

    right: str = ""
    """The other."""


@scalar
class Holder(MarketConvertible):
    """Something holding one of each."""

    one: Ticker = dataclasses.field(default_factory=Ticker)
    """A shape of one member."""

    two: Pair = dataclasses.field(default_factory=Pair)
    """A shape of two."""


def test_a_nested_shape_of_one_member_is_that_member_and_not_a_struct_of_one() -> None:
    """A struct of one is a nesting level carrying nothing, and it costs a pushdown."""
    one = Holder.into_field().field("one").dtype
    assert isinstance(one, Newtype)
    assert one.storage_type == pyarrow.string()
    assert one.shape_name == "Ticker", "and the class it came from is still on it"
    assert not pyarrow.types.is_struct(one)


def test_a_nested_shape_of_two_members_is_still_a_struct() -> None:
    two = Holder.into_field().field("two").dtype
    assert pyarrow.types.is_struct(two) and two.num_fields == 2


def test_the_storage_is_what_a_store_that_never_heard_of_the_extension_sees() -> None:
    """Which is why this is safe to publish: the bytes are the storage type's."""
    column = pyarrow.array(["AAPL", "MSFT"])
    wrapped = pyarrow.ExtensionArray.from_storage(Holder.into_field().field("one").dtype, column)
    assert wrapped.storage.equals(column)
    assert wrapped.type.storage_type == pyarrow.string()


def test_a_shape_of_one_member_projected_on_its_own_is_still_a_struct() -> None:
    """The rule is about a member of something else, not about a table."""
    assert pyarrow.types.is_struct(Ticker.into_field().dtype)
    assert single_member(Ticker) == ("Ticker", str)
    assert single_member(Pair) is None
    assert single_member(str) is None


# -- dictionary encoding, which is not a mapping -----------------------------

CODES = pyarrow.array([410, 210, 410, 610], type=pyarrow.int32())
ENCODED = pyarrow.dictionary(pyarrow.int8(), pyarrow.int32())


def test_a_column_of_codes_encodes_when_it_already_holds_the_values() -> None:
    """The first question, and the cheap one."""
    built = dictionary_arrow(CODES, ENCODED)
    assert built.type == ENCODED
    assert built.to_pylist() == CODES.to_pylist()
    assert len(built.dictionary) == 3, "four rows, three distinct codes"


def test_a_column_that_already_holds_indices_is_taken_as_indices() -> None:
    """Asked second, and it has to be: an index and a value can be the same width."""
    indices = pyarrow.array([0, 1, 0, 2], type=pyarrow.int8())
    built = dictionary_arrow(indices, ENCODED)
    assert built.type == ENCODED
    assert built.indices.to_pylist() == indices.to_pylist(), "not re-encoded"
    assert built.to_pylist() == [0, 1, 0, 2]


def test_a_column_of_the_wrong_width_is_cast_and_then_encoded() -> None:
    """The third question, and the only one that costs a pass over the data."""
    wide = pyarrow.array([410, 210, 410], type=pyarrow.int64())
    built = dictionary_arrow(wide, ENCODED)
    assert built.type == ENCODED and built.to_pylist() == [410, 210, 410]


def test_an_encoded_column_decodes_back_to_exactly_what_went_in() -> None:
    assert dictionary_arrow(dictionary_arrow(CODES, ENCODED), pyarrow.int32()).equals(CODES)


def test_encoding_an_encoded_column_changes_nothing() -> None:
    once = dictionary_arrow(CODES, ENCODED)
    assert dictionary_arrow(once, ENCODED).equals(once)


def test_a_re_encoded_column_keeps_its_values_through_a_width_change() -> None:
    once = dictionary_arrow(CODES, ENCODED)
    wider = dictionary_arrow(once, pyarrow.dictionary(pyarrow.int16(), pyarrow.int64()))
    assert wider.to_pylist() == CODES.to_pylist()


def test_a_chunked_column_encodes_chunk_by_chunk() -> None:
    chunked = pyarrow.chunked_array([CODES.slice(0, 2), CODES.slice(2, 2)])
    built = dictionary_arrow(chunked, ENCODED)
    assert built.type == ENCODED and built.to_pylist() == CODES.to_pylist()


def test_no_rows_encodes_to_no_rows() -> None:
    empty = pyarrow.array([], type=pyarrow.int32())
    assert len(dictionary_arrow(empty, ENCODED)) == 0
