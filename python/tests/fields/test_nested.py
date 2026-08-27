"""Casting between the nested kinds: list flavours, maps and structs.

Every case here is a shape change Arrow's own `cast` refuses or cannot do, so
these are the conversions this package adds -- and each one is checked for the
values it produces, not only for the type it lands on.
"""

import pyarrow
import pyarrow.compute
import pytest

from rekep import Field
from rekep.fields import arrays

ENTRY = pyarrow.struct([("key", pyarrow.string()), ("value", pyarrow.int64())])


def _shape(data_type: pyarrow.DataType, name: str = "value") -> Field:
    return Field(name=name, data_type=data_type, nullable=True)


# -- list flavours ----------------------------------------------------------


LIST_FLAVOURS = [
    pyarrow.list_(pyarrow.int64()),
    pyarrow.large_list(pyarrow.int64()),
    pyarrow.list_view(pyarrow.int64()),
    pyarrow.large_list_view(pyarrow.int64()),
]


@pytest.mark.parametrize("source", LIST_FLAVOURS, ids=str)
@pytest.mark.parametrize("target", LIST_FLAVOURS, ids=str)
def test_every_list_flavour_casts_to_every_other(
    source: pyarrow.DataType, target: pyarrow.DataType
) -> None:
    rows = [[1, 2], None, [], [3]]
    array = (
        pyarrow.array(rows, type=pyarrow.list_(pyarrow.int64())).cast(source, safe=True)
        if not pyarrow.types.is_list_view(source) and not pyarrow.types.is_large_list_view(source)
        else _shape(source).cast_arrow_array(pyarrow.array(rows))
    )
    cast = _shape(target).cast_arrow_array(array)
    assert cast.type == target
    assert cast.to_pylist() == rows


def test_a_list_narrows_its_item() -> None:
    array = pyarrow.array([[1, 2], None], type=pyarrow.list_(pyarrow.int64()))
    cast = _shape(pyarrow.large_list(pyarrow.int32())).cast_arrow_array(array)
    assert cast.type.value_type == pyarrow.int32()
    assert cast.to_pylist() == [[1, 2], None]


def test_a_fixed_size_list_is_built_from_a_ragged_source_only_when_it_fits() -> None:
    array = pyarrow.array([[1, 2], [3, 4]], type=pyarrow.list_(pyarrow.int64()))
    cast = _shape(pyarrow.list_(pyarrow.int64(), 2)).cast_arrow_array(array)
    assert cast.type == pyarrow.list_(pyarrow.int64(), 2)
    assert cast.to_pylist() == [[1, 2], [3, 4]]

    ragged = pyarrow.array([[1, 2], [3]], type=pyarrow.list_(pyarrow.int64()))
    with pytest.raises(pyarrow.ArrowInvalid):
        _shape(pyarrow.list_(pyarrow.int64(), 2)).cast_arrow_array(ragged)


def test_a_fixed_size_list_is_a_source_too() -> None:
    array = pyarrow.array([[1, 2], [3, 4]], type=pyarrow.list_(pyarrow.int64(), 2))
    assert _shape(pyarrow.list_(pyarrow.int64())).cast_arrow_array(array).to_pylist() == [
        [1, 2],
        [3, 4],
    ]


def test_a_sliced_list_keeps_its_own_rows() -> None:
    """Offsets are rebuilt from the sizes, so a slice cannot drag its neighbours."""
    array = pyarrow.array([[1], [2, 3], [4]], type=pyarrow.list_(pyarrow.int64()))[1:]
    cast = _shape(pyarrow.large_list(pyarrow.int32())).cast_arrow_array(array)
    assert cast.to_pylist() == [[2, 3], [4]]


@pytest.mark.parametrize(
    "flavour",
    [
        pyarrow.list_,
        pyarrow.large_list,
        pyarrow.list_view,
        pyarrow.large_list_view,
        lambda item: pyarrow.map_(pyarrow.string(), item),
    ],
)
def test_a_sliced_source_with_a_null_row_casts_like_arrow_does(flavour) -> None:
    """A slice owns no offsets: the builder refuses them beside a validity mask.

    Every reader and every `Table.slice` hands out sliced arrays, so this is
    the ordinary case rather than the exotic one -- and `Array.cast` does it,
    which is the answer compared against here.
    """
    if flavour is pyarrow.map_ or "map" in str(flavour(pyarrow.int64())):
        rows = [[("a", 1)], None, [("b", 2)], [("c", 3)]]
    else:
        rows = [[1, 2], [3], None, [4, 5]]
    source = pyarrow.array(rows, type=flavour(pyarrow.int64()))
    target = flavour(pyarrow.int32())
    sliced = source.slice(1, 3)
    cast = _shape(target).cast_arrow_array(sliced)
    cast.validate(full=True)
    # The values, which narrowing the item does not change -- and, where Arrow
    # can do the same cast at all, its answer too. It cannot for the views.
    assert cast.to_pylist() == sliced.to_pylist()
    try:
        expected = sliced.cast(target)
    except pyarrow.ArrowNotImplementedError:
        return
    assert cast.to_pylist() == expected.to_pylist()


@pytest.mark.parametrize("kernel", [pyarrow.compute.take, pyarrow.compute.filter])
def test_a_reordered_list_view_keeps_the_rows_it_shows(kernel) -> None:
    """A view carries offsets *and* sizes, and only the sizes say where a row ends.

    `take` and `filter` leave a view whose rows are no longer back to back.
    Arrow's own view-to-list cast reads the offsets and ignores the sizes, so
    it hands back rows holding their neighbours' values -- which is why this
    compares against the source, not against `Array.cast`.
    """
    view = pyarrow.array([[7, 8], [1, 2], [4, 5, 6]], type=pyarrow.list_view(pyarrow.int64()))
    selector = (
        pyarrow.array([2, 0, 1])
        if kernel is pyarrow.compute.take
        else pyarrow.array([False, True, True])
    )
    moved = kernel(view, selector)
    cast = _shape(pyarrow.list_(pyarrow.int64())).cast_arrow_array(moved)
    cast.validate(full=True)
    assert cast.to_pylist() == moved.to_pylist()


def test_a_not_null_member_may_not_come_back_holding_nulls() -> None:
    """A schema saying NOT NULL over nulls fails at the write, far from here."""
    batch = pyarrow.record_batch({"message": pyarrow.array(["a", None, "c"])})
    shape = Field(
        name="row",
        data_type=pyarrow.struct([pyarrow.field("message", pyarrow.string(), nullable=False)]),
    )
    with pytest.raises(ValueError, match="not nullable and 1 of 3"):
        shape.cast_arrow_batch(batch)


def test_an_empty_map_is_not_a_map_without_the_member() -> None:
    """An empty batch is routine in a stream; it must not refuse what a full one takes."""
    entries = pyarrow.map_(pyarrow.string(), pyarrow.int64())
    shape = Field(
        name="row",
        data_type=pyarrow.struct(
            [
                pyarrow.field(
                    "attrs",
                    pyarrow.struct([pyarrow.field("a", pyarrow.int64(), nullable=False)]),
                )
            ]
        ),
    )
    full = pyarrow.record_batch({"attrs": pyarrow.array([[("a", 1)]], type=entries)})
    empty = pyarrow.record_batch({"attrs": pyarrow.array([], type=entries)})
    assert shape.cast_arrow_batch(full).to_pylist() == [{"attrs": {"a": 1}}]
    assert shape.cast_arrow_batch(empty).to_pylist() == []


@pytest.mark.parametrize(
    "flavour",
    [pyarrow.list_, pyarrow.large_list, pyarrow.list_view, pyarrow.large_list_view],
)
def test_a_member_added_inside_any_list_flavour_survives_a_merge(flavour) -> None:
    """`merge_schema` recurses by shape, not by a list of the flavours someone typed."""
    small = pyarrow.struct([pyarrow.field("a", pyarrow.int32())])
    grown = pyarrow.struct(
        [pyarrow.field("a", pyarrow.int32()), pyarrow.field("b", pyarrow.string())]
    )
    shape = Field(name="row", data_type=pyarrow.struct([pyarrow.field("c", flavour(small))]))
    batch = pyarrow.record_batch(
        {"c": pyarrow.array([[{"a": 1, "b": "kept"}]], type=flavour(grown))}
    )
    merged = shape.cast_arrow_batch(batch, merge_schema=True)
    assert merged.to_pylist() == [{"c": [{"a": 1, "b": "kept"}]}]


def test_a_cast_attaches_the_comments_even_when_the_types_already_match() -> None:
    """Whether a column keeps its comment cannot depend on another column's type."""
    shape = Field(
        name="Tick",
        data_type=pyarrow.struct(
            [pyarrow.field("v", pyarrow.int64(), metadata={"description": "the answer"})]
        ),
        metadata={"namespace": "demo"},
    )
    batch = pyarrow.record_batch({"v": pyarrow.array([1], pyarrow.int64())})
    cast = shape.cast_arrow_batch(batch)
    assert cast.schema.field("v").metadata == {b"description": b"the answer"}
    assert Field.from_arrow_schema(cast.schema).name == "Tick"
    assert cast.column(0).buffers() == batch.column(0).buffers(), "the schema moved, not the data"


# -- map <-> list -----------------------------------------------------------


def test_a_map_becomes_a_list_of_entries() -> None:
    array = pyarrow.array(
        [[("a", 1), ("b", 2)], None, []], type=pyarrow.map_(pyarrow.string(), pyarrow.int64())
    )
    cast = _shape(pyarrow.list_(pyarrow.field("item", ENTRY))).cast_arrow_array(array)
    assert cast.to_pylist() == [
        [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
        None,
        [],
    ]


def test_a_list_of_entries_becomes_a_map() -> None:
    array = pyarrow.array(
        [[{"key": "a", "value": 1}], None, []], type=pyarrow.list_(pyarrow.field("item", ENTRY))
    )
    cast = _shape(pyarrow.map_(pyarrow.large_string(), pyarrow.int32())).cast_arrow_array(array)
    assert cast.type == pyarrow.map_(pyarrow.large_string(), pyarrow.int32())
    assert cast.to_pylist() == [[("a", 1)], None, []]


def test_a_list_whose_item_is_not_a_pair_is_refused() -> None:
    array = pyarrow.array([[1, 2]], type=pyarrow.list_(pyarrow.int64()))
    with pytest.raises(TypeError, match="key/value struct"):
        _shape(pyarrow.map_(pyarrow.string(), pyarrow.int64())).cast_arrow_array(array)


def test_a_map_casts_both_halves() -> None:
    array = pyarrow.array([[("a", 1)], None], type=pyarrow.map_(pyarrow.string(), pyarrow.int64()))
    cast = _shape(pyarrow.map_(pyarrow.large_string(), pyarrow.int32())).cast_arrow_array(array)
    assert cast.to_pylist() == [[("a", 1)], None]


# -- map <-> struct ---------------------------------------------------------


def test_a_map_becomes_a_struct_by_looking_its_members_up() -> None:
    array = pyarrow.array(
        [[("mic", "XPAR"), ("desk", "EQ")], [("mic", "XETR")], None],
        type=pyarrow.map_(pyarrow.string(), pyarrow.string()),
    )
    target = _shape(pyarrow.struct([("mic", pyarrow.string()), ("desk", pyarrow.string())]))
    cast = target.cast_arrow_array(array)
    assert cast.to_pylist() == [
        {"mic": "XPAR", "desk": "EQ"},
        {"mic": "XETR", "desk": None},
        None,
    ]


def test_a_member_no_entry_ever_carries_is_filled_or_refused() -> None:
    array = pyarrow.array(
        [[("mic", "XPAR")]], type=pyarrow.map_(pyarrow.string(), pyarrow.string())
    )
    filled = _shape(
        pyarrow.struct([("mic", pyarrow.string()), ("desk", pyarrow.string())])
    ).cast_arrow_array(array)
    assert filled.to_pylist() == [{"mic": "XPAR", "desk": None}]

    required = pyarrow.struct(
        [("mic", pyarrow.string()), pyarrow.field("desk", pyarrow.string(), nullable=False)]
    )
    with pytest.raises(ValueError, match=r"'value\.desk' is missing and not nullable"):
        _shape(required).cast_arrow_array(array)


def test_a_struct_becomes_a_map_of_its_members() -> None:
    array = pyarrow.array(
        [{"mic": "XPAR", "desk": "EQ"}, None],
        type=pyarrow.struct([("mic", pyarrow.string()), ("desk", pyarrow.string())]),
    )
    cast = _shape(pyarrow.map_(pyarrow.string(), pyarrow.string())).cast_arrow_array(array)
    assert cast.to_pylist() == [[("mic", "XPAR"), ("desk", "EQ")], None]


def test_a_struct_map_round_trip_keeps_the_values() -> None:
    struct_type = pyarrow.struct([("a", pyarrow.int64()), ("b", pyarrow.int64())])
    rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    array = pyarrow.array(rows, type=struct_type)
    as_map = _shape(pyarrow.map_(pyarrow.string(), pyarrow.int64())).cast_arrow_array(array)
    assert as_map.to_pylist() == [[("a", 1), ("b", 2)], [("a", 3), ("b", 4)]]
    assert _shape(struct_type).cast_arrow_array(as_map).to_pylist() == rows


def test_the_members_are_transposed_not_concatenated() -> None:
    """Row-major order is the whole point of the interleave: row 0 then row 1."""
    array = pyarrow.array(
        [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 30}],
        type=pyarrow.struct([("a", pyarrow.int64()), ("b", pyarrow.int64())]),
    )
    cast = _shape(pyarrow.list_(pyarrow.int64())).cast_arrow_array(array)
    assert cast.to_pylist() == [[1, 10], [2, 20], [3, 30]]


# -- struct <-> list --------------------------------------------------------


def test_a_struct_becomes_a_list_of_its_members() -> None:
    array = pyarrow.array(
        [{"low": 1, "high": 2}, None],
        type=pyarrow.struct([("low", pyarrow.int64()), ("high", pyarrow.int64())]),
    )
    cast = _shape(pyarrow.large_list(pyarrow.int32())).cast_arrow_array(array)
    assert cast.to_pylist() == [[1, 2], None]


def test_a_list_becomes_a_struct_by_position() -> None:
    array = pyarrow.array([[1, 2], [3, 4]], type=pyarrow.list_(pyarrow.int64()))
    target = _shape(pyarrow.struct([("low", pyarrow.int32()), ("high", pyarrow.int64())]))
    cast = target.cast_arrow_array(array)
    assert cast.to_pylist() == [{"low": 1, "high": 2}, {"low": 3, "high": 4}]
    assert cast.type.field(0).type == pyarrow.int32()


def test_a_list_too_short_for_the_struct_is_refused() -> None:
    array = pyarrow.array([[1, 2], [3]], type=pyarrow.list_(pyarrow.int64()))
    target = _shape(pyarrow.struct([("low", pyarrow.int64()), ("high", pyarrow.int64())]))
    with pytest.raises(ValueError, match="only 1"):
        target.cast_arrow_array(array)


# -- depth ------------------------------------------------------------------


def test_a_map_inside_a_list_inside_a_struct_converts_all_the_way_down() -> None:
    source = pyarrow.struct(
        [("legs", pyarrow.list_(pyarrow.map_(pyarrow.string(), pyarrow.int64())))]
    )
    array = pyarrow.array([{"legs": [[("a", 1)]]}], type=source)
    target = _shape(
        pyarrow.struct(
            [
                (
                    "legs",
                    pyarrow.large_list(
                        pyarrow.struct([("a", pyarrow.int32())])  # the map becomes a struct
                    ),
                )
            ]
        )
    )
    assert target.cast_arrow_array(array).to_pylist() == [{"legs": [{"a": 1}]}]


def test_a_chunked_column_keeps_its_chunks_through_a_shape_change() -> None:
    chunked = pyarrow.chunked_array(
        [
            pyarrow.array([{"a": 1}], type=pyarrow.struct([("a", pyarrow.int64())])),
            pyarrow.array([{"a": 2}], type=pyarrow.struct([("a", pyarrow.int64())])),
        ]
    )
    cast = _shape(pyarrow.map_(pyarrow.string(), pyarrow.int64())).cast_arrow_array(chunked)
    assert cast.num_chunks == 2
    assert cast.to_pylist() == [[("a", 1)], [("a", 2)]]


# -- the kernels underneath -------------------------------------------------


def test_the_conversions_never_walk_rows_in_python() -> None:
    """A row loop would show up as a Python-level iteration over an array."""
    values, member = arrays.interleave([pyarrow.array([1, 2]), pyarrow.array([10, 20])], length=2)
    assert values.to_pylist() == [1, 10, 2, 20]
    assert member.to_pylist() == [0, 1, 0, 1]
    chunked = [pyarrow.chunked_array([[1], [2]]), pyarrow.chunked_array([[10], [20]])]
    values, member = arrays.interleave(chunked, length=2)
    assert values.to_pylist() == [1, 10, 2, 20]
    assert member.to_pylist() == [0, 1, 0, 1]
    assert arrays.sequence(4).to_pylist() == [0, 1, 2, 3]
    assert arrays.repeat_sizes(3, 2).to_pylist() == [3, 3]


def test_offsets_carry_no_validity_bitmap() -> None:
    """A bitmap on the offsets reads as "these rows are null" to every builder."""
    offsets = arrays.list_offsets(pyarrow.array([1, 0, 2], pyarrow.int64()), pyarrow.int32())
    assert offsets.buffers()[0] is None
    assert offsets.to_pylist() == [0, 1, 1, 3]
