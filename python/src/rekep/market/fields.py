"""What a market declaration means: the four rules this package's shapes add.

`FieldBuilder` already turns type hints into fields. Four things it cannot
know are decided once here, in a subclass wired onto every market shape
through `FIELD_BUILDER`, rather than repeated on the hundred members that
would otherwise each have to state them:

1. **A ranged code is `int32`.** Every enum in `enums.py` is a banded integer,
   and the band arithmetic only ever needs four bytes. The base builder would
   widen it to `int64`, doubling every state, side and kind column in the
   package for nothing -- and a narrower width is also what lets an engine
   keep the column's statistics in cache.
2. **A key belongs to a table, not to a struct.** A shape nested inside
   another -- an `Instrument` inside a `MarketEvent` -- keeps its
   documentation and loses its primary and partition keys, because
   `primary_keys()` and `partition_keys()` read the top level and nothing
   reads a nested one. Left in, they would publish a contract that says a
   column identifies a row when nothing treats it that way, which is the one
   thing a contract may not do.
3. **An enum says what its codes mean, in the schema.** The column is a number
   and the enum is ours, so a consumer that never imports this package has
   nothing to decode `410` with. The name, the value type and the whole member
   table ride under `enum:` keys, next to the `fix:` ones -- which is what
   makes a contract file readable by the people it is *for*.
4. **A shape with one member is that member.** A `struct` of one is a nesting
   level that carries no information and costs a filter its pushdown on every
   engine below. It becomes an Arrow extension type over the member's own
   storage instead: one column, the class name still on it, and a store that
   has never heard of the extension writes the storage type and reads it back.
"""

from __future__ import annotations

import dataclasses
import enum
import json
from typing import Any

import pyarrow

from rekep.fields import PARTITION_KEY, PRIMARY_KEY, Field, FieldBuilder
from rekep.market.enums import Ranged

#: The prefix the enum keys ride under, like `fix:` and `iceberg:`.
ENUM = "enum"


class MarketFieldBuilder(FieldBuilder):
    """`FieldBuilder` with the three rules above, wired on with `FIELD_BUILDER`."""

    def scalar(self, annotation: Any) -> pyarrow.DataType | None:
        """A ranged code is `int32`; everything else is the base builder's answer.

        Checked before the base class, which would see an `IntEnum` and take
        the width of its values -- `int64`, because that is what a Python int
        is.
        """
        if isinstance(annotation, type) and issubclass(annotation, Ranged):
            return pyarrow.int32()
        return super().scalar(annotation)

    def data_type(self, annotation: Any) -> pyarrow.DataType:
        """A member's type, with the keys stripped from anything nested inside it.

        Only members reach here -- a class projected as a whole goes through
        `struct()` -- so this is the one place that can tell "this shape is
        someone's column" from "this shape is a table".
        """
        single = single_member(annotation)
        if single is not None:
            return Newtype(self.data_type(single[1]), single[0])
        return unkeyed(super().data_type(annotation))

    def field(self, name: str, annotation: Any, *, description: str | None = None) -> Field:
        """One member, with what its enum means written into the schema beside it."""
        built = super().field(name, annotation, description=description)
        declared = enum_of(annotation)
        if declared is not None:
            describe_enum(built, declared)
        return built


def fix_tag(name: str, tag: int, **declared: Any) -> Field:
    """A declaration naming the FIX field a member carries.

    The name and the tag ride under the `fix:` prefix that `Field.fix` already
    reads, so a market column says which wire field it came from wherever the
    schema travels -- and `tests/market/test_fix.py` checks every one of them
    against the published dictionary in `data/fix.zip`, so a tag typed from
    memory fails the build rather than mislabelling a column forever.

    The *name* is what is checked and the tag is what is written, because a
    tag is a number that transposes without looking wrong::

        px: Annotated[float | None, fix_tag("Price", 44)]
    """
    built = Field(**declared)
    built.fix["name"] = name
    built.fix["tag"] = str(int(tag))
    return built


def unkeyed(arrow_type: pyarrow.DataType) -> pyarrow.DataType:
    """`arrow_type` with every nested primary and partition key declaration dropped.

    Recursive through structs, lists and maps, because a key is just as
    misleading three levels down as it is one -- and everything else the
    members carry, the descriptions above all, is left exactly as it was.

    Every list flavour rebuilds as itself: a walk that spelled all five `list`
    would narrow a 64-bit offset and drop a `fixed_size_list`'s width in
    silence, which is the failure `Field` names in its own dump.
    """
    kinds = pyarrow.types
    if kinds.is_struct(arrow_type):
        return pyarrow.struct(
            [_unkeyed(arrow_type.field(index)) for index in range(arrow_type.num_fields)]
        )
    if kinds.is_map(arrow_type):
        return pyarrow.map_(
            _unkeyed(arrow_type.key_field),
            _unkeyed(arrow_type.item_field),
            keys_sorted=arrow_type.keys_sorted,
        )
    if kinds.is_fixed_size_list(arrow_type):
        return pyarrow.list_(_unkeyed(arrow_type.field(0)), arrow_type.list_size)
    for matches, build in _LISTS:
        if matches(arrow_type):
            return build(_unkeyed(arrow_type.field(0)))
    return arrow_type


# -- helpers ----------------------------------------------------------------

#: How each list flavour is recognised and rebuilt, asked in order. A view is
#: also a list to `is_list`-shaped questions in some Arrow builds, so the
#: narrow predicates are asked first and the plain one last.
_LISTS: tuple[tuple[Any, Any], ...] = (
    (pyarrow.types.is_large_list_view, pyarrow.large_list_view),
    (pyarrow.types.is_list_view, pyarrow.list_view),
    (pyarrow.types.is_large_list, pyarrow.large_list),
    (pyarrow.types.is_list, pyarrow.list_),
)


def _unkeyed(field: pyarrow.Field) -> pyarrow.Field:
    """One Arrow field with the key metadata gone, recursing into its type."""
    metadata = {
        key: value
        for key, value in (field.metadata or {}).items()
        if key not in (PRIMARY_KEY.encode(), PARTITION_KEY.encode())
    }
    return pyarrow.field(
        field.name, unkeyed(field.type), nullable=field.nullable, metadata=metadata or None
    )


class Newtype(pyarrow.ExtensionType):
    """A one-member shape as one column, with the shape's name still on it.

    Arrow's extension mechanism is exactly the right size for this: the column
    *is* its storage -- an `int64`, a `string`, sixteen fixed bytes -- and the
    name rides in the field's metadata under `ARROW:extension:name`. A reader
    that knows the extension gets the class back; parquet, Iceberg, Spark and
    Doris, which do not, all see the storage type and read it correctly. That
    is the whole difference from a `struct` of one member, which every one of
    them sees as a nesting level and none of them pushes a filter through.
    """

    def __init__(self, storage: pyarrow.DataType, name: str) -> None:
        self._name = name
        super().__init__(storage, f"rekep.market.{name.lower()}")

    @property
    def shape_name(self) -> str:
        """The class this column came from."""
        return self._name

    def __arrow_ext_serialize__(self) -> bytes:
        """The class name, which is the only thing the storage does not carry."""
        return self._name.encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage: pyarrow.DataType, serialized: bytes) -> Newtype:
        return cls(storage, serialized.decode())


def single_member(annotation: Any) -> tuple[str, Any] | None:
    """`(class name, the one member's annotation)` when `annotation` is a shape of one.

    A `@field` class with exactly one member and nothing else to say. Anything
    with two members, or none, is a struct like any other.
    """
    if not (isinstance(annotation, type) and dataclasses.is_dataclass(annotation)):
        return None
    members = dataclasses.fields(annotation)
    if len(members) != 1:
        return None
    from typing import get_type_hints

    hints = get_type_hints(annotation, include_extras=True)
    return annotation.__name__, hints[members[0].name]


def enum_of(annotation: Any) -> type[enum.Enum] | None:
    """The enum an annotation declares, through `Annotated` and `| None` alike."""
    _, inner = Field.unwrap(annotation)
    for candidate in (inner, *getattr(inner, "__args__", ())):
        if isinstance(candidate, type) and issubclass(candidate, enum.Enum):
            return candidate
    return None


def describe_enum(built: Field, declared: type[enum.Enum]) -> None:
    """Write what `declared`'s codes mean into `built`'s metadata.

    Under an `enum:` prefix, the way `fix:` and `iceberg:` keys ride: the
    class, whether the values are numbers or text, and the whole member table
    as JSON. A consumer that has never imported this package reads `410` out of
    a column and finds `FILLED` in the schema that came with it -- which is the
    difference between a contract and a number.

    The **value type is read off the members**, not guessed from the base
    class: `class Kind(str, Enum)` and `class Kind(IntEnum)` both subclass
    something, and only the values say which of the two the column holds.
    """
    values = {member.name: member.value for member in declared}
    kinds = {type(value) for value in values.values()}
    keys = built.protocol(ENUM)
    keys["name"] = declared.__name__
    keys["type"] = "int" if kinds == {int} else "str" if kinds == {str} else "mixed"
    keys["values"] = json.dumps(
        {str(value): name for name, value in values.items()}, separators=(",", ":")
    )


def dictionary_arrow(array: Any, target: pyarrow.DataType) -> Any:
    """`array` as `target`, where either side may be dictionary-encoded.

    Arrow's `dictionary` is an **encoding, not a type** -- the same values,
    stored once each with an index per row -- which is what makes it the right
    shape for a code column whose whole point is that it repeats. It is not a
    `map`: a map is a column of key/value pairs, one set per row, and nothing
    here wants that.

    Three cases, asked in this order, because the first two are free and the
    third is a pass over the data:

    1. **Same value type** -- the array already holds what the dictionary
       holds, so it is encoded as it stands.
    2. **Same index type** -- the array already holds *indices*, so it is taken
       as them rather than encoded again. This is the case that has to be
       checked, and checked second: a `dictionary<int32, int32>` of ranged
       codes has an index type and a value type that are the same width, and
       an array of indices encoded as values doubles the dictionary and points
       every row at the wrong member.
    3. **Neither** -- the values are cast to the dictionary's value type first,
       and then encoded. That is the only case that costs a pass, and it is the
       one a producer that sent `int64` codes into an `int32` column lands in.

    A `target` that is not a dictionary decodes instead, which is the same
    three questions from the other side.
    """
    if isinstance(array, pyarrow.ChunkedArray):
        return pyarrow.chunked_array([dictionary_arrow(chunk, target) for chunk in array.chunks])
    if array.type == target:
        return array
    if not pyarrow.types.is_dictionary(target):
        decoded = array.dictionary_decode() if pyarrow.types.is_dictionary(array.type) else array
        return decoded if decoded.type == target else decoded.cast(target, safe=False)
    if pyarrow.types.is_dictionary(array.type):
        return (
            array.dictionary_decode()
            .cast(target.value_type, safe=False)
            .dictionary_encode()
            .cast(target, safe=False)
        )
    if array.type == target.value_type:
        return array.dictionary_encode().cast(target, safe=False)
    if array.type == target.index_type:
        return pyarrow.DictionaryArray.from_arrays(array, _values_of(array, target))
    return array.cast(target.value_type, safe=False).dictionary_encode().cast(target, safe=False)


def _values_of(indices: Any, target: pyarrow.DataType) -> Any:
    """The dictionary an array of bare indices points into.

    There is nothing to look them up in, so the dictionary is the indices'
    own range: index `i` means value `i`, which is exactly true for a `Ranged`
    code stored as itself and is the only reading that loses nothing. Built
    with `cumulative_sum` over `repeat`, never a Python `range`.
    """
    compute = pyarrow.compute
    highest = compute.max(indices).as_py()
    size = 0 if highest is None else int(highest) + 1
    counted = compute.cumulative_sum(pyarrow.repeat(pyarrow.scalar(1, pyarrow.int64()), size))
    return compute.subtract(counted, 1).cast(target.value_type, safe=False)
