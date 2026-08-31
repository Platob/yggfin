"""What a market declaration means: the four rules this package's shapes add."""

from __future__ import annotations

import dataclasses
import enum
import functools
from collections.abc import Iterable, Mapping
from typing import Any, Self

import pyarrow

from rekep.convert import Convertible
from rekep.enums import Ascii32
from rekep.fields import ENUM, PARTITION_KEY, PRIMARY_KEY, Field, FieldBuilder
from rekep.fix.registry import FixRegistry
from rekep.market.identity import ROW_SPELLED, read_member, stored_member


class MarketFieldBuilder(FieldBuilder):
    """`FieldBuilder` with the four rules market declarations add."""

    def scalar(self, annotation: Any) -> pyarrow.DataType | None:
        """Market codes store their declared width; other scalars use the base.

        An ASCII code's storage is the index type of its cached dictionary
        type, so the width one declaration states is the width every column
        carries. Checked first because the base sees an `IntEnum` as Python
        `int64`.
        """
        if isinstance(annotation, type) and issubclass(annotation, Ascii32):
            return annotation.into_arrow_type().index_type
        return super().scalar(annotation)

    def arrow_type(self, annotation: Any) -> pyarrow.DataType:
        """A member's type, with the keys stripped from anything nested inside it.

        Only members reach here -- a class projected as a whole goes through
        `struct()` -- so this is the one place that can tell "this shape is
        someone's column" from "this shape is a table".
        """
        single = single_member(annotation)
        if single is not None:
            return Newtype(self.arrow_type(single[1]), single[0])
        return unkeyed(super().arrow_type(annotation))

    def field(self, name: str, annotation: Any, *, description: str | None = None) -> Field:
        """One member, with what its enum means written into the schema beside it."""
        built = super().field(name, annotation, description=description)
        declared = enum_of(annotation)
        if declared is not None:
            describe_enum(built, declared)
        if isinstance(declared, type) and issubclass(declared, Ascii32):
            built.protocol(ENUM).update(declared.schema_metadata())
        return built


class MarketConvertible(Convertible):
    """Use the market field projection for a scalar declaration."""

    __slots__ = ()

    @classmethod
    @functools.cache
    def into_field_builder(cls) -> type[FieldBuilder]:
        """Projection builder shared by market declarations."""
        return MarketFieldBuilder

    @classmethod
    @functools.cache
    def into_float_members(cls) -> tuple[str, ...]:
        """Top-level float members whose Python value must match the Arrow contract."""
        return tuple(
            member.name
            for member in cls.into_field().fields
            if pyarrow.types.is_floating(member.dtype)
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Read either spelling: a document's integers or a row's stored bytes."""
        return Convertible.from_dict.__func__(
            cls, {name: read_member(name, value) for name, value in mapping.items()}
        )

    @classmethod
    def from_entries(
        cls,
        entries: Iterable[Any],
        *,
        registry: FixRegistry | None = None,
        version: str | None = None,
        **overrides: Any,
    ) -> Self:
        """Build from raw entries read through one registry version."""
        from rekep.entries import Entry
        from rekep.fix.access import FieldAccess
        from rekep.market.event import _entry_values

        source = (entries,) if isinstance(entries, Entry) else entries
        selected = registry or FixRegistry.from_builtin()
        access = FieldAccess.of(selected, version)
        values = _entry_values(cls, tuple(access.entries_of(source)), access)
        values.update(overrides)
        return cls.from_dict(values)

    #: One member as its column holds it, for the builder that assembles a
    #: batch member by member. Only the spelling is asked here: `stored_member`
    #: answers for wide event hashes wherever they appear, and the builder
    #: walks a nested shape itself rather than being handed a document of it.
    into_column_value = staticmethod(stored_member)

    def into_row(self) -> dict[str, Any]:
        """This value as a stored row: every member as the column holds it.

        Read off the live members rather than off `into_dict`, because the two
        spellings differ: a document renders a date as text and an event hash
        as a number, while a column wants the date and the sixteen bytes.
        Nested values convert through their own `into_row`, so a book's orders
        and levels follow -- and a plain map of identifiers does not, however its
        keys are spelled. `from_dict` reads either spelling back.
        """
        return {
            member.name: _row_value(member.name, getattr(self, member.name))
            for member in dataclasses.fields(self)
        }

    def normalize_float_members(self) -> None:
        """Canonicalise numeric inputs before identity bytes are derived."""
        for name in type(self).into_float_members():
            value = getattr(self, name)
            if value is not None and not isinstance(value, float):
                setattr(self, name, float(value))


def _row_value(name: str, value: Any) -> Any:
    """One member as a column holds it, recursing through declared shapes."""
    if value is None or name in ROW_SPELLED:
        return stored_member(name, value)
    if isinstance(value, MarketConvertible):
        return value.into_row()
    if isinstance(value, Convertible):
        return value.into_dict()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            member.name: _row_value(member.name, getattr(value, member.name))
            for member in dataclasses.fields(value)
        }
    if isinstance(value, list | tuple):
        return [_row_value(name, one) for one in value]
    return value


def fix_tag(name: str, **declared: Any) -> Field:
    """A model annotation backed by the packaged FIX registry.

    The member it annotates carries the folded name; the dictionary's spelling
    of it is kept as the display.
    """
    from rekep.fix.columns import column_metadata, physical_type

    registry = FixRegistry.from_builtin().scalar(name, dtype=None)
    registry.metadata = column_metadata(registry.metadata)
    built = registry.merge(Field(**declared))
    built.dtype = physical_type(built)
    built.fix.display = registry.fix.canonical
    return built


def unkeyed(dtype: pyarrow.DataType) -> pyarrow.DataType:
    """`dtype` with every nested primary and partition key declaration dropped."""
    kinds = pyarrow.types
    if kinds.is_struct(dtype):
        return pyarrow.struct([_unkeyed(dtype.field(index)) for index in range(dtype.num_fields)])
    if kinds.is_map(dtype):
        return pyarrow.map_(
            _unkeyed(dtype.key_field),
            _unkeyed(dtype.item_field),
            keys_sorted=dtype.keys_sorted,
        )
    if kinds.is_fixed_size_list(dtype):
        return pyarrow.list_(_unkeyed(dtype.field(0)), dtype.list_size)
    for matches, build in _LISTS:
        if matches(dtype):
            return build(_unkeyed(dtype.field(0)))
    return dtype


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
    """A one-member shape as one column, with the shape's name still on it."""

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

    A `@scalar` class with exactly one member and nothing else to say. Anything
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
    """Write what `declared`'s codes mean into `built`'s metadata."""
    values = {member.name: member.value for member in declared}
    kinds = {type(value) for value in values.values()}
    keys = built.enum
    keys.name = declared.__name__
    if isinstance(declared, type) and issubclass(declared, Ascii32):
        keys.key_type = str(declared.into_arrow_type().index_type)
    else:
        keys.key_type = "int32" if kinds == {int} else "utf8" if kinds == {str} else "mixed"
    keys.value_type = "utf8"
    keys.members = {str(value): name for name, value in values.items()}
    mapping = getattr(declared, "fix_mapping", None)
    if mapping is not None:
        keys.fix_values = {
            str(tag): {wire: int(member) for wire, member in values.items()}
            for tag, values in mapping().items()
        }


def dictionary_arrow(array: Any, target: pyarrow.DataType) -> Any:
    """`array` as `target`, where either side may be dictionary-encoded."""
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


#: How far the identity fallback will go before it is certain the indices are
#: not positions. A dictionary of more entries than this is never what a
#: caller meant -- a packed ASCII code would ask for millions.
_IDENTITY_LIMIT = 1 << 16


def _values_of(indices: Any, target: pyarrow.DataType) -> Any:
    """The dictionary an array of bare indices points into.

    A fallback for the one case with nothing better: the array arrived as
    indices alone, with no dictionary to look them up in, so all that is left
    is the identity mapping -- index `i` means value `i`. It asserts nothing
    about what those codes mean; a caller that holds the real dictionary
    should encode against that instead. Built with `cumulative_sum` over
    `repeat`, never a Python `range`.
    """
    compute = pyarrow.compute
    highest = compute.max(indices).as_py()
    size = 0 if highest is None else int(highest) + 1
    if size > _IDENTITY_LIMIT:
        raise ValueError(
            f"cannot encode {size:,} identity dictionary entries: these indices are "
            "values in their own right, not positions. Encode against the dictionary "
            "that spells them -- a stable code's is `EnumName.into_arrow_array`."
        )
    counted = compute.cumulative_sum(pyarrow.repeat(pyarrow.scalar(1, pyarrow.int64()), size))
    return compute.subtract(counted, 1).cast(target.value_type, safe=False)
