"""What a market declaration means: the three rules this package's shapes add.

`FieldBuilder` already turns type hints into fields. Three things it cannot
know are decided once here, in a subclass wired onto every market shape
through `FIELD_BUILDER`, rather than repeated on the hundred members that
would otherwise each have to state them:

1. **An identifier is sixteen fixed bytes.** `uuid.UUID` is the Python type
   and `fixed_size_binary[16]` is the column; the reasoning is in
   `identity.py`. The base builder spells a UUID as a string, which is 36
   bytes and a pointer chase per comparison.
2. **A ranged code is `int32`.** Every enum in `enums.py` is a banded integer,
   and the band arithmetic only ever needs four bytes. The base builder would
   widen it to `int64`, doubling every state, side and kind column in the
   package for nothing -- and a narrower width is also what lets an engine
   keep the column's statistics in cache.
3. **A key belongs to a table, not to a struct.** A shape nested inside
   another -- a `BookSide` inside a `Book`, an `Instrument` inside a
   `MarketEvent` -- keeps its documentation and loses its primary and
   partition keys, because `primary_keys()` and `partition_keys()` read the
   top level and nothing reads a nested one. Left in, they would publish a
   contract that says a column identifies a row when nothing treats it that
   way, which is the one thing a contract may not do.
"""

from __future__ import annotations

import uuid
from typing import Any, ClassVar

import pyarrow

from rekep.fields import PARTITION_KEY, PRIMARY_KEY, Field, FieldBuilder
from rekep.market.enums import Ranged
from rekep.market.identity import H128


class MarketFieldBuilder(FieldBuilder):
    """`FieldBuilder` with the three rules above, wired on with `FIELD_BUILDER`."""

    SCALARS: ClassVar[dict[type, pyarrow.DataType]] = {
        **FieldBuilder.SCALARS,
        uuid.UUID: H128,
    }

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
        return unkeyed(super().data_type(annotation))


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
