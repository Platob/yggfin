"""The reverse projection: a class rebuilt from a field."""

from __future__ import annotations

import datetime
import decimal
import functools
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any

import pyarrow

from rekep.convert import Convertible
from rekep.fields.builder import FieldBuilder
from rekep.fields.field import DESCRIPTION, NAMESPACE, Field, StructField, scalar


class ClassBuilder:
    """Builds a `@scalar` *class* back out of a field."""

    @classmethod
    @functools.cache
    def into_base(cls) -> type:
        """Base generated classes extend when none is given."""
        return Convertible

    def dataclass(
        self, source: StructField, name: str | None = None, base: type | None = None
    ) -> type:
        """One class, one member per field, in declaration order.

        A field this package built carries its class name and module, and the
        generated class takes that identity back -- so a clone of `FixMsg` *is*
        named FixMsg, in its module. An explicit `name` wins; a field without the
        metadata falls back to its own name, then to `ArrowFields`.
        """
        annotations: dict[str, Any] = {}
        namespace: dict[str, Any] = {"__annotations__": annotations}
        for member in source.fields:
            annotations[member.name] = self.annotation(member)
            if member.nullable:
                namespace[member.name] = None
        metadata = dict(source.metadata or {})
        described = metadata.get(DESCRIPTION)
        if described:
            namespace["__doc__"] = described
        module = metadata.get(NAMESPACE)
        if module:
            namespace["__module__"] = module
        declared = {
            key: value for key, value in metadata.items() if key not in (DESCRIPTION, NAMESPACE)
        }
        # Set even when empty: a generated subclass must not inherit contract
        # metadata from `base` that the source schema never declared.
        namespace["into_field_metadata"] = _class_value(declared)
        built = type(name or source.name or "ArrowFields", (base or self.into_base(),), namespace)
        return scalar(kw_only=True)(built)

    def annotation(self, member: Field) -> Any:
        """The Python annotation that projects back to exactly `member`."""
        inner = self.python_type(member.dtype, member.name)
        declared = self.declaration(member, inner)
        if member.nullable:
            inner = inner | None
        return Annotated[inner, declared] if declared != Field() else inner

    def declaration(self, member: Field, inner: Any) -> Field:
        """What `Annotated` must carry so nothing of `member` is lost.

        The type is only carried when inference would produce a different one:
        an `int32` has to be said, an `int64` says itself.
        """
        inferred = FieldBuilder().field("probe", inner).dtype
        return Field(
            dtype=member.dtype if inferred != member.dtype else None,
            metadata=member.metadata,
        )

    def python_type(self, dtype: pyarrow.DataType, name: str) -> Any:
        """Plainest Python annotation for one Arrow type; exactness is the
        declaration's job."""
        kinds = pyarrow.types
        if kinds.is_struct(dtype):
            return self.dataclass(Field.from_arrow_type(dtype, name), _class_name(name))
        if kinds.is_list(dtype) or kinds.is_large_list(dtype):
            return list[self.annotation(Field.from_arrow_field(dtype.field(0)))]
        if kinds.is_map(dtype):
            key = self.python_type(dtype.key_type, f"{name}_key")
            return dict[key, self.annotation(Field.from_arrow_field(dtype.item_field))]
        if kinds.is_boolean(dtype):
            return bool
        if kinds.is_integer(dtype):
            return int
        if kinds.is_floating(dtype):
            return float
        if kinds.is_decimal(dtype):
            return decimal.Decimal
        if kinds.is_timestamp(dtype):
            return datetime.datetime
        if kinds.is_date(dtype):
            return datetime.date
        if kinds.is_time(dtype):
            return datetime.time
        if kinds.is_duration(dtype):
            return datetime.timedelta
        if kinds.is_binary(dtype) or kinds.is_large_binary(dtype):
            return bytes
        return str


def _class_name(field_name: str) -> str:
    """`order_book` -> `OrderBook`: a field name as a class name."""
    cleaned = re.sub(r"\W", "_", field_name)
    return "".join(part.capitalize() or "_" for part in cleaned.split("_")) or "Anonymous"


def _class_value(value: Any) -> classmethod:
    """A cached classmethod returning one generated declaration value."""
    declared = MappingProxyType(dict(value)) if isinstance(value, Mapping) else value

    @functools.cache
    def into_value(_owner: type) -> Any:
        return declared

    return classmethod(into_value)
