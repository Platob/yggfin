"""The reverse projection: a class rebuilt from a field."""

from __future__ import annotations

import datetime
import decimal
import re
from typing import Annotated, Any, ClassVar

import pyarrow

from rekep.convert import Convertible
from rekep.fields.field import DESCRIPTION, NAMESPACE, Field, FieldBuilder, field


class ClassBuilder:
    """Builds a `@field` *class* back out of a field.

    The inverse of `FieldBuilder`, for schemas that arrive from outside -- a
    parquet footer, another team's contract -- so they can be handled with the
    same machinery as a hand-declared class.

    The round trip is lossless by construction: every generated member is
    `Annotated[..., Field(...)]` carrying the exact original type whenever the
    default projection would differ, plus the original metadata. Nested structs
    become nested classes; generated classes are keyword-only, because Arrow
    field order owes nothing to Python's defaults-last rule.
    """

    #: Base class generated classes extend when none is given. `Convertible`
    #: is what makes an instance of one serialise itself.
    BASE: ClassVar[type] = Convertible

    def dataclass(self, source: Field, name: str | None = None, base: type | None = None) -> type:
        """One class, one member per field, in declaration order.

        A field this package built carries its class name and module, and the
        generated class takes that identity back -- so a clone of `Log` *is*
        named Log, in its module. An explicit `name` wins; a field without the
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
        built = type(name or source.name or "ArrowFields", (base or self.BASE,), namespace)
        return field(kw_only=True)(built)

    def annotation(self, member: Field) -> Any:
        """The Python annotation that projects back to exactly `member`."""
        inner = self.python_type(member.arrow_type, member.name)
        declared = self.declaration(member, inner)
        if member.nullable:
            inner = inner | None
        return Annotated[inner, declared] if declared != Field() else inner

    def declaration(self, member: Field, inner: Any) -> Field:
        """What `Annotated` must carry so nothing of `member` is lost.

        The type is only carried when inference would produce a different one:
        an `int32` has to be said, an `int64` says itself.
        """
        inferred = FieldBuilder().field("probe", inner).arrow_type
        return Field(
            arrow_type=member.arrow_type if inferred != member.arrow_type else None,
            metadata=member.metadata,
        )

    def python_type(self, data_type: pyarrow.DataType, name: str) -> Any:
        """Plainest Python annotation for one Arrow type; exactness is the
        declaration's job."""
        kinds = pyarrow.types
        if kinds.is_struct(data_type):
            return self.dataclass(Field.from_arrow_type(data_type, name), _class_name(name))
        if kinds.is_list(data_type) or kinds.is_large_list(data_type):
            return list[self.annotation(Field.from_arrow_field(data_type.field(0)))]
        if kinds.is_map(data_type):
            key = self.python_type(data_type.key_type, f"{name}_key")
            return dict[key, self.annotation(Field.from_arrow_field(data_type.item_field))]
        if kinds.is_boolean(data_type):
            return bool
        if kinds.is_integer(data_type):
            return int
        if kinds.is_floating(data_type):
            return float
        if kinds.is_decimal(data_type):
            return decimal.Decimal
        if kinds.is_timestamp(data_type):
            return datetime.datetime
        if kinds.is_date(data_type):
            return datetime.date
        if kinds.is_time(data_type):
            return datetime.time
        if kinds.is_duration(data_type):
            return datetime.timedelta
        if kinds.is_binary(data_type) or kinds.is_large_binary(data_type):
            return bytes
        return str


def _class_name(field_name: str) -> str:
    """`order_book` -> `OrderBook`: a field name as a class name."""
    cleaned = re.sub(r"\W", "_", field_name)
    return "".join(part.capitalize() or "_" for part in cleaned.split("_")) or "Anonymous"
