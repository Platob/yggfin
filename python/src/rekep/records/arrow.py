"""Projecting a record onto Arrow."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import itertools
import pathlib
import re
import types
import uuid
from collections.abc import Iterator, Mapping
from typing import Annotated, Any, ClassVar, Union, get_args, get_origin, get_type_hints

import pyarrow

from rekep.records.annotations import (
    MAPPING_ORIGINS,
    SEQUENCE_ORIGINS,
    SET_ORIGINS,
    docstring_attributes,
    docstring_summary,
    item_annotation,
    unwrap_annotated,
    unwrap_optional,
)


@dataclasses.dataclass(frozen=True)
class Arrow:
    """Per-field overrides for the Arrow projection, carried in `Annotated`.

    Inference gets the common cases right; this is for the ones it cannot know
    -- a narrower width, a unit, a column comment the docstring does not carry::

        size: Annotated[int, Arrow(type=pyarrow.int32(), metadata={"unit": "lots"})]

    A bare `pyarrow.DataType`, `Mapping` or `str` inside `Annotated` is read as
    a type, metadata or description respectively, so the short forms work too.

    `iceberg` carries per-field properties for the Iceberg side and lands in
    field metadata under protocol-prefixed keys (`iceberg:partition`), so one
    namespace's keys can never collide with another's. `partition` and `key`
    are the first-class spellings of the two that matter most::

        day: Annotated[datetime.date, Arrow(partition="day", key=True)]

    `partition` takes True for identity or an Iceberg transform name
    (`"day"`, `"bucket[16]"`, ...); `key` marks the field as part of the
    primary key -- Iceberg's identifier fields -- and therefore requires the
    field to be non-nullable.
    """

    type: pyarrow.DataType | None = None
    metadata: Mapping[str, str] | None = None
    description: str | None = None
    nullable: bool | None = None
    iceberg: Mapping[str, str] | None = None
    partition: bool | str | None = None
    key: bool | None = None

    @classmethod
    def unwrap(cls, annotation: Any) -> tuple[Arrow, Any]:
        """Split `Annotated[X, ...]` into the overrides it carries and X."""
        extras, inner = unwrap_annotated(annotation)
        overrides = cls()
        for extra in extras:
            overrides = overrides.merge(cls.of(extra))
        return overrides, inner

    @classmethod
    def of(cls, extra: Any) -> Arrow:
        """Read one `Annotated` argument as overrides."""
        if isinstance(extra, Arrow):
            return extra
        if isinstance(extra, pyarrow.DataType):
            return cls(type=extra)
        if isinstance(extra, Mapping):
            return cls(metadata={str(k): str(v) for k, v in extra.items()})
        if isinstance(extra, str):
            return cls(description=extra)
        return cls()

    def merge(self, other: Arrow) -> Arrow:
        """Combine with `other`, letting `other` win where it says anything."""
        return Arrow(
            type=other.type if other.type is not None else self.type,
            metadata={**(self.metadata or {}), **(other.metadata or {})} or None,
            description=other.description if other.description is not None else self.description,
            nullable=other.nullable if other.nullable is not None else self.nullable,
            iceberg={**(self.iceberg or {}), **(other.iceberg or {})} or None,
            partition=other.partition if other.partition is not None else self.partition,
            key=other.key if other.key is not None else self.key,
        )


#: Field metadata key that Arrow's parquet writer and pyiceberg both read
#: field ids from. Ecosystem-owned, hence the foreign prefix.
FIELD_ID_KEY = b"PARQUET:field_id"

#: Field metadata keys the partition and primary-key declarations land under.
PARTITION_KEY = b"iceberg:partition_key"
PRIMARY_KEY = b"iceberg:primary_key"


class ArrowFieldBuilder:
    """Projects Python type hints onto Arrow fields, one case at a time.

    The cases are: `Annotated` unwraps to its overrides, `X | None` becomes a
    nullable field and **anything else becomes a non-nullable one**, a dataclass
    becomes a struct, a sequence becomes a list of a field (so item nullability
    survives), a mapping becomes a map, an enum becomes its value type, and a
    leaf is looked up in `SCALARS`.

    Subclass and extend `SCALARS`, or override `scalar`, to teach it a type it
    does not know; `Record.ARROW_BUILDER` selects which builder a record uses.
    """

    #: Stamp Iceberg-style field ids into `FIELD_ID_KEY` metadata on `schema`.
    #: Ids follow the Iceberg fresh-assignment order -- siblings before any
    #: descent -- so the Arrow schema, the Iceberg schema and parquet files
    #: written from it all agree on which column is which by id, not by name.
    FIELD_IDS: ClassVar[bool] = True

    #: Leaf Python type -> Arrow type. Matched exactly first, then by subclass.
    SCALARS: ClassVar[dict[type, pyarrow.DataType]] = {
        bool: pyarrow.bool_(),
        int: pyarrow.int64(),
        float: pyarrow.float64(),
        str: pyarrow.string(),
        bytes: pyarrow.binary(),
        datetime.datetime: pyarrow.timestamp("us"),
        datetime.date: pyarrow.date32(),
        datetime.time: pyarrow.time64("us"),
        datetime.timedelta: pyarrow.duration("us"),
        decimal.Decimal: pyarrow.decimal128(38, 9),
        uuid.UUID: pyarrow.string(),
        pathlib.PurePath: pyarrow.string(),
    }

    def __init__(self) -> None:
        #: Records currently being built, so a cycle is reported not chased.
        self._building: list[type] = []

    # -- entry points -------------------------------------------------------

    def schema(self, cls: type) -> pyarrow.Schema:
        """Flat schema for `cls`, with its summary line as schema metadata.

        The default metadata keys are written here too: `name` and
        `namespace` identify the record the schema came from, so the schema
        is self-describing wherever it travels, and `description` carries the
        summary when there is one.

        This is where field ids are stamped: they are schema-scoped, so a
        record embedded in another (via `struct`/`field`) is numbered by the
        schema it lands in, not by itself.
        """
        metadata = {"name": cls.__qualname__, "namespace": cls.__module__}
        summary = docstring_summary(cls)
        if summary:
            metadata["description"] = summary
        fields = self.fields(cls)
        if self.FIELD_IDS:
            fields = _stamp_siblings(fields, itertools.count(1))
        return pyarrow.schema(fields, metadata=metadata)

    def struct(self, cls: type) -> pyarrow.DataType:
        """Struct type for `cls`."""
        if cls in self._building:
            cycle = " -> ".join(c.__name__ for c in (*self._building, cls))
            raise TypeError(f"Arrow has no recursive types, but the fields cycle: {cycle}")
        self._building.append(cls)
        try:
            return pyarrow.struct(self.fields(cls))
        finally:
            self._building.pop()

    def fields(self, cls: type) -> list[pyarrow.Field]:
        """One Arrow field per dataclass field, in declaration order."""
        if not dataclasses.is_dataclass(cls):
            raise TypeError(f"{cls.__name__} must be a dataclass to be projected onto Arrow")
        hints = get_type_hints(cls, include_extras=True)
        described = docstring_attributes(cls)
        return [
            self.field(f.name, hints[f.name], description=described.get(f.name))
            for f in dataclasses.fields(cls)
        ]

    # -- cases --------------------------------------------------------------

    def field(self, name: str, annotation: Any, *, description: str | None = None) -> pyarrow.Field:
        """One Arrow field, applying overrides over what inference produced."""
        overrides, annotation = Arrow.unwrap(annotation)
        optional, annotation = unwrap_optional(annotation)
        if description is not None:
            overrides = Arrow(description=description).merge(overrides)

        nullable = optional if overrides.nullable is None else overrides.nullable
        if overrides.key and nullable:
            raise TypeError(
                f"field {name!r} is a primary key and cannot be nullable; drop `| None` or the key"
            )
        metadata = dict(overrides.metadata or {})
        if overrides.description:
            metadata.setdefault("description", overrides.description)
        for key, value in (overrides.iceberg or {}).items():
            metadata.setdefault(f"iceberg:{key}", str(value))
        if overrides.partition:
            transform = "identity" if overrides.partition is True else overrides.partition
            metadata.setdefault(PARTITION_KEY.decode(), transform)
        if overrides.key:
            metadata.setdefault(PRIMARY_KEY.decode(), "true")
        return pyarrow.field(
            name,
            overrides.type if overrides.type is not None else self.data_type(annotation),
            nullable=nullable,
            metadata=metadata or None,
        )

    def data_type(self, annotation: Any) -> pyarrow.DataType:
        """Arrow type for `annotation`, recursing through containers."""
        overrides, annotation = Arrow.unwrap(annotation)
        if overrides.type is not None:
            return overrides.type

        origin = get_origin(annotation)
        if origin in SEQUENCE_ORIGINS or origin in SET_ORIGINS:
            return pyarrow.list_(self.field("item", item_annotation(annotation)))
        if origin is tuple:
            return self._tuple(get_args(annotation))
        if origin in MAPPING_ORIGINS:
            key, value = (get_args(annotation) or (str, Any))[:2]
            return pyarrow.map_(self.data_type(key), self.field("value", value))
        if origin in (Union, types.UnionType):
            named = ", ".join(getattr(a, "__name__", str(a)) for a in get_args(annotation))
            raise TypeError(f"Arrow cannot infer a type for the union of {named}")

        if dataclasses.is_dataclass(annotation):
            return self.struct(annotation)

        inferred = self.scalar(annotation)
        if inferred is None:
            raise TypeError(f"no Arrow type for {annotation!r}; annotate it with Arrow(type=...)")
        return inferred

    def scalar(self, annotation: Any) -> pyarrow.DataType | None:
        """Arrow type for a leaf, or None when this builder does not know it."""
        if not isinstance(annotation, type):
            return None
        if annotation in self.SCALARS:
            return self.SCALARS[annotation]
        if issubclass(annotation, enum.Enum):
            return self._enum(annotation)
        for python_type, arrow_type in self.SCALARS.items():
            if issubclass(annotation, python_type):
                return arrow_type
        return None

    def _tuple(self, args: tuple[Any, ...]) -> pyarrow.DataType:
        """`tuple[X, ...]` is a list; a fixed tuple is a positional struct."""
        if not args:
            return pyarrow.list_(pyarrow.string())
        if len(args) == 2 and args[1] is Ellipsis:
            return pyarrow.list_(self.field("item", args[0]))
        return pyarrow.struct([self.field(f"f{i}", arg) for i, arg in enumerate(args)])

    def _enum(self, annotation: type[enum.Enum]) -> pyarrow.DataType:
        """An enum is stored as its values, which is what `_encode` writes."""
        values = {type(member.value) for member in annotation}
        if len(values) == 1:
            inferred = self.scalar(values.pop())
            if inferred is not None:
                return inferred
        return pyarrow.string()


def _stamp_siblings(fields: list[pyarrow.Field], counter: itertools.count) -> list[pyarrow.Field]:
    """Assign ids the way Iceberg does: all siblings first, then each subtree."""
    ids = [next(counter) for _ in fields]
    return [_stamp(field, i, counter) for field, i in zip(fields, ids, strict=True)]


def _stamp(field: pyarrow.Field, field_id: int, counter: itertools.count) -> pyarrow.Field:
    metadata = dict(field.metadata or {})
    metadata[FIELD_ID_KEY] = str(field_id).encode()
    return pyarrow.field(
        field.name, _stamp_type(field.type, counter), nullable=field.nullable, metadata=metadata
    )


def _stamp_type(data_type: pyarrow.DataType, counter: itertools.count) -> pyarrow.DataType:
    types = pyarrow.types
    if types.is_struct(data_type):
        children = [data_type.field(i) for i in range(data_type.num_fields)]
        return pyarrow.struct(_stamp_siblings(children, counter))
    if types.is_list(data_type):
        return pyarrow.list_(_stamp(data_type.field(0), next(counter), counter))
    if types.is_large_list(data_type):
        return pyarrow.large_list(_stamp(data_type.field(0), next(counter), counter))
    if types.is_map(data_type):
        key_id, item_id = next(counter), next(counter)  # both drawn before either descent
        return pyarrow.map_(
            _stamp(data_type.key_field, key_id, counter),
            _stamp(data_type.item_field, item_id, counter),
        )
    return data_type


class ArrowRecordBuilder:
    """Builds a record *class* back out of an Arrow schema or field.

    The inverse of `ArrowFieldBuilder`, for schemas that arrive from outside
    -- a parquet footer, an Iceberg table, another team's contract -- so they
    can be handled with the same machinery as a hand-declared record.

    The round trip is lossless by construction: every generated field is
    `Annotated[..., Arrow(...)]` carrying the exact original type whenever the
    default projection would differ, plus the original metadata and
    description. Nested structs become nested record classes; generated
    classes are keyword-only, because Arrow field order owes nothing to
    Python's defaults-last rule.
    """

    #: Base class generated records extend when none is given.
    BASE: ClassVar[type | None] = None

    def record_class(
        self,
        schema: pyarrow.Schema | pyarrow.StructType,
        name: str | None = None,
        base: type | None = None,
    ) -> type:
        """One record class, one field per Arrow field, in schema order.

        A schema written by `ArrowFieldBuilder` carries `name` and
        `namespace` metadata, and the generated class takes that identity
        back -- so a clone of `Log` *is* named Log, in its
        module. An explicit `name` wins; a schema without the metadata falls
        back to `ArrowRecord`.
        """
        from rekep.records.record import record

        metadata = getattr(schema, "metadata", None) or {}
        annotations: dict[str, Any] = {}
        namespace: dict[str, Any] = {"__annotations__": annotations}
        for field in schema:
            annotations[field.name] = self.annotation(field)
            if field.nullable:
                namespace[field.name] = None
        description = metadata.get(b"description")
        if description:
            namespace["__doc__"] = description.decode()
        module = metadata.get(b"namespace")
        if module:
            namespace["__module__"] = module.decode()
        named = name or (metadata.get(b"name") or b"").decode() or "ArrowRecord"
        cls = type(named, (base or self.BASE or _base(),), namespace)
        return record(kw_only=True)(cls)

    def annotation(self, field: pyarrow.Field) -> Any:
        """The Python annotation that projects back to exactly `field`."""
        inner = self.python_type(field.type, field.name)
        overrides = self.overrides(field, inner)
        if field.nullable:
            inner = inner | None
        return Annotated[inner, overrides] if overrides != Arrow() else inner

    def overrides(self, field: pyarrow.Field, inner: Any) -> Arrow:
        """What `Annotated` must carry so nothing of `field` is lost."""
        metadata = {key.decode(): value.decode() for key, value in (field.metadata or {}).items()}
        metadata.pop(FIELD_ID_KEY.decode(), None)  # restamped on projection, never carried
        description = metadata.pop("description", None)
        partition = metadata.pop(PARTITION_KEY.decode(), None)
        key = metadata.pop(PRIMARY_KEY.decode(), None)
        keep_type = ArrowFieldBuilder().field("probe", inner).type != field.type
        return Arrow(
            type=field.type if keep_type else None,
            metadata=metadata or None,
            description=description,
            partition=True if partition == "identity" else partition,
            key=key is not None or None,
        )

    def python_type(self, data_type: pyarrow.DataType, name: str) -> Any:
        """Plainest Python annotation for one Arrow type; exactness is the
        override's job."""
        types = pyarrow.types
        if types.is_struct(data_type):
            return self.record_class(data_type, _class_name(name))
        if types.is_list(data_type) or types.is_large_list(data_type):
            return list[self.annotation(data_type.field(0))]
        if types.is_map(data_type):
            key = self.python_type(data_type.key_type, f"{name}_key")
            return dict[key, self.annotation(data_type.field(1))]
        if types.is_boolean(data_type):
            return bool
        if types.is_integer(data_type):
            return int
        if types.is_floating(data_type):
            return float
        if types.is_decimal(data_type):
            return decimal.Decimal
        if types.is_timestamp(data_type):
            return datetime.datetime
        if types.is_date(data_type):
            return datetime.date
        if types.is_time(data_type):
            return datetime.time
        if types.is_duration(data_type):
            return datetime.timedelta
        if types.is_binary(data_type) or types.is_large_binary(data_type):
            return bytes
        return str


# -- schema helpers -----------------------------------------------------


def primary_keys(schema: pyarrow.Schema) -> list[str]:
    """Fields declared part of the primary key, in schema order.

    The one reader of `PRIMARY_KEY` metadata: Iceberg's identifier fields,
    Doris' key columns, a DDL `PRIMARY KEY` clause and a `merge_by` upsert
    all mean this same list, so they all come through here rather than each
    re-walking the metadata.
    """
    return [field.name for field in schema if (field.metadata or {}).get(PRIMARY_KEY)]


def partition_keys(schema: pyarrow.Schema) -> dict[str, str]:
    """Declared partition fields, in schema order, mapped to their transform.

    The transform is spelled as it was declared -- `"identity"`, `"day"`,
    `"bucket[16]"` -- and stays a string here: what it means is the reading
    protocol's business (Iceberg parses it, Doris translates it, the file
    writer only handles `identity`).
    """
    found = {}
    for field in schema:
        transform = (field.metadata or {}).get(PARTITION_KEY, b"").decode()
        if transform:
            found[field.name] = transform
    return found


def merge_schemas(target: pyarrow.Schema, incoming: pyarrow.Schema) -> pyarrow.Schema:
    """`target`, extended with the fields only `incoming` has.

    Union by name, and the two halves are treated differently on purpose:

    - A field **both** schemas have keeps `target`'s spelling entirely --
      its type, its nullability, its metadata. That is what makes this a
      *merge* rather than a takeover: the shared columns are still the
      target's, so the data is cast onto them (`cast_batch`), never the
      other way round. A source calling a column `int64` does not get to
      widen a table that declared `int32`.
    - A field **only `incoming`** has is appended, and forced nullable
      whatever the source said: rows already in the target predate the
      column and have nothing to put in it, so a NOT NULL new column is a
      constraint the existing data cannot satisfy. Iceberg refuses exactly
      this, and so does anything else with rows already written.

    New fields are renumbered from after `target`'s highest field id, so
    column identity stays unique across the merged schema -- Iceberg and
    parquet both match columns by id, and a duplicate id is a silently
    wrong read rather than an error.
    """
    known = set(target.names)
    additions = [field for field in incoming if field.name not in known]
    if not additions:
        return target
    counter = itertools.count(_max_field_id(target) + 1)
    fresh = [_stamp(field.with_nullable(True), next(counter), counter) for field in additions]
    return pyarrow.schema([*target, *fresh], metadata=target.metadata)


def _max_field_id(schema: pyarrow.Schema) -> int:
    """The highest `FIELD_ID_KEY` in `schema`, nested fields included, or 0."""
    highest = 0
    for field in schema:
        highest = max(highest, _max_field_id_of(field))
    return highest


def _max_field_id_of(field: pyarrow.Field) -> int:
    highest = int((field.metadata or {}).get(FIELD_ID_KEY, b"0"))
    data_type = field.type
    for index in range(data_type.num_fields):
        highest = max(highest, _max_field_id_of(data_type.field(index)))
    return highest


def cast_batch(
    batch: pyarrow.RecordBatch, schema: pyarrow.Schema, *, safe: bool = False
) -> pyarrow.RecordBatch:
    """`batch` reshaped to `schema`: columns cast, missing filled, extras dropped.

    The gap this closes is the one every real pipeline hits: a transform
    produces *almost* the target shape -- an `int64` where the table wants
    `int32`, a column the source never had, its columns in another order --
    and the write fails on a schema comparison rather than on the data.

    `safe=False` by default, deliberately: this is `pyarrow.compute.cast`'s
    unsafe mode, the one that lets a value narrow or a timestamp lose
    precision instead of raising. A cast to a *target schema* is a
    declaration that the target's types are the authority, so the
    truncation is the intent, not an accident; pass `safe=True` to get
    Arrow's checking back.

    A column the batch does not have is filled with nulls -- but only if the
    target field is nullable. A missing non-nullable field is refused by
    name: filling a NOT NULL column with nulls builds a batch that only
    fails later, at the write, where the cause is much harder to see.
    """
    if batch.schema.equals(schema):
        return batch
    arrays = []
    for field in schema:
        if field.name in batch.schema.names:
            column = batch.column(field.name)
            arrays.append(column if column.type == field.type else column.cast(field.type, safe))
        elif field.nullable:
            arrays.append(pyarrow.nulls(batch.num_rows, field.type))
        else:
            raise ValueError(
                f"column {field.name!r} is missing and not nullable, so it cannot be filled "
                "with nulls; produce it upstream or make the field optional"
            )
    return pyarrow.RecordBatch.from_arrays(arrays, schema=schema)


def cast_reader(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
    schema: pyarrow.Schema,
    *,
    safe: bool = False,
    merge_schema: bool = False,
) -> pyarrow.RecordBatchReader:
    """`cast_batch` over a whole stream, still one batch at a time.

    Takes a plain iterator of batches too -- what `Job.arrow_transform`
    yields -- so a job's output becomes a reader of the target shape in one
    step, without the caller building a `RecordBatchReader` by hand first.

    `merge_schema=True` widens the target with `merge_schemas` first, so a
    column the source has and the target does not is **kept** instead of
    dropped. It has to look at the incoming schema to do that, which for a
    plain iterator means pulling one batch early (put straight back, so
    nothing is lost or read twice); a reader already declares its schema and
    is not touched. An empty iterator leaves the target as it was: there was
    no incoming schema to merge.

    The widened schema is decided **once**, from the reader's own schema or
    the first batch, and every later batch is cast onto it -- a stream is
    one shape, and a `RecordBatchReader` cannot say otherwise. A hand-rolled
    iterator whose batches disagree is therefore resolved in the target's
    favour: a column a later batch drops comes back as nulls, a column only
    a later batch has is dropped. Widening again mid-stream would mean a
    reader whose schema changes under its consumer, which no downstream
    writer accepts.
    """
    if merge_schema:
        source, incoming = _peek_schema(source)
        if incoming is not None:
            schema = merge_schemas(schema, incoming)

    def generate() -> Iterator[pyarrow.RecordBatch]:
        for batch in source:
            yield cast_batch(batch, schema, safe=safe)

    return pyarrow.RecordBatchReader.from_batches(schema, generate())


def _peek_schema(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
) -> tuple[Any, pyarrow.Schema | None]:
    """`(source, its schema)`, reading one batch only when it has to.

    A `RecordBatchReader` states its schema up front, so it comes back
    untouched and still fully lazy. A plain iterator only reveals its shape
    by producing a batch, so one is pulled and then chained back on the
    front -- the caller still sees every batch, in order, exactly once.
    """
    if isinstance(source, pyarrow.RecordBatchReader):
        return source, source.schema
    iterator = iter(source)
    first = next(iterator, None)
    if first is None:
        return iter(()), None
    return itertools.chain([first], iterator), first.schema


def _class_name(field_name: str) -> str:
    """`order_book` -> `OrderBook`: a field name as a class name."""
    cleaned = re.sub(r"\W", "_", field_name)
    return "".join(part.capitalize() or "_" for part in cleaned.split("_")) or "Anonymous"


def _base() -> type:
    from rekep.records.record import Record

    return Record
