"""Records: dataclasses that are data products."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import functools
import io
import json
import os
import pathlib
import re
import tomllib
import types
import typing
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Self, Union, get_args, get_origin, get_type_hints

import pyarrow
import pyarrow.fs

from rekep.convert import Convertible
from rekep.records.annotations import (
    MAPPING_ORIGINS,
    NONE_TYPE,
    SEQUENCE_ORIGINS,
    SET_ORIGINS,
    docstring_summary,
    hide_private,
    item_annotation,
    unwrap_annotated,
)
from rekep.records.arrow import (
    ArrowFieldBuilder,
    ArrowRecordBuilder,
    cast_batch,
    cast_reader,
    merge_schemas,
    partition_keys,
    primary_keys,
)
from rekep.records.ddl import IcebergDdlBuilder
from rekep.require import require

#: A destination or source: an open file, a path, a URI, or -- to be handed the
#: bytes back instead of writing them -- None, `str` or `bytes`.
Target = typing.Union[str, os.PathLike[str], typing.IO[bytes], typing.IO[str], type, None]  # noqa: UP007

#: A path is treated as a URI only with an explicit scheme, so a Windows drive
#: letter (`C:\...`) is never mistaken for one.
URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")


class dualmethod:
    """A method that binds to the instance, or to the class when there is none.

    The `into_*` serialisers are dual: on an instance they dump its values, on
    the class they dump its declaration. One descriptor keeps that a single
    method with a single name instead of a parallel `class_into_*` family.
    """

    def __init__(self, method: Any) -> None:
        self.method = method
        functools.update_wrapper(self, method)

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        return types.MethodType(self.method, instance if instance is not None else owner)


def record(cls: type | None = None, /, **kwargs: Any) -> Any:
    """Declare a data product: a dataclass whose fields are its schema.

    Wraps `dataclasses.dataclass`, so every keyword it takes is accepted here,
    and drops any annotation whose name starts with `__` before the dataclass
    machinery sees it. That is how a record carries private working state --
    caches, handles, memoised views -- without it becoming a field, an
    `__init__` argument, or a column::

        @record
        class Venue(Record):
            mic: str
            __cache: dict = {}   # state, not schema

    Python mangles those names inside a class body, so both the written and the
    mangled spelling are excluded.
    """

    def wrap(target: type) -> type:
        hide_private(target)
        return dataclasses.dataclass(**kwargs)(target)

    return wrap if cls is None else wrap(cls)


class Record(Convertible):
    """A data product: a dataclass that is its own schema and its own file.

    Subclasses are ordinary dataclasses -- declare them with `@record` so that
    `__`-prefixed annotations stay out of the schema. The field declarations
    drive everything: dumping walks them recursively so nested records become
    nested tables, loading walks them in reverse so a nested mapping comes back
    as the record it declares, and `into_arrow_schema` projects the same
    declarations onto Arrow.

    Two rules keep the three text formats interchangeable. Fields that are None
    are omitted rather than written as null, because TOML has no null and
    because a missing key falls back to the dataclass default on the way in.
    Unknown keys are ignored on load, so a config carrying extra sections still
    parses.

        @record
        class Venue(Record):
            mic: str
            timeout: float | None = None

    JSON always works, and so does reading TOML; writing TOML needs the `toml`
    extra and YAML needs `yaml`, each raising an `ImportError` naming the extra
    if it is missing.

    Every text method accepts an open file, a path or a URI, and an optional
    `filesystem` for storage Arrow cannot infer from the string alone. Pass
    nothing -- or `str`/`bytes` -- to be handed the encoded bytes instead. The
    generic `from_`/`into_` pick the format from the extension or the requested
    type, so a caller does not have to branch.
    """

    REDIRECTS: ClassVar[dict[Any, str]] = {
        ".yaml": "yaml",
        ".yml": "yaml",
        ".toml": "toml",
        ".json": "json",
        dict: "dict",
        Mapping: "dict",
        pyarrow.Schema: "arrow_schema",
        pyarrow.Field: "arrow_field",
        pyarrow.DataType: "arrow_type",
    }

    #: Builder used to project this record onto Arrow. Override to extend it.
    ARROW_BUILDER: ClassVar[type[ArrowFieldBuilder]] = ArrowFieldBuilder

    #: Builder used to project this record onto Iceberg. None means the
    #: default, bound lazily -- the Iceberg module also declares records, so
    #: a top-level import here would be circular.
    ICEBERG_BUILDER: ClassVar[Any] = None

    @classmethod
    def _iceberg_builder(cls) -> Any:
        if cls.ICEBERG_BUILDER is not None:
            return cls.ICEBERG_BUILDER
        from rekep.records.iceberg import IcebergFieldBuilder

        return IcebergFieldBuilder

    #: Builder used to render this record as Iceberg DDL. Override to extend it.
    DDL_BUILDER: ClassVar[type[IcebergDdlBuilder]] = IcebergDdlBuilder

    # -- arrow --------------------------------------------------------------
    #
    # All three are cached per class: a projection walks the type hints, the
    # docstrings and every nested record, and none of that can change once the
    # class is declared. Arrow schemas, types and fields are immutable, so the
    # cached value is safe to hand out. Redeclare the class to invalidate.

    @classmethod
    @functools.cache
    def into_arrow_schema(cls) -> pyarrow.Schema:
        """Flat Arrow schema of this record's fields."""
        return cls.ARROW_BUILDER().schema(cls)

    @classmethod
    @functools.cache
    def into_arrow_type(cls) -> pyarrow.DataType:
        """This record as an Arrow struct type."""
        return cls.ARROW_BUILDER().struct(cls)

    @classmethod
    @functools.cache
    def into_arrow_field(cls, name: str | None = None, *, nullable: bool = False) -> pyarrow.Field:
        """This record as one Arrow field, non-nullable unless asked otherwise."""
        summary = docstring_summary(cls)
        return pyarrow.field(
            name or cls.__name__,
            cls.into_arrow_type(),
            nullable=nullable,
            metadata={"description": summary} if summary else None,
        )

    # -- reshaping an incoming batch onto this record's schema ---------------

    @classmethod
    def cast_arrow_batch(cls, batch: pyarrow.RecordBatch, *, safe: bool = False) -> Any:
        """`batch` reshaped onto this record's schema: cast, filled, reordered.

        The record is the authority on what the data *is*, so a batch that
        is only nearly the right shape -- a wider integer, a column in
        another order, one this source never produced -- is adapted to it
        rather than rejected. Unsafe by default: see `records.arrow.cast_batch`.
        """
        return cast_batch(batch, cls.into_arrow_schema(), safe=safe)

    @classmethod
    def cast_arrow_reader(
        cls, reader: Any, *, safe: bool = False, merge_schema: bool = False
    ) -> pyarrow.RecordBatchReader:
        """`cast_arrow_batch` over a whole stream, still one batch at a time.

        Takes a plain iterator of batches too, so `Job.arrow_transform`'s
        output becomes a reader of this record's shape in one step.

        `merge_schema=True` keeps the columns the stream has and this record
        does not, appended after the declared ones rather than dropped --
        see `records.arrow.merge_schemas`.
        """
        return cast_reader(reader, cls.into_arrow_schema(), safe=safe, merge_schema=merge_schema)

    @classmethod
    def merge_arrow_schema(cls, incoming: pyarrow.Schema) -> pyarrow.Schema:
        """This record's schema, extended with the fields only `incoming` has.

        The target of a `merge_schema` write: shared columns stay this
        record's (so the data is cast onto them), new ones are appended,
        nullable, with fresh field ids.
        """
        return merge_schemas(cls.into_arrow_schema(), incoming)

    @classmethod
    def primary_keys(cls) -> list[str]:
        """Fields this record declares `Arrow(key=True)`, in declaration order.

        The same list Iceberg calls identifier fields, Doris calls key
        columns and an upsert joins on -- declared once, read from the Arrow
        schema like every other projection.
        """
        return primary_keys(cls.into_arrow_schema())

    @classmethod
    def partition_keys(cls) -> dict[str, str]:
        """Fields this record declares `Arrow(partition=...)`, mapped to transform."""
        return partition_keys(cls.into_arrow_schema())

    # -- iceberg ------------------------------------------------------------
    #
    # Cached on the same terms as the Arrow projections, which they are built
    # from. pyiceberg is imported inside the builder, not here: importing
    # `pyiceberg.io.pyarrow` costs seconds, and a record that never reaches a
    # table should not pay it.

    @classmethod
    @functools.cache
    def into_iceberg_schema(cls) -> Any:
        """This record as a `pyiceberg.schema.Schema`, ids numbered from one."""
        return cls._iceberg_builder()().schema(cls)

    @classmethod
    @functools.cache
    def into_iceberg_type(cls) -> Any:
        """This record as an Iceberg struct type."""
        return cls._iceberg_builder()().struct(cls)

    @classmethod
    @functools.cache
    def into_iceberg_field(
        cls, name: str | None = None, *, field_id: int = 1, required: bool = True
    ) -> Any:
        """This record as one Iceberg field, required unless asked otherwise."""
        return cls._iceberg_builder()().field(cls, name, field_id=field_id, required=required)

    #: Builder used to rebuild a record class from an Arrow schema.
    ARROW_RECORD_BUILDER: ClassVar[type[ArrowRecordBuilder]] = ArrowRecordBuilder

    @classmethod
    def from_arrow_schema(cls, schema: pyarrow.Schema, name: str | None = None) -> type[Self]:
        """Build a record class whose projection is exactly `schema`.

        For schemas that arrive from outside -- a parquet footer, an Iceberg
        table -- so they get the same machinery as a hand-declared record.
        """
        return cls.ARROW_RECORD_BUILDER().record_class(schema, name, base=cls)

    @classmethod
    def from_arrow_field(cls, field: pyarrow.Field) -> type[Self]:
        """Build a record class from one field: its struct, or it alone."""
        builder = cls.ARROW_RECORD_BUILDER()
        if pyarrow.types.is_struct(field.type):
            return builder.record_class(field.type, _identifier(field.name), base=cls)
        return builder.record_class(pyarrow.schema([field]), _identifier(field.name), base=cls)

    @classmethod
    @functools.cache
    def into_iceberg_partition_spec(cls) -> Any:
        """The `pyiceberg.partitioning.PartitionSpec` this record declares."""
        return cls._iceberg_builder()().partition_spec(cls)

    @classmethod
    def into_doris_ddl(cls, table_name: str | None = None, **kwargs: Any) -> str:
        """This record as a Doris CREATE TABLE statement.

        Imported at the point of use to keep record.py free of a circular
        import -- the Doris builder's config is itself a Record.
        """
        from rekep.records.doris import DorisDdlBuilder

        builder: Any = getattr(cls, "DORIS_BUILDER", DorisDdlBuilder)
        return builder().create_table(cls, table_name or _snake(cls.__name__), **kwargs)

    @classmethod
    def doris_table_name(cls) -> str:
        """Default Doris table name: the record's snake_case name."""
        return _snake(cls.__name__)

    @classmethod
    def into_iceberg_ddl(cls, table_name: str | None = None, **kwargs: Any) -> str:
        """This record as a CREATE TABLE statement.

        Not cached: `kwargs` carries unhashable mappings, and DDL is emitted
        once per deploy, not once per row.
        """
        return cls.DDL_BUILDER().create_table(cls, table_name or _snake(cls.__name__), **kwargs)

    # -- dump ---------------------------------------------------------------

    @dualmethod
    def into_dict(self) -> dict[str, Any]:
        """As plain containers: an instance's values, or a class's declaration.

        Called on the class, the dump is the *contract* -- one entry per Arrow
        field with its type, nullability, description and metadata -- so a
        schema can be reviewed, diffed and published without a single row.
        """
        if isinstance(self, type):
            return _describe(self)
        if not dataclasses.is_dataclass(self):
            raise TypeError(f"{type(self).__name__} must be a dataclass to be serialised")
        return _encode(self)

    @dualmethod
    def into_yaml(
        self, target: Target = None, filesystem: pyarrow.fs.FileSystem | None = None
    ) -> bytes | None:
        """Write this record to `target` as YAML, or return the bytes."""
        yaml = require("yaml", "yaml")
        payload = yaml.safe_dump(self.into_dict(), sort_keys=False, allow_unicode=True)
        return _write(payload.encode(), target, filesystem)

    @dualmethod
    def into_toml(
        self, target: Target = None, filesystem: pyarrow.fs.FileSystem | None = None
    ) -> bytes | None:
        """Write this record to `target` as TOML, or return the bytes."""
        tomli_w = require("tomli_w", "toml")
        return _write(tomli_w.dumps(_toml_ordered(self.into_dict())).encode(), target, filesystem)

    @dualmethod
    def into_json(
        self, target: Target = None, filesystem: pyarrow.fs.FileSystem | None = None
    ) -> bytes | None:
        """Write this record to `target` as JSON, or return the bytes."""
        payload = json.dumps(self.into_dict(), indent=2, ensure_ascii=False) + "\n"
        return _write(payload.encode(), target, filesystem)

    # -- load ---------------------------------------------------------------

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Rebuild this record from plain containers."""
        return _decode_dataclass(cls, mapping)

    @classmethod
    def from_yaml(cls, source: Target, filesystem: pyarrow.fs.FileSystem | None = None) -> Self:
        """Read this record from `source` as YAML."""
        yaml = require("yaml", "yaml")
        return cls.from_dict(yaml.safe_load(_read(source, filesystem)) or {})

    @classmethod
    def from_toml(cls, source: Target, filesystem: pyarrow.fs.FileSystem | None = None) -> Self:
        """Read this record from `source` as TOML."""
        return cls.from_dict(tomllib.loads(_read(source, filesystem).decode()))

    @classmethod
    def from_json(cls, source: Target, filesystem: pyarrow.fs.FileSystem | None = None) -> Self:
        """Read this record from `source` as JSON."""
        return cls.from_dict(json.loads(_read(source, filesystem)))


# -- encoding ---------------------------------------------------------------


def _encode(value: Any) -> Any:
    """Reduce `value` to containers every one of the three encoders accepts.

    None is dropped rather than emitted: TOML cannot express it, and on the way
    back a missing key is what lets the dataclass default apply.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            f.name: _encode(attribute)
            for f in dataclasses.fields(value)
            if (attribute := getattr(value, f.name)) is not None
        }
    if isinstance(value, enum.Enum):  # before str: a str-valued enum is also a str
        return _encode(value.value)
    if isinstance(value, Mapping):
        return {str(k): _encode(v) for k, v in value.items() if v is not None}
    if isinstance(value, (str, bytes)):
        return value
    if isinstance(value, (Sequence, set, frozenset)):
        return [_encode(v) for v in value if v is not None]
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (pathlib.PurePath, uuid.UUID, decimal.Decimal)):
        return str(value)
    return value


def _toml_ordered(value: Any) -> Any:
    """Reorder mappings so every scalar precedes every table.

    TOML binds a bare key to whichever table header last opened, so a scalar
    written after a nested table would silently land inside it. Field order is
    the author's, not TOML's, so it is fixed up here rather than in `_encode`.
    """
    if isinstance(value, dict):
        items = [(k, _toml_ordered(v)) for k, v in value.items()]
        return dict(
            [(k, v) for k, v in items if not _is_table(v)]
            + [(k, v) for k, v in items if _is_table(v)]
        )
    if isinstance(value, list):
        return [_toml_ordered(v) for v in value]
    return value


def _is_table(value: Any) -> bool:
    """Whether TOML would render `value` as a table or an array of tables."""
    if isinstance(value, dict):
        return True
    return isinstance(value, list) and bool(value) and all(isinstance(v, dict) for v in value)


# -- decoding ---------------------------------------------------------------


def _decode_dataclass(cls: type, mapping: Mapping[str, Any]) -> Any:
    """Build `cls` from `mapping`, decoding each value to its declared type."""
    if not dataclasses.is_dataclass(cls):
        raise TypeError(f"{cls.__name__} must be a dataclass to be deserialised")
    if not isinstance(mapping, Mapping):
        raise TypeError(f"{cls.__name__} expects a mapping, got {type(mapping).__name__}")
    hints = get_type_hints(cls)
    return cls(
        **{
            f.name: _decode(mapping[f.name], hints.get(f.name, Any))
            for f in dataclasses.fields(cls)
            if f.init and f.name in mapping
        }
    )


def _decode(value: Any, annotation: Any) -> Any:
    """Coerce a loaded value to `annotation`, recursing through containers."""
    if value is None:
        return None

    _, annotation = unwrap_annotated(annotation)
    origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        return _decode_union(value, get_args(annotation))
    if origin in SEQUENCE_ORIGINS:
        return [_decode(v, item_annotation(annotation)) for v in value]
    if origin is tuple:
        return _decode_tuple(value, get_args(annotation))
    if origin in SET_ORIGINS:
        container = frozenset if origin is frozenset else set
        return container(_decode(v, item_annotation(annotation)) for v in value)
    if origin in MAPPING_ORIGINS:
        key_type, value_type = (get_args(annotation) or (Any, Any))[:2]
        return {_decode(k, key_type): _decode(v, value_type) for k, v in value.items()}

    if annotation is Any:
        return value  # untyped: trust the plain container a text format gave back
    if dataclasses.is_dataclass(annotation):
        return _decode_dataclass(annotation, value)
    if isinstance(annotation, type):
        return _decode_scalar(value, annotation)
    return value


def _decode_union(value: Any, args: tuple[Any, ...]) -> Any:
    """Try each non-None member in declaration order, first success wins."""
    for candidate in (a for a in args if a is not NONE_TYPE):
        try:
            return _decode(value, candidate)
        except (TypeError, ValueError, KeyError, AttributeError):
            continue
    return value


def _decode_tuple(value: Any, args: tuple[Any, ...]) -> tuple[Any, ...]:
    if not args:
        return tuple(value)
    if len(args) == 2 and args[1] is Ellipsis:
        return tuple(_decode(v, args[0]) for v in value)
    return tuple(_decode(v, a) for v, a in zip(value, args, strict=True))


def _decode_scalar(value: Any, annotation: type) -> Any:
    if isinstance(value, annotation) and not issubclass(annotation, enum.Enum):
        return value  # YAML and TOML already give back dates, bools and numbers
    if issubclass(annotation, enum.Enum):
        return annotation(value)
    if issubclass(annotation, datetime.datetime):  # before date: datetime is a date
        return annotation.fromisoformat(value)
    if issubclass(annotation, (datetime.date, datetime.time)):
        return annotation.fromisoformat(value)
    if issubclass(annotation, (pathlib.PurePath, uuid.UUID, decimal.Decimal)):
        return annotation(value)
    if issubclass(annotation, (str, int, float, bool)):
        return annotation(value)
    return value


# -- io ---------------------------------------------------------------------


def _resolve(target: Any, filesystem: pyarrow.fs.FileSystem | None) -> tuple[Any, str]:
    """Pair `target` with the filesystem that can open it."""
    path = os.fspath(target)
    if filesystem is not None:
        return filesystem, path
    if URI_SCHEME.match(path):
        return pyarrow.fs.FileSystem.from_uri(path)
    return pyarrow.fs.LocalFileSystem(), os.path.abspath(path)


def _write(
    payload: bytes, target: Target, filesystem: pyarrow.fs.FileSystem | None
) -> bytes | None:
    """Write `payload` to `target`, or return it when there is nowhere to write.

    `None`, `str` and `bytes` are all "hand it back": the two types are there so
    a caller can say which they mean at the call site rather than relying on a
    bare `None`.
    """
    if target is None or target is str or target is bytes:
        return payload
    if hasattr(target, "write"):
        target.write(payload.decode() if _is_text(target) else payload)
        return None
    fs, path = _resolve(target, filesystem)
    with fs.open_output_stream(path) as stream:
        stream.write(payload)
    return None


def _read(source: Target, filesystem: pyarrow.fs.FileSystem | None) -> bytes:
    if isinstance(source, bytes):
        return source
    if hasattr(source, "read"):
        data = source.read()
        return data.encode() if isinstance(data, str) else data
    fs, path = _resolve(source, filesystem)
    with fs.open_input_stream(path) as stream:
        return stream.read()


def _is_text(target: Any) -> bool:
    """Whether an open file wants str rather than bytes."""
    return isinstance(target, io.TextIOBase) or "b" not in getattr(target, "mode", "b")


def _snake(name: str) -> str:
    """`Log` -> `log_record`, for default table names."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


def _identifier(name: str) -> str:
    """A field name as a Python class name."""
    cleaned = re.sub(r"\W", "_", name)
    return "".join(part.capitalize() or "_" for part in cleaned.split("_")) or "Anonymous"


def _describe(cls: type) -> dict[str, Any]:
    """A class's declaration as plain containers, one entry per Arrow field.

    The envelope is just `name`: where the class lives is Python's business,
    and the Arrow schema metadata still carries the namespace for tooling that
    needs to resolve it.
    """
    schema: pyarrow.Schema = cls.into_arrow_schema()
    metadata = schema.metadata or {}
    described: dict[str, Any] = {
        "name": (metadata.get(b"name") or cls.__qualname__.encode()).decode(),
    }
    summary = metadata.get(b"description")
    if summary:
        described["description"] = summary.decode()
    described["fields"] = [_describe_field(field) for field in schema]
    return described


def _describe_field(field: pyarrow.Field) -> dict[str, Any]:
    """One field as plain containers, nesting recursively.

    A struct is a `fields:` list like the top level, a list an `item:`, a map
    a `key:`/`value:` pair -- never a flat `struct<...>` string, which would
    bury the nested descriptions it exists to show. Scalars stay one line.
    """
    nested = _describe_type(field.type)
    described: dict[str, Any] = {"name": field.name, "type": nested.pop("type")}
    if field.nullable:
        described["nullable"] = True

    metadata = {key.decode(): value.decode() for key, value in (field.metadata or {}).items()}
    metadata["iceberg:field_id"] = metadata.pop("PARQUET:field_id", None)  # iceberg's, really
    description = metadata.pop("description", None)
    if description:
        described["description"] = description

    # Protocol-prefixed keys group under their protocol; the rest stay ours.
    plain: dict[str, Any] = {}
    protocols: dict[str, dict[str, Any]] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        protocol, prefixed, suffix = key.partition(":")
        if prefixed:
            protocols.setdefault(protocol, {})[suffix] = _plain_value(suffix, value)
        else:
            plain[key] = value
    if plain:
        described["metadata"] = plain
    described.update(protocols)
    described.update(nested)  # fields/item/key/value blocks read best last
    return described


def _plain_value(key: str, value: str) -> Any:
    """Cosmetics for protocol values: ids are ints, flags are booleans."""
    if value.isdigit():
        return int(value)
    if value in ("identity", "true", "yes"):
        return True
    return value


def _describe_type(data_type: pyarrow.DataType) -> dict[str, Any]:
    types = pyarrow.types
    if types.is_struct(data_type):
        return {
            "type": "struct",
            "fields": [_describe_field(data_type.field(i)) for i in range(data_type.num_fields)],
        }
    if types.is_list(data_type) or types.is_large_list(data_type):
        return {"type": "list", "item": _describe_item(data_type.field(0))}
    if types.is_map(data_type):
        return {
            "type": "map",
            "key": _describe_item(data_type.key_field),
            "value": _describe_item(data_type.item_field),
        }
    return {"type": str(data_type)}


def _describe_item(field: pyarrow.Field) -> dict[str, Any]:
    """A list item or map value: a field whose name is the builder's, not ours."""
    described = _describe_field(field)
    described.pop("name", None)
    return described
