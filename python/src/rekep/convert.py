"""Generic conversion dispatch, and the dataclass serialisation it dispatches to."""

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
from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, Self, Union, get_args, get_origin, get_type_hints

import pyarrow.fs

from rekep.annotations import (
    MAPPING_ORIGINS,
    NONE_TYPE,
    SEQUENCE_ORIGINS,
    SET_ORIGINS,
    item_annotation,
    unwrap_annotated,
)
from rekep.filesystems import resolve
from rekep.require import require

#: Splits a path or URI on either separator, whatever platform wrote it.
SEPARATORS = re.compile(r"[\\/]")

#: A destination or source: an open file, a path, a URI, or -- to be handed the
#: bytes back instead of writing them -- None, `str` or `bytes`.
Target = typing.Union[str, os.PathLike[str], typing.IO[bytes], typing.IO[str], type, None]  # noqa: UP007

_REDIRECTS = MappingProxyType(
    {
        ".yaml": "yaml",
        ".yml": "yaml",
        ".toml": "toml",
        ".json": "json",
        dict: "dict",
        Mapping: "dict",
    }
)


class Convertible:
    """Gives a class paired `from_*` builders and `into_*` converters."""

    __slots__ = ()

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Dispatch keys for generic `from_` and `into_` inference."""
        return _REDIRECTS

    @classmethod
    def from_(cls, source: Any, *args: Any, **kwargs: Any) -> Self:
        """Build an instance from `source`, inferring which builder it means."""
        return getattr(cls, f"from_{cls.redirect_of(source)}")(source, *args, **kwargs)

    def into_(self, target: Any, *args: Any, **kwargs: Any) -> Any:
        """Convert to `target`, inferring which converter it means.

        A type is the requested result and is consumed here; anything else is a
        destination and is handed on.
        """
        convert = getattr(self, f"into_{self.redirect_of(target)}")
        if isinstance(target, type):
            return convert(*args, **kwargs)
        return convert(target, *args, **kwargs)

    @classmethod
    def from_file(cls, source: Target, filesystem: pyarrow.fs.FileSystem | None = None) -> Self:
        """Build from a document, its format taken from the name's extension."""
        redirects = cls.into_redirects()
        for key in cls._keys(source):
            stem = redirects.get(key) if isinstance(key, str) else None
            if stem:
                return getattr(cls, f"from_{stem}")(source, filesystem)
        formats = sorted(key for key in redirects if isinstance(key, str))
        raise ValueError(
            f"{_name_of(source) or source!r} is not a document this can read: "
            f"name it {', '.join(formats)}"
        )

    @classmethod
    def redirect_of(cls, value: Any, redirects: Mapping[Any, str] | None = None) -> str:
        """Method stem `value` redirects to, most specific key first.

        `redirects` is the mapping to read, defaulting to `into_redirects()`.
        A class with a second family of methods to infer between
        (casting, writing) passes its own rather than reimplementing the
        lookup.
        """
        redirects = cls.into_redirects() if redirects is None else redirects
        for key in cls._keys(value):
            stem = redirects.get(key)
            if stem is not None:
                return stem
        for key, stem in redirects.items():
            if isinstance(key, type) and _matches(value, key):
                return stem
        raise TypeError(f"{cls.__name__} cannot infer a conversion for {value!r}")

    @classmethod
    def _keys(cls, value: Any) -> Iterator[Any]:
        """Candidate dispatch keys for `value`, narrowest first.

        Extensions are yielded longest-first so `.txt.gz` can be claimed by a
        class that cares, before the plain `.gz` fallback.
        """
        if isinstance(value, type):
            yield from value.__mro__
            return
        name = _name_of(value)
        if name:
            suffixes = _suffixes(name)
            for start in range(len(suffixes)):
                yield "".join(suffixes[start:])
        yield from type(value).__mro__

    # -- dump ---------------------------------------------------------------

    def into_dict(self) -> dict[str, Any]:
        """This instance's values as plain containers, nested ones included."""
        if not dataclasses.is_dataclass(self):
            raise TypeError(f"{type(self).__name__} must be a dataclass to be serialised")
        return _encode_dataclass(self)

    def into_yaml(
        self, target: Target = None, filesystem: pyarrow.fs.FileSystem | None = None
    ) -> bytes | None:
        """Write this instance to `target` as YAML, or return the bytes."""
        yaml = require("yaml", "yaml")
        payload = yaml.safe_dump(self.into_dict(), sort_keys=False, allow_unicode=True)
        return _write(payload.encode(), target, filesystem)

    def into_toml(
        self, target: Target = None, filesystem: pyarrow.fs.FileSystem | None = None
    ) -> bytes | None:
        """Write this instance to `target` as TOML, or return the bytes."""
        tomli_w = require("tomli_w", "toml")
        return _write(tomli_w.dumps(_toml_ordered(self.into_dict())).encode(), target, filesystem)

    def into_json(
        self, target: Target = None, filesystem: pyarrow.fs.FileSystem | None = None
    ) -> bytes | None:
        """Write this instance to `target` as JSON, or return the bytes."""
        payload = json.dumps(self.into_dict(), indent=2, ensure_ascii=False) + "\n"
        return _write(payload.encode(), target, filesystem)

    # -- load ---------------------------------------------------------------

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Rebuild an instance from plain containers."""
        return _decode_dataclass(cls, mapping)

    @classmethod
    def from_yaml(cls, source: Target, filesystem: pyarrow.fs.FileSystem | None = None) -> Self:
        """Read an instance from `source` as YAML."""
        yaml = require("yaml", "yaml")
        return cls.from_dict(yaml.safe_load(_read(source, filesystem)) or {})

    @classmethod
    def from_toml(cls, source: Target, filesystem: pyarrow.fs.FileSystem | None = None) -> Self:
        """Read an instance from `source` as TOML."""
        return cls.from_dict(tomllib.loads(_read(source, filesystem).decode()))

    @classmethod
    def from_json(cls, source: Target, filesystem: pyarrow.fs.FileSystem | None = None) -> Self:
        """Read an instance from `source` as JSON."""
        return cls.from_dict(json.loads(_read(source, filesystem)))


# -- dispatch ---------------------------------------------------------------


def _matches(value: Any, key: type) -> bool:
    """Whether `value` -- a requested type or a plain value -- fits `key`."""
    return issubclass(value, key) if isinstance(value, type) else isinstance(value, key)


def _name_of(value: Any) -> str:
    """The path `value` denotes: itself if it is one, else its `name`."""
    if isinstance(value, (str, os.PathLike)):
        return os.fspath(value)
    return str(getattr(value, "name", "") or "")


def _suffixes(name: str) -> list[str]:
    """Extensions of `name`, taken from its last path segment.

    Split by hand rather than through `PurePath`, so a URI scheme is not read as
    a Windows drive letter.
    """
    return pathlib.PurePosixPath(SEPARATORS.split(name)[-1]).suffixes


# -- encoding ---------------------------------------------------------------


#: Types that encode as themselves, matched on the **exact** type rather than
#: with `isinstance`. Almost every field of almost every row is one of these,
#: and settling them in one frozenset probe is what stopped `_encode` walking
#: a six-branch subclass chain per field: on ten thousand `Order` rows it was
#: 678,000 `isinstance` calls and 56.5 us a row.
#:
#: Exact, and that is the point: `Ranged` is an `int` and must not pass
#: through, `datetime` is a `date` and must not either. A subclass falls to the
#: chain below and is encoded as what it is.
_VERBATIM = frozenset({int, float, str, bool, bytes, type(None)})


@functools.cache
def _members(cls: type) -> tuple[str, ...]:
    """A dataclass's field names, read once per class rather than once per row.

    `dataclasses.fields` walks `__dataclass_fields__` and filters on every
    call, and a row of forty fields with a nested shape in it called it twice.
    """
    return tuple(member.name for member in dataclasses.fields(cls))


@functools.cache
def _dumps_itself(cls: Any) -> bool:
    """`_owns(cls, "into_dict")`, cached: it is a property of the class."""
    return _owns(cls, "into_dict")


def _encode(value: Any) -> Any:
    """Reduce `value` to containers every one of the three encoders accepts."""
    kind = type(value)
    if kind in _VERBATIM:
        return value
    if _dumps_itself(kind):
        return _encode(value.into_dict())
    if dataclasses.is_dataclass(kind):
        return _encode_dataclass(value)
    if isinstance(value, enum.Enum):  # before str: a str-valued enum is also a str
        return _encode(value.value)
    if isinstance(value, Mapping):
        return {str(k): _encode(v) for k, v in value.items()}
    if isinstance(value, (str, bytes)):
        return value
    if isinstance(value, (Sequence, set, frozenset)):
        return [one if type(one) in _VERBATIM else _encode(one) for one in value]
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (pathlib.PurePath, uuid.UUID, decimal.Decimal)):
        return str(value)
    return value


def _encode_dataclass(value: Any) -> dict[str, Any]:
    """A dataclass without redispatching through an overridden `into_dict`."""
    # The `_VERBATIM` probe is repeated here rather than left to the call: a
    # row of forty columns is forty calls that do nothing else.
    encoded = {}
    for name in _members(type(value)):
        attribute = getattr(value, name)
        if attribute is None:
            continue
        encoded[name] = attribute if type(attribute) in _VERBATIM else _encode(attribute)
    return encoded


def _owns(cls: Any, method: str) -> bool:
    """Whether `cls` defines its own `method` rather than inheriting ours."""
    if not isinstance(cls, type) or not issubclass(cls, Convertible):
        return False
    mine = getattr(getattr(cls, method), "__func__", getattr(cls, method))
    ours = getattr(getattr(Convertible, method), "__func__", getattr(Convertible, method))
    return mine is not ours


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
        # A text format gives a mapping back; Arrow gives a list of pairs, which
        # is what a `map` column reads as. Both are the same mapping.
        pairs = value.items() if isinstance(value, Mapping) else value
        return {_decode(k, key_type): _decode(v, value_type) for k, v in pairs}

    if annotation is Any:
        return value  # untyped: trust the plain container a text format gave back
    if _owns(annotation, "from_dict"):
        return annotation.from_dict(value)  # the other half of `_encode`'s rule
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
    if isinstance(value, Mapping) and args and not (len(args) == 2 and args[1] is Ellipsis):
        names = (
            ("key", "value")
            if len(args) == 2 and set(value) == {"key", "value"}
            else tuple(f"f{index}" for index in range(len(args)))
        )
        if set(value) != set(names):
            raise ValueError(f"tuple expects struct members {names}, got {tuple(value)}")
        value = tuple(value[name] for name in names)
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
    """Pair `target` with the filesystem that can open it.

    Through the one parser, so a document is read from wherever data is --
    a path, a URI, an object store with an endpoint and a secret in it.
    """
    path = os.fspath(target)
    if filesystem is not None:
        return filesystem, path
    return resolve(path)


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
