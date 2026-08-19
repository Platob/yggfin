"""Generic conversion dispatch shared by every rekep class."""

from __future__ import annotations

import os
import pathlib
import re
from collections.abc import Iterator, Mapping
from typing import Any, ClassVar, Self

#: Splits a path or URI on either separator, whatever platform wrote it.
SEPARATORS = re.compile(r"[\\/]")


class Convertible:
    """Gives a class paired `from_*` builders and `into_*` converters.

    Every conversion is a named method, so it can be called directly, read in a
    traceback, and overridden by a subclass::

        Book.from_yaml("book.yaml")     # build
        book.into_toml("book.toml")     # convert

    `from_` and `into_` are the generic forms: they infer which named method the
    argument means and redirect to it, so a caller holding a path from config or
    a type from a signature does not have to branch::

        Book.from_("book.yaml")         # -> from_yaml
        book.into_("book.toml")         # -> into_toml
        log.into_(pyarrow.Table)        # -> into_arrow_table

    The rule is that a *type* argument asks "convert to this", so it is consumed
    by the dispatch, while a *value* argument is a source or a destination and
    is passed through. Subclasses declare what may be redirected to in
    `REDIRECTS`, keyed by file extension or by type.
    """

    #: Dispatch key -> `from_`/`into_` method stem. Keys are extensions
    #: (".yaml") matched against a path, or types matched against the argument.
    REDIRECTS: ClassVar[Mapping[Any, str]] = {}

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
    def redirect_of(cls, value: Any) -> str:
        """Method stem `value` redirects to, most specific key first."""
        for key in cls._keys(value):
            stem = cls.REDIRECTS.get(key)
            if stem is not None:
                return stem
        for key, stem in cls.REDIRECTS.items():
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
