"""The classes a deployment declares, found by name.

A side file has to point at Python somehow. It used to do that with a dotted
path -- `rekep.models.Log`, `rekep.jobs.FilesToLogs` -- which named the class
by *where it happens to live*: a file move renamed every reference to it, two
spellings of one class (a module and its re-export) looked like two things,
and a data product's identity in a config was an import statement wearing a
disguise.

So nothing points at a module any more. `@record` **declares** the class it
decorates under its own name, `find` looks a name back up, and what a side
file carries is a name (`files_to_logs`) or, for a record, the URI that names
it like every other resource here (`rekep:///records/log`).

Declaring means importing: a class Python has never executed cannot be in a
registry. rekep imports its own `models` and `jobs` packages for that reason,
and `REKEP_MODULES` is how a deployment adds its own -- a comma-separated list
of *modules*, imported once, the first time a lookup misses. That is the
honest shape of the problem (Python must run the file), rather than smuggling
the import back into every reference.
"""

from __future__ import annotations

import importlib
import os
import re
from typing import Any

#: Every declared class, by name. A list per name rather than one class:
#: a duplicate is only a problem for whoever looks that name up, and refusing
#: it at declaration would mean two test modules could not each declare a
#: `Venue`.
DECLARED: dict[str, list[type]] = {}

#: Extra modules to import before giving up on a name, comma-separated. Read
#: at lookup time rather than at import, so a test or a notebook can set it.
MODULES_VAR = "REKEP_MODULES"

#: The modules already imported for `MODULES_VAR`; a miss costs the import
#: once, not once per lookup.
_IMPORTED: set[str] = set()


def snake(name: str) -> str:
    """`ParsedMessage` -> `parsed_message`: a class's name as a name.

    One transformation, used for the registry key, the record URI and the
    default table name alike -- a class that answered to two spellings would
    be two things to whoever writes them down.
    """
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


def declare(cls: type) -> type:
    """Register `cls` under its own name, and return it.

    Called by `@record`, so declaring a record, a job or a dag is exactly
    writing it -- there is no second list to keep in step with the code.
    """
    entries = DECLARED.setdefault(snake(cls.__name__), [])
    if cls not in entries:
        entries.append(cls)
    return cls


def find[T](reference: str, base: type[T]) -> type[T]:
    """The declared subclass of `base` that `reference` names.

    `reference` is a name (`log`, `files_to_logs`, or the class's own
    `FilesToLogs` -- one key, so both spellings arrive at it) or, for a
    record, its URI (`rekep:///records/log`). Anything not declared is
    refused with what *is*, because the failure is almost always a module
    nobody imported and a list of names is what tells you so.
    """
    name = snake(_named(reference))
    found = _matching(name, base)
    if not found and _import_modules():
        found = _matching(name, base)
    if len(found) == 1:
        return found[0]
    if len(found) > 1:
        # Where they live, spelled as a location rather than as a reference:
        # nothing may point at a class that way any more, but the reader of
        # this message still has to find the two files.
        where = ", ".join(f"{cls.__qualname__} in {cls.__module__}" for cls in found)
        raise ValueError(
            f"{reference!r} names more than one {base.__name__.lower()} ({where}); "
            "rename one of them"
        )
    others = DECLARED.get(name)
    if others:
        raise TypeError(f"{reference!r} is {others[0].__name__}, not a {base.__name__}")
    known = ", ".join(
        sorted(declared for declared, entries in DECLARED.items() if _any(entries, base))
    )
    raise KeyError(
        f"no {base.__name__.lower()} named {name!r}; import the module that declares it "
        f"(or name it in ${MODULES_VAR}). Declared: {known or 'none'}"
    )


def declared[T](base: type[T]) -> list[type[T]]:
    """Every declared subclass of `base`, in name order."""
    return [cls for name in sorted(DECLARED) for cls in _matching(name, base)]


# -- internals --------------------------------------------------------------


def _named(reference: str) -> str:
    """The bare name inside `reference`, which may be a record URI."""
    text = str(reference).strip()
    if "/" not in text and ":" not in text:
        return text
    from rekep.namespace import ResourceUri

    uri = ResourceUri.parse(text, service="records")
    if uri.service != "records":
        raise ValueError(
            f"{reference!r} names a {uri.service[:-1]}, not a class; a class is named by name"
        )
    return uri.name()


def _matching[T](name: str, base: type[T]) -> list[type[T]]:
    return [cls for cls in DECLARED.get(name, ()) if _is(cls, base)]


def _any(entries: list[type], base: type) -> bool:
    return any(_is(cls, base) for cls in entries)


def _is(cls: Any, base: type) -> bool:
    return isinstance(cls, type) and issubclass(cls, base)


def _import_modules() -> bool:
    """Import whatever `REKEP_MODULES` names, once; True if anything was new."""
    fresh = False
    for module in (os.environ.get(MODULES_VAR) or "").split(","):
        module = module.strip()
        if not module or module in _IMPORTED:
            continue
        _IMPORTED.add(module)
        try:
            importlib.import_module(module)
        except ImportError as error:
            raise ImportError(f"cannot import {module!r} from ${MODULES_VAR}") from error
        fresh = True
    return fresh
