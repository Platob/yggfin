"""What a declaration says: its type hints and its documentation.

Both readers here answer questions about a class as it was *written* -- what a
member is declared as, and what it is documented as. `convert` needs the first
to decode a value back to its declared type; `fields` needs both to project a
class onto Arrow. Neither has to depend on the other to reach them.
"""

from __future__ import annotations

import ast
import collections.abc
import dataclasses
import inspect
import itertools
import re
import textwrap
import types
from typing import Annotated, Any, Union, get_args, get_origin

NONE_TYPE = type(None)

#: Origins that decode back into a concrete container.
SEQUENCE_ORIGINS = {list, collections.abc.Sequence, collections.abc.MutableSequence}
SET_ORIGINS = {set, frozenset, collections.abc.Set, collections.abc.MutableSet}
MAPPING_ORIGINS = {dict, collections.abc.Mapping, collections.abc.MutableMapping}

# -- declaration ------------------------------------------------------------


def hide_private(cls: type) -> dict[str, Any]:
    """Drop `__`-prefixed annotations so they never become dataclass fields.

    A name written `__cache` in a class body reaches `__annotations__` mangled
    to `_Venue__cache`, so both spellings are checked. Whatever value was
    assigned stays put as a plain class attribute.
    """
    annotations = cls.__dict__.get("__annotations__")
    if not annotations:
        return {}
    mangled = f"_{cls.__name__.lstrip('_')}__"
    hidden = [name for name in annotations if name.startswith("__") or name.startswith(mangled)]
    declared = {name: annotations[name] for name in hidden}
    for name in hidden:
        del annotations[name]
    return declared


def restore_private_slots(cls: type, annotations: dict[str, Any]) -> None:
    """Restore hidden annotations as non-init dataclass slots."""
    if not annotations:
        return
    cls.__annotations__.update(annotations)
    for name in annotations:
        default = cls.__dict__.get(name, dataclasses.MISSING)
        declared: dict[str, Any] = {
            "init": False,
            "repr": False,
            "compare": False,
        }
        if default is not dataclasses.MISSING:
            declared["default"] = default
        setattr(cls, name, dataclasses.field(**declared))


# -- type hints -------------------------------------------------------------


def unwrap_annotated(annotation: Any) -> tuple[tuple[Any, ...], Any]:
    """Split `Annotated[X, ...]` into its extra arguments and X.

    What those extras *mean* is each reader's business; the unwrapping they all
    need is all that happens here.
    """
    if get_origin(annotation) is not Annotated:
        return (), annotation
    inner, *extras = get_args(annotation)
    return tuple(extras), inner


def unwrap_optional(annotation: Any) -> tuple[bool, Any]:
    """Split an optional annotation into its nullability and its inner type."""
    if get_origin(annotation) not in (Union, types.UnionType):
        return False, annotation
    args = get_args(annotation)
    rest = tuple(a for a in args if a is not NONE_TYPE)
    if len(rest) == len(args):
        return False, annotation
    return True, rest[0] if len(rest) == 1 else Union[rest]  # noqa: UP007


def item_annotation(annotation: Any) -> Any:
    args = get_args(annotation)
    return args[0] if args else Any


# -- documentation ----------------------------------------------------------

#: `Attributes:` / `Args:` heading of a Google-style docstring.
_SECTION = re.compile(r"^[ \t]*(?:Attributes|Args|Arguments|Parameters)[ \t]*:[ \t]*$")

#: `name: description` entry inside such a section, type annotation optional.
_ENTRY = re.compile(r"^[ \t]+(?P<name>\*{0,2}\w+)[ \t]*(?:\([^)]*\))?[ \t]*:[ \t]*(?P<text>.*)$")

#: `:param name:` / `:ivar name:` of a Sphinx-style docstring.
_SPHINX = re.compile(
    r"^[ \t]*:(?:param|parameter|arg|ivar|var|attribute)[ \t]+(?:[\w\[\], .]+[ \t]+)?"
    r"(?P<name>\w+)[ \t]*:[ \t]*(?P<text>.*)$"
)


def docstring_summary(cls: type) -> str:
    """First paragraph of `cls`'s own docstring, folded to one line."""
    doc = cls.__dict__.get("__doc__")
    if not doc:
        return ""
    paragraph: list[str] = []
    for line in doc.strip().splitlines():
        if not line.strip():
            break
        paragraph.append(line.strip())
    return " ".join(paragraph)


def docstring_attributes(cls: type) -> dict[str, str]:
    """Member descriptions for `cls`, gathered from every source, weakest first.

    A description written under its own member wins over one collected into an
    `Attributes:` or `:param:` section of the class docstring, and a derived
    class wins over a base, so a subclass can redescribe what it inherits.
    Google and Sphinx sections are read for the sake of dataclasses declared
    elsewhere; numpydoc is not.
    """
    described: dict[str, str] = {}
    for base in reversed(cls.__mro__):
        doc = base.__dict__.get("__doc__")
        if doc:
            described.update(_parse_docstring(doc))
        described.update(_attribute_docstrings(base))
    return described


def _attribute_docstrings(cls: type) -> dict[str, str]:
    """Descriptions written as a bare string literal directly under a member::

        seqnum: int
        '''Line counter the writer printed.'''

    Python evaluates and discards those literals, so they reach neither
    `__doc__` nor the class -- the source has to be re-read to recover them,
    which is what Sphinx and pdoc do too. A class whose source cannot be found
    (a REPL, an `exec`) simply contributes nothing.
    """
    if not cls.__dict__.get("__annotations__"):
        return {}
    try:
        parsed = ast.parse(textwrap.dedent(inspect.getsource(cls))).body[0]
    except (OSError, TypeError, SyntaxError, IndentationError):
        return {}
    if not isinstance(parsed, ast.ClassDef) or parsed.name != cls.__name__:
        return {}

    described: dict[str, str] = {}
    for member, following in itertools.pairwise(parsed.body):
        if not isinstance(member, ast.AnnAssign) or not isinstance(member.target, ast.Name):
            continue
        if isinstance(following, ast.Expr) and isinstance(following.value, ast.Constant):
            if isinstance(following.value.value, str):
                described[member.target.id] = fold(following.value.value)
    return described


def fold(text: str) -> str:
    """Collapse a docstring to the single line Arrow metadata holds."""
    return " ".join(inspect.cleandoc(text).split())


def _parse_docstring(doc: str) -> dict[str, str]:
    described: dict[str, str] = {}
    lines = doc.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        index += 1

        sphinx = _SPHINX.match(line)
        if sphinx:
            described[sphinx["name"]] = sphinx["text"].strip()
            continue
        if not _SECTION.match(line):
            continue

        current: str | None = None
        while index < len(lines):
            entry_line = lines[index]
            if entry_line.strip() and not entry_line[:1].isspace():
                break
            entry = _ENTRY.match(entry_line)
            if entry:
                current = entry["name"].lstrip("*")
                described[current] = entry["text"].strip()
            elif current and entry_line.strip():
                described[current] = f"{described[current]} {entry_line.strip()}".strip()
            elif not entry_line.strip() and current:
                current = None
            index += 1
    return {name: fold(text) for name, text in described.items() if text}
