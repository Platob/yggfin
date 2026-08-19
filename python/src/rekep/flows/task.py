"""Task: one unit of work, a record with a callable and record lineage."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence
from typing import Any

from rekep.imports import locate
from rekep.records import Record, record


@record
class Task(Record):
    """One unit of work in a dag: a callable bound to record lineage.

    The record half is what side files and orchestrators see -- name,
    lineage, a dotted path to the function. The program half is the function
    itself, carried on the instance when the task was declared with `@task`,
    resolved through `callable` otherwise -- so a task round-trips through
    YAML and still runs.
    """

    name: str
    """Task identifier inside its dag."""

    callable: str | None = None
    """Dotted path of the function; None only for inline-bound tasks."""

    consumes: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this task reads."""

    produces: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this task writes."""

    __fn: Any = None  # the bound function: working state, not schema

    # -- the program --------------------------------------------------------

    def function(self) -> Callable[..., Any]:
        """The bound function, resolving `callable` when nothing is bound.

        The decorator replaces the module attribute with the Task itself, so
        a resolved path usually lands on a sibling Task -- whose *bound*
        function is the one to run.
        """
        bound = getattr(self, "_Task__fn", None)
        if bound is not None:
            return bound
        if not self.callable:
            raise ValueError(f"task {self.name!r} has no callable to run")
        resolved = locate(self.callable)
        if isinstance(resolved, Task):
            sibling = getattr(resolved, "_Task__fn", None)
            if sibling is None:
                raise ValueError(f"task {self.name!r}: {self.callable} is an unbound Task")
            return sibling
        return resolved

    def run(self, *args: Any, **kwargs: Any) -> Any:
        return self.function()(*args, **kwargs)

    def bind(self, fn: Callable[..., Any]) -> Task:
        """This task with `fn` attached; the dotted path stays authoritative."""
        self._Task__fn = fn
        return self

    # -- lineage ------------------------------------------------------------

    def consumed_records(self) -> list[type[Record]]:
        return [_record_class(path) for path in self.consumes]

    def produced_records(self) -> list[type[Record]]:
        return [_record_class(path) for path in self.produces]


def task(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    consumes: Sequence[type[Record] | str] = (),
    produces: Sequence[type[Record] | str] = (),
) -> Any:
    """Declare a task: the decorated function becomes a bound `Task`.

    Lineage takes record classes or dotted paths interchangeably; either way
    the Task stores dotted paths, so it serialises like any record. Works
    bare (`@task`) and called (`@task(produces=[Log])`).
    """

    def wrap(target: Callable[..., Any]) -> Task:
        return Task(
            name=name or target.__name__,
            callable=f"{target.__module__}.{target.__qualname__}",
            consumes=[_dotted(entry) for entry in consumes],
            produces=[_dotted(entry) for entry in produces],
        ).bind(target)

    return wrap if fn is None else wrap(fn)


def _dotted(entry: type[Record] | str) -> str:
    if isinstance(entry, str):
        return entry
    return f"{entry.__module__}.{entry.__qualname__}"


def _record_class(dotted: str) -> type[Record]:
    cls = locate(dotted)
    if not (isinstance(cls, type) and issubclass(cls, Record)):
        raise TypeError(f"{dotted} is not a Record")
    return cls
