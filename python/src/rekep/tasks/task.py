"""A unit of work, declared in a document rather than written as a script."""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Mapping
from typing import Any, ClassVar, Self

from rekep.convert import Convertible


@dataclasses.dataclass
class TaskRun(Convertible):
    """What a task did, reported rather than logged.

    Every maintenance verb in this package returns what it did instead of
    printing it, and a task is no different: a caller that wants to log it
    can, one that wants to assert on it can, and a notebook can render it.
    Printing it here would make it the only shape in the package a program
    cannot check.
    """

    task: str = ""
    """Name of the task that ran."""

    rows: int = 0
    """Rows read from the source."""

    written: dict[str, int] = dataclasses.field(default_factory=dict)
    """Rows landed per target, keyed by the target's name."""

    skipped: int = 0
    """Rows a target already held, and so did not write again."""

    seconds: float = 0.0
    """Wall-clock time the run took."""

    @property
    def landed(self) -> int:
        """Rows written across every target."""
        return sum(self.written.values())

    def __str__(self) -> str:
        targets = ", ".join(f"{name}={count:,}" for name, count in sorted(self.written.items()))
        return (
            f"{self.task}: {self.rows:,} read, {self.landed:,} written"
            f"{f', {self.skipped:,} already stored' if self.skipped else ''}"
            f" in {self.seconds:.2f}s"
            f"{f' -- {targets}' if targets else ''}"
        )


@dataclasses.dataclass
class Task(Convertible):
    """One unit of work: what to read, what to do with it, where to put it.

    A task is **configuration**, not a stored shape -- so it is a plain
    `Convertible` dataclass rather than a `@field` class, and the same
    `from_yaml`/`from_json` that reads a schema contract reads one of these::

        Task.from_yaml("tasks/parse_logs.yml").run()

    The document says which task it is with a `kind`, and `from_dict`
    dispatches on it the way `from_`/`into_` dispatch everywhere else: one
    lookup in `KINDS`, keyed by the `KIND` each subclass declares. That is what
    lets a scheduler read a directory of documents it has never seen the
    classes for.
    """

    #: What a document's `kind` says to reach this class. Subclasses set it.
    KIND: ClassVar[str] = ""

    #: Every kind that has been declared, filled by `__init_subclass__` so a
    #: subclass is reachable from a document by existing, not by registering.
    KINDS: ClassVar[dict[str, type[Task]]] = {}

    #: Declared as a member and not written by `into_dict`, because a
    #: `Convertible` that overrides `into_dict` is dumped *by* it -- including
    #: when it is the value being dumped, which recurses until the stack ends.
    kind: str = ""
    """Which task this is; taken from the class when a document leaves it out."""

    name: str = ""
    """What to call this run in its report; the kind, when it is not given."""

    def __post_init__(self) -> None:
        """The kind is the class's, so a document and the object always agree."""
        self.kind = self.kind or type(self).KIND

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.KIND:
            Task.KINDS[cls.KIND] = cls

    def run(self) -> TaskRun:
        """Do the work, and say what was done."""
        raise NotImplementedError(f"{type(self).__name__} does not say what it does")

    # -- reading one from a document ----------------------------------------

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Build the task a document declares, dispatching on its `kind`.

        Called on `Task` it picks the subclass; called on a subclass it builds
        that one, and refuses a document that names a different kind rather
        than quietly building the wrong task from the right fields.
        """
        kind = str(mapping.get("kind", "") or "")
        if cls is Task:
            if not kind:
                raise ValueError(
                    "a task document says which task it is: add a `kind`, one of "
                    f"{sorted(Task.KINDS) or ['none declared']}"
                )
            built = Task.KINDS.get(kind)
            if built is None:
                raise ValueError(f"no task of kind {kind!r}; there is {sorted(Task.KINDS)}")
            return built.from_dict(mapping)  # type: ignore[return-value]
        if kind and kind != cls.KIND:
            raise ValueError(f"{cls.__name__} is {cls.KIND!r}, and the document says {kind!r}")
        return super().from_dict(mapping)

    # -- helpers for the subclasses -----------------------------------------

    def _named(self) -> str:
        """This run's name: what was given, or the kind it is."""
        return self.name or type(self).KIND or type(self).__name__

    def _timed(self) -> float:
        """A monotonic start, so a report's seconds are not a wall clock's."""
        return time.perf_counter()
