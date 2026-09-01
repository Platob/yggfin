"""What the library did, for whoever reads the run afterwards.

`Console` renders for the person who just typed a command; this records what
the library did for the operator reading a task log an hour later. One fact
belongs to exactly one of them.

Levels are the whole policy, so they are stated once, here:

- **INFO** is a completed operation: a commit landed, a table was created, a
  maintenance pass settled. One record per public verb, whatever it did
  inside -- a write that commits forty chunks is one INFO, not forty.
- **DEBUG** is the detail under it: a cast, a projection, a scan plan, a file
  opened, a registry index built. Per stream and per file, never per batch or
  per row.

Nothing here runs at import. A library that configures logging has decided for
its caller, and with no handler installed the standard library's last resort
already carries WARNING and above to `stderr` -- so an application that never
calls `configure` sees exactly what it saw before this module existed.
"""

from __future__ import annotations

import dataclasses
import datetime
import logging
import sys
import time
from collections.abc import Mapping
from typing import Any

#: The parent of every logger in the package. Each module holds its own
#: `logging.getLogger(__name__)`, so a record says which module emitted it and
#: a grep for that name reaches the emitting line; this is what an operator
#: filters or silences the whole package by.
ROOT = "rekep"

#: What a task run shows when nothing says otherwise. A task log is read after
#: the fact by somebody asking what happened, which is what INFO answers.
TASK_LEVEL = "INFO"

#: What the CLI shows when nothing says otherwise. A person at a terminal is
#: reading `Console`, and a second stream of records over it is noise.
COMMAND_LEVEL = "WARNING"

#: Level, logger, message. No colour: `Console` owns escape sequences, and a
#: log stream is read by `grep` at least as often as by an eye.
FORMAT = "%(levelname)s %(name)s %(message)s"

#: This module's own records: the two that bracket a task. Every other line in
#: a run comes from the module that did the work.
LOGGER = logging.getLogger(__name__)


def configure(level: str | int = TASK_LEVEL) -> logging.Logger:
    """Send this package's records to `stderr` at `level`, once.

    Idempotent, and scoped to `rekep` rather than the root logger: configuring
    the root would decide the level for every library the caller also imports.
    Repeated calls move the level and leave one handler, so a process that
    runs two tasks does not print every record twice.
    """
    logger = logging.getLogger(ROOT)
    logger.setLevel(logging.getLevelNamesMapping()[level] if isinstance(level, str) else level)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    # Resolved now rather than held: a handler built at import would keep
    # whatever `sys.stderr` was then, and a runner replaces it.
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)
    # The records are this package's own; passing them up would print them a
    # second time wherever the application has configured a root handler.
    logger.propagate = False
    return logger


# -- a task's run ------------------------------------------------------------


@dataclasses.dataclass
class Stage:
    """One task's run: what it opened on, and the result it closes with.

    The numbers are the application's -- a task is a job, and jobs live under
    `tasks/`. What lives here is the *shape* they are reported in and the two
    records that say them, because seven applications agreeing on that by hand
    is seven chances to disagree, and they had: `read` was an integer in five
    of them and a mapping in the sixth, `skipped` was in three, and the table a
    stage wrote was `target` in four and `targets` in two, spelled two ways.

    Every task returns the same keys -- `task`, `read`, `written`, `skipped`,
    `sources`, `targets`, `window`, `elapsed_ms` -- and whatever else it alone
    knows, under its own name. `validated` is the same shape read back: a
    runner writes the result and Airflow routes on it, so both ends check it
    rather than trusting a mapping across a process boundary.
    """

    #: What every result carries. A key here is a key a route may read.
    KEYS = (
        "task",
        "read",
        "written",
        "skipped",
        "sources",
        "targets",
        "window",
        "elapsed_ms",
    )

    #: The task document's own name, so a result identifies itself.
    task: str

    #: What was read and what was written, keyed by the role this stage calls
    #: them: `{"messages": "logs.messages"}`, `{"market": "fix.market"}`. A
    #: role a run did not use is left out rather than stored as null.
    sources: dict[str, str] = dataclasses.field(default_factory=dict)
    targets: dict[str, str] = dataclasses.field(default_factory=dict)

    #: The half-open interval this run covers, in nanoseconds since the epoch.
    #: `(None, None)` is every row the source holds.
    window: tuple[int | None, int | None] = (None, None)

    def __post_init__(self) -> None:
        self.__dict__["_opened"] = time.monotonic()
        LOGGER.info(
            "%s reading %s%s",
            self.task,
            _named(self.sources) or "its source",
            _spelled(self.window),
        )

    def says(self, line: str, *arguments: Any) -> None:
        """One fact this stage alone knows, between its own two records."""
        LOGGER.info(f"{self.task} {line}", *arguments)

    def finished(
        self,
        *,
        read: int,
        written: int,
        skipped: int | None = None,
        **held: Any,
    ) -> dict[str, Any]:
        """The result this task returns, recorded as the same numbers.

        `skipped` defaults to what was read and not written, which is what it
        means everywhere it was spelled out. Anything else a task knows comes
        through `held` and keeps its own name.
        """
        elapsed = time.monotonic() - self.__dict__["_opened"]
        result = {
            "task": self.task,
            "read": read,
            "written": written,
            "skipped": read - written if skipped is None else skipped,
            "sources": dict(self.sources),
            "targets": dict(self.targets),
            "window": {"start": self.window[0], "end": self.window[1]},
            "elapsed_ms": round(elapsed * 1000),
            **held,
        }
        LOGGER.info(
            "%s finished: %d read, %d written, %d skipped%s in %.1fs",
            self.task,
            result["read"],
            result["written"],
            result["skipped"],
            f" {chr(0x2192)} {_named(self.targets)}" if self.targets else "",
            elapsed,
        )
        return result

    @staticmethod
    def validated(result: Any) -> dict[str, Any]:
        """`result` as a task result, or the reason it is not one."""
        if not isinstance(result, Mapping):
            raise TypeError(f"a task result is a mapping, not {type(result).__name__}")
        missing = sorted(set(Stage.KEYS) - set(result))
        if missing:
            raise ValueError(f"a task result is missing {', '.join(missing)}")
        if not isinstance(result["task"], str) or not result["task"]:
            raise ValueError("a task result names the task that returned it")
        for name in ("read", "written", "skipped", "elapsed_ms"):
            _count(result[name], name)
        for name in ("sources", "targets"):
            places = result[name]
            if not isinstance(places, Mapping) or any(
                not isinstance(role, str) or not isinstance(place, str)
                for role, place in places.items()
            ):
                raise TypeError(f"a task result spells {name} as role=name text")
        window = result["window"]
        if not isinstance(window, Mapping) or set(window) != {"start", "end"}:
            raise ValueError("a task result window is a start and an end")
        for bound in window.values():
            if bound is not None and (isinstance(bound, bool) or not isinstance(bound, int)):
                raise TypeError("a window bound is epoch nanoseconds or null")
        return dict(result)


def _count(value: Any, name: str) -> int:
    """One non-negative count, refusing the `bool` that is also an `int`."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"a task result spells {name} as an integer")
    if value < 0:
        raise ValueError(f"a task result {name} is not negative")
    return value


def _named(places: dict[str, str]) -> str:
    """`{"orders": "market.orders"}` as `orders=market.orders`, in one string."""
    return ", ".join(f"{role}={name}" for role, name in places.items())


def _spelled(window: tuple[int | None, int | None]) -> str:
    """A half-open nanosecond interval as the instants a person reads."""
    lower, upper = window
    if lower is None and upper is None:
        return ""
    return f" over [{_instant(lower)}, {_instant(upper)})"


def _instant(value: int | None) -> str:
    """One epoch-nanosecond bound, or the open end it stands for."""
    if value is None:
        return "-"
    moment = datetime.datetime.fromtimestamp(value / 1_000_000_000, tz=datetime.UTC)
    return moment.isoformat(timespec="seconds").replace("+00:00", "Z")
