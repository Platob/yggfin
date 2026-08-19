"""Dag: an ordered set of tasks with record lineage, engine-agnostic."""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable
from typing import Any

from rekep.flows.task import Task, _record_class
from rekep.records import Record, record

logger = logging.getLogger("rekep.flows")


@record
class Dag(Record):
    """An ordered set of tasks: rekep's own dag, no orchestrator required.

    A dag is a record, so it round-trips through side files like everything
    else, and its lineage is the union of its tasks'. `run` is the reference
    executor -- sequential, logged -- and `into_airflow` hands the same
    declaration to Airflow when one is around.
    """

    name: str
    """Dag identifier; becomes the Airflow dag_id."""

    schedule: str | None = None
    """Cron expression or schedule alias, None for manual runs."""

    description: str | None = None
    """One line on what this dag is for."""

    tags: list[str] = dataclasses.field(default_factory=list)
    """Extra orchestrator tags, on top of the derived lineage tags."""

    tasks: list[Task] = dataclasses.field(default_factory=list)
    """The tasks, in execution order."""

    # -- lineage ------------------------------------------------------------

    def consumed_records(self) -> list[type[Record]]:
        """Everything any task reads, deduplicated, declaration order."""
        return _union(task.consumes for task in self.tasks)

    def produced_records(self) -> list[type[Record]]:
        """Everything any task writes, deduplicated, declaration order."""
        return _union(task.produces for task in self.tasks)

    # -- executors ----------------------------------------------------------

    def run(self, **context: Any) -> dict[str, Any]:
        """The reference executor: tasks in order, each result kept by name.

        Each task receives the accumulated results as keyword context, so a
        later task can use an earlier one's output without an orchestrator.
        """
        results: dict[str, Any] = {}
        for task in self.tasks:
            logger.info("dag %s: task %s: run", self.name, task.name)
            results[task.name] = task.run(**{**context, **results})
        logger.info("dag %s: %d task(s) done", self.name, len(self.tasks))
        return results

    def into_airflow(self) -> Any:
        """This dag as an Airflow DAG, lineage tagged and documented."""
        from rekep.airflow.decorators import DAG
        from rekep.airflow.decorators import task as airflow_task

        consumes, produces = self.consumed_records(), self.produced_records()
        with DAG(
            self.name,
            description=self.description,
            schedule=self.schedule,
            tags=list(self.tags),
            consumes=consumes,
            produces=produces,
            catchup=False,
        ) as built:
            previous = None
            for entry in self.tasks:
                current = airflow_task(
                    task_id=entry.name,
                    consumes=entry.consumed_records(),
                    produces=entry.produced_records(),
                )(entry.function())()
                if previous is not None:
                    previous >> current  # declared order is execution order
                previous = current
        return built


def dag(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    schedule: str | None = None,
    tags: list[str] | None = None,
) -> Any:
    """Declare a dag: the decorated function returns or yields its tasks.

    The function body is plain Python -- build tasks with `@task`, order them
    in a list -- and its docstring becomes the dag description::

        @dag(schedule="@daily")
        def ingest():
            \"\"\"Land the day's logs.\"\"\"
            return [extract, transform]
    """

    def wrap(target: Callable[..., Any]) -> Dag:
        tasks = list(target())
        for entry in tasks:
            if not isinstance(entry, Task):
                raise TypeError(f"dag {target.__name__!r} yielded {entry!r}, not a Task")
        summary = (target.__doc__ or "").strip().splitlines()
        return Dag(
            name=name or target.__name__,
            schedule=schedule,
            description=summary[0] if summary else None,
            tags=list(tags or []),
            tasks=tasks,
        )

    return wrap if fn is None else wrap(fn)


def _union(groups: Any) -> list[type[Record]]:
    seen: dict[str, type[Record]] = {}
    for group in groups:
        for dotted in group:
            if dotted not in seen:
                seen[dotted] = _record_class(dotted)
    return list(seen.values())
