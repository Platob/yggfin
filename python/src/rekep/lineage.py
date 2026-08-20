"""Lineage: who is told about a run, and whether anyone is at all.

`Run`/`RunEvent` (`rekep.run`) describe *what* happened. This module is the
other half: **a client**, which is anything with `emit(event)`, and the
boundary that builds a run's events around one operation.

Lineage is opt-in, and that is the whole design. A `Dataset` or a `Job` with
no client bound does not build a `Run`, does not stamp a time, does not
compose a schema facet and does not wrap a reader to count its rows -- the
tracking is not merely discarded, it never happens. Reads pay for this most:
tracking a lazy read means a Python generator hop per batch, and no client
means the reader is handed straight back instead.

Binding one is a call, not a field: `dataset.with_lineage(client)` returns
the same object, so it chains, and nothing about the resource's *declaration*
changes -- a client is a runtime handle, and a side file has no business
carrying one.

`Collector` is a client that keeps events in a list. It is what the internal
`events()` bookkeeping used to be, made explicit: something you pass in when
you want the events, rather than a second sink every resource carries
whether or not anyone reads it.

A real OpenLineage client is duck-typed here on purpose. `openlineage-python`
is not a dependency, its `emit()` type-checks its argument against its own
classes, and its wire shape is camelCase with a string `eventTime` -- so
handing it a `rekep.run.RunEvent` unconverted would not work anyway. One
protocol, one adapter when someone needs it, and test doubles for free.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Protocol, runtime_checkable

from rekep.run import Run, RunEvent, RunState
from rekep.run import now as _now


@runtime_checkable
class LineageClient(Protocol):
    """Anything a run's events can be handed to."""

    def emit(self, event: RunEvent) -> None:
        """Take one event. What happens to it is the client's business."""


class Collector:
    """A client that keeps every event it is given.

    The obvious client, and the one tests want: bind it, run the thing, read
    `events`. It is deliberately not something a `Dataset` grows on its own
    -- an unbounded list on a long-lived object is a leak when nobody reads
    it, which is exactly what an opt-in client avoids.
    """

    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)

    def of(self, state: RunState) -> list[RunEvent]:
        """Just the events in `state`, in the order they arrived."""
        return [event for event in self.events if event.event_type is state]

    def __len__(self) -> int:
        return len(self.events)


@dataclasses.dataclass
class Lineage:
    """One operation's run: START on the way in, COMPLETE or FAIL on the way out.

    Built by `Dataset.lineage()`/`Job.lineage()`, which return None when no
    client is bound -- so a call site reads as "track it if anyone is
    listening", and the untracked path costs one attribute lookup.

    The terminal event may carry *different* dataset references than START
    did: a row count is not knowable until the work is done, so `complete`
    takes the refs again rather than the caller mutating what START sent.
    """

    client: Any
    """Where events go."""

    job: Any
    """The `Job` this run belongs to."""

    inputs: list[Any] = dataclasses.field(default_factory=list)
    """Datasets this run reads."""

    outputs: list[Any] = dataclasses.field(default_factory=list)
    """Datasets this run writes."""

    run: Run = dataclasses.field(default_factory=Run)
    """The run itself, one id across every event."""

    def start(self) -> RunEvent:
        """Emit START, with the references known before the work begins."""
        return self._emit(RunState.START, self.inputs, self.outputs)

    def complete(
        self, inputs: list[Any] | None = None, outputs: list[Any] | None = None
    ) -> RunEvent:
        """Emit COMPLETE, with whatever the work turned the references into."""
        return self._emit(
            RunState.COMPLETE,
            self.inputs if inputs is None else inputs,
            self.outputs if outputs is None else outputs,
        )

    def fail(self, error: BaseException | None = None) -> RunEvent:
        """Emit FAIL, carrying the exception as an `errorMessage` run facet.

        The facet is the point: a FAIL that does not say what went wrong
        makes the lineage record strictly less useful than the traceback the
        caller is about to see anyway.
        """
        if error is not None:
            self.run = dataclasses.replace(
                self.run,
                facets={
                    **self.run.facets,
                    "errorMessage": {
                        "message": f"{type(error).__name__}: {error}",
                        "programmingLanguage": "PYTHON",
                    },
                },
            )
        return self._emit(RunState.FAIL, self.inputs, self.outputs)

    def _emit(self, state: RunState, inputs: list[Any], outputs: list[Any]) -> RunEvent:
        event = RunEvent(
            event_type=state,
            event_time=_now(),
            run=self.run,
            job=self.job,
            inputs=list(inputs),
            outputs=list(outputs),
        )
        self.client.emit(event)
        return event
