"""OpenLineage run events over the file transport -- no Marquez/HTTP backend.

`OpenLineage.from_path` opens a local (or fsspec) log file; `start_run` wraps
one job execution in a START event and returns the `Run` that closes it with
COMPLETE or FAIL. Nothing here imports `openlineage` at module scope: the
extra is optional, so the import happens at the point of use (house rule #8),
the same way `rekep.airflow.sdk` defers `airflow` itself.
"""

from __future__ import annotations

import dataclasses
import datetime
import os
import pathlib
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from rekep.openlineage.datasets import dataset_of
from rekep.records.record import Record
from rekep.require import require

if TYPE_CHECKING:  # pragma: no cover - openlineage is imported at the point of use
    from openlineage.client.client import OpenLineageClient

Records = Sequence[type[Record]]

#: Default log location: runtime output, not deployment config, so it sits
#: alongside `stacks/iceberg/catalog.db` -- tracked as a path, gitignored as
#: state.
DEFAULT_LOG_PATH = "stacks/openlineage/events.log"


@dataclasses.dataclass(eq=False)
class OpenLineage:
    """A file-based OpenLineage emitter: one client, one namespace.

    `client` is the real `OpenLineageClient`, built by `from_path` with a
    `FileTransport` so events land in a local (or fsspec) log file -- nothing
    to stand up, matching the rest of the stack's fully-local defaults.
    """

    client: OpenLineageClient
    namespace: str = "rekep"

    @classmethod
    def from_path(
        cls,
        path: str | os.PathLike[str] = DEFAULT_LOG_PATH,
        *,
        namespace: str = "rekep",
        append: bool = True,
    ) -> OpenLineage:
        """A client that writes run events to `path`, one JSON line per event.

        `append=True` here, unlike the underlying client's own default: a
        run's START and its COMPLETE/FAIL should land in the same file, so
        `path` reads back as one lineage log instead of a timestamped file
        per event. Pass `append=False` for that per-event behaviour instead.

        The parent directory is created for a plain local path -- `FileTransport`
        opens the file itself but never the folder around it, and a fresh
        checkout has no `stacks/openlineage/` yet. A URL (`s3://`, `file://`,
        ...) is left to its own filesystem, local or otherwise.
        """
        require("openlineage.client", "openlineage")
        from openlineage.client.client import OpenLineageClient
        from openlineage.client.transport.file import FileConfig, FileTransport

        if "://" not in str(path):
            pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)

        transport = FileTransport(FileConfig(log_file_path=str(path), append=append))
        return cls(client=OpenLineageClient(transport=transport), namespace=namespace)

    def start_run(
        self,
        job: str,
        *,
        consumes: Records = (),
        produces: Records = (),
        run_id: str | None = None,
    ) -> Run:
        """Emit a START event for `job` and return the `Run` that closes it.

        `consumes`/`produces` are record classes, projected to OpenLineage
        datasets via `dataset_of` -- the same lineage a `Dag` or `Flow`
        already declares, just re-emitted in the OpenLineage shape.
        """
        run = Run(
            emitter=self,
            job=job,
            run_id=run_id or str(uuid.uuid4()),
            inputs=[dataset_of(record, "input") for record in consumes],
            outputs=[dataset_of(record, "output") for record in produces],
        )
        run._emit("START")
        return run


@dataclasses.dataclass(eq=False)
class Run:
    """One job execution: a run id and the datasets it opened with.

    Returned by `OpenLineage.start_run`, already past its START event --
    call `complete()` or `fail()` exactly once when the work is done;
    OpenLineage expects one terminal event per run.
    """

    emitter: OpenLineage
    job: str
    run_id: str
    inputs: list[Any] = dataclasses.field(default_factory=list)
    outputs: list[Any] = dataclasses.field(default_factory=list)

    def complete(self) -> None:
        """Emit COMPLETE: the run finished, its outputs are as declared."""
        self._emit("COMPLETE")

    def fail(self, error: BaseException | None = None) -> None:
        """Emit FAIL, the error carried as an `errorMessage` run facet."""
        self._emit("FAIL", error=error)

    def _emit(self, event_type: str, *, error: BaseException | None = None) -> None:
        from openlineage.client import event_v2
        from openlineage.client.facet_v2 import error_message_run

        run_facets: dict[str, Any] = {}
        if error is not None:
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=str(error), programmingLanguage="python"
            )
        self.emitter.client.emit(
            event_v2.RunEvent(
                eventTime=datetime.datetime.now(datetime.UTC).isoformat(),
                eventType=getattr(event_v2.RunState, event_type),
                run=event_v2.Run(runId=self.run_id, facets=run_facets),
                job=event_v2.Job(namespace=self.emitter.namespace, name=self.job),
                inputs=list(self.inputs),
                outputs=list(self.outputs),
            )
        )
