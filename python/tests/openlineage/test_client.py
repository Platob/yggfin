"""File-based OpenLineage emission: one JSON line per event, no server."""

import json
import pathlib
import uuid

import pytest

pytest.importorskip("openlineage.client")

from rekep.models import Log
from rekep.openlineage import OpenLineage


def test_from_path_opens_a_file_transport(tmp_path: pathlib.Path) -> None:
    emitter = OpenLineage.from_path(tmp_path / "events.log")
    assert emitter.namespace == "rekep"
    assert emitter.client.transport.kind == "file"


def test_from_path_creates_the_parent_directory(tmp_path: pathlib.Path) -> None:
    """A fresh checkout has no `stacks/openlineage/` yet; `from_path` makes it."""
    path = tmp_path / "nested" / "events.log"
    OpenLineage.from_path(path).start_run("job").complete()
    assert path.exists()


def test_start_then_complete_appends_two_lines(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "events.log"
    run = OpenLineage.from_path(path).start_run("job", consumes=[Log], produces=[Log])
    run.complete()

    start, complete = (json.loads(line) for line in path.read_text().splitlines())
    assert (start["eventType"], complete["eventType"]) == ("START", "COMPLETE")
    assert start["run"]["runId"] == complete["run"]["runId"] == run.run_id
    assert start["job"]["namespace"] == "rekep"
    assert start["job"]["name"] == "job"
    assert start["inputs"][0]["name"] == "Log"
    assert start["outputs"][0]["name"] == "Log"


def test_fail_carries_the_error_message(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "events.log"
    run = OpenLineage.from_path(path).start_run("job")
    run.fail(ValueError("boom"))

    _, failed = (json.loads(line) for line in path.read_text().splitlines())
    assert failed["eventType"] == "FAIL"
    assert failed["run"]["facets"]["errorMessage"]["message"] == "boom"


def test_run_id_is_generated_when_not_given(tmp_path: pathlib.Path) -> None:
    run = OpenLineage.from_path(tmp_path / "events.log").start_run("job")
    uuid.UUID(run.run_id)  # does not raise


def test_a_given_run_id_is_kept(tmp_path: pathlib.Path) -> None:
    run = OpenLineage.from_path(tmp_path / "events.log").start_run("job", run_id=str(uuid.uuid4()))
    assert run.run_id
