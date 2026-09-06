"""Message ingestion over the checked-in fixture and a replay."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from rekep import cli
from rekep.iceberg import IcebergCatalog

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "python" / "tests" / "data" / "app_messages_sample.txt"

WORKFLOW = (("parse_messages", {}),)

#: What the fixture's fourteen physical rows produce, first run.
FIRST = {
    "parse_messages": {"read": 14, "written": 14, "skipped": 0},
}

#: What a replay of the same input produces: nothing at all. Each source
#: resumes above the line it was last read to, so a replay that has nothing
#: new settles it without parsing it rather than reading it to discard it.
REPLAY = {name: {"read": 0, "written": 0, "skipped": 0} for name in FIRST}

#: Stored rows, and the one snapshot each table holds after both runs.
STORED = {
    "logs.messages": 14,
}


class Ran:
    """One catalog, and the tasks run against it."""

    def __init__(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        self.warehouse = tmp_path / "warehouse"
        self.catalog = {
            "name": "rekep",
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
                "warehouse": (tmp_path / "warehouse").as_uri(),
            },
        }
        self._capsys = capsys

    def task(self, name: str, **overrides: Any) -> dict[str, Any]:
        """One task, with its result read back off `stdout`."""
        argv = [
            "task",
            "run",
            str(ROOT / "tasks" / name / f"{name}.json"),
            "--parameter",
            f"catalog={json.dumps(self.catalog)}",
        ]
        for parameter, value in overrides.items():
            argv += ["--parameter", f"{parameter}={json.dumps(value)}"]
        assert cli.main(argv) == 0, name
        return json.loads(self._capsys.readouterr().out)

    def workflow(self, **overrides: Any) -> dict[str, dict[str, Any]]:
        """Every publishing task instance, in dependency order, by stage name."""
        results = {}
        for name, held in WORKFLOW:
            first = {"filesystem": FIXTURE.as_uri()} if name == "parse_messages" else {}
            result = self.task(name, **first, **held, **overrides)
            results[result["task"]] = result
        return results

    def rows(self) -> dict[str, int]:
        store = IcebergCatalog.from_dict(self.catalog)
        try:
            return {
                dataset.identifier: dataset.read_arrow_table().num_rows
                for dataset in store.datasets(None)
            }
        finally:
            store.close()

    def snapshots(self) -> dict[str, int]:
        store = IcebergCatalog.from_dict(self.catalog)
        try:
            return {
                dataset.identifier: len(
                    store.catalog.load_table(dataset.identifier).metadata.snapshots
                )
                for dataset in store.datasets(None)
            }
        finally:
            store.close()


@pytest.fixture()
def ran(tmp_path: Path, capsys: pytest.CaptureFixture) -> Iterator[Ran]:
    yield Ran(tmp_path, capsys)


def counted(result: dict[str, Any]) -> dict[str, int]:
    return {name: result[name] for name in ("read", "written", "skipped")}


def test_the_workflow_publishes_the_fixture_and_a_replay_writes_nothing(ran: Ran) -> None:
    first = ran.workflow()
    assert {name: counted(result) for name, result in first.items()} == FIRST
    assert ran.rows() == STORED

    replay = ran.workflow()
    assert {name: counted(result) for name, result in replay.items()} == REPLAY
    assert replay["parse_messages"]["settled"] == 1, "the one source was settled, not re-read"
    assert ran.rows() == STORED, "an idempotent replay adds no row"
    assert ran.snapshots() == {name: int(bool(rows)) for name, rows in STORED.items()}


def test_every_result_is_the_shape_a_route_reads(ran: Ran) -> None:
    from rekep.logs import Stage

    for name, result in ran.workflow().items():
        assert Stage.validated(result) == result
        assert result["task"] == name
        assert len(json.dumps(result)) < 4096, "XCom carries a summary, never a payload"


def test_an_empty_capture_is_read_and_produces_nothing(ran: Ran, tmp_path: Path) -> None:
    """Zero rows is a run, not a failure: the route skips what has no input."""
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "quiet.log").write_text("", encoding="utf-8")

    result = ran.task("parse_messages", filesystem=empty.as_uri())

    assert counted(result) == {"read": 0, "written": 0, "skipped": 0}
    assert result["targets"] == {"messages": "logs.messages"}


def test_a_capture_missing_altogether_is_reported(ran: Ran, tmp_path: Path) -> None:
    argv = [
        "task",
        "run",
        str(ROOT / "tasks" / "parse_messages" / "parse_messages.json"),
        "--parameter",
        f"catalog={json.dumps(ran.catalog)}",
        "--parameter",
        f"filesystem={json.dumps((tmp_path / 'absent').as_uri())}",
    ]
    assert cli.main(argv) == 1


def test_several_files_are_one_capture(ran: Ran, tmp_path: Path) -> None:
    """A capture is a directory, opened one naturally sorted path at a time."""
    capture = tmp_path / "capture"
    capture.mkdir()
    lines = FIXTURE.read_text(encoding="utf-8").splitlines(keepends=True)
    (capture / "a.log").write_text("".join(lines[:6]), encoding="utf-8")
    (capture / "b.log").write_text("".join(lines[6:]), encoding="utf-8")

    result = ran.task("parse_messages", filesystem=capture.as_uri())

    assert counted(result) == {"read": 14, "written": 14, "skipped": 0}
    assert ran.rows() == {"logs.messages": 14}


def test_maintenance_visits_every_table_and_reports_what_it_changed(ran: Ran) -> None:
    ran.workflow()

    result = ran.task("optimize_iceberg")

    assert result["task"] == "optimize_iceberg"
    assert result["tables"] == len(STORED)
    assert counted(result) == {"read": len(STORED), "written": 0, "skipped": len(STORED)}
    assert (result["expired"], result["deleted"], result["byte_size"]) == (0, 0, 0)
    assert set(result["reports"]) == {name.split(".", 1)[1] for name in STORED}
    assert ran.rows() == STORED, "a settled catalog is left as it was"
