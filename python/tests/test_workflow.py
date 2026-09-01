"""The seven applications, run in order over the checked-in fixture.

This is the parity contract: the counts each task returns and the rows each
table holds, for a first run, a replay of the same input, the direct market
mode and a maintenance pass. Every number here was measured, so a producer
that starts writing something else cannot move both sides of the assertion.
"""

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

#: The workflow, in dependency order.
WORKFLOW = (
    "parse_messages",
    "parse_fix",
    "parse_instruments",
    "parse_market",
    "flatten_orders",
    "flatten_executions",
)

#: What the fixture's eleven records produce, first run.
FIRST = {
    "parse_messages": {"read": 11, "written": 11, "skipped": 0},
    "parse_fix": {"read": 10, "written": 10, "skipped": 0},
    "parse_instruments": {"read": 1, "written": 1, "skipped": 0},
    "parse_market": {"read": 2, "written": 2, "skipped": 0},
    "flatten_orders": {"read": 2, "written": 2, "skipped": 0},
    "flatten_executions": {"read": 1, "written": 1, "skipped": 0},
}

#: What a replay of the same input produces: the same reads, no writes.
REPLAY = {
    name: {"read": counts["read"], "written": 0, "skipped": counts["read"]}
    for name, counts in FIRST.items()
}

#: Stored rows, and the one snapshot each table holds after both runs.
STORED = {
    "logs.messages": 11,
    "fix.market": 2,
    "fix.misc": 8,
    "market.instruments": 1,
    "market.books": 2,
    "market.orders": 2,
    "market.executions": 1,
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
            str(ROOT / "tasks" / name / f"{name}.yml"),
            "--parameter",
            f"catalog={json.dumps(self.catalog)}",
        ]
        for parameter, value in overrides.items():
            argv += ["--parameter", f"{parameter}={json.dumps(value)}"]
        assert cli.main(argv) == 0, name
        return json.loads(self._capsys.readouterr().out)

    def workflow(self, **overrides: Any) -> dict[str, dict[str, Any]]:
        """The six publishing tasks, in dependency order."""
        first = {"source": str(FIXTURE)}
        return {
            name: self.task(name, **(first if name == "parse_messages" else {}), **overrides)
            for name in WORKFLOW
        }

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

    assert first["parse_fix"]["routed"] == {"market": 2, "misc": 8}
    assert first["parse_fix"]["unixsource"] == {"SendingTime": 1, "TransactTime": 1, "recorded": 8}
    assert (first["parse_fix"]["tickered"], first["parse_fix"]["errors"]) == (5, 0)
    assert first["parse_market"]["mode"] == "books"
    assert first["parse_market"]["products"]["read"] == {"books": 2, "orders": 2, "executions": 1}
    assert first["parse_market"]["flatten"] == {"orders": 2, "executions": 1}
    assert ran.rows() == STORED

    replay = ran.workflow()
    assert {name: counted(result) for name, result in replay.items()} == REPLAY
    assert replay["parse_fix"]["routed"] == first["parse_fix"]["routed"], "and it still routes"
    assert ran.rows() == STORED, "an idempotent replay adds no row"
    assert set(ran.snapshots().values()) == {1}, "and commits no snapshot"


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

    result = ran.task("parse_messages", source=str(empty))

    assert counted(result) == {"read": 0, "written": 0, "skipped": 0}
    assert result["targets"] == {"messages": "logs.messages"}


def test_a_capture_missing_altogether_is_reported(ran: Ran, tmp_path: Path) -> None:
    argv = [
        "task",
        "run",
        str(ROOT / "tasks" / "parse_messages" / "parse_messages.yml"),
        "--parameter",
        f"catalog={json.dumps(ran.catalog)}",
        "--parameter",
        f"source={json.dumps(str(tmp_path / 'absent'))}",
    ]
    assert cli.main(argv) == 1


def test_several_files_are_one_capture(ran: Ran, tmp_path: Path) -> None:
    """A capture is a directory, opened one naturally sorted path at a time."""
    capture = tmp_path / "capture"
    capture.mkdir()
    lines = FIXTURE.read_text(encoding="utf-8").splitlines(keepends=True)
    (capture / "a.log").write_text("".join(lines[:6]), encoding="utf-8")
    (capture / "b.log").write_text("".join(lines[6:]), encoding="utf-8")

    result = ran.task("parse_messages", source=str(capture))

    assert counted(result) == {"read": 11, "written": 11, "skipped": 0}
    assert ran.rows() == {"logs.messages": 11}


def test_a_batch_bound_below_the_input_changes_nothing_but_the_batches(ran: Ran) -> None:
    """One row per batch crosses every boundary the streaming path has."""
    result = ran.task("parse_messages", source=str(FIXTURE), batch_row_size=1, commit_batch_num=1)

    assert counted(result) == {"read": 11, "written": 11, "skipped": 0}
    assert ran.rows() == {"logs.messages": 11}


def test_a_limit_stops_the_read_where_it_says(ran: Ran) -> None:
    result = ran.task("parse_messages", source=str(FIXTURE), limit=4)

    assert counted(result) == {"read": 4, "written": 4, "skipped": 0}


def test_direct_market_mode_writes_the_events_and_leaves_nothing_to_flatten(ran: Ran) -> None:
    """`books: false` bypasses the fold and writes the FIX-carried events."""
    ran.task("parse_messages", source=str(FIXTURE))
    ran.task("parse_fix")
    result = ran.task("parse_market", books=False)

    assert result["mode"] == "events"
    assert result["flatten"] == {"orders": 0, "executions": 0}
    assert counted(result) == {"read": 3, "written": 3, "skipped": 0}
    assert result["products"]["read"] == {"books": 0, "orders": 2, "executions": 1}
    assert ran.rows() == {
        "logs.messages": 11,
        "fix.market": 2,
        "fix.misc": 8,
        "market.orders": 2,
        "market.executions": 1,
    }


def test_maintenance_visits_every_table_and_reports_what_it_changed(ran: Ran) -> None:
    ran.workflow()

    result = ran.task("optimize_iceberg")

    assert result["task"] == "optimize_iceberg"
    assert result["tables"] == len(STORED)
    assert counted(result) == {"read": 7, "written": 0, "skipped": 7}
    assert (result["expired"], result["deleted"], result["byte_size"]) == (0, 0, 0)
    assert set(result["reports"]) == {name.split(".", 1)[1] for name in STORED}
    assert ran.rows() == STORED, "a settled catalog is left as it was"
