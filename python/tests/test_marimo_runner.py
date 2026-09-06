"""The small Marimo child runner used by Airflow."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tasks" / "airflow" / "marimo_runner.py"


def _runner() -> ModuleType:
    specification = importlib.util.spec_from_file_location("marimo_runner", RUNNER)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_the_runner_executes_an_application_without_the_rekep_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    application = tmp_path / "sample.py"
    document = tmp_path / "sample.json"
    parameters = tmp_path / "parameters.json"
    result = tmp_path / "result.json"
    application.write_text("app = None\n", encoding="utf-8")
    document.write_text(
        json.dumps(
            {
                "name": "sample",
                "application": "sample.py",
                "parameters": {"value": 1},
            }
        ),
        encoding="utf-8",
    )
    parameters.write_text('{"value":2}', encoding="utf-8")

    runner = _runner()

    def execute(*, defs: dict[str, object]) -> tuple[None, dict[str, object]]:
        rows = defs["value"]
        return None, {
            "result": {
                "task": "sample",
                "read": rows,
                "written": rows,
                "skipped": 0,
                "sources": {},
                "targets": {"rows": "fix.messages"},
                "window": {"start": None, "end": None},
                "elapsed_ms": 0,
            }
        }

    monkeypatch.setattr(runner, "_application", lambda path: SimpleNamespace(run=execute))
    runner.run(document, parameters, result)

    assert json.loads(result.read_text(encoding="utf-8"))["read"] == 2
    assert not list(tmp_path.glob("*.partial"))
    assert "rekep.cli" not in RUNNER.read_text(encoding="utf-8")
