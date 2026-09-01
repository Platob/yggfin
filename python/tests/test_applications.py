"""The seven Marimo applications this repository schedules.

An application is a job, not a feature of the package, so what is pinned here
is the contract the runner and the DAG hold it to: it exports an `app`, its
parameter cell defines exactly what its document declares and reads its
defaults out of it, every name is defined once, and `marimo check --strict`
is clean.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
TASKS = ROOT / "tasks"

#: Every job the workflow and the maintenance DAG are made of.
NAMES = (
    "flatten_executions",
    "flatten_orders",
    "optimize_iceberg",
    "parse_fix",
    "parse_instruments",
    "parse_market",
    "parse_messages",
)

#: The one cell a runner replaces. Everything else runs.
PARAMETERS = "parameters"


def application(name: str) -> Any:
    """The `app` one task application exports, imported from its own file."""
    path = TASKS / name / f"{name}.py"
    specification = importlib.util.spec_from_file_location(f"rekep_task_{name}", path)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module.app


def cells(app: Any) -> dict[str, Any]:
    """Each cell of `app`, by the name its function carries."""
    return {cell.name: cell._cell for _, cell in app._cell_manager.valid_cells()}


def declared(name: str) -> dict[str, Any]:
    """What the adjacent document says this application takes."""
    document = yaml.safe_load((TASKS / name / f"{name}.yml").read_text(encoding="utf-8"))
    return document["parameters"]


def test_the_repository_ships_exactly_these_applications() -> None:
    assert (
        tuple(sorted(path.stem for path in TASKS.glob("*/*.py") if path.parent.name != "airflow"))
        == NAMES
    )


@pytest.mark.parametrize("name", NAMES)
def test_marimo_check_is_clean_under_strict(name: str) -> None:
    """`--strict` makes a warning an error, which is what a review wants."""
    checked = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "marimo", "check", "--strict", str(TASKS / name / f"{name}.py")],
        capture_output=True,
        text=True,
        check=False,
    )

    assert checked.returncode == 0, checked.stdout + checked.stderr


@pytest.mark.parametrize("name", NAMES)
def test_the_parameter_cell_defines_exactly_what_the_document_declares(name: str) -> None:
    """`app.run(defs=...)` replaces the defining cell, so a name the document
    forgets is a name no cell would bind."""
    defined = set(cells(application(name))[PARAMETERS].defs)

    assert defined == set(declared(name))


@pytest.mark.parametrize("name", NAMES)
def test_the_parameter_cell_reads_its_defaults_out_of_the_document(name: str) -> None:
    """Opened interactively, an application is configured by the same YAML."""
    source = (TASKS / name / f"{name}.py").read_text(encoding="utf-8")

    assert "Task.from_yaml" in source
    assert 'pathlib.Path(__file__).with_suffix(".yml")' in source
    assert not any(
        f"{key} = " in source.split("def parameters", 1)[1].split("return", 1)[0]
        and f'_defaults["{key}"]' not in source
        for key in declared(name)
    )


@pytest.mark.parametrize("name", NAMES)
def test_one_cell_defines_a_name_and_the_result_is_the_last_of_them(name: str) -> None:
    app = application(name)
    seen: dict[str, str] = {}
    for cell_name, cell in cells(app).items():
        for definition in cell.defs:
            assert definition not in seen, f"{definition} is defined by two cells"
            seen[definition] = cell_name

    assert "result" in seen, "a task that returns nothing cannot be routed"
    assert seen["result"] != PARAMETERS


@pytest.mark.parametrize("name", NAMES)
def test_the_level_is_in_force_before_any_cell_can_emit_a_record(name: str) -> None:
    """One cell configures logging, as soon as the level is known, and every
    cell that can emit reads its `records` back.

    Marimo takes a cell's edges from its body and not from its signature, so
    naming `records` in the parameter list orders nothing; reading it does.
    """
    ordered = [cell for _, cell in application(name)._cell_manager.valid_cells()]
    configuring = [index for index, cell in enumerate(ordered) if "records" in cell._cell.defs]
    reading = [index for index, cell in enumerate(ordered) if "records" in cell._cell.refs]
    declaring = next(index for index, cell in enumerate(ordered) if cell.name == PARAMETERS)

    assert configuring == [declaring + 1]
    assert reading and min(reading) > declaring
    assert (TASKS / name / f"{name}.py").read_text(encoding="utf-8").count("configure(") == 1


@pytest.mark.parametrize("name", NAMES)
def test_no_application_caches_a_cell_that_writes(name: str) -> None:
    """Caching a commit would skip it; nothing here is cached at all."""
    source = (TASKS / name / f"{name}.py").read_text(encoding="utf-8")

    assert "persistent_cache" not in source
    assert "mo.cache" not in source


@pytest.mark.parametrize("name", NAMES)
def test_the_defaults_a_document_declares_are_what_an_interactive_run_binds(
    name: str,
) -> None:
    """The parameter cell alone, run against its own document."""
    app = application(name)
    cell = cells(app)[PARAMETERS]
    namespace: dict[str, Any] = {
        "__file__": str(TASKS / name / f"{name}.py"),
        **{
            definition: value
            for definition, value in _setup(app).items()
            if definition in cell.refs
        },
    }
    exec(compile(cell.code, str(TASKS / name / f"{name}.py"), "exec"), namespace)  # noqa: S102

    assert {key: namespace[key] for key in declared(name)} == declared(name)


def _setup(app: Any) -> dict[str, Any]:
    """What the setup block bound when the application module was imported."""
    return dict(app._setup._glbls) if app._setup is not None else {}
