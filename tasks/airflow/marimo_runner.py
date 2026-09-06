"""Run one task document's Marimo application without the Rekep CLI."""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import os
import pathlib
import sys
from collections.abc import Mapping
from typing import Any


def run(document: pathlib.Path, parameters_file: pathlib.Path, result_file: pathlib.Path) -> None:
    """Run the document once and publish its validated result atomically."""
    from rekep.logs import Stage
    from rekep.tasks import Task

    document = document.resolve()
    task = Task.from_json(str(document))
    application = task.into_application_path(document)
    parameters = dict(task.parameters)
    parameters.update(_parameters(parameters_file))

    app = _application(application)
    with contextlib.redirect_stdout(sys.stderr):
        _, definitions = app.run(defs=parameters)
    if "result" not in definitions:
        raise ValueError(f"{application} defines no result")
    result = Stage.validated(definitions["result"])
    named = result["task"]
    if named != task.name and not named.startswith(f"{task.name}_"):
        raise ValueError(f"{application} returned {named!r}, not a {task.name} run")
    _publish(result_file, json.dumps(result, ensure_ascii=False, separators=(",", ":")))


def _parameters(path: pathlib.Path) -> Mapping[str, Any]:
    """Read one JSON mapping of parameter overrides."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, Mapping):
        raise TypeError(f"{path} is not a JSON object of parameters")
    return document


def _application(path: pathlib.Path) -> Any:
    """Import the Marimo application exported by ``path``."""
    specification = importlib.util.spec_from_file_location(
        f"rekep_task_{path.stem}_{os.getpid()}", path
    )
    if specification is None or specification.loader is None:
        raise ImportError(f"{path} is not an importable module")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    app = getattr(module, "app", None)
    if app is None:
        raise AttributeError(f"{path} exports no marimo app")
    return app


def _publish(path: pathlib.Path, payload: str) -> None:
    """Replace the result only after its complete JSON is durable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f"{path.name}.{os.getpid()}.partial")
    with staged.open("w", encoding="utf-8") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(staged, path)


def main(argv: list[str] | None = None) -> int:
    """Run from an Airflow child process."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("document", type=pathlib.Path)
    parser.add_argument("--parameters-file", type=pathlib.Path, required=True)
    parser.add_argument("--result-file", type=pathlib.Path, required=True)
    arguments = parser.parse_args(argv)
    run(arguments.document, arguments.parameters_file, arguments.result_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
