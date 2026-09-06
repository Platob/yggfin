"""What a task document declares, and what it refuses to declare."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from rekep.tasks import Task

ROOT = Path(__file__).resolve().parents[3]

#: Every job this repository schedules, as the document that configures it.
DOCUMENTS = sorted((ROOT / "tasks").glob("*/*.json"))

#: Text ingestion, native FIX parsing, and generic Iceberg maintenance.
NAMES = (
    "optimize_iceberg",
    "parse_fix",
    "parse_messages",
)


def test_the_repository_declares_every_task_once() -> None:
    assert tuple(sorted(path.stem for path in DOCUMENTS)) == NAMES
    assert not list((ROOT / "tasks").glob("*/*.yml"))
    assert not list((ROOT / "tasks").glob("*/*.yaml"))


@pytest.mark.parametrize("document", DOCUMENTS, ids=lambda path: path.stem)
def test_every_document_resolves_the_application_beside_it(document: Path) -> None:
    task = Task.from_json(str(document))

    assert task.name == document.stem, "a task is named after the document that configures it"
    assert task.application == f"{document.stem}.py"
    application = task.into_application_path(document)
    assert application == document.with_suffix(".py")
    assert "app = marimo.App(" in application.read_text(encoding="utf-8")


@pytest.mark.parametrize("document", DOCUMENTS, ids=lambda path: path.stem)
def test_every_document_declares_its_parameters(document: Path) -> None:
    parameters = Task.from_json(str(document)).parameters

    assert parameters, "a task with no parameters would have nothing to configure"
    assert all(isinstance(name, str) for name in parameters)
    assert "catalog" in parameters
    if document.stem == "parse_messages":
        assert set(parameters) == {"filesystem", "direction", "catalog"}
    elif document.stem == "parse_fix":
        assert set(parameters) == {"registry", "version", "dedup", "catalog"}
    else:
        assert "log_level" in parameters


@pytest.mark.parametrize("document", DOCUMENTS, ids=lambda path: path.stem)
def test_every_document_is_canonical_json(document: Path) -> None:
    task = Task.from_json(str(document))
    assert document.read_bytes() == task.into_json()


def _written(tmp_path: Path, body: Any, *, application: str = "job.py") -> Path:
    document = tmp_path / "job.json"
    document.write_text(json.dumps(body), encoding="utf-8")
    (tmp_path / application).write_text("import marimo\n\napp = marimo.App()\n", encoding="utf-8")
    return document


def test_an_undeclared_key_is_refused_rather_than_ignored(tmp_path: Path) -> None:
    """A misspelled `application:` would otherwise run the wrong job."""
    document = _written(
        tmp_path,
        {"name": "job", "application": "job.py", "applicaton": "other.py", "parameters": {}},
    )

    with pytest.raises(TypeError, match="unexpected applicaton"):
        Task.from_json(str(document))


def test_a_document_without_a_name_is_refused(tmp_path: Path) -> None:
    document = _written(tmp_path, {"application": "job.py", "parameters": {}})

    with pytest.raises(ValueError, match="must name its task"):
        Task.from_json(str(document))


def test_a_document_without_an_application_is_refused(tmp_path: Path) -> None:
    document = _written(tmp_path, {"name": "job", "parameters": {}})

    with pytest.raises(ValueError, match="must point to a Marimo application"):
        Task.from_json(str(document))


def test_parameters_that_are_not_a_mapping_are_refused(tmp_path: Path) -> None:
    document = _written(tmp_path, {"name": "job", "application": "job.py", "parameters": [1, 2]})

    with pytest.raises(TypeError, match="parameters must be a mapping"):
        Task.from_json(str(document))


def test_native_nested_values_survive_the_document(tmp_path: Path) -> None:
    """A parameter is what the JSON spells, not the string it prints as."""
    document = _written(
        tmp_path,
        {
            "name": "job",
            "application": "job.py",
            "parameters": {
                "books": False,
                "limit": None,
                "commit_batch_num": 8,
                "null_values": ["", "null"],
                "catalog": {
                    "name": "rekep",
                    "properties": {"type": "sql", "uri": "sqlite:///data/catalog.db"},
                },
                "branch_keys": {"XmlApi": {"clientid": "ClOrdID"}},
            },
        },
    )

    parameters = Task.from_json(str(document)).parameters

    assert parameters["books"] is False
    assert parameters["limit"] is None
    assert parameters["commit_batch_num"] == 8
    assert parameters["null_values"] == ["", "null"]
    assert parameters["catalog"]["properties"]["uri"] == "sqlite:///data/catalog.db"
    assert parameters["branch_keys"] == {"XmlApi": {"clientid": "ClOrdID"}}


def test_an_application_outside_the_task_directory_is_refused(tmp_path: Path) -> None:
    """Containment, so a document cannot reach out of the checkout it ships in."""
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "elsewhere" / "job.py").write_text("app = None\n", encoding="utf-8")
    document = _written(
        tmp_path, {"name": "job", "application": "../elsewhere/job.py", "parameters": {}}
    )

    with pytest.raises(ValueError, match="is outside"):
        Task.from_json(str(document)).into_application_path(document)


def test_a_missing_application_is_reported_where_it_was_looked_for(tmp_path: Path) -> None:
    document = _written(tmp_path, {"name": "job", "application": "absent.py", "parameters": {}})

    with pytest.raises(FileNotFoundError, match="absent.py"):
        Task.from_json(str(document)).into_application_path(document)


def test_an_absolute_application_inside_the_directory_is_allowed(tmp_path: Path) -> None:
    document = _written(
        tmp_path, {"name": "job", "application": str(tmp_path / "job.py"), "parameters": {}}
    )

    assert Task.from_json(str(document)).into_application_path(document) == tmp_path / "job.py"
