"""The bundled tasks: what each one ships, and the one way it runs."""

from __future__ import annotations

import inspect
import json
import re
import subprocess
import sys
from importlib import resources
from pathlib import Path
from typing import Any

import pytest

import rekep.tasks
from rekep.deploy import TABLES
from rekep.logs import Stage
from rekep.tasks import TASKS, Task

#: Every task, in the order the supported graph runs them.
NAMES = (
    "parse_messages",
    "parse_fix_raw",
    "parse_fix_refined",
    "parse_books",
    "parse_orders",
    "parse_quotes",
    "parse_executions",
    "build_dbt",
    "optimize_iceberg",
)

#: The streaming stages, whose tables `rekep.deploy` declares.
INGESTION = NAMES[:7]

#: Where the pipeline writes when nothing says otherwise.
CATALOG = {
    "name": "rekep",
    "properties": {
        "type": "sql",
        "uri": "sqlite:///data/catalog.db",
        "warehouse": "data/warehouse",
    },
}

PACKAGE = Path(rekep.tasks.__file__).parent


def document(name: str) -> bytes:
    """The defaults one task ships, as the bytes the package holds."""
    return resources.files("rekep.tasks").joinpath(f"{name}.json").read_bytes()


# -- the registry ------------------------------------------------------------


def test_the_registry_is_every_task_in_graph_order() -> None:
    assert tuple(task.name for task in TASKS) == NAMES
    assert all(Task(name) == task for name, task in zip(NAMES, TASKS, strict=True))


def test_the_package_ships_one_document_and_one_module_per_task() -> None:
    files = sorted(path.name for path in PACKAGE.iterdir() if path.name != "__pycache__")

    assert files == sorted(
        [f"{name}.json" for name in NAMES]
        + [f"{name}.py" for name in NAMES]
        + ["__init__.py", "task.py", "events.py"]
    )


def test_an_unknown_task_is_refused_with_the_names_it_could_be() -> None:
    with pytest.raises(ValueError, match="no task is named 'parse_message'; the tasks are "):
        Task("parse_message")


# -- the documents -----------------------------------------------------------


@pytest.mark.parametrize("name", NAMES)
def test_every_document_is_canonical_json_of_the_parameters_run_takes(name: str) -> None:
    parameters = json.loads(document(name))

    assert isinstance(parameters, dict)
    assert document(name) == (json.dumps(parameters, indent=2, ensure_ascii=False) + "\n").encode()
    signature = inspect.signature(Task(name).module.run).parameters.values()
    assert [parameter.name for parameter in signature] == list(parameters)
    assert {parameter.kind for parameter in signature} == {inspect.Parameter.KEYWORD_ONLY}
    assert all(parameter.default is inspect.Parameter.empty for parameter in signature), (
        "the document is the one place a default is stated"
    )


@pytest.mark.parametrize("name", NAMES)
def test_every_task_declares_where_it_writes(name: str) -> None:
    """One catalog for every task that opens one, so a deploy and a run agree."""
    parameters = Task(name).parameters

    assert parameters["catalog"] == (None if name == "build_dbt" else CATALOG)


def test_each_task_ships_the_defaults_its_stage_needs() -> None:
    windowed = {"start": None, "end": None}
    shipped = {name: Task(name).parameters for name in NAMES}

    assert shipped["parse_messages"] == {
        "filesystem": "file:data/capture",
        "rowheader": None,
        **windowed,
        "catalog": CATALOG,
    }
    assert shipped["parse_fix_raw"] == {
        "messages": "logs.messages",
        "registry": None,
        "codec_options": None,
        **windowed,
        "catalog": CATALOG,
    }
    assert shipped["parse_fix_refined"] == {
        "raw": "fix.raw",
        "registry": None,
        "codec_options": None,
        **windowed,
        "catalog": CATALOG,
    }
    assert shipped["parse_books"] == {
        "refined": "fix.refined",
        "registry": None,
        "codec_options": None,
        "snapshot_millis": 0,
        **windowed,
        "catalog": CATALOG,
    }
    for kind in ("orders", "quotes", "executions"):
        assert shipped[f"parse_{kind}"] == {
            "books": "market.books",
            # A standalone run pins the head it finds.
            "snapshot_id": None,
            **windowed,
            "catalog": CATALOG,
        }
    assert shipped["build_dbt"] == {
        "project": "data/dbt",
        "profiles": None,
        "target": None,
        "select": None,
        "catalog": None,
        "log_level": "INFO",
    }
    assert shipped["optimize_iceberg"] == {
        "catalog": CATALOG,
        "namespace": None,
        "branch": "root",
        "min_files": 2,
        "retain": 24,
        "snapshot_age_days": 7,
        "orphan_age_days": 3,
        "remove_orphans": True,
        "metadata": True,
        "log_level": "INFO",
    }


def test_the_parameters_are_a_fresh_copy_on_every_read() -> None:
    task = Task("parse_messages")
    parameters = task.parameters
    parameters["catalog"]["properties"]["uri"] = "sqlite:///elsewhere.db"
    parameters["start"] = "2026-08-14"

    assert task.parameters["catalog"] == CATALOG
    assert task.parameters["start"] is None


# -- the modules -------------------------------------------------------------


@pytest.mark.parametrize("name", NAMES)
def test_every_module_summarizes_itself_and_names_its_tables(name: str) -> None:
    task = Task(name)

    assert task.summary
    assert task.summary == task.module.__doc__.strip().splitlines()[0]
    assert isinstance(task.module.TARGETS, tuple)
    assert task.targets == task.module.TARGETS
    assert all(re.fullmatch(r"[a-z_]+\.[a-z_]+", table) for table in task.targets)


def test_the_ingestion_tasks_write_the_deployed_tables_in_graph_order() -> None:
    assert tuple(table for name in INGESTION for table in Task(name).targets) == tuple(
        shape.table for shape in TABLES
    )
    assert all(len(Task(name).targets) == 1 for name in INGESTION)


def test_the_products_and_maintenance_declare_no_deployed_table() -> None:
    assert Task("build_dbt").targets == ("orders.events", "orders.current", "executions.fills")
    assert Task("optimize_iceberg").targets == ()


def _imported_after(statement: str) -> list[str]:
    """The modules a fresh interpreter holds after `statement`."""
    probe = f"import json, sys\n{statement}\nprint(json.dumps(sorted(sys.modules)))"
    ran = subprocess.run(  # noqa: S603
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    return json.loads(ran.stdout)


def test_importing_the_dbt_task_does_not_import_dbt() -> None:
    """dbt is the runner group's, imported only when a build runs."""
    modules = _imported_after("import rekep.tasks.build_dbt")

    assert "rekep.tasks.build_dbt" in modules
    assert not [name for name in modules if name == "dbt" or name.startswith("dbt.")]


def test_reading_the_defaults_imports_no_task_module() -> None:
    """A scheduler declares a task without loading what running it needs."""
    modules = _imported_after(
        "from rekep.tasks import TASKS\nfor task in TASKS:\n    task.parameters"
    )

    assert "rekep.tasks.task" in modules
    assert not [name for name in (*NAMES, "events") if f"rekep.tasks.{name}" in modules]


# -- resolving, running and deploying ------------------------------------------


def test_overrides_replace_the_defaults_they_name() -> None:
    task = Task("parse_messages")

    assert task.resolved() == task.parameters
    assert task.resolved({"start": "2026-08-14", "rowheader": "[%s]"}) == {
        **task.parameters,
        "start": "2026-08-14",
        "rowheader": "[%s]",
    }


def test_an_undeclared_override_is_refused_with_what_the_task_takes() -> None:
    with pytest.raises(
        TypeError,
        match=r"^parse_messages takes no rows, source; "
        r"it takes filesystem, rowheader, start, end, catalog$",
    ):
        Task("parse_messages").resolved({"source": "file:x", "rows": 1, "start": None})


def replaced(
    monkeypatch: pytest.MonkeyPatch, answer: Any = None, *, task: str = "parse_messages"
) -> list[dict[str, Any]]:
    """Stand in for `parse_messages`'s `run` and collect what each call took.

    It behaves as every task does -- prints, records at INFO and answers a
    `Stage` result -- and reads nothing. `answer` builds the result from the
    opened stage; `task` is the name the stage opens under.
    """
    seen: list[dict[str, Any]] = []

    def stand_in(**parameters: Any) -> Any:
        seen.append(parameters)
        print("a task writes to stdout")
        stage = Stage(
            task,
            sources={"capture": "file:data/capture"},
            targets={"messages": "logs.messages"},
        )
        stage.says("holding %d characters of configuration", len(parameters["filesystem"]))
        return answer(stage) if answer else stage.finished(read=1, written=1)

    monkeypatch.setattr(Task("parse_messages").module, "run", stand_in)
    return seen


def test_a_run_takes_the_resolved_parameters_and_answers_its_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = replaced(monkeypatch)

    result = Task("parse_messages").run({"start": "2026-08-14"})

    assert seen == [{**Task("parse_messages").parameters, "start": "2026-08-14"}]
    assert result["task"] == "parse_messages"
    assert (result["read"], result["written"], result["skipped"]) == (1, 1, 0)
    assert set(result) == set(Stage.KEYS)


def test_an_undeclared_override_is_refused_before_anything_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = replaced(monkeypatch)

    with pytest.raises(TypeError, match="takes no rows"):
        Task("parse_messages").run({"rows": 1})
    assert seen == []


def test_a_malformed_result_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    replaced(monkeypatch, lambda stage: {**stage.finished(read=1, written=1), "read": -1})

    with pytest.raises(ValueError, match="read is not negative"):
        Task("parse_messages").run()


@pytest.mark.parametrize("named", ["parse_fix_raw", "parse_messages_replay"])
def test_a_result_naming_another_task_is_refused(
    monkeypatch: pytest.MonkeyPatch, named: str
) -> None:
    """A result names exactly the task that returned it, not a variant of it."""
    replaced(monkeypatch, task=named)

    with pytest.raises(ValueError, match=f"returned '{named}', not a parse_messages run"):
        Task("parse_messages").run()


@pytest.mark.parametrize("flag", ["remove_orphans", "metadata"])
@pytest.mark.parametrize("spelled", ["False", "false", 0, None])
def test_maintenance_refuses_a_flag_that_is_not_a_boolean(flag: str, spelled: Any) -> None:
    """`--parameter remove_orphans=False` is the text "False", which is true."""
    with pytest.raises(TypeError, match=f"{flag} must be true or false"):
        Task("optimize_iceberg").run({flag: spelled})


@pytest.mark.parametrize("name", ["build_dbt", "optimize_iceberg"])
def test_a_task_with_no_deployed_table_deploys_without_opening_its_catalog(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    from rekep.iceberg import IcebergCatalog

    def opened(*_: Any, **__: Any) -> None:
        raise AssertionError("a deploy with nothing to create opened a catalog")

    monkeypatch.setattr(IcebergCatalog, "from_dict", opened)
    task = Task(name)

    assert task.deploy() == {"catalog": task.parameters["catalog"], "tables": {}}
    assert task.deploy(dry_run=True)["tables"] == {}
    with pytest.raises(TypeError, match=f"{name} takes no rows"):
        task.deploy({"rows": 1})


@pytest.mark.parametrize(
    ("catalog", "reported"),
    [
        (
            {"catalog_name": "legacy", "properties": {}},
            "accepts only name and properties; unexpected catalog_name",
        ),
        ({"name": "rekep", "properties": {}, "tables": []}, "unexpected tables"),
        (None, "a task catalog is a mapping of name and properties"),
        ("sqlite:///data/catalog.db", "a task catalog is a mapping of name and properties"),
    ],
    ids=["legacy", "extra", "null", "text"],
)
def test_a_catalog_that_is_not_a_name_and_properties_is_refused(
    catalog: Any, reported: str
) -> None:
    with pytest.raises(TypeError, match=reported):
        Task("parse_messages").deploy({"catalog": catalog}, dry_run=True)
