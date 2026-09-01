"""The operator Airflow runs this repository's task applications with.

The DAG is not part of the package, so it is imported from where Airflow
imports it -- the same way `tests/test_ci.py` reaches the release script.
"""

from __future__ import annotations

import importlib.util
import json
import os
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("airflow", reason="the operator runs under Airflow, which is POSIX-only")

ROOT = Path(__file__).resolve().parents[2]
DAGS = ROOT / "tasks" / "airflow"


def _module(name: str) -> ModuleType:
    """One DAG-folder module, as Airflow's bundle path makes importable."""
    if str(DAGS) not in sys.path:
        sys.path.insert(0, str(DAGS))
    path = DAGS / f"{name}.py"
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


OPERATOR = _module("marimo_operator")
MarimoOperator = OPERATOR.MarimoOperator

#: What one attempt returned, in the shape every task returns.
RESULT = {
    "task": "sample",
    "read": 2,
    "written": 2,
    "skipped": 0,
    "sources": {"input": "logs.messages"},
    "targets": {"output": "market.orders"},
    "window": {"start": None, "end": None},
    "elapsed_ms": 5,
}


#: An application that outlives the test unless the process group is signalled.
SLEEPER = """
import marimo

app = marimo.App()

with app.setup:
    import pathlib
    import time

    from rekep.tasks import Task


@app.cell
def parameters():
    _document = pathlib.Path(__file__).with_suffix(".yml")
    seconds = Task.from_yaml(str(_document)).parameters["seconds"]
    return (seconds,)


@app.cell
def _(seconds):
    time.sleep(seconds)
    result = {}
    return (result,)
"""


class Ran:
    """A `SubprocessHook` that records its commands instead of running them."""

    calls: list[dict[str, Any]] = []
    result: Any = RESULT
    exit_code: int = 0

    def __init__(self) -> None:
        self.sent = False

    def run_command(
        self, command: list[str], env: dict[str, str] | None = None, cwd: str | None = None
    ) -> SimpleNamespace:
        Ran.calls.append({"command": list(command), "env": dict(env or {}), "cwd": cwd})
        if Ran.result is not None and Ran.exit_code == 0:
            Path(command[command.index("--result-file") + 1]).write_text(
                json.dumps(Ran.result), encoding="utf-8"
            )
        return SimpleNamespace(exit_code=Ran.exit_code, output="")

    def send_sigterm(self) -> None:
        self.sent = True


@pytest.fixture(autouse=True)
def _recorded(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    Ran.calls = []
    Ran.result = RESULT
    Ran.exit_code = 0
    monkeypatch.setattr(OPERATOR, "SubprocessHook", Ran)
    yield


def context(**held: Any) -> dict[str, Any]:
    """The task execution context, with only what the operator reads."""
    return {
        "params": held.pop("params", {}),
        "run_id": held.pop("run_id", "manual__2026-08-21T10:00:00+00:00"),
        "task_instance": SimpleNamespace(
            map_index=held.pop("map_index", -1), try_number=held.pop("try_number", 1)
        ),
        **held,
    }


def operator(**held: Any) -> Any:
    held.setdefault("task_id", "parse_messages")
    held.setdefault("repository", str(ROOT))
    held.setdefault("document", "tasks/parse_messages/parse_messages.yml")
    return MarimoOperator(**held)


def commands() -> list[list[str]]:
    return [call["command"] for call in Ran.calls]


# -- the command it builds ---------------------------------------------------


def test_the_task_command_is_the_locked_offline_argv() -> None:
    """Pinned: an option this loses is a resolution during a scheduled run.

    The environment itself is the deployment's, made once with
    `uv sync --locked --group runner`; a task never writes to it.
    """
    operator().execute(context())

    (run,) = commands()
    parameters = run[run.index("--parameters-file") + 1]
    result = run[run.index("--result-file") + 1]
    assert run == [
        "uv",
        "run",
        "--project",
        str(ROOT / "python"),
        "--group",
        "runner",
        "--no-sync",
        "--offline",
        "--no-progress",
        "--no-env-file",
        "--",
        "rekep",
        "task",
        "run",
        str(ROOT / "tasks" / "parse_messages" / "parse_messages.yml"),
        "--parameters-file",
        parameters,
        "--result-file",
        result,
    ]


def test_every_path_it_names_is_absolute_and_it_runs_in_the_checkout() -> None:
    operator().execute(context())

    for command in commands():
        assert all(
            Path(argument).is_absolute()
            for argument in command
            if "/" in argument or "\\\\" in argument
        )
    assert {call["cwd"] for call in Ran.calls} == {str(ROOT)}


def test_nothing_in_the_command_reaches_a_shell() -> None:
    """An argv list, so a path with a space stays one argument and `;` is text."""
    operator().execute(context())

    for command in commands():
        assert isinstance(command, list)
        assert all(isinstance(argument, str) for argument in command)
        assert not any(argument.startswith("-") and " " in argument for argument in command), (
            "no option carries a second word it could smuggle a command in"
        )


def test_a_configured_cache_directory_reaches_the_child() -> None:
    operator(cache_dir="/var/lib/rekep/uv").execute(context())

    assert {call["env"]["UV_CACHE_DIR"] for call in Ran.calls} == {"/var/lib/rekep/uv"}


def test_the_environment_is_the_worker_plus_what_the_operator_configures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    operator(environment={"S3_ENDPOINT_URL": "http://minio:9000"}).execute(context())

    environment = Ran.calls[-1]["env"]
    assert environment["AWS_REGION"] == "eu-west-1"
    assert environment["S3_ENDPOINT_URL"] == "http://minio:9000"


# -- what the child is handed ------------------------------------------------


def written(index: int = -1) -> dict[str, Any]:
    """The parameter document the operator wrote for one command."""
    command = Ran.calls[index]["command"]
    return json.loads(Path(command[command.index("--parameters-file") + 1]).read_text())


class Held:
    """Keeps the attempt directory long enough to read what was written."""

    def __init__(self) -> None:
        self.paths: list[Path] = []

    def __call__(self, path: Path, **held: Any) -> None:
        self.paths.append(Path(path))


@pytest.fixture()
def kept(monkeypatch: pytest.MonkeyPatch) -> Iterator[Held]:
    """Leave every attempt directory in place, so a test can inspect it."""
    held = Held()
    real = OPERATOR.shutil.rmtree
    monkeypatch.setattr(
        OPERATOR.shutil,
        "rmtree",
        lambda path, **kwargs: held(path) or None,
    )
    yield held
    for path in held.paths:
        real(path, ignore_errors=True)


def test_the_document_defaults_reach_the_application(kept: Held) -> None:
    operator().execute(context())

    parameters = written()
    assert parameters["target"] == "logs.messages"
    assert parameters["technical_plugins"] == ["jolokia"]
    assert parameters["catalog"]["properties"]["type"] == "sql"


def test_a_declared_param_replaces_the_document_default_with_its_native_value(
    kept: Held,
) -> None:
    """`books=false` is the boolean, never the truthy string `"False"`."""
    market = operator(task_id="parse_market", document="tasks/parse_market/parse_market.yml")
    market.execute(context(params={"branch": "wip", "books": False}))

    parameters = written()
    assert parameters["books"] is False
    assert parameters["branch"] == "wip"


def test_a_param_the_task_does_not_declare_is_not_injected(kept: Held) -> None:
    """`books` is `parse_market`'s question; nothing else is handed it."""
    operator().execute(context(params={"branch": "wip", "books": False}))

    parameters = written()
    assert "books" not in parameters
    assert parameters["branch"] == "wip"


def test_the_interval_fills_only_a_declared_start_and_end(kept: Held) -> None:
    import datetime

    lower = datetime.datetime(2026, 8, 21, 10, tzinfo=datetime.UTC)
    upper = datetime.datetime(2026, 8, 21, 11, tzinfo=datetime.UTC)

    operator().execute(context(data_interval_start=lower, data_interval_end=upper))
    assert written()["start"] == "2026-08-21T10:00:00+00:00"
    assert written()["end"] == "2026-08-21T11:00:00+00:00"

    Ran.calls = []
    maintenance = operator(
        task_id="optimize_iceberg", document="tasks/optimize_iceberg/optimize_iceberg.yml"
    )
    maintenance.execute(context(data_interval_start=lower, data_interval_end=upper))
    parameters = written()
    assert "start" not in parameters and "end" not in parameters


def test_an_explicit_parameter_is_overridden_by_the_param_of_the_same_name(
    kept: Held,
) -> None:
    """Sources merge once, in one order: document, operator, Params, interval."""
    operator(parameters={"branch": "operator", "limit": 5}).execute(
        context(params={"branch": "params"})
    )

    parameters = written()
    assert parameters["branch"] == "params"
    assert parameters["limit"] == 5


def test_an_explicit_parameter_the_document_does_not_declare_is_refused() -> None:
    from airflow.sdk.exceptions import AirflowException

    with pytest.raises(AirflowException, match="declares no bokos"):
        operator(parameters={"bokos": True}).execute(context())


def test_a_nested_parameter_survives_the_document_the_operator_writes(kept: Held) -> None:
    catalog = {
        "name": "rekep",
        "properties": {
            "type": "glue",
            "warehouse": "s3://bucket/rekep",
            "glue.region": "eu-west-1",
        },
    }
    operator(parameters={"catalog": catalog, "null_values": ["", "n/a"]}).execute(context())

    parameters = written()
    assert parameters["catalog"] == catalog
    assert parameters["null_values"] == ["", "n/a"]
    assert parameters["limit"] is None, "and a null stays a null"


# -- the attempt directory ---------------------------------------------------


def test_the_parameter_document_is_readable_only_by_the_worker(kept: Held) -> None:
    operator().execute(context())

    command = Ran.calls[-1]["command"]
    parameters = Path(command[command.index("--parameters-file") + 1])
    assert stat.S_IMODE(parameters.stat().st_mode) == 0o600
    assert stat.S_IMODE(parameters.parent.stat().st_mode) == 0o700


def test_the_attempt_directory_names_the_attempt_that_owns_it(kept: Held) -> None:
    operator().execute(
        context(run_id="scheduled__2026-08-21T10:00:00+00:00", map_index=3, try_number=2)
    )

    command = Ran.calls[-1]["command"]
    directory = Path(command[command.index("--result-file") + 1]).parent
    stem, _, unique = directory.name.rpartition("-")
    assert stem.endswith("parse_messages-scheduled__2026-08-21T10_00_00_00_00-3-2")
    assert ":" not in directory.name and "+" not in directory.name
    assert unique and unique.isalnum(), "and a unique suffix nothing else can hold"


def test_two_attempts_of_one_task_never_share_a_directory(kept: Held) -> None:
    one = operator()
    one.execute(context())
    two = operator()
    two.execute(context())

    directories = {
        Path(call["command"][call["command"].index("--result-file") + 1]).parent
        for call in Ran.calls
        if "--result-file" in call["command"]
    }
    assert len(directories) == 2


def test_everything_written_for_an_attempt_is_removed_when_it_lands() -> None:
    operator().execute(context())

    command = Ran.calls[-1]["command"]
    assert not Path(command[command.index("--parameters-file") + 1]).parent.exists()


def test_everything_written_for_an_attempt_is_removed_when_it_raises() -> None:
    from airflow.sdk.exceptions import AirflowException

    Ran.exit_code = 3
    with pytest.raises(AirflowException):
        operator().execute(context())

    command = Ran.calls[-1]["command"]
    assert not Path(command[command.index("--parameters-file") + 1]).parent.exists()


# -- what it returns ---------------------------------------------------------


def test_the_validated_result_is_what_the_operator_returns() -> None:
    assert operator().execute(context()) == RESULT


def test_a_failed_child_is_an_airflow_failure_naming_the_code() -> None:
    from airflow.sdk.exceptions import AirflowException

    Ran.exit_code = 7
    with pytest.raises(AirflowException, match="parse_messages exited with 7"):
        operator().execute(context())


def test_a_run_that_published_nothing_is_a_failure() -> None:
    from airflow.sdk.exceptions import AirflowException

    Ran.result = None
    with pytest.raises(AirflowException, match="published no result"):
        operator().execute(context())


@pytest.mark.parametrize(
    "published",
    [{"task": "sample"}, {**RESULT, "read": -1}, {**RESULT, "window": {}}, [1, 2]],
    ids=["short", "negative", "window", "list"],
)
def test_a_malformed_result_is_refused_rather_than_pushed_to_xcom(published: Any) -> None:
    Ran.result = published
    with pytest.raises((ValueError, TypeError)):
        operator().execute(context())


def test_the_counts_reach_every_outlet_the_task_says_it_wrote() -> None:
    from airflow.sdk import Asset

    written_asset = Asset(name="market.orders")
    other = Asset(name="market.books")
    events: dict[Any, Any] = {
        written_asset: SimpleNamespace(extra={}),
        other: SimpleNamespace(extra={}),
    }

    operator(outlets=[written_asset, other]).execute(context(outlet_events=events))

    assert events[written_asset].extra == {
        "task": "sample",
        "read": 2,
        "written": 2,
        "skipped": 0,
    }
    assert events[other].extra == {}, "a table this run did not write claims nothing"


# -- cancellation and serialization ------------------------------------------


def test_on_kill_stops_the_process_group_through_the_retained_hook() -> None:
    running = operator()
    running.hook = Ran()

    running.on_kill()

    assert running.hook.sent


def test_on_kill_before_a_child_exists_is_quiet() -> None:
    operator().on_kill()


def test_a_live_hook_never_reaches_the_serialized_dag() -> None:
    from airflow.serialization.serialized_objects import OperatorSerialization

    built = operator()
    built.hook = Ran()

    serialized = OperatorSerialization.serialize_operator(built)

    assert "hook" not in serialized
    assert json.dumps(serialized)


# -- where it runs -----------------------------------------------------------


def test_a_repository_that_is_not_a_checkout_is_refused(tmp_path: Path) -> None:
    from airflow.sdk.exceptions import AirflowException

    with pytest.raises(AirflowException, match="is not a rekep checkout"):
        operator(repository=str(tmp_path)).execute(context())


def test_a_document_outside_the_repository_is_refused(tmp_path: Path) -> None:
    from airflow.sdk.exceptions import AirflowException

    with pytest.raises(AirflowException, match="is outside"):
        operator(document="../elsewhere/job.yml").execute(context())


def test_a_document_that_is_not_there_is_refused() -> None:
    from airflow.sdk.exceptions import AirflowException

    with pytest.raises(AirflowException, match="is not a task document"):
        operator(document="tasks/parse_messages/absent.yml").execute(context())


# -- against a real child ----------------------------------------------------


@pytest.mark.integration
def test_a_real_child_publishes_a_result_through_the_locked_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The whole path: uv, the runner, the application, and the result file."""
    monkeypatch.undo()
    warehouse = tmp_path / "warehouse"
    catalog = {
        "name": "rekep",
        "properties": {
            "type": "sql",
            "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
            "warehouse": f"file://{warehouse}",
        },
    }
    built = MarimoOperator(
        task_id="parse_messages",
        repository=str(ROOT),
        document="tasks/parse_messages/parse_messages.yml",
        parameters={
            "source": "python/tests/data/app_messages_sample.txt",
            "catalog": catalog,
        },
    )

    result = built.execute(context())

    assert result["task"] == "parse_messages"
    assert (result["read"], result["written"]) == (11, 11)
    assert built.hook is None


@pytest.mark.integration
def test_terminating_the_task_stops_uv_and_the_application_under_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`on_kill` signals the process group, so nothing is left holding a table."""
    monkeypatch.undo()
    # A checkout of one task: the real project, so `uv run` resolves the same
    # locked environment, and a task directory the operator will accept.
    (tmp_path / "python").symlink_to(ROOT / "python")
    directory = tmp_path / "tasks" / "sleeper"
    directory.mkdir(parents=True)
    (directory / "sleeper.yml").write_text(
        "name: sleeper\napplication: sleeper.py\nparameters:\n  seconds: 120\n",
        encoding="utf-8",
    )
    (directory / "sleeper.py").write_text(SLEEPER, encoding="utf-8")
    built = MarimoOperator(
        task_id="sleeper", repository=str(tmp_path), document="tasks/sleeper/sleeper.yml"
    )
    raised: list[BaseException] = []

    def _run() -> None:
        try:
            built.execute(context())
        except BaseException as error:  # noqa: BLE001 - the thread reports it back
            raised.append(error)

    thread = threading.Thread(target=_run)
    thread.start()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if built.hook is not None and getattr(built.hook, "sub_process", None) is not None:
            children = subprocess.run(
                ["pgrep", "-g", str(os.getpgid(built.hook.sub_process.pid))],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.split()
            if len(children) > 1:
                break
        time.sleep(0.2)
    else:  # pragma: no cover - the child never started
        pytest.fail("the child never started")

    group = os.getpgid(built.hook.sub_process.pid)
    built.on_kill()
    thread.join(timeout=60)

    assert not thread.is_alive()
    assert raised, "a killed task fails rather than returning nothing"
    settled = time.monotonic() + 30
    while time.monotonic() < settled:
        if not subprocess.run(
            ["pgrep", "-g", str(group)], capture_output=True, text=True, check=False
        ).stdout.split():
            break
        time.sleep(0.2)
    assert not subprocess.run(
        ["pgrep", "-g", str(group)], capture_output=True, text=True, check=False
    ).stdout.split(), "uv and the application under it are both gone"
