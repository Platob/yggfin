"""The operator Airflow runs this repository's task applications with.

The DAG is not part of the package, so it is imported from where Airflow
imports it -- the same way `tests/test_ci.py` reaches the release script.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import stat
import string
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
PIPELINE = _module("pipeline")

#: What one attempt returned, in the shape every task returns.
RESULT = {
    "task": "sample",
    "read": 2,
    "written": 2,
    "skipped": 0,
    "sources": {"input": "logs.messages"},
    "targets": {"output": "logs.messages"},
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
    _document = pathlib.Path(__file__).with_suffix(".json")
    seconds = Task.from_json(str(_document)).parameters["seconds"]
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
    held.setdefault("document", "tasks/parse_messages/parse_messages.json")
    return MarimoOperator(**held)


def commands() -> list[list[str]]:
    return [call["command"] for call in Ran.calls]


# -- the DAG -----------------------------------------------------------------


def test_the_ingestion_dag_is_exactly_the_two_streamed_stages() -> None:
    dag = PIPELINE.ingestion

    assert dag.dag_id == "rekep_ingestion"
    assert set(dag.task_dict) == {"parse_messages", "parse_fix"}
    messages = dag.get_task("parse_messages")
    fixed = dag.get_task("parse_fix")
    assert messages.downstream_task_ids == {"parse_fix"}
    assert fixed.upstream_task_ids == {"parse_messages"}
    assert [asset.name for asset in messages.outlets] == ["logs.messages"]
    assert [asset.name for asset in fixed.outlets] == ["fix.messages"]
    assert dag.params["filesystem"] == "file:data/capture"
    assert dag.params["registry"] is None
    assert dag.params["branch"] == "ulbridge"
    assert dag.params["version"] is None
    assert "dedup" not in dag.params


# -- the command it builds ---------------------------------------------------


def test_the_task_command_runs_the_standalone_marimo_runner() -> None:
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
        "python",
        str(ROOT / "tasks" / "airflow" / "marimo_runner.py"),
        str(ROOT / "tasks" / "parse_messages" / "parse_messages.json"),
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


def test_a_configured_cache_directory_reaches_uv() -> None:
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
    assert parameters["filesystem"] == "file:data/capture"
    assert parameters["catalog"]["properties"]["type"] == "sql"


def test_a_param_the_task_does_not_declare_is_not_injected(kept: Held) -> None:
    """Only parameters declared by message ingestion reach its application."""
    operator().execute(context(params={"branch": "wip", "unknown": False}))

    parameters = written()
    assert "unknown" not in parameters
    assert "branch" not in parameters


def test_the_interval_fills_only_a_declared_start_and_end(kept: Held) -> None:
    import datetime

    lower = datetime.datetime(2026, 8, 21, 10, tzinfo=datetime.UTC)
    upper = datetime.datetime(2026, 8, 21, 11, tzinfo=datetime.UTC)

    operator().execute(context(data_interval_start=lower, data_interval_end=upper))
    parameters = written()
    assert "start" not in parameters and "end" not in parameters


def test_an_explicit_parameter_is_overridden_by_the_param_of_the_same_name(
    kept: Held,
) -> None:
    """Sources merge once, in one order: document, operator, Params, interval."""
    parser = operator(parameters={"filesystem": "file:data/operator"})
    parser.execute(context(params={"filesystem": "file:data/params"}))

    parameters = written()
    assert parameters["filesystem"] == "file:data/params"


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
    parser = operator(parameters={"catalog": catalog})
    parser.execute(context())

    parameters = written()
    assert parameters["catalog"] == catalog


# -- the attempt directory ---------------------------------------------------


def test_the_parameter_document_is_readable_only_by_the_worker(kept: Held) -> None:
    operator().execute(context())

    command = Ran.calls[-1]["command"]
    parameters = Path(command[command.index("--parameters-file") + 1])
    assert stat.S_IMODE(parameters.stat().st_mode) == 0o600
    assert stat.S_IMODE(parameters.parent.stat().st_mode) == 0o700


#: What `tempfile.mkdtemp` builds its random suffix from.
_MKDTEMP_ALPHABET = frozenset(string.ascii_lowercase + string.digits + "_")


def test_the_attempt_directory_names_the_attempt_that_owns_it(kept: Held) -> None:
    operator().execute(
        context(run_id="scheduled__2026-08-21T10:00:00+00:00", map_index=3, try_number=2)
    )

    command = Ran.calls[-1]["command"]
    directory = Path(command[command.index("--result-file") + 1]).parent
    stem, _, unique = directory.name.rpartition("-")
    assert stem.endswith("parse_messages-scheduled__2026-08-21T10_00_00_00_00-3-2")
    assert ":" not in directory.name and "+" not in directory.name
    # `mkdtemp` names its suffix out of lowercase letters, digits and `_`, so
    # the check is that alphabet and not `isalnum`: one suffix in five holds an
    # underscore, and the test failed on those.
    assert unique and set(unique) <= _MKDTEMP_ALPHABET, "and a unique suffix nothing else can hold"


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

    written_asset = Asset(name="logs.messages")
    other = Asset(name="logs.archive")
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
        operator(document="../elsewhere/job.json").execute(context())


def test_a_document_that_is_not_there_is_refused() -> None:
    from airflow.sdk.exceptions import AirflowException

    with pytest.raises(AirflowException, match="is not a task document"):
        operator(document="tasks/parse_messages/absent.json").execute(context())


# -- against a real child ----------------------------------------------------

#: The bridge fixture the CLI route is pinned against in `test_workflow.py`,
#: so scheduling it changes the counts nowhere.
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"

#: What each stage returns the first time it sees those 111 physical rows, and
#: what a replay of the same capture returns.
LANDED = {
    "parse_messages": {"read": 111, "written": 111, "skipped": 0},
    "parse_fix": {"read": 111, "written": 111, "skipped": 0},
}
REPLAYED = {
    name: {"read": counts["read"], "written": 0, "skipped": counts["read"]}
    for name, counts in LANDED.items()
}

#: The table each node publishes, which is also the Asset it declares.
PUBLISHED = {"parse_messages": "logs.messages", "parse_fix": "fix.messages"}


def counted(result: dict[str, Any]) -> dict[str, int]:
    """The three numbers a schedule is compared on."""
    return {name: result[name] for name in ("read", "written", "skipped")}


def _scheduled(tmp_path: Path) -> dict[str, Any]:
    """A SQLite catalog and file warehouse of this test's own."""
    return {
        "name": "rekep",
        "properties": {
            "type": "sql",
            "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
            "warehouse": f"file://{tmp_path / 'warehouse'}",
        },
    }


def _pass(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Every DAG node, in its declared order, run through its own operator.

    The outlets come off the shipped DAG rather than being spelled again here,
    so a node that stops declaring the Asset it writes fails this test.
    """
    landed = {}
    for task_id in [task.task_id for task in PIPELINE.ingestion.topological_sort()]:
        node = PIPELINE.ingestion.get_task(task_id)
        held: dict[str, Any] = {"catalog": catalog}
        if task_id == "parse_messages":
            held["filesystem"] = FIXTURE.as_uri()
        built = MarimoOperator(
            task_id=task_id,
            repository=str(ROOT),
            document=f"tasks/{task_id}/{task_id}.json",
            parameters=held,
            outlets=list(node.outlets),
        )
        events = {asset: SimpleNamespace(extra={}) for asset in node.outlets}
        result = built.execute(context(outlet_events=events))
        assert built.hook is None, "a landed attempt keeps no child"
        landed[result["task"]] = {
            "result": result,
            "assets": {asset.name: dict(event.extra) for asset, event in events.items()},
        }
    return landed


def _rows(catalog: dict[str, Any]) -> dict[str, int]:
    from rekep.iceberg import IcebergCatalog

    store = IcebergCatalog.from_dict(catalog)
    try:
        return {
            dataset.identifier: dataset.read_arrow_table().num_rows
            for dataset in store.datasets(None)
        }
    finally:
        store.close()


def _snapshots(catalog: dict[str, Any]) -> dict[str, int]:
    from rekep.iceberg import IcebergCatalog

    store = IcebergCatalog.from_dict(catalog)
    try:
        return {
            dataset.identifier: len(store.catalog.load_table(dataset.identifier).metadata.snapshots)
            for dataset in store.datasets(None)
        }
    finally:
        store.close()


@pytest.mark.integration
def test_the_scheduled_graph_publishes_the_bridge_fixture_and_replays_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The whole scheduled route: uv, runner, both applications, both tables.

    `test_workflow.py` pins these same counts for `rekep task run`. Pinning
    them here as well is what says the two routes are one pipeline: a
    scheduled run reads the same capture into the same two products, and its
    replay writes nothing, exactly as the command-line run does.
    """
    monkeypatch.undo()
    catalog = _scheduled(tmp_path)

    landed = _pass(catalog)

    assert {name: counted(held["result"]) for name, held in landed.items()} == LANDED
    for name, table in PUBLISHED.items():
        assert landed[name]["result"]["targets"] == {
            "messages" if name == "parse_messages" else "fix": table
        }
        # The counts ride on the Asset event, which is what a downstream DAG
        # scheduled on that table reads.
        assert landed[name]["assets"] == {
            table: {"task": name, **LANDED[name]},
        }
    assert _rows(catalog) == {"logs.messages": 111, "fix.messages": 111}

    replayed = _pass(catalog)

    assert {name: counted(held["result"]) for name, held in replayed.items()} == REPLAYED
    assert _rows(catalog) == {"logs.messages": 111, "fix.messages": 111}
    assert _snapshots(catalog) == {"logs.messages": 1, "fix.messages": 1}, (
        "a replayed schedule commits no empty snapshot"
    )


@pytest.mark.integration
def test_a_scheduled_result_is_the_shape_a_route_reads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """What the operator pushes to XCom, checked as the contract it is."""
    from rekep.logs import Stage

    monkeypatch.undo()

    landed = _pass(_scheduled(tmp_path))

    for name, held in landed.items():
        result = held["result"]
        assert Stage.validated(result) == result
        assert result["task"] == name
        # `window` is a mapping in every result, never null: both the runner
        # and the operator validate it, so the contract is the same both ways.
        assert result["window"] == {"start": None, "end": None}
        assert len(json.dumps(result)) < 4096, "XCom carries a summary, never a payload"


@pytest.mark.integration
def test_terminating_the_task_stops_the_runner_process_group(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`on_kill` signals the process group, so nothing is left holding a table."""
    monkeypatch.undo()
    # A checkout of one task: the real locked project environment, and a task
    # directory the operator will accept.
    (tmp_path / "python").symlink_to(ROOT / "python")
    directory = tmp_path / "tasks" / "sleeper"
    directory.mkdir(parents=True)
    # The runner the operator hands `uv` lives beside the tasks it runs, so a
    # checkout without it never reaches a child process at all.
    (tmp_path / "tasks" / "airflow").symlink_to(DAGS)
    (directory / "sleeper.json").write_text(
        json.dumps(
            {
                "name": "sleeper",
                "application": "sleeper.py",
                "parameters": {"seconds": 30},
            }
        ),
        encoding="utf-8",
    )
    (directory / "sleeper.py").write_text(SLEEPER, encoding="utf-8")
    built = MarimoOperator(
        task_id="sleeper", repository=str(tmp_path), document="tasks/sleeper/sleeper.json"
    )
    raised: list[BaseException] = []

    def _run() -> None:
        try:
            built.execute(context())
        except BaseException as error:  # noqa: BLE001 - the thread reports it back
            raised.append(error)

    thread = threading.Thread(target=_run)
    thread.start()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if built.hook is not None and getattr(built.hook, "sub_process", None) is not None:
            children = subprocess.run(
                ["pgrep", "-g", str(os.getpgid(built.hook.sub_process.pid))],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.split()
            if children:
                break
        time.sleep(0.2)
    else:  # pragma: no cover - the child never started
        pytest.fail("the child never started")

    group = os.getpgid(built.hook.sub_process.pid)
    built.on_kill()
    thread.join(timeout=15)

    assert not thread.is_alive()
    assert raised, "a killed task fails rather than returning nothing"
    settled = time.monotonic() + 10
    while time.monotonic() < settled:
        if not subprocess.run(
            ["pgrep", "-g", str(group)], capture_output=True, text=True, check=False
        ).stdout.split():
            break
        time.sleep(0.2)
    assert not subprocess.run(
        ["pgrep", "-g", str(group)], capture_output=True, text=True, check=False
    ).stdout.split(), "uv and the runner process group are gone"


@pytest.mark.integration
def test_a_real_dag_run_publishes_both_tables_from_its_conf(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The scheduler itself, not just the operator: `airflow dags test`.

    Everything above builds operators by hand, so nothing there proves the DAG
    parses under Airflow's own bundle loading, serializes, or that a run's
    `--conf` reaches both nodes as declared Params. This runs the shipped DAG
    the way `docs/pipeline/airflow.md` says to trigger it, in a private
    `AIRFLOW_HOME`, and reads the two products back.
    """
    from rekep.iceberg import IcebergCatalog

    monkeypatch.undo()
    home = tmp_path / "airflow"
    home.mkdir()
    catalog = _scheduled(tmp_path)
    environment = {
        **os.environ,
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{home / 'airflow.db'}",
        "AIRFLOW__CORE__DAGS_FOLDER": str(DAGS),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    }
    migrated = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "airflow", "db", "migrate"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert migrated.returncode == 0, migrated.stdout + migrated.stderr

    run = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "airflow",
            "dags",
            "test",
            "rekep_ingestion",
            "--conf",
            json.dumps({"filesystem": FIXTURE.as_uri(), "catalog": catalog}),
        ],
        env=environment,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )

    printed = run.stdout + run.stderr
    assert run.returncode == 0, printed
    # The scheduler's own record of the run, not the CLI's prose: that line is
    # only printed to a terminal, and this child has none.
    assert re.search(r"DagRun Finished:.*state=success", printed), printed
    # Both nodes ran the locked runner, and the run's conf reached them: the
    # catalog they wrote is this test's, not the document's relative default.
    for task_id in PUBLISHED:
        assert f"tasks/{task_id}/{task_id}.json" in printed, printed
    store = IcebergCatalog.from_dict(catalog)
    try:
        stored = {
            dataset.identifier: dataset.read_arrow_table().num_rows
            for dataset in store.datasets(None)
        }
    finally:
        store.close()
    assert stored == {"logs.messages": 111, "fix.messages": 111}
