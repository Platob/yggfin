"""The operator Airflow runs this repository's task applications with.

The DAG is not part of the package, so it is imported from where Airflow
imports it -- the same way `tests/test_ci.py` reaches the release script.
"""

from __future__ import annotations

import datetime
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
PRODUCTS = _module("products")

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


#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

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


def test_the_ingestion_dag_fans_out_from_one_committed_book_stage() -> None:
    dag = PIPELINE.ingestion

    assert dag.dag_id == "rekep_ingestion"
    assert dag.schedule == "@daily", "one run a day, covering its own interval"
    assert set(dag.task_dict) == {
        "parse_messages",
        "parse_fix_raw",
        "parse_fix_refined",
        "parse_books",
        "parse_orders",
        "parse_quotes",
        "parse_executions",
    }
    messages = dag.get_task("parse_messages")
    raw = dag.get_task("parse_fix_raw")
    refined = dag.get_task("parse_fix_refined")
    books = dag.get_task("parse_books")
    assert messages.downstream_task_ids == {"parse_fix_raw"}
    assert raw.upstream_task_ids == {"parse_messages"}
    assert raw.downstream_task_ids == {"parse_fix_refined"}
    assert refined.upstream_task_ids == {"parse_fix_raw"}
    assert refined.downstream_task_ids == {"parse_books"}
    assert books.upstream_task_ids == {"parse_fix_refined"}
    assert books.downstream_task_ids == {"parse_orders", "parse_quotes", "parse_executions"}
    assert books.upstream_task_id is None
    for kind in ("orders", "quotes", "executions"):
        child = dag.get_task(f"parse_{kind}")
        assert child.upstream_task_ids == {"parse_books"}
        assert child.downstream_task_ids == set()
        assert child.upstream_task_id == "parse_books"
        assert [asset.name for asset in child.outlets] == [f"market.{kind}"]
    assert [asset.name for asset in messages.outlets] == ["logs.messages"]
    assert [asset.name for asset in raw.outlets] == ["fix.raw"]
    assert [asset.name for asset in refined.outlets] == ["fix.refined"]
    assert [asset.name for asset in books.outlets] == ["market.books"]
    assert dag.params["filesystem"] == "file:data/capture"
    assert dag.params["registry"] is None
    # Each FIX stage names the table it reads under a name of its own, so one
    # Params mapping over the documents hands neither the other's source.
    assert dag.params["messages"] == "logs.messages"
    assert dag.params["raw"] == "fix.raw"
    assert dag.params["refined"] == "fix.refined"
    assert dag.params["books"] == "market.books"
    assert "snapshot_id" not in dag.params
    assert dag.params["start"] is None and dag.params["end"] is None, "the interval fills them"


def test_the_products_dag_starts_when_the_fix_table_is_written() -> None:
    """The second DAG is scheduled on an Asset the first one publishes, so the
    two are one route without either naming the other's tasks."""
    dag = PRODUCTS.products

    assert dag.dag_id == "rekep_products"
    assert set(dag.task_dict) == {"build_dbt"}
    assert [asset.name for asset in dag.timetable.asset_condition.objects] == ["fix.refined"]
    built = dag.get_task("build_dbt")
    assert [asset.name for asset in built.outlets] == [
        "orders.events",
        "orders.current",
        "executions.fills",
    ]
    assert dag.params["project"] == "data/dbt"
    assert dag.params["catalog"] is None


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


def test_the_interval_fills_a_declared_start_and_end(kept: Held) -> None:
    lower = datetime.datetime(2026, 8, 21, 10, tzinfo=UTC)
    upper = datetime.datetime(2026, 8, 21, 11, tzinfo=UTC)

    operator().execute(context(data_interval_start=lower, data_interval_end=upper))
    parameters = written()
    assert parameters["start"] == "2026-08-21T10:00:00+00:00"
    assert parameters["end"] == "2026-08-21T11:00:00+00:00"

    built = operator(task_id="build_dbt", document="tasks/build_dbt/build_dbt.json")
    built.execute(context(data_interval_start=lower, data_interval_end=upper))
    parameters = written()
    assert "start" not in parameters and "end" not in parameters, "a task that declares none"


def test_an_interval_with_no_width_leaves_the_task_its_own_window(kept: Held) -> None:
    """A manual run of an unscheduled DAG infers a point, which is no interval."""
    moment = datetime.datetime(2026, 8, 21, 10, tzinfo=UTC)

    operator().execute(context(data_interval_start=moment, data_interval_end=moment))
    parameters = written()
    assert parameters["start"] is None and parameters["end"] is None


def test_a_bound_the_run_conf_names_wins_over_the_interval(kept: Held) -> None:
    """A person triggering a run for one day means that day."""
    lower = datetime.datetime(2026, 8, 21, 10, tzinfo=UTC)
    upper = datetime.datetime(2026, 8, 21, 11, tzinfo=UTC)
    conf = {"start": "2026-08-14", "end": "2026-08-14"}

    operator().execute(
        context(
            params={**conf},
            dag_run=SimpleNamespace(conf=conf),
            data_interval_start=lower,
            data_interval_end=upper,
        )
    )
    parameters = written()
    assert (parameters["start"], parameters["end"]) == ("2026-08-14", "2026-08-14")


def test_an_explicit_parameter_is_overridden_by_the_param_of_the_same_name(
    kept: Held,
) -> None:
    """Sources merge once, in one order: document, operator, Params, interval."""
    parser = operator(parameters={"filesystem": "file:data/operator"})
    parser.execute(context(params={"filesystem": "file:data/params"}))

    parameters = written()
    assert parameters["filesystem"] == "file:data/params"


@pytest.mark.parametrize("snapshot_id", [0, 42])
def test_fanout_pins_the_actual_parent_window_snapshot_and_table(
    kept: Held,
    snapshot_id: int,
) -> None:
    from rekep.times import unix_of, window_of

    conf = {"start": "2026-08-14", "end": "2026-08-14"}
    interval = {
        "data_interval_start": datetime.datetime(2026, 8, 21, tzinfo=UTC),
        "data_interval_end": datetime.datetime(2026, 8, 22, tzinfo=UTC),
    }
    parent = operator(task_id="parse_books", document="tasks/parse_books/parse_books.json")
    parent.execute(context(params=conf, dag_run=SimpleNamespace(conf=conf), **interval))
    actual = written()
    assert {name: actual[name] for name in conf} == conf
    bounds = window_of(actual["start"], actual["end"])
    upstream = {
        **RESULT,
        "task": "parse_books",
        "targets": {"books": "market.books"},
        "window": dict(zip(("start", "end"), map(unix_of, bounds), strict=True)),
        "snapshot_id": snapshot_id,
    }
    pulled = []

    def pull(*, task_ids: str) -> dict[str, Any]:
        pulled.append(task_ids)
        return upstream

    child = operator(
        task_id="parse_orders",
        document="tasks/parse_orders/parse_orders.json",
        upstream_task_id="parse_books",
    )
    child.execute(
        context(
            params={
                "start": "2025-01-01",
                "end": "2025-01-02",
                "snapshot_id": 99,
                "books": "another.books",
            },
            task_instance=SimpleNamespace(xcom_pull=pull),
            **interval,
        )
    )
    pinned = written()
    assert pulled == ["parse_books"]
    assert {name: pinned[name] for name in ("start", "end")} == upstream["window"]
    assert window_of(pinned["start"], pinned["end"]) == bounds
    assert pinned["snapshot_id"] == snapshot_id
    assert pinned["books"] == "market.books"


@pytest.mark.parametrize(
    "changed",
    [
        {"snapshot_id": None},
        {"snapshot_id": -1},
        {"snapshot_id": True},
        {"window": {"start": None, "end": None}},
        {"window": {"start": 2, "end": 1}},
        {"targets": {"unrelated": "market.books"}},
    ],
)
def test_an_unusable_upstream_snapshot_is_refused_before_launch(changed: dict[str, Any]) -> None:
    from airflow.sdk.exceptions import AirflowException

    upstream = {
        **RESULT,
        "targets": {"books": "market.books"},
        "window": {"start": 1_000, "end": 2_000},
        "snapshot_id": 42,
        **changed,
    }
    child = operator(
        task_id="parse_orders",
        document="tasks/parse_orders/parse_orders.json",
        upstream_task_id="parse_books",
    )
    with pytest.raises(AirflowException, match="parse_books published no usable snapshot"):
        child.execute(context(task_instance=SimpleNamespace(xcom_pull=lambda **_: upstream)))
    assert not Ran.calls


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

#: Valid market capture: administration, quotes, a partial update and trade,
#: an order and a two-sided trade report. The historical bridge fixture stays
#: covered by `test_workflow.py`; its incomplete trade sides are refused by books.
FRAMES = (
    "8=FIX.4.4|35=0|34=1|52=20260814-10:00:00|10=0|",
    "8=FIX.4.4|35=W|34=2|52=20260814-10:00:01|55=AAPL|268=2|"
    "269=0|278=B1|270=100|271=10|269=1|278=A1|270=102|271=12|10=0|",
    "8=FIX.4.4|35=X|34=3|52=20260814-10:00:02|55=AAPL|268=2|"
    "279=1|269=0|278=B1|270=101|271=11|279=0|269=2|278=T1|270=101|271=2|10=0|",
    "8=FIX.4.4|35=D|34=4|52=20260814-10:00:03|11=O1|55=AAPL|54=1|38=5|44=99|59=1|10=0|",
    "8=FIX.4.4|35=AE|34=5|52=20260814-10:00:04|571=T2|150=F|55=AAPL|"
    "32=10|31=101.25|60=20260814-10:00:04|552=2|"
    "54=1|1427=BUY-EXEC|1009=4|37=BUY-ORDER|54=2|1427=SELL-EXEC|1009=6|37=SELL-ORDER|10=0|",
)

#: The day the fixture was captured on, named because a task covers the last
#: day when nothing says otherwise. `end: 2026-08-14` is the exclusive end of it.
WINDOW = {"start": "2026-08-14", "end": "2026-08-14"}

#: The raw stage omits the heartbeat; all four market frames survive the walk
#: and publish events in the three projected tables. A replay replaces them.
LANDED = {
    "parse_messages": {"read": 5, "written": 5, "skipped": 0},
    "parse_fix_raw": {"read": 5, "written": 4, "skipped": 0},
    "parse_fix_refined": {"read": 4, "written": 4, "skipped": 0},
    "parse_books": {"read": 4, "written": 4, "skipped": 0},
    "parse_orders": {"read": 4, "written": 1, "skipped": 0},
    "parse_quotes": {"read": 4, "written": 3, "skipped": 0},
    "parse_executions": {"read": 4, "written": 3, "skipped": 0},
}
REPLAYED = LANDED

#: The table each node publishes, which is also the Asset it declares, and
#: the name each result reports it under.
PUBLISHED = {
    "parse_messages": "logs.messages",
    "parse_fix_raw": "fix.raw",
    "parse_fix_refined": "fix.refined",
    "parse_books": "market.books",
    "parse_orders": "market.orders",
    "parse_quotes": "market.quotes",
    "parse_executions": "market.executions",
}
TARGETS = {
    "parse_messages": "messages",
    "parse_fix_raw": "raw",
    "parse_fix_refined": "refined",
    "parse_books": "books",
    "parse_orders": "orders",
    "parse_quotes": "quotes",
    "parse_executions": "executions",
}
STORED = {
    "logs.messages": 5,
    "fix.raw": 4,
    "fix.refined": 4,
    "market.books": 4,
    "market.orders": 1,
    "market.quotes": 3,
    "market.executions": 3,
}


def counted(result: dict[str, Any]) -> dict[str, int]:
    """The three numbers a schedule is compared on."""
    return {name: result[name] for name in ("read", "written", "skipped")}


def _scheduled(tmp_path: Path) -> dict[str, Any]:
    """A SQLite catalog and file warehouse of this test's own."""
    return {
        "name": "rekep",
        "properties": {
            "type": "sql",
            "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
            "warehouse": (tmp_path / "warehouse").as_uri(),
        },
    }


def _capture(tmp_path: Path) -> Path:
    """One dated native FIX capture, admitted by every stage of the DAG."""
    capture = tmp_path / "market.log"
    capture.write_text(
        "".join(
            f"2026-08-14 10:00:0{index}.000 [250-e7256476:9effef3e6a:72504] "
            f"[ULBridge] (INFO) Sending : {frame}\n"
            for index, frame in enumerate(FRAMES)
        ),
        encoding="utf-8",
    )
    return capture


def _pass(catalog: dict[str, Any], capture: Path) -> dict[str, dict[str, Any]]:
    """Every DAG node, in its declared order, run through its own operator.

    The outlets come off the shipped DAG rather than being spelled again here,
    so a node that stops declaring the Asset it writes fails this test.
    """
    landed = {}
    for task_id in [task.task_id for task in PIPELINE.ingestion.topological_sort()]:
        node = PIPELINE.ingestion.get_task(task_id)
        held: dict[str, Any] = {"catalog": catalog, **WINDOW}
        if task_id == "parse_messages":
            held["filesystem"] = capture.as_uri()
        built = MarimoOperator(
            task_id=task_id,
            repository=str(ROOT),
            document=f"tasks/{task_id}/{task_id}.json",
            parameters=held,
            outlets=list(node.outlets),
            upstream_task_id=node.upstream_task_id,
        )
        events = {asset: SimpleNamespace(extra={}) for asset in node.outlets}
        instance = SimpleNamespace(xcom_pull=lambda *, task_ids: landed[task_ids]["result"])
        result = built.execute(context(outlet_events=events, task_instance=instance))
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
def test_the_scheduled_graph_publishes_the_market_capture_and_replays_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The whole scheduled route: uv, runner, seven applications, seven tables.

    A scheduled run reads each book once per child from the same immutable
    snapshot. Replaying the window replaces every output in one new snapshot.
    """
    monkeypatch.undo()
    catalog = _scheduled(tmp_path)
    capture = _capture(tmp_path)

    landed = _pass(catalog, capture)

    assert {name: counted(held["result"]) for name, held in landed.items()} == LANDED
    for kind in ("orders", "quotes", "executions"):
        assert (
            landed[f"parse_{kind}"]["result"]["source_snapshot_id"]
            == landed["parse_books"]["result"]["snapshot_id"]
        )
        assert (
            landed[f"parse_{kind}"]["result"]["window"] == landed["parse_books"]["result"]["window"]
        )
    for name, table in PUBLISHED.items():
        assert landed[name]["result"]["targets"] == {TARGETS[name]: table}
        # The counts ride on the Asset event, which is what a downstream DAG
        # scheduled on that table reads.
        assert landed[name]["assets"] == {
            table: {"task": name, **LANDED[name]},
        }
    assert _rows(catalog) == STORED

    replayed = _pass(catalog, capture)

    assert {name: counted(held["result"]) for name, held in replayed.items()} == REPLAYED
    assert _rows(catalog) == STORED
    assert _snapshots(catalog) == {name: 2 for name in STORED}, (
        "a replayed schedule replaces its window in one commit per table"
    )


@pytest.mark.integration
def test_a_scheduled_result_is_the_shape_a_route_reads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """What the operator pushes to XCom, checked as the contract it is."""
    from rekep.logs import Stage

    monkeypatch.undo()

    landed = _pass(_scheduled(tmp_path), _capture(tmp_path))

    for name, held in landed.items():
        result = held["result"]
        assert Stage.validated(result) == result
        assert result["task"] == name
        # `window` is a mapping in every result, never null: both the runner
        # and the operator validate it, so the contract is the same both ways.
        assert result["window"] == {
            "start": 1_786_665_600_000_000_000,
            "end": 1_786_752_000_000_000_000,
        }, "the day the run was handed, in epoch nanoseconds"
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
def test_a_real_dag_run_publishes_its_seven_tables_from_its_conf(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The scheduler itself, not just the operator: `airflow dags test`.

    Everything above builds operators by hand, so nothing there proves the DAG
    parses under Airflow's own bundle loading, serializes, or that a run's
    `--conf` reaches every node as declared Params. This runs the shipped DAG
    the way `docs/pipeline/airflow.md` says to trigger it, in a private
    `AIRFLOW_HOME`, and reads the seven tables back.
    """
    from rekep.iceberg import IcebergCatalog

    monkeypatch.undo()
    home = tmp_path / "airflow"
    home.mkdir()
    catalog = _scheduled(tmp_path)
    capture = _capture(tmp_path)
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
            json.dumps({"filesystem": capture.as_uri(), "catalog": catalog, **WINDOW}),
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
    # Every node ran the locked runner, and the run's conf reached them: the
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
    assert stored == STORED
