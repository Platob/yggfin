"""Dispatching a bundled task to a pod on EKS: `EksRekepOperator` and `dispatch`.

Nothing here reaches a cluster: `EksPodOperator` launching a pod is replaced
by a recorder, and the pod itself is the built task image, run with the
arguments the operator hands it when `REKEP_IMAGE` names one.
"""

from __future__ import annotations

import contextlib
import datetime
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("airflow", reason="the operator runs under Airflow, which is POSIX-only")

from rekep import cli  # noqa: E402
from rekep.tasks import Task  # noqa: E402

from .test_rekep_operator import DAGS, RESULT, ROOT, _module, context  # noqa: E402

OPERATOR = _module("rekep_operator")
EKS = _module("eks_rekep_operator")
DISPATCH = _module("dispatch")
EksRekepOperator = EKS.EksRekepOperator

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

#: One day's interval, as a daily schedule hands it to every node.
INTERVAL = {
    "data_interval_start": datetime.datetime(2026, 8, 14, tzinfo=UTC),
    "data_interval_end": datetime.datetime(2026, 8, 15, tzinfo=UTC),
}


class Pod:
    """What `EksPodOperator` was asked to launch, instead of launching it."""

    launched: list[dict[str, Any]] = []
    result: Any = RESULT


@pytest.fixture(autouse=True)
def _launched(monkeypatch: pytest.MonkeyPatch) -> None:
    Pod.launched = []
    Pod.result = dict(RESULT)

    def execute(self: Any, context: Any) -> Any:
        Pod.launched.append(
            {
                "cmds": list(self.cmds),
                "arguments": list(self.arguments),
                "do_xcom_push": self.do_xcom_push,
                "image": self.image,
                "cluster_name": self.cluster_name,
                "namespace": self.namespace,
            }
        )
        return None if self.deferrable else Pod.result

    monkeypatch.setattr(EKS.EksPodOperator, "execute", execute)
    monkeypatch.setattr(EKS.EksPodOperator, "trigger_reentry", lambda self, c, e: Pod.result)


def eks(**held: Any) -> Any:
    held.setdefault("task_id", "parse_messages")
    held.setdefault("task_name", held["task_id"])
    held.setdefault("repository", str(ROOT))
    held.setdefault("image", "rekep:test")
    held.setdefault("cluster_name", "market-data")
    return EksRekepOperator(**held)


def expanded(argument: str) -> str:
    """One container argument as Kubernetes hands it over: `$(NAME)` expanded
    from the container's environment, which is empty here, and `$$` read as `$`."""
    return re.sub(r"\$\$|\$\((\w+)\)", lambda found: "" if found.group(1) else "$", argument)


def read_back(arguments: list[str]) -> dict[str, Any]:
    """The parameters the pod's own `rekep` reads its arguments as."""
    parsed = cli._parser().parse_args([expanded(argument) for argument in arguments])
    assert parsed.action == "run"
    assert parsed.result_file == EKS.RESULT
    return cli._overrides(parsed)


# -- the pod it launches ------------------------------------------------------


def test_the_pod_runs_the_bundled_task_and_publishes_where_the_sidecar_reads() -> None:
    result = eks(namespace="rekep").execute(context(**INTERVAL))

    (pod,) = Pod.launched
    assert pod["cmds"] == ["rekep"]
    assert pod["arguments"][:3] == ["tasks", "parse_messages", "run"]
    assert pod["arguments"][-2:] == ["--result-file", "/airflow/xcom/return.json"]
    assert pod["do_xcom_push"] is True, "the sidecar is what hands the result back"
    assert (pod["image"], pod["cluster_name"], pod["namespace"]) == (
        "rekep:test",
        "market-data",
        "rekep",
    )
    assert result == RESULT


def test_every_argument_reads_back_as_the_parameter_it_spells() -> None:
    """Each value is its JSON, so text that parses as something else stays text."""
    awkward = {
        "filesystem": "s3://capture/$(HOME)/$$day",
        "rowheader": 'a=b "quoted" \\ é 2026',
        "catalog": {"name": "rekep", "properties": {"type": "glue", "glue.region": "eu-west-1"}},
    }
    eks(parameters=awkward).execute(context(**INTERVAL))

    (pod,) = Pod.launched
    assert read_back(pod["arguments"]) == {
        **Task("parse_messages").parameters,
        **awkward,
        "start": "2026-08-14T00:00:00+00:00",
        "end": "2026-08-15T00:00:00+00:00",
    }


def test_the_pod_takes_the_parameters_the_worker_would_run() -> None:
    """One resolution for both operators: defaults, operator, Params, interval."""
    held = context(params={"rowheader": "[%(ts)s]", "books": "ignored"}, **INTERVAL)
    local = OPERATOR.RekepOperator(
        task_id="parse_messages", task_name="parse_messages", repository=str(ROOT)
    )

    eks().execute(held)

    (pod,) = Pod.launched
    assert read_back(pod["arguments"]) == local._resolved(held)
    assert "books" not in read_back(pod["arguments"])


def test_a_fanout_pod_reads_the_books_snapshot_its_upstream_committed() -> None:
    upstream = {
        **RESULT,
        "task": "parse_books",
        "targets": {"books": "market.books"},
        "window": {"start": 1_000, "end": 2_000},
        "snapshot_id": 42,
    }
    Pod.result = {**RESULT, "task": "parse_orders"}
    child = eks(task_id="parse_orders", upstream_task_id="parse_books")

    child.execute(
        context(
            params={"snapshot_id": 7, "books": "another.books"},
            task_instance=SimpleNamespace(xcom_pull=lambda **_: upstream),
            **INTERVAL,
        )
    )

    (pod,) = Pod.launched
    pinned = read_back(pod["arguments"])
    assert (pinned["start"], pinned["end"], pinned["snapshot_id"]) == (1_000, 2_000, 42)
    assert pinned["books"] == "market.books"


@pytest.mark.parametrize(
    "held",
    [
        {"task_name": "parse_nothing"},
        {"task_name": "../tasks/parse_messages"},
        {"parameters": {"bokos": True}},
    ],
    ids=["unknown", "traversal", "undeclared"],
)
def test_nothing_is_launched_for_a_run_the_checkout_does_not_declare(
    held: dict[str, Any],
) -> None:
    from airflow.sdk.exceptions import AirflowException

    with pytest.raises(AirflowException, match="no task is named|takes no bokos"):
        eks(**held).execute(context(**INTERVAL))
    assert not Pod.launched


@pytest.mark.parametrize("published", [None, {"task": "parse_messages"}, {**RESULT, "read": -1}])
def test_a_result_the_sidecar_hands_back_is_validated(published: Any) -> None:
    Pod.result = published
    with pytest.raises((TypeError, ValueError)):
        eks().execute(context(**INTERVAL))


def test_the_counts_reach_every_outlet_the_pod_says_it_wrote() -> None:
    from airflow.sdk import Asset

    written_asset, other = Asset(name="logs.messages"), Asset(name="logs.archive")
    events: dict[Any, Any] = {
        written_asset: SimpleNamespace(extra={}),
        other: SimpleNamespace(extra={}),
    }

    eks(outlets=[written_asset, other]).execute(context(outlet_events=events, **INTERVAL))

    assert events[written_asset].extra == {
        "task": "parse_messages",
        "read": 2,
        "written": 2,
        "skipped": 0,
    }
    assert events[other].extra == {}


def test_a_deferred_pod_publishes_when_the_triggerer_hands_it_back() -> None:
    deferred = eks(deferrable=True)

    assert deferred.execute(context(**INTERVAL)) is None, "the pod is still running"
    (pod,) = Pod.launched
    assert pod["arguments"][:3] == ["tasks", "parse_messages", "run"]
    assert deferred.trigger_reentry(context(**INTERVAL), {"status": "success"}) == RESULT

    Pod.result = {"task": "parse_messages"}
    with pytest.raises(ValueError, match="missing"):
        deferred.trigger_reentry(context(**INTERVAL), {"status": "success"})


def test_a_deferring_pod_already_done_publishes_from_execute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The provider re-enters inline when the pod finished before it could
    defer, and drops what re-entry returned: `execute` hands it back instead."""

    def done_before_deferring(self: Any, context: Any) -> None:
        self.trigger_reentry(context, {"status": "success"})

    monkeypatch.setattr(EKS.EksPodOperator, "execute", done_before_deferring)

    assert eks(deferrable=True).execute(context(**INTERVAL)) == RESULT


def test_the_pod_fields_render_as_text_under_a_native_dag() -> None:
    """Kubernetes takes strings; the task's own fields keep the DAG's native
    rendering, so a templated parameter can still be a list."""
    from airflow.sdk import DAG
    from kubernetes.client import models as k8s

    with DAG("native", schedule=None, render_template_as_native_obj=True):
        built = eks(
            env_vars={"THREADS": "8", "REKEP_DBT_CATALOG": '{"name": "rekep"}'},
            labels={"team": "1"},
            container_resources=k8s.V1ResourceRequirements(requests={"cpu": "2"}),
            parameters={"rowheader": "{{ params.header }}"},
        )

    built.render_template_fields({"params": {"header": ["[", "]"]}})

    assert [(env.name, env.value) for env in built.env_vars] == [
        ("THREADS", "8"),
        ("REKEP_DBT_CATALOG", '{"name": "rekep"}'),
    ]
    assert built.labels == {"team": "1"}
    assert built.container_resources.requests == {"cpu": "2"}
    assert built.parameters == {"rowheader": ["[", "]"]}


@pytest.mark.parametrize("owned", ["cmds", "arguments", "do_xcom_push"])
def test_the_command_and_its_sidecar_are_the_operators_own(owned: str) -> None:
    with pytest.raises(TypeError, match=owned):
        eks(**{owned: ["sh"] if owned != "do_xcom_push" else False})


def test_the_pod_is_the_task_container_beside_the_xcom_sidecar() -> None:
    """The spec `KubernetesPodOperator` itself builds, without a cluster."""
    from kubernetes.client import models as k8s

    built = eks(
        task_id="parse_fix_raw",
        namespace="rekep",
        service_account_name="rekep",
        env_vars={"AWS_REGION": "eu-west-1"},
        container_resources=k8s.V1ResourceRequirements(requests={"memory": "8Gi"}),
    )
    built.arguments = built._arguments(built._resolved(context(**INTERVAL)))

    pod = built.build_pod_request_obj(None, dry_run=True)

    base, sidecar = pod.spec.containers
    assert (base.image, base.command) == ("rekep:test", ["rekep"])
    assert base.args == built.arguments
    assert [(mount.name, mount.mount_path) for mount in base.volume_mounts] == [
        ("xcom", str(Path(EKS.RESULT).parent))
    ]
    assert base.resources.requests == {"memory": "8Gi"}
    assert [(env.name, env.value) for env in base.env] == [("AWS_REGION", "eu-west-1")]
    assert sidecar.name == "airflow-xcom-sidecar"
    assert (pod.metadata.namespace, pod.spec.service_account_name) == ("rekep", "rekep")
    assert pod.spec.restart_policy == "Never"


# -- where each node runs -----------------------------------------------------


def dispatched(path: Path | None, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Both DAGs, parsed afresh under `REKEP_EKS_CONFIG` naming `path`."""
    if path is None:
        monkeypatch.delenv(DISPATCH.EKS, raising=False)
    else:
        monkeypatch.setenv(DISPATCH.EKS, str(path))
    dags = {}
    for name in ("pipeline", "products"):
        specification = importlib.util.spec_from_file_location(
            f"{name}_dispatched", DAGS / f"{name}.py"
        )
        assert specification is not None and specification.loader is not None
        module = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(module)
        dags[name] = module
    return {
        node.task_id: node
        for dag in (dags["pipeline"].ingestion, dags["products"].products)
        for node in dag.tasks
    }


def settings(tmp_path: Path, document: Any) -> Path:
    path = tmp_path / "eks.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


EKS_DOCUMENT = {
    "cluster_name": "market-data",
    "image": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/rekep:abc123",
    "namespace": "rekep",
    "service_account_name": "rekep",
    "region": "eu-west-1",
    "container_resources": {"requests": {"cpu": "2", "memory": "8Gi"}},
    "tasks": {"build_dbt": {"container_resources": {"requests": {"memory": "16Gi"}}}},
}


def test_unset_every_node_runs_on_the_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    nodes = dispatched(None, monkeypatch)

    assert {type(node).__name__ for node in nodes.values()} == {"RekepOperator"}


def test_a_dispatch_document_runs_every_node_in_a_pod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    nodes = dispatched(settings(tmp_path, EKS_DOCUMENT), monkeypatch)

    assert {type(node).__name__ for node in nodes.values()} == {"EksRekepOperator"}
    assert len(nodes) == 8
    for name, node in nodes.items():
        assert node.task_name == name
        assert node.cluster_name == "market-data"
        assert node.image == EKS_DOCUMENT["image"]
        assert (node.namespace, node.service_account_name, node.region) == (
            "rekep",
            "rekep",
            "eu-west-1",
        )
        assert tuple(asset.name for asset in node.outlets) == Task(name).targets
    assert nodes["parse_fix_raw"].container_resources.requests == {"cpu": "2", "memory": "8Gi"}
    assert nodes["build_dbt"].container_resources.requests == {"memory": "16Gi"}, (
        "a task's own keyword replaces the shared one whole"
    )
    assert nodes["parse_orders"].upstream_task_id == "parse_books"


@pytest.mark.parametrize(
    ("document", "refusal"),
    [
        ([1, 2], "JSON object of EksPodOperator keywords"),
        ({**EKS_DOCUMENT, "tasks": {"parse_nothing": {}}}, "no bundled task named parse_nothing"),
    ],
    ids=["not-an-object", "unknown-task"],
)
def test_a_dispatch_document_that_names_nothing_real_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, document: Any, refusal: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=refusal):
        dispatched(settings(tmp_path, document), monkeypatch)


def test_parsing_a_dispatched_dag_imports_no_task_code(tmp_path: Path) -> None:
    """The scheduler parses the DAGs; only the pod runs `rekep`."""
    parsed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-c",
            "import sys; import pipeline, products; "
            "print(sorted({type(n).__name__ for n in pipeline.ingestion.tasks})); "
            "print(sorted(name for name in sys.modules if name.split('.')[0] == 'rekep'))",
        ],
        env={
            **os.environ,
            "PYTHONPATH": str(DAGS),
            DISPATCH.EKS: str(settings(tmp_path, EKS_DOCUMENT)),
        },
        capture_output=True,
        text=True,
        check=True,
    )
    assert parsed.stdout.split("\n")[:2] == ["['EksRekepOperator']", "[]"], parsed.stderr


def test_a_dispatched_dag_serializes_with_its_pods(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from airflow.serialization.serialized_objects import DagSerialization

    nodes = dispatched(settings(tmp_path, EKS_DOCUMENT), monkeypatch)
    dag = nodes["parse_orders"].dag

    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))

    node = restored.get_task("parse_orders")
    assert (node.task_name, node.upstream_task_id) == ("parse_orders", "parse_books")
    assert node.cluster_name == "market-data"


# -- the image the pod runs ---------------------------------------------------

#: A built task image (`docker build -t <name> .` at the repository root).
IMAGE = os.environ.get("REKEP_IMAGE")

#: The bridge capture the ingestion stages read.
FIXTURE = ROOT / "data" / "capture" / "ulbridge.log"

#: The ingestion stages the bridge fixture runs through, and what each reads,
#: writes and skips.
STAGES = (
    ("parse_messages", (144, 144, 0)),
    ("parse_fix_raw", (144, 49, 30)),
    ("parse_fix_refined", (49, 19, 0)),
    ("build_dbt", (29, 31, 0)),
)


class Image:
    """The task image run as each pod runs it, over one shared directory.

    The directory is mounted at its own path, so the catalog's absolute
    locations mean the same file inside the container and out of it. A pod
    writes as the image's own user and this process as its own, and a bind
    mount keeps each file's owner where the object store a real pod writes
    keeps none: `shared` hands the directory over around a write of this
    process's own.
    """

    def __init__(self, image: str, root: Path) -> None:
        self.image, self.root = image, root
        root.mkdir()
        root.chmod(0o777)
        self.catalog = {
            "name": "rekep",
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / 'catalog.db'}",
                "warehouse": (root / "warehouse").as_uri(),
            },
        }

    def run(self, name: str, held: dict[str, Any], **options: Any) -> dict[str, Any]:
        """One stage's pod: the operator's own command and arguments, as
        Kubernetes hands them over, with its result read where the sidecar reads."""
        xcom = self.root / f"xcom-{name}"
        xcom.mkdir()
        xcom.chmod(0o777)
        pod = eks(task_id=name, **options)
        arguments = [expanded(argument) for argument in pod._arguments(pod._resolved(held))]
        ran = subprocess.run(  # noqa: S603
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                *pod.cmds,
                "--volume",
                f"{self.root}:{self.root}",
                "--volume",
                f"{xcom}:{Path(EKS.RESULT).parent}",
                self.image,
                *arguments,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert ran.returncode == 0, ran.stderr[-2000:]
        return pod._published(held, json.loads((xcom / "return.json").read_text()))

    @contextlib.contextmanager
    def shared(self) -> Iterator[None]:
        """This process writing between pods: everything under the directory
        is made everyone's to write before the write and after it -- the
        catalog's SQLite file included, which SQLite creates owner-writable
        whatever the umask. Only an owner or root may change a mode, so the
        image's root does it."""
        self._chmod()
        yield
        self._chmod()

    def _chmod(self) -> None:
        subprocess.run(  # noqa: S603
            [
                "docker",
                "run",
                "--rm",
                "--user",
                "0",
                "--entrypoint",
                "chmod",
                "--volume",
                f"{self.root}:{self.root}",
                self.image,
                "-R",
                "a+rwX",
                str(self.root),
            ],
            capture_output=True,
            check=True,
        )


@pytest.mark.integration
@pytest.mark.skipif(
    not IMAGE or shutil.which("docker") is None, reason="REKEP_IMAGE names no built task image"
)
def test_the_image_runs_what_each_pod_is_handed(tmp_path: Path) -> None:
    """Every task's pod, but for the cluster: the ingestion stages over the
    bridge fixture, the market stages over the market frames with each event
    pod pinned to the book pod's snapshot, and maintenance over all of it."""
    from .test_market import FRAMES
    from .test_market_pipeline import COUNTS, refined
    from .test_market_pipeline import WINDOW as MARKET

    image = Image(str(IMAGE), tmp_path / "pods")
    shutil.copy(FIXTURE, image.root / FIXTURE.name)
    day = context(**INTERVAL)

    for name, counts in STAGES:
        overrides: dict[str, Any] = {"catalog": image.catalog}
        if name == "parse_messages":
            overrides["filesystem"] = (image.root / FIXTURE.name).as_uri()
        result = image.run(name, day, parameters=overrides)
        assert (result["read"], result["written"], result["skipped"]) == counts, name

    with image.shared():
        refined(image, FRAMES)
    market = context(params=MARKET, dag_run=SimpleNamespace(conf=MARKET))
    books = image.run("parse_books", market, parameters={"catalog": image.catalog})
    assert (books["read"], books["written"]) == (5, 4)
    pinned = context(
        params=MARKET,
        dag_run=SimpleNamespace(conf=MARKET),
        task_instance=SimpleNamespace(xcom_pull=lambda **_: books),
    )
    for kind, written in COUNTS.items():
        events = image.run(
            f"parse_{kind}",
            pinned,
            parameters={"catalog": image.catalog},
            upstream_task_id="parse_books",
        )
        assert events["source_snapshot_id"] == books["snapshot_id"], kind
        assert (events["read"], events["written"]) == (4, written), kind

    maintained = image.run(
        "optimize_iceberg", day, parameters={"catalog": image.catalog, "remove_orphans": False}
    )
    assert maintained["tables"] == 10


@pytest.mark.integration
@pytest.mark.skipif(
    not IMAGE or shutil.which("docker") is None, reason="REKEP_IMAGE names no built task image"
)
def test_the_image_fails_a_pod_left_on_the_local_default_catalog(tmp_path: Path) -> None:
    """The image's `data/` is not the task's to write: a run that names no
    shared catalog fails rather than landing in a filesystem the pod discards."""
    ran = subprocess.run(  # noqa: S603
        ["docker", "run", "--rm", str(IMAGE), "tasks", "parse_messages", "deploy"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert ran.returncode == 1
    assert "unable to open database file" in ran.stderr
