"""The Iceberg and Doris service stacks: CRUD, priorities, drift refusal."""

import dataclasses
import logging
import pathlib

import pytest

from rekep import record
from rekep.doris import Doris
from rekep.iceberg import Iceberg
from rekep.models import Log
from rekep.records import (
    DorisDeployment,
    IcebergCatalog,
    IcebergDeployment,
    IcebergNamespace,
    IcebergTable,
)

REPO_DATA = pathlib.Path(__file__).parents[2] / "stacks"


@record
class LogV2(Log):
    """One parsed line of a trading log."""

    severity: str | None = None
    """Log level, once parsed out."""


@pytest.fixture
def stack(tmp_path: pathlib.Path) -> Iceberg:
    """A real, fully local Iceberg stack: SQLite catalog, file warehouse."""
    root = tmp_path.as_posix()
    return Iceberg(
        IcebergDeployment(
            catalogs=[
                IcebergCatalog(uri=f"sqlite:///{root}/cat.db", warehouse=f"file://{root}/wh")
            ],
            namespaces=[IcebergNamespace(name="yggfin", catalog="iceberg")],
            tables=[IcebergTable(record="rekep.models.Log", name="logs", namespace="yggfin")],
        )
    )


# -- iceberg CRUD, against a live local catalog ------------------------------


def test_deploy_creates_the_whole_stack_in_order(stack: Iceberg) -> None:
    done = stack.deploy()
    assert done == {
        "catalogs": ["iceberg"],
        "namespaces": ["iceberg.yggfin"],
        "tables": ["yggfin.logs"],
    }
    live = stack.tables.get(stack.tables.list()[0])
    assert len(live.schema().fields) == len(Log.into_iceberg_schema().fields)
    assert str(live.spec().fields[0].transform) == "identity"


def test_deploy_twice_is_a_noop(stack: Iceberg, caplog: pytest.LogCaptureFixture) -> None:
    stack.deploy()
    with caplog.at_level(logging.INFO, logger="rekep.iceberg"):
        stack.deploy()
    assert "exists, nothing to do" in caplog.text
    assert "schema converged" in caplog.text


def test_namespaces_get_or_create_and_delete(stack: Iceberg) -> None:
    namespace = stack.namespaces.list()[0]
    assert not stack.namespaces.exists(namespace)
    stack.namespaces.get_or_create(namespace)
    assert stack.namespaces.exists(namespace)
    stack.namespaces.get_or_create(namespace)  # noop
    stack.namespaces.delete(namespace)
    assert not stack.namespaces.exists(namespace)


def test_create_or_update_unions_new_columns(
    stack: Iceberg, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A widened record adds its column to the live table, nothing lost."""
    stack.deploy()
    monkeypatch.setattr(IcebergTable, "record_class", lambda self: LogV2)
    live = stack.tables.create_or_update(stack.tables.list()[0])
    names = [field.name for field in live.schema().fields]
    assert "severity" in names
    assert set(names) >= set(Log.into_arrow_schema().names)


def test_table_delete(stack: Iceberg) -> None:
    stack.deploy()
    table = stack.tables.list()[0]
    stack.tables.delete(table)
    assert not stack.tables.exists(table)


def test_drifted_side_file_refuses_to_deploy(stack: Iceberg) -> None:
    table = stack.tables.list()[0]
    stale = dataclasses.replace(table, fields=[{"name": "wrong", "type": "string", "field_id": 1}])
    with pytest.raises(ValueError, match="drifted from the record"):
        stack.tables.create(stale)


# -- doris plan --------------------------------------------------------------


def test_doris_deploy_orders_catalog_namespace_table() -> None:
    plan = Doris.load(REPO_DATA / "doris").deploy()
    kinds = [
        "CATALOG" if "CATALOG" in statement else "DATABASE" if "DATABASE" in statement else "TABLE"
        for statement in plan
    ]
    assert kinds == sorted(kinds, key=["CATALOG", "DATABASE", "TABLE"].index)
    assert kinds[0] == "CATALOG", "the lakehouse catalog leads the plan"
    assert all("IF NOT EXISTS" in statement for statement in plan)


def test_doris_executor_receives_every_statement() -> None:
    executed: list[str] = []
    plan = Doris.load(REPO_DATA / "doris", execute=executed.append).deploy()
    assert executed == plan


def test_doris_external_catalog_leads_the_plan() -> None:
    from rekep.records import DorisCatalog, DorisNamespace, DorisTable

    deployment = DorisDeployment(
        catalogs=[DorisCatalog(name="lake", type="iceberg", properties={"uri": "http://r"})],
        namespaces=[DorisNamespace(name="prod", catalog="lake")],
        tables=[DorisTable(record="rekep.models.Log", namespace="prod")],
    )
    plan = Doris(deployment).deploy()
    assert plan[0].startswith("CREATE CATALOG IF NOT EXISTS `lake`")
    assert '"iceberg.catalog.type" = "rest"' in plan[0]


# -- shipped side files ------------------------------------------------------


def test_the_shipped_stacks_declare_no_tables() -> None:
    """Tables deploy autonomously now; see `test_dataset.py`'s shipped-dataset tests."""
    assert IcebergDeployment.load(REPO_DATA / "iceberg").tables == []
    assert DorisDeployment.load(REPO_DATA / "doris").tables == []


# -- dry run -----------------------------------------------------------------


def test_dry_run_deploy_creates_nothing(stack: Iceberg, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="rekep.iceberg"):
        done = stack.deploy(dry_run=True)
    assert done["tables"] == ["yggfin.logs"], "the plan still covers everything"
    assert "would create" in caplog.text
    assert not stack.namespaces.exists(stack.namespaces.list()[0])
    assert not stack.tables.exists(stack.tables.list()[0])


def test_dry_run_update_reports_without_touching(
    stack: Iceberg, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stack.deploy()
    monkeypatch.setattr(IcebergTable, "record_class", lambda self: LogV2)
    with caplog.at_level(logging.INFO, logger="rekep.iceberg"):
        live = stack.tables.create_or_update(stack.tables.list()[0], dry_run=True)
    assert "would add columns [severity]" in caplog.text
    assert "severity" not in [field.name for field in live.schema().fields]


def test_dry_run_delete_leaves_the_table(stack: Iceberg) -> None:
    stack.deploy()
    table = stack.tables.list()[0]
    stack.tables.delete(table, dry_run=True)
    assert stack.tables.exists(table)


def test_doris_dry_run_never_reaches_the_executor() -> None:
    executed: list[str] = []
    plan = Doris.load(REPO_DATA / "doris", execute=executed.append).deploy(dry_run=True)
    assert plan, "the plan is still rendered in full"
    assert executed == []


# -- deploy_folder -----------------------------------------------------------


def test_iceberg_deploy_folder_one_call(tmp_path: pathlib.Path) -> None:
    """`Iceberg.deploy_folder` still converges tables explicitly given it --
    it just never reads them from a `tables/` folder any more."""
    root = tmp_path.as_posix()
    catalogs = tmp_path / "catalogs"
    catalogs.mkdir()
    (catalogs / "iceberg.yaml").write_text(
        f'type: sql\nuri: "sqlite:///{root}/cat.db"\nwarehouse: "file://{root}/wh"\n'
    )

    done = Iceberg.deploy_folder(tmp_path, parallel=False)
    assert done["tables"] == [], "no tables/ folder any more"
    again = Iceberg.deploy_folder(tmp_path, dry_run=True)
    assert again["catalogs"] == ["iceberg"]


def test_dataset_deploys_autonomously_one_call(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A `Dataset` needs no `tables/` side file: `deploy_iceberg` converges
    the catalog, namespace and table its own config names, in one call."""
    from rekep.dataset import Dataset

    root = tmp_path.as_posix()
    catalogs = tmp_path / "catalogs"
    catalogs.mkdir()
    (catalogs / "iceberg.yaml").write_text(
        f'type: sql\nuri: "sqlite:///{root}/cat.db"\nwarehouse: "file://{root}/wh"\n'
    )

    stack = Iceberg.load(tmp_path)
    dataset = Dataset(schema="rekep.models.Log", uri="ds:/logs")
    dataset.deploy_iceberg(stack)
    assert stack.catalogs.connect("iceberg").table_exists("default.logs")

    with caplog.at_level(logging.INFO, logger="rekep.iceberg"):
        dataset.deploy_iceberg(stack)  # idempotent, no-op
    assert "schema converged" in caplog.text


def test_doris_deploy_folder_parallel_keeps_level_order() -> None:
    plan = Doris.deploy_folder(REPO_DATA / "doris", parallel=True, dry_run=True)
    kinds = [
        "CATALOG" if "CATALOG" in statement else "DATABASE" if "DATABASE" in statement else "TABLE"
        for statement in plan
    ]
    assert kinds == sorted(kinds, key=["CATALOG", "DATABASE", "TABLE"].index)
