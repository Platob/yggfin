"""Doris DDL and the deployment registry that configures it."""

import datetime
import pathlib
from typing import Annotated

import pytest

from rekep import Arrow, Record, record
from rekep.models import Log
from rekep.records import DorisCatalog, DorisDeployment, DorisNamespace, DorisTable

REPO_DORIS = pathlib.Path(__file__).parents[3] / "stacks" / "doris"


@record
class Fill(Record):
    """One fill."""

    day: Annotated[datetime.date, Arrow(partition="day", key=True)]
    """Trading day."""

    order_id: Annotated[str, Arrow(key=True)]
    """Exchange order id."""

    account: Annotated[str, Arrow(partition="bucket[16]")]
    """Account code."""

    qty: int
    """Signed quantity."""


def declare(root: pathlib.Path, folder: str, name: str, content: str = "") -> None:
    (root / folder).mkdir(exist_ok=True)
    (root / folder / f"{name}.yaml").write_text(content or "{}\n")


# -- the registry -----------------------------------------------------------


def test_an_empty_folder_is_a_working_deployment(tmp_path: pathlib.Path) -> None:
    deployment = DorisDeployment.load(tmp_path)
    assert deployment.catalogs == [DorisCatalog()]
    assert deployment.catalogs[0].type == "iceberg", "lakehouse-first default"
    assert deployment.namespaces == [DorisNamespace()]
    assert deployment.tables == []
    assert "CREATE TABLE IF NOT EXISTS iceberg.default.fill (" in deployment.ddl_for(Fill)


def test_catalogs_and_namespaces_come_from_their_folders(tmp_path: pathlib.Path) -> None:
    declare(tmp_path, "catalogs", "olap")
    declare(tmp_path, "namespaces", "lake", "catalog: olap\nreplication_num: 3\n")
    deployment = DorisDeployment.load(tmp_path)
    assert deployment.catalog("olap").name == "olap"
    assert deployment.namespace("lake").replication_num == 3
    ddl = deployment.ddl_for(Fill, namespace="lake")
    assert "CREATE TABLE IF NOT EXISTS olap.lake.fill (" in ddl
    assert '"replication_num" = "3"' in ddl


def test_a_tables_folder_is_no_longer_loaded(tmp_path: pathlib.Path) -> None:
    """Tables deploy autonomously now (`rekep.dataset.Dataset`) -- a stray
    `tables/` folder from an older-style deployment is simply never read."""
    declare(tmp_path, "tables", "fills", "record: rekep:///records/log\n")
    assert DorisDeployment.load(tmp_path).tables == []


def test_the_file_stem_defaults_the_name(tmp_path: pathlib.Path) -> None:
    declare(tmp_path, "catalogs", "olap")
    assert DorisDeployment.load(tmp_path).catalogs[0].name == "olap"


def test_registry_files_render_jinja(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DORIS_DB", "prod")
    declare(tmp_path, "namespaces", "any", 'name: "{{ env.DORIS_DB }}"\n')
    assert DorisDeployment.load(tmp_path).namespaces[0].name == "prod"


def test_an_unknown_namespace_is_refused_by_name() -> None:
    with pytest.raises(KeyError, match="no namespace named 'absent'"):
        DorisDeployment().ddl_for(Fill, namespace="absent")


def test_a_dangling_catalog_fails_at_lookup(tmp_path: pathlib.Path) -> None:
    declare(tmp_path, "namespaces", "lake", "catalog: ghost\n")
    with pytest.raises(KeyError, match="no catalog named 'ghost'"):
        DorisDeployment.load(tmp_path).namespace("lake")


def test_a_table_naming_an_undeclared_record_is_refused() -> None:
    """The message lists what is declared, because the cause is almost always
    a module nobody imported."""
    with pytest.raises(KeyError, match="no record named 'nowhere'"):
        DorisTable(record="rekep:///records/nowhere").record_class()


def test_properties_flow_catalog_then_namespace_then_table(tmp_path: pathlib.Path) -> None:
    declare(tmp_path, "catalogs", "iceberg", "properties: {a: catalog, b: catalog}\n")
    declare(tmp_path, "namespaces", "default", "properties: {b: namespace, c: namespace}\n")
    ddl = DorisDeployment.load(tmp_path).ddl_for(Fill, properties={"c": "call"})
    assert '"a" = "catalog"' in ddl
    assert '"b" = "namespace"' in ddl
    assert '"c" = "call"' in ddl


# -- ddl --------------------------------------------------------------------


@pytest.fixture(scope="module")
def ddl() -> str:
    return Fill.into_doris_ddl("fills")


def test_statement_shape(ddl: str) -> None:
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS iceberg.default.fills (")
    assert "ENGINE=OLAP" in ddl
    assert ddl.rstrip().endswith(";")


def test_key_columns_lead_and_become_unique_key(ddl: str) -> None:
    """Doris requires UNIQUE KEY columns to be the leading columns."""
    columns = [line.strip().split()[0] for line in ddl.splitlines() if line.startswith("    `")]
    assert columns[:2] == ["`day`", "`order_id`"]
    assert "UNIQUE KEY(`day`, `order_id`)" in ddl


def test_date_partition_becomes_auto_range(ddl: str) -> None:
    assert "AUTO PARTITION BY RANGE (date_trunc(`day`, 'day')) ()" in ddl


def test_bucket_partition_becomes_the_distribution(ddl: str) -> None:
    assert "DISTRIBUTED BY HASH(`account`) BUCKETS 16" in ddl


def test_comments_use_double_quotes(ddl: str) -> None:
    assert 'COMMENT "Trading day."' in ddl
    assert 'COMMENT "One fill."' in ddl


def test_no_keys_means_no_key_clause_and_first_column_hash() -> None:
    @record
    class Plain(Record):
        value: int
        other: str

    ddl = Plain.into_doris_ddl()
    assert "UNIQUE KEY" not in ddl
    assert "DISTRIBUTED BY HASH(`value`) BUCKETS AUTO" in ddl


def test_time_maps_to_string_not_a_refusal() -> None:
    ddl = Log.into_doris_ddl("log_records")
    assert "`time` STRING NOT NULL" in ddl
    assert "`date` DATE NOT NULL" in ddl
    assert "AUTO PARTITION BY RANGE (date_trunc(`date`, 'day')) ()" in ddl


# -- the shipped deployment -------------------------------------------------


def test_the_shipped_deployment_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DORIS_REPLICATION", raising=False)
    deployment = DorisDeployment.load(REPO_DORIS)
    assert deployment.namespace("default").catalog == "iceberg"
    assert deployment.catalog("iceberg").type == "iceberg"
    assert deployment.tables == [], "no tables/ folder any more; see stacks/datasets"
