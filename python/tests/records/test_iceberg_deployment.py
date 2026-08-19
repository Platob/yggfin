"""The Iceberg deployment registry, mirror of the Doris one."""

import pathlib

import pytest

from rekep.models import Log
from rekep.records import IcebergCatalog, IcebergDeployment

REPO_ICEBERG = pathlib.Path(__file__).parents[3] / "stacks" / "iceberg"


def declare(root: pathlib.Path, folder: str, name: str, content: str = "") -> None:
    (root / folder).mkdir(exist_ok=True)
    (root / folder / f"{name}.yaml").write_text(content or "{}\n")


def test_the_default_catalog_is_fully_local() -> None:
    catalog = IcebergCatalog()
    assert catalog.type == "sql"
    assert catalog.uri.startswith("sqlite:///")
    assert catalog.warehouse.startswith("file://")
    assert catalog.pyiceberg_properties()["type"] == "sql"


def test_an_empty_folder_is_a_working_deployment(tmp_path: pathlib.Path) -> None:
    ddl = IcebergDeployment.load(tmp_path).ddl_for(Log)
    assert "CREATE TABLE IF NOT EXISTS iceberg.default.log (" in ddl
    assert "LOCATION 'file://stacks/iceberg/warehouse/default/log'" in ddl
    assert "PARTITIONED BY (date)" in ddl


def test_registry_folders_load(tmp_path: pathlib.Path) -> None:
    declare(tmp_path, "catalogs", "lake", "type: rest\nuri: http://rest:8181\nwarehouse: s3://wh\n")
    declare(tmp_path, "namespaces", "prod", "catalog: lake\n")
    declare(tmp_path, "tables", "logs", "record: rekep.models.Log\nnamespace: prod\n")
    deployment = IcebergDeployment.load(tmp_path)
    ddl = deployment.ddl_for(Log)
    assert "CREATE TABLE IF NOT EXISTS lake.prod.logs (" in ddl
    assert "LOCATION 's3://wh/prod/logs'" in ddl


def test_namespace_location_overrides_the_warehouse(tmp_path: pathlib.Path) -> None:
    declare(tmp_path, "namespaces", "prod", "location: s3://elsewhere/prod\n")
    ddl = IcebergDeployment.load(tmp_path).ddl_for(Log, namespace="prod")
    assert "LOCATION 's3://elsewhere/prod/log'" in ddl


def test_properties_flow_catalog_then_namespace_then_table(tmp_path: pathlib.Path) -> None:
    declare(tmp_path, "catalogs", "iceberg", "properties: {a: catalog, b: catalog}\n")
    declare(tmp_path, "namespaces", "default", "properties: {b: namespace}\n")
    ddl = IcebergDeployment.load(tmp_path).ddl_for(Log, properties={"c": "call"})
    assert "'a' = 'catalog'" in ddl
    assert "'b' = 'namespace'" in ddl
    assert "'c' = 'call'" in ddl


def test_unknown_names_are_refused() -> None:
    with pytest.raises(KeyError, match="no namespace named 'absent'"):
        IcebergDeployment().ddl_for(Log, namespace="absent")


def test_the_shipped_deployment_loads() -> None:
    deployment = IcebergDeployment.load(REPO_ICEBERG)
    assert deployment.namespace("default").catalog == "iceberg"
    (table,) = deployment.tables
    assert table.record_class() is Log
    ddl = deployment.ddl(table)
    assert "iceberg.default.log (" in ddl
    assert "'write.format.default' = 'parquet'" in ddl
