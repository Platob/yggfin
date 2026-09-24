"""The declared table layout, and creating it before a run fills it."""

import json
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, FixRegistry, cli
from rekep.deploy import TABLES, deploy
from rekep.iceberg import IcebergCatalog
from rekep.tasks import Task

from .conftest import catalog_properties

#: The tasks whose tables `rekep.deploy` declares, in graph order.
INGESTION = (
    "parse_messages",
    "parse_fix_raw",
    "parse_fix_refined",
    "parse_books",
    "parse_orders",
    "parse_quotes",
    "parse_executions",
)


def run(*argv: str) -> int:
    return cli.main(list(argv))


def deployed(name: str, properties: dict[str, str], *options: str) -> int:
    """`rekep tasks <name> deploy` against the catalog `properties` name."""
    catalog = json.dumps({"name": "rekep", "properties": properties})
    return run("tasks", name, "deploy", "--parameter", f"catalog={catalog}", *options)


def test_every_declared_table_builds_the_shape_it_names() -> None:
    """A table identifier is a namespace and a name, and it names its schema."""
    for shape in TABLES:
        field = shape.into_field()
        assert field.name == shape.table
        assert "." in shape.table
        for column in shape.sort_by or ():
            assert column in [member.name for member in field], (
                f"{shape.table} sorts by a column it has not got"
            )


def test_pipeline_tables_do_not_prescribe_a_physical_sort() -> None:
    assert all(shape.sort_by is None for shape in TABLES)


def test_deploying_a_table_the_pipeline_does_not_write_is_refused() -> None:
    with pytest.raises(ValueError, match="no such table"):
        deploy(IcebergCatalog(name="rekep"), tables=["logs.unknown"])


@pytest.mark.integration
def test_a_deployment_creates_every_table_once(tmp_path: Path) -> None:
    """Idempotent: the second pass finds them and changes nothing."""
    properties = catalog_properties(tmp_path)
    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        missing = deploy(store, dry_run=True)
        assert set(missing.values()) == {"missing"}
        assert store.tables() == []

        created = deploy(store)
        assert created == {shape.table: "created" for shape in TABLES}

        again = deploy(store)
        assert again == {shape.table: "present" for shape in TABLES}
    finally:
        store.close()


@pytest.mark.integration
def test_deployed_message_table_has_no_implicit_sort_order(tmp_path: Path) -> None:
    properties = catalog_properties(tmp_path)
    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        deploy(store, tables=["logs.messages"])
        assert store.namespace_exists("logs")
        assert not store.load_table("logs.messages").sort_order().fields
    finally:
        store.close()


# -- rekep tasks <name> deploy -------------------------------------------------


@pytest.mark.integration
@pytest.mark.parametrize("name", INGESTION)
def test_a_task_deploys_the_table_it_writes_once(
    tmp_path: Path, capsys: pytest.CaptureFixture, name: str
) -> None:
    """Created by the first deploy, found by the second, and nothing else."""
    properties = catalog_properties(tmp_path)
    (table,) = Task(name).targets

    assert deployed(name, properties) == 0
    assert json.loads(capsys.readouterr().out) == {
        "catalog": {"name": "rekep", "properties": properties},
        "tables": {table: "created"},
    }
    assert deployed(name, properties) == 0
    assert json.loads(capsys.readouterr().out)["tables"] == {table: "present"}

    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        assert store.tables() == [table]
    finally:
        store.close()


@pytest.mark.integration
def test_a_dry_run_reports_the_table_missing_and_creates_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    properties = catalog_properties(tmp_path)

    assert deployed("parse_fix_raw", properties, "--dry-run") == 0

    captured = capsys.readouterr()
    assert json.loads(captured.out)["tables"] == {"fix.raw": "missing"}
    assert "fix.raw" in captured.err
    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        assert store.tables() == []
    finally:
        store.close()


@pytest.mark.integration
def test_deploying_every_ingestion_task_deploys_every_declared_table(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """What `rekep.deploy.deploy` creates at once, the tasks create one by one."""
    properties = catalog_properties(tmp_path)

    for name in INGESTION:
        assert deployed(name, properties) == 0
    capsys.readouterr()

    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        assert deploy(store, dry_run=True) == {shape.table: "present" for shape in TABLES}
    finally:
        store.close()


@pytest.mark.integration
def test_a_table_property_lands_on_the_table_it_creates(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    properties = catalog_properties(tmp_path)

    assert deployed("parse_messages", properties, "--table-property", "rekep.owner=pipeline") == 0
    capsys.readouterr()

    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        assert store.load_table("logs.messages").properties["rekep.owner"] == "pipeline"
    finally:
        store.close()


def test_a_table_property_option_is_a_pair(capsys: pytest.CaptureFixture) -> None:
    """A property with no value is a typo, and it is said rather than guessed at."""
    assert run("tasks", "parse_messages", "deploy", "--table-property", "rekep.owner") == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "a property is name=value, not 'rekep.owner'" in captured.err


def test_a_task_catalog_refuses_legacy_or_misspelled_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    overrides = tmp_path / "parameters.json"
    overrides.write_text(
        json.dumps({"catalog": {"catalog_name": "legacy", "properties": {}}}), encoding="utf-8"
    )

    assert (
        run("tasks", "parse_messages", "deploy", "--parameters-file", str(overrides), "--dry-run")
        == 1
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "accepts only name and properties" in captured.err
    assert "catalog_name" in captured.err


@pytest.mark.parametrize("name", ["build_dbt", "optimize_iceberg"])
def test_a_task_that_declares_no_table_deploys_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture, name: str
) -> None:
    """A dbt product is created by the model that first commits it, and
    maintenance creates nothing; neither opens the catalog it names."""
    monkeypatch.chdir(tmp_path)
    properties = catalog_properties(tmp_path)

    assert deployed(name, properties) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "catalog": {"name": "rekep", "properties": properties},
        "tables": {},
    }
    assert f"{name} declares no table to create ahead of a run" in captured.err
    assert run("tasks", name, "deploy") == 0
    assert json.loads(capsys.readouterr().out) == {
        "catalog": Task(name).parameters["catalog"],
        "tables": {},
    }
    assert sorted(path.name for path in tmp_path.iterdir()) == ["warehouse"], (
        "no catalog database, not even the default one"
    )


def narrow_registry(root: Path) -> Path:
    """A dictionary of two fields: the clock and one specification field."""
    registry = root / "narrow-fix-registry"
    registry.mkdir()
    sending_time = Field("sendingtime", pyarrow.timestamp("ns", tz="UTC"), nullable=True)
    sending_time.fix.tag = 52
    symbol = Field("symbol", "utf8", nullable=True)
    symbol.fix.tag = 55
    FixRegistry.from_fields([sending_time, symbol]).write_into(registry)
    return registry


@pytest.mark.integration
@pytest.mark.parametrize(
    ("name", "table"), [("parse_fix_raw", "fix.raw"), ("parse_fix_refined", "fix.refined")]
)
def test_a_fix_table_is_deployed_in_the_shape_its_registry_types(
    tmp_path: Path, capsys: pytest.CaptureFixture, name: str, table: str
) -> None:
    """The table a deploy creates is the one the task's own run would create."""
    properties = catalog_properties(tmp_path)
    registry = narrow_registry(tmp_path)

    assert deployed(name, properties, "--parameter", f"registry={registry.as_uri()}") == 0
    assert json.loads(capsys.readouterr().out)["tables"] == {table: "created"}

    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        columns = [column.name for column in store.load_table(table).schema().fields]
    finally:
        store.close()
    assert "symbol" in columns and "msgtype" not in columns
    assert len(columns) == 35


@pytest.mark.integration
def test_a_registry_the_run_refuses_is_refused_before_any_table(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    properties = catalog_properties(tmp_path)
    empty = tmp_path / "empty-fix-registry"
    empty.mkdir()

    assert deployed("parse_fix_raw", properties, "--parameter", f"registry={empty.as_uri()}") == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "parse_fix_raw" in captured.err
    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        assert store.tables() == []
    finally:
        store.close()


def test_a_codec_option_the_run_refuses_is_refused_before_any_table(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """`codec_options` reach the codec that types the table, which validates
    every keyword before a catalog is opened."""
    properties = catalog_properties(tmp_path)

    assert (
        deployed("parse_fix_raw", properties, "--parameter", 'codec_options={"nonsense_option": 1}')
        == 1
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "nonsense_option" in captured.err
    assert not (tmp_path / "warehouse.db").exists(), "the catalog was never opened"
