"""The declared table layout, and creating it before a run fills it."""

import json
from pathlib import Path

import pytest

from rekep import cli
from rekep.deploy import TABLES, deploy
from rekep.fix.rules import MARKET_CATEGORY, MISC_CATEGORY, UNKNOWN_CATEGORY
from rekep.iceberg import IcebergCatalog

from .conftest import catalog_properties

#: The task documents the deployment reads its catalog settings from.
TASKS = Path(__file__).resolve().parents[2] / "tasks"


def run(*argv: str) -> int:
    return cli.main(list(argv))


def test_every_declared_table_builds_the_shape_it_names() -> None:
    """A table identifier is a namespace and a name, and it names its schema."""
    for shape in TABLES:
        field = shape.into_field()
        assert field.name == shape.table
        assert "." in shape.table
        built = field.into_arrow_schema()
        for column in shape.sort_by or ():
            assert column in built.names, f"{shape.table} sorts by a column it has not got"


def test_the_fix_tables_are_the_routers_own_categories() -> None:
    """Adding a category to the router adds a table here, not a second spelling."""
    routed = {MARKET_CATEGORY, MISC_CATEGORY, UNKNOWN_CATEGORY}
    deployed = {shape.table.partition(".")[2] for shape in TABLES if shape.table.startswith("fix.")}
    assert deployed == routed


def test_pipeline_tables_do_not_prescribe_a_physical_sort() -> None:
    assert all(shape.sort_by is None for shape in TABLES)


def test_deploying_a_table_the_pipeline_does_not_write_is_refused() -> None:
    with pytest.raises(ValueError, match="no such table"):
        deploy(IcebergCatalog(name="rekep"), tables=["market.quotes"])


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
def test_deployed_pipeline_tables_have_no_implicit_sort_order(tmp_path: Path) -> None:
    properties = catalog_properties(tmp_path)
    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        deploy(store, tables=["fix.market", "market.instruments"])
        assert not store.load_table("fix.market").sort_order().fields
        assert store.namespace_exists("fix")
        assert not store.load_table("market.instruments").sort_order().fields
    finally:
        store.close()


@pytest.mark.integration
def test_the_command_reads_the_catalog_a_task_document_names(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """One place says where the pipeline writes, so a deployment cannot miss it."""
    properties = catalog_properties(tmp_path)
    assert (
        run(
            "iceberg",
            "deploy",
            str(TASKS / "parse_fix" / "parse_fix.yml"),
            "--property",
            f"uri={properties['uri']}",
            "--property",
            f"warehouse={properties['warehouse']}",
            "--table",
            "logs.messages",
        )
        == 0
    )
    reported = json.loads(capsys.readouterr().out)
    assert reported == {
        "catalog": {"name": "rekep", "properties": properties},
        "tables": {"logs.messages": "created"},
    }

    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        assert store.tables("logs") == ["logs.messages"]
    finally:
        store.close()


def test_a_property_option_is_a_pair(capsys: pytest.CaptureFixture) -> None:
    """A property with no value is a typo, and it is said rather than guessed at."""
    assert run("iceberg", "deploy", "--property", "warehouse") == 1
    assert "a property is name=value" in capsys.readouterr().err


def test_a_task_catalog_refuses_legacy_or_misspelled_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    document = tmp_path / "task.yml"
    document.write_text(
        "name: invalid\n"
        "notebook: task.ipynb\n"
        "parameters:\n"
        "  catalog:\n"
        "    catalog_name: legacy\n"
        "    properties: {}\n",
        encoding="utf-8",
    )

    assert run("iceberg", "deploy", str(document), "--dry-run") == 1
    error = capsys.readouterr().err
    assert "accepts only name and properties" in error
    assert "catalog_name" in error
