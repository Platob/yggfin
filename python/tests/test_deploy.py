"""The declared table layout, and creating it before a run fills it."""

import json
from pathlib import Path

import pytest

from rekep import cli
from rekep.deploy import TABLES, deploy
from rekep.iceberg import IcebergCatalog

from .conftest import catalog_properties

#: The task documents the deployment reads its catalog settings from.
TASKS = Path(__file__).resolve().parents[2] / "tasks"


def run(*argv: str) -> int:
    return cli.main(list(argv))


def test_every_declared_table_builds_the_shape_it_names() -> None:
    """A table identifier is a namespace and a name, and it names its schema."""
    for shape in TABLES:
        field = shape.field()
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
            str(TASKS / "parse_messages" / "parse_messages.json"),
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
    document = tmp_path / "task.json"
    document.write_text(
        json.dumps(
            {
                "name": "invalid",
                "application": "task.py",
                "parameters": {"catalog": {"catalog_name": "legacy", "properties": {}}},
            }
        ),
        encoding="utf-8",
    )

    assert run("iceberg", "deploy", str(document), "--dry-run") == 1
    error = capsys.readouterr().err
    assert "accepts only name and properties" in error
    assert "catalog_name" in error
