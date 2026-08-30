"""The declared table layout, and creating it before a run fills it."""

import json
from pathlib import Path

import pytest

from rekep import cli
from rekep.deploy import EVENT_SORT, FIX_SORT, TABLES, deploy
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


def test_event_tables_use_the_time_anchored_hash_as_their_only_sort_key() -> None:
    assert EVENT_SORT == ("hash",)
    assert FIX_SORT == ("hash",)
    for shape in TABLES:
        assert shape.sort_by in (None, EVENT_SORT, FIX_SORT)


def test_deploying_a_table_the_pipeline_does_not_write_is_refused() -> None:
    with pytest.raises(ValueError, match="no such table"):
        deploy("rekep", tables=["market.quotes"])


@pytest.mark.integration
def test_a_deployment_creates_every_table_once(tmp_path: Path) -> None:
    """Idempotent: the second pass finds them and changes nothing."""
    properties = catalog_properties(tmp_path)

    missing = deploy("rekep", properties=properties, dry_run=True)
    assert set(missing.values()) == {"missing"}

    store = IcebergCatalog(name="rekep", properties=properties)
    assert store.tables() == []
    store.close()

    created = deploy("rekep", properties=properties)
    assert created == {shape.table: "created" for shape in TABLES}

    again = deploy("rekep", properties=properties)
    assert again == {shape.table: "present" for shape in TABLES}


@pytest.mark.integration
def test_a_deployed_table_records_the_sort_order_it_declared(tmp_path: Path) -> None:
    """What a scan skips row groups by is written on the table, not wished for."""
    properties = catalog_properties(tmp_path)
    deploy("rekep", properties=properties, tables=["fix.market", "market.instruments"])

    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        table = store.load_table("fix.market")
        schema = table.schema()
        ordered = [schema.find_column_name(field.source_id) for field in table.sort_order().fields]
        assert tuple(ordered) == FIX_SORT
        assert store.namespace_exists("fix")
        # The reference table takes its own declared key, which is the same
        # time-anchored identity used by every event table.
        instruments = store.load_table("market.instruments")
        keys = instruments.schema()
        ordered = [
            keys.find_column_name(field.source_id) for field in instruments.sort_order().fields
        ]
        assert tuple(ordered) == EVENT_SORT
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
    # `rekep` is the catalog name the shipped document carries.
    assert reported == {"catalog": "rekep", "tables": {"logs.messages": "created"}}

    store = IcebergCatalog(name="rekep", properties=properties)
    try:
        assert store.tables("logs") == ["logs.messages"]
    finally:
        store.close()


def test_a_property_option_is_a_pair(capsys: pytest.CaptureFixture) -> None:
    """A property with no value is a typo, and it is said rather than guessed at."""
    assert run("iceberg", "deploy", "--property", "warehouse") == 1
    assert "a property is name=value" in capsys.readouterr().err
