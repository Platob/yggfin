"""The declared table layout, and creating it before a run fills it."""

from pathlib import Path

import pyarrow
import pytest

from rekep import Field, FixRegistry
from rekep.deploy import TABLES, deploy
from rekep.fix import fix_codec, fix_registry
from rekep.iceberg import IcebergCatalog
from rekep.pipeline import MESSAGES, RAW, REFINED

from .conftest import catalog_properties


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


@pytest.mark.integration
def test_a_table_property_lands_on_the_table_it_creates(tmp_path: Path) -> None:
    store = IcebergCatalog(name="rekep", properties=catalog_properties(tmp_path))
    try:
        deploy(store, tables=[MESSAGES], table_properties={"rekep.owner": "pipeline"})
        assert store.load_table(MESSAGES).properties["rekep.owner"] == "pipeline"
    finally:
        store.close()


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
def test_a_fix_table_is_deployed_in_the_shape_its_codec_types(tmp_path: Path) -> None:
    """The table a deploy creates is the one a stage parsing with the same
    codec would create."""
    codec = fix_codec(fix_registry(narrow_registry(tmp_path).as_uri()))
    store = IcebergCatalog(name="rekep", properties=catalog_properties(tmp_path))
    try:
        assert deploy(store, tables=[RAW, REFINED], codec=codec) == {
            RAW: "created",
            REFINED: "created",
        }
        for table in (RAW, REFINED):
            columns = [column.name for column in store.load_table(table).schema().fields]
            assert "symbol" in columns and "msgtype" not in columns, table
            assert len(columns) == 35, table
    finally:
        store.close()


def test_a_registry_that_types_nothing_is_refused_before_any_table(tmp_path: Path) -> None:
    """The codec is built before a deploy is called, so a dictionary it
    refuses never reaches the catalog."""
    empty = tmp_path / "empty-fix-registry"
    empty.mkdir()
    store = IcebergCatalog(name="rekep", properties=catalog_properties(tmp_path))
    try:
        with pytest.raises(ValueError, match="no specification fields"):
            deploy(store, tables=[RAW], codec=fix_codec(fix_registry(empty.as_uri())))
        assert store.tables() == []
    finally:
        store.close()
