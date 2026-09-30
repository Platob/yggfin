"""`deploy`: the tables the graph writes, created in the catalog of their layer ahead of a run."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest
from pyiceberg.schema import index_by_name
from pyiceberg.types import FixedType, UUIDType

from rekep import Field, FixRegistry, Storages
from rekep.deploy import TABLES, deploy
from rekep.fix import EVENT_CLOCK, FixCodec
from rekep.pipeline import BOOKS, EVENTS, FIX_MESSAGES, FIX_MESSAGES_RAW, LOG_MESSAGES
from rekep.storages import LAYERS

from .conftest import Landing, layout

pytestmark = pytest.mark.integration

#: What every table the graph writes is laid out by: its own identity, the
#: hour of its instant, and the order a chain reads in.
LAYOUT = {
    "key": {"curruuid"},
    "spec": [(EVENT_CLOCK, "hour")],
    "sort": [(EVENT_CLOCK, "identity"), ("seqnum", "identity"), ("curruuid", "identity")],
}

#: The identities every table the graph writes holds at its top level.
IDENTITIES = {"curruuid", "crossuuid", "prevuuid", "srcuuids.element"}


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


def test_every_declared_table_is_named_in_its_layer_and_builds_its_shape() -> None:
    assert [shape.table for shape in TABLES] == [
        LOG_MESSAGES,
        FIX_MESSAGES_RAW,
        FIX_MESSAGES,
        BOOKS,
        *EVENTS.values(),
    ], "the tables, in production order"
    for shape in TABLES:
        assert shape.layer in LAYERS
        assert f"{shape.layer}.{shape.name}" == shape.table
        assert shape.into_field().name == shape.name
        # The order is declared on the field alone; nothing beside it states
        # a second one.
        assert shape.sort_by is None
    assert {shape.layer for shape in TABLES} == {"bronze", "silver"}, "no task writes gold"


def test_deploying_a_table_the_graph_does_not_write_is_refused(storages: Storages) -> None:
    with pytest.raises(ValueError, match="no such table"):
        deploy(storages, tables=["bronze.record_keeping.unknown"])
    assert list(storages.tables()) == []


def test_a_deployment_creates_every_table_once_in_its_layer(storages: Storages) -> None:
    """Idempotent: a dry run creates nothing, the first pass creates every
    table in its layer's catalog laid out as declared, and the second finds
    them and changes nothing."""
    assert deploy(storages, dry_run=True) == {shape.table: "missing" for shape in TABLES}
    assert list(storages.tables()) == []

    assert deploy(storages) == {shape.table: "created" for shape in TABLES}

    for layer in LAYERS:
        assert sorted(storages.catalog(layer).tables()) == sorted(
            shape.name for shape in TABLES if shape.layer == layer
        ), f"{layer} holds its own tables and no other layer's"
    for shape in TABLES:
        assert layout(storages, shape.table) == LAYOUT, shape.table
    assert storages.gold.tables() == []
    assert deploy(storages) == {shape.table: "present" for shape in TABLES}


def test_every_identity_a_deployment_creates_is_an_iceberg_uuid(storages: Storages) -> None:
    """The key, the chain's and the predecessor's identity, every source and
    every identity nested in a book are created as Iceberg's `uuid`, and no
    column is created as the `fixed[16]` beneath it."""
    deploy(storages)
    for shape in TABLES:
        schema = storages.catalog(shape.layer).load_table(shape.name).schema()
        kinds = {name: schema.find_type(name) for name in index_by_name(schema)}
        identities = {name for name, kind in kinds.items() if isinstance(kind, UUIDType)}
        assert IDENTITIES <= identities, shape.table
        assert not [name for name, kind in kinds.items() if isinstance(kind, FixedType)]
    books = storages.silver.load_table("record_keeping.books").schema()
    assert isinstance(books.find_type("deltas.element.srcuuids.element"), UUIDType)
    assert isinstance(books.find_type("bidlimits.element.uuids.element"), UUIDType)


def test_a_deployment_creates_only_the_tables_it_names(storages: Storages) -> None:
    assert deploy(storages, tables=[LOG_MESSAGES]) == {LOG_MESSAGES: "created"}
    assert list(storages.tables()) == [LOG_MESSAGES]
    assert deploy(storages, tables=[LOG_MESSAGES]) == {LOG_MESSAGES: "present"}


def test_a_table_property_lands_on_the_table_it_creates(storages: Storages) -> None:
    deploy(storages, tables=[BOOKS], table_properties={"rekep.owner": "pipeline"})
    held = storages.silver.load_table("record_keeping.books")
    assert held.properties["rekep.owner"] == "pipeline"


def test_a_fix_table_is_deployed_in_the_shape_its_codec_types(
    storages: Storages, tmp_path: Path
) -> None:
    """The table a deploy creates is the one a task parsing with the same
    codec would create."""
    codec = FixCodec(FixRegistry.from_handle(narrow_registry(tmp_path)))
    assert deploy(storages, tables=[FIX_MESSAGES_RAW, FIX_MESSAGES], codec=codec) == {
        FIX_MESSAGES_RAW: "created",
        FIX_MESSAGES: "created",
    }
    for table in (FIX_MESSAGES_RAW, FIX_MESSAGES):
        layer, _, name = table.partition(".")
        schema = storages.catalog(layer).load_table(name).schema()
        columns = [column.name for column in schema.fields]
        assert "symbol" in columns and "msgtype" not in columns, table
        assert len(columns) == 34, table
    assert sorted(storages.tables()) == [FIX_MESSAGES_RAW, FIX_MESSAGES]


def test_a_registry_that_types_nothing_is_refused_before_any_table(storages: Storages) -> None:
    """A codec over a dictionary that defines nothing is refused where the FIX
    tables' shape is built, so neither reaches the catalog."""
    with pytest.raises(ValueError, match="no specification fields"):
        deploy(storages, tables=[FIX_MESSAGES_RAW, FIX_MESSAGES], codec=FixCodec(FixRegistry()))
    assert list(storages.tables()) == []


def test_a_run_lands_the_very_tables_a_deployment_declares(
    landing: Landing, storages: Storages
) -> None:
    """Both paths create a table through `create_with_field`, so a deployment
    finds every table a run created, and a deployed table is the run's: the
    same columns, types and requiredness, keyed, partitioned and sorted
    alike."""
    assert deploy(landing.storages, dry_run=True) == {shape.table: "present" for shape in TABLES}
    deploy(storages)
    for shape in TABLES:
        run = landing.storages.catalog(shape.layer).load_table(shape.name).schema()
        deployed = storages.catalog(shape.layer).load_table(shape.name).schema()
        assert [(field.name, str(field.field_type), field.required) for field in run.fields] == [
            (field.name, str(field.field_type), field.required) for field in deployed.fields
        ], shape.table
        assert layout(landing.storages, shape.table) == layout(storages, shape.table)
