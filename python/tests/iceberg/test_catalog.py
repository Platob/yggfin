"""Catalog and namespace CRUD against a real, fully local catalog."""

from pathlib import Path
from typing import Annotated

import pytest

from rekep import Convertible, Field, scalar
from rekep.iceberg import IcebergCatalog, IcebergDataset
from rekep.iceberg.catalog import PYARROW_FILE_IO


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    size: int
    """Quantity."""


@pytest.fixture
def catalog(tmp_path: Path) -> IcebergCatalog:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return IcebergCatalog(
        name="test",
        properties={
            "type": "sql",
            "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
            "warehouse": warehouse.as_uri(),
        },
    )


# -- the catalog ------------------------------------------------------------


def test_arrow_is_the_default_file_io(catalog: IcebergCatalog) -> None:
    """Everything else here reads and writes through pyarrow.fs; so does Iceberg."""
    assert catalog.catalog.properties["py-io-impl"] == PYARROW_FILE_IO


def test_s3_location_settings_are_normalized_before_the_catalog_uses_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The catalog creates table locations; FileIO normalization is too late."""
    import pyiceberg.catalog

    seen = {}

    def loaded(name: str, **properties: str) -> object:
        seen.update(properties)
        return object()

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", loaded)
    warehouse = "s3://key:secret@bucket/wh?endpoint_override=minio%3A9000&scheme=http"
    _ = IcebergCatalog(properties={"warehouse": warehouse}).catalog

    assert seen["warehouse"] == "s3://key:secret@bucket/wh"
    assert seen["s3.endpoint"] == "http://minio:9000"


def test_s3_process_defaults_reach_the_catalog_before_a_table_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pyiceberg.catalog

    seen = {}

    def loaded(name: str, **properties: str) -> object:
        seen.update(properties)
        return object()

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", loaded)
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_REGION", "eu-west-1")
    _ = IcebergCatalog(properties={"type": "in-memory"}).catalog
    assert seen["s3.endpoint"] == "http://minio:9000"
    assert seen["s3.region"] == "eu-west-1"


def test_a_named_file_io_wins(tmp_path: Path) -> None:
    named = IcebergCatalog(name="test", properties={"type": "in-memory", "py-io-impl": "x.Y"})
    assert named.properties["py-io-impl"] == "x.Y"


def test_the_catalog_is_loaded_once(catalog: IcebergCatalog) -> None:
    assert catalog.catalog is catalog.catalog


def test_close_is_lazy_and_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Teardown must neither open a catalog nor close one twice."""
    import pyiceberg.catalog

    loaded = 0

    class Opened:
        closed = 0

        def close(self) -> None:
            self.closed += 1

    opened = Opened()

    def load(*_args, **_kwargs) -> Opened:
        nonlocal loaded
        loaded += 1
        return opened

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", load)
    catalog = IcebergCatalog()
    catalog.close()
    assert loaded == 0

    assert catalog.catalog is opened
    catalog.close()
    catalog.close()
    assert (loaded, opened.closed) == (1, 1)


def test_a_dataset_only_closes_the_catalog_it_owns(monkeypatch: pytest.MonkeyPatch) -> None:
    """A catalog sweep lends one connection to every dataset it creates."""
    catalog = IcebergCatalog()
    closed = 0

    def close() -> None:
        nonlocal closed
        closed += 1

    monkeypatch.setattr(catalog, "close", close)
    shared = catalog.dataset("trading.quotes")
    shared.close()
    assert closed == 0

    owned = IcebergDataset(name="trading.quotes")
    owned.__dict__["store"] = catalog
    owned.__dict__["_owns_store"] = True
    owned.close()
    owned.close()
    assert closed == 1


# -- namespaces -------------------------------------------------------------


def test_namespaces_are_created_listed_and_dropped(catalog: IcebergCatalog) -> None:
    assert catalog.namespaces() == []
    catalog.create_namespace("trading")
    assert catalog.namespaces() == ["trading"]
    assert catalog.namespace_exists("trading")
    catalog.drop_namespace("trading")
    assert catalog.namespaces() == []


def test_creating_a_namespace_twice_is_not_an_error(catalog: IcebergCatalog) -> None:
    catalog.create_namespace("trading")
    catalog.create_namespace("trading")
    assert catalog.namespaces() == ["trading"]


def test_creating_a_namespace_twice_can_be_refused(catalog: IcebergCatalog) -> None:
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    catalog.create_namespace("trading")
    with pytest.raises(NamespaceAlreadyExistsError):
        catalog.create_namespace("trading", exists_ok=False)


def test_dropping_a_namespace_that_is_not_there_is_not_an_error(catalog: IcebergCatalog) -> None:
    catalog.drop_namespace("absent")


def test_namespace_properties_round_trip(catalog: IcebergCatalog) -> None:
    space = catalog.create_namespace("trading", {"owner": "desk"})
    assert space.properties["owner"] == "desk"
    space.update_properties({"owner": "risk"})
    assert space.properties["owner"] == "risk"


def test_a_namespace_hands_out_its_own_datasets(catalog: IcebergCatalog) -> None:
    space = catalog.create_namespace("trading")
    dataset = space.dataset("quotes", field=Quote.into_field())
    assert isinstance(dataset, IcebergDataset)
    assert dataset.name == "trading.quotes"


# -- tables -----------------------------------------------------------------


def test_tables_are_listed_per_namespace_and_across_them(catalog: IcebergCatalog) -> None:
    catalog.create_namespace("trading")
    catalog.create_namespace("risk")
    catalog.dataset("trading.quotes", field=Quote.into_field()).create_with()
    catalog.dataset("risk.limits", field=Quote.into_field()).create_with()
    assert catalog.tables("trading") == ["trading.quotes"]
    assert sorted(catalog.tables()) == ["risk.limits", "trading.quotes"]


def test_tables_reach_nested_namespaces(catalog: IcebergCatalog) -> None:
    """`list_namespaces` is one level deep, so a sweep silently skipped the rest.

    `for dataset in catalog.datasets(): dataset.optimize()` never touched
    `trading.eu.paris.quotes`, and reported no skip.
    """
    for name in ("ops.quotes", "trading.quotes", "trading.eu.quotes", "trading.eu.paris.quotes"):
        catalog.dataset(name, field=Quote.into_field()).create_with()
    assert sorted(catalog.tables()) == [
        "ops.quotes",
        "trading.eu.paris.quotes",
        "trading.eu.quotes",
        "trading.quotes",
    ]
    assert catalog.tables("trading") == ["trading.quotes"], "one namespace is still one"
    assert "trading.eu" in catalog.namespaces("trading")
    assert sorted(catalog.namespaces(recursive=True)) == [
        "ops",
        "trading",
        "trading.eu",
        "trading.eu.paris",
    ]


def test_a_sweep_loads_one_catalog(catalog: IcebergCatalog) -> None:
    """Loading a pyiceberg catalog builds an engine, or asks a REST server."""
    import pyiceberg.catalog

    for index in range(6):
        catalog.dataset(f"trading.q{index}", field=Quote.into_field()).create_with()
    loaded = 0
    original = pyiceberg.catalog.load_catalog

    def counted(*args, **kwargs):
        nonlocal loaded
        loaded += 1
        return original(*args, **kwargs)

    pyiceberg.catalog.load_catalog = counted
    try:
        names = [dataset.name for dataset in catalog.datasets()]
    finally:
        pyiceberg.catalog.load_catalog = original
    assert len(names) == 6
    assert loaded == 0, "the catalog it came from is the catalog it uses"


def test_a_table_is_dropped_and_purged(catalog: IcebergCatalog) -> None:
    dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
    dataset.create_with()
    assert catalog.table_exists("trading.quotes")
    catalog.drop_table("trading.quotes")
    assert not catalog.table_exists("trading.quotes")
    catalog.drop_table("trading.quotes"), "dropping what is gone is not an error"


def test_a_table_is_renamed(catalog: IcebergCatalog) -> None:
    catalog.dataset("trading.quotes", field=Quote.into_field()).create_with()
    catalog.rename_table("trading.quotes", "trading.ticks")
    assert catalog.tables("trading") == ["trading.ticks"]


def test_every_table_comes_back_as_a_dataset(catalog: IcebergCatalog) -> None:
    catalog.dataset("trading.quotes", field=Quote.into_field()).create_with()
    catalog.dataset("trading.ticks", field=Quote.into_field()).create_with()
    found = {dataset.name for dataset in catalog.datasets("trading")}
    assert found == {"trading.quotes", "trading.ticks"}
    for dataset in catalog.datasets("trading"):
        assert dataset.into_struct_field().names == ["symbol", "size"]


def test_the_catalog_is_a_document(catalog: IcebergCatalog) -> None:
    rebuilt = IcebergCatalog.from_yaml(catalog.into_yaml())
    assert (rebuilt.name, rebuilt.properties) == (catalog.name, catalog.properties)
