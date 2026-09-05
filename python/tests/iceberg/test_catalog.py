"""Catalog and namespace CRUD against a real, fully local catalog."""

import os
from pathlib import Path
from typing import Annotated

import pyarrow.fs
import pytest

from rekep import Convertible, Field, scalar
from rekep.iceberg import IcebergCatalog, IcebergDataset
from rekep.iceberg.catalog import PYARROW_FILE_IO
from rekep.iceberg.file_io import IcebergFileIO


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    size: int
    """Quantity."""


class CustomArrowFileIO(IcebergFileIO):
    """A distinct configured FileIO."""


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


def test_standard_s3_properties_reach_pyiceberg_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pyiceberg.catalog

    seen = {}

    def loaded(name: str, **properties: str) -> object:
        seen.update(properties)
        return object()

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", loaded)
    properties = {
        "type": "in-memory",
        "warehouse": "s3://bucket/warehouse",
        "s3.endpoint": "http://minio:9000",
        "s3.region": "eu-west-1",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "secret",
    }
    _ = IcebergCatalog(properties=properties).catalog
    assert {name: seen[name] for name in properties} == properties


def test_relative_local_locations_become_absolute_file_uris(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    catalog = IcebergCatalog(properties={"warehouse": "data/warehouse"})
    dataset = catalog.dataset(
        "trading.quotes",
        field=Quote.into_field(),
        location="data/tables/quotes",
        table_properties={
            "write.data.path": "data/files",
            "write.metadata.path": "data/metadata",
        },
    )

    assert catalog.properties["warehouse"] == (tmp_path / "data/warehouse").as_uri()
    assert dataset.location == (tmp_path / "data/tables/quotes").as_uri()
    assert dataset.table_properties == {
        "write.data.path": (tmp_path / "data/files").as_uri(),
        "write.metadata.path": (tmp_path / "data/metadata").as_uri(),
    }


def test_a_named_file_io_wins(tmp_path: Path) -> None:
    named = IcebergCatalog(name="test", properties={"type": "in-memory", "py-io-impl": "x.Y"})
    assert named.properties["py-io-impl"] == "x.Y"


def test_a_named_file_io_is_wrapped_with_output_ownership(tmp_path: Path) -> None:
    from rekep.iceberg.file_io import TRACKED_FILE_IO, TrackedFileIO

    warehouse = tmp_path / "custom-warehouse"
    warehouse.mkdir()
    catalog = IcebergCatalog(
        name="custom",
        properties={
            "type": "sql",
            "uri": f"sqlite:///{(tmp_path / 'custom.db').as_posix()}",
            "warehouse": warehouse.as_uri(),
            "py-io-impl": f"{__name__}.CustomArrowFileIO",
        },
    )
    table = catalog.dataset("t.quotes", field=Quote.into_field()).get_or_create_table()

    assert catalog.catalog.properties["py-io-impl"] == TRACKED_FILE_IO
    assert isinstance(table.io, TrackedFileIO)
    assert isinstance(table.io.delegate, CustomArrowFileIO)


def test_the_catalog_is_loaded_once(catalog: IcebergCatalog) -> None:
    assert catalog.catalog is catalog.catalog


def test_concurrent_first_access_loads_one_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two first readers share the handle initialized under the location guard."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    import pyiceberg.catalog

    second_waiting = threading.Event()
    loaded = 0

    class Guard:
        def __init__(self) -> None:
            self.lock = threading.RLock()
            self.attempts = 0

        def __enter__(self) -> None:
            self.attempts += 1
            if self.attempts == 2:
                second_waiting.set()
            self.lock.acquire()

        def __exit__(self, *_args: object) -> None:
            self.lock.release()

    class Opened:
        def close(self) -> None:
            pass

    opened = Opened()

    def load(*_args: object, **_kwargs: object) -> Opened:
        nonlocal loaded
        loaded += 1
        if loaded == 1:
            assert second_waiting.wait(timeout=5)
        return opened

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", load)
    catalog = IcebergCatalog()
    catalog.__dict__["_location_guard"] = Guard()

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(lambda: catalog.catalog)
        second = workers.submit(lambda: catalog.catalog)
        handles = first.result(timeout=5), second.result(timeout=5)

    assert handles == (opened, opened)
    assert loaded == 1


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
    shared = catalog.dataset("trading.quotes", field=Quote.into_field())
    shared.close()
    assert closed == 0

    owned = IcebergDataset(name="quotes", namespace="trading", field=Quote.into_field())
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
    assert dataset.name == "quotes"
    assert dataset.identifier == "trading.quotes"
    assert dataset.namespace == "trading"
    assert dataset.field.name == "quotes"
    assert dataset.store is catalog
    assert dataset.catalog is catalog.catalog


@pytest.mark.parametrize(
    ("name", "namespace", "message"),
    [
        ("", "trading", "name must be non-empty and unqualified"),
        ("trading.quotes", "trading", "name must be non-empty and unqualified"),
        ("quotes", "", "namespace must be non-empty"),
        ("quotes", "trading..eu", "namespace must be non-empty"),
    ],
)
def test_a_direct_dataset_requires_explicit_coordinates(
    name: str, namespace: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        IcebergDataset(name=name, namespace=namespace, field=Quote.into_field())


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
    assert found == {"quotes", "ticks"}
    for dataset in catalog.datasets("trading"):
        assert dataset.into_struct_field().names == ["symbol", "size"]


def test_the_catalog_is_a_document(catalog: IcebergCatalog) -> None:
    assert set(catalog.into_dict()) == {"name", "properties"}
    rebuilt = IcebergCatalog.from_yaml(catalog.into_yaml())
    assert (rebuilt.name, rebuilt.properties) == (
        catalog.name,
        catalog.properties,
    )


def test_a_catalog_name_is_explicit_and_nonempty() -> None:
    with pytest.raises(TypeError, match="must be a string"):
        IcebergCatalog(name=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-empty"):
        IcebergCatalog(name="")


def test_maintenance_reaches_the_store_the_catalog_was_configured_with() -> None:
    """Maintenance reuses the FileIO's configured endpoint and credentials."""
    from rekep.iceberg.dataset import _store_of

    configured = pyarrow.fs.SubTreeFileSystem("/warehouse", pyarrow.fs.LocalFileSystem())
    file_io = IcebergFileIO({"s3.endpoint": "http://minio:9000"})
    file_io.fs_by_scheme = lambda _scheme, _netloc: configured

    class Table:
        io = file_io

    filesystem, base = _store_of(Table(), "s3://bucket/wh/db/t/data")

    assert filesystem is configured
    assert base == "bucket/wh/db/t/data"

    class Bare:
        pass

    class Unbacked:
        io = Bare()

    with pytest.raises(TypeError, match="cannot expose its configured Arrow store"):
        _store_of(Unbacked(), "s3://bucket/wh")


def test_the_sweep_deletes_through_yggdryl_and_tolerates_absence(tmp_path: Path) -> None:
    from rekep.iceberg.dataset import IcebergDataset

    orphan = tmp_path / "orphan.avro"
    orphan.write_bytes(b"old")
    dataset = object.__new__(IcebergDataset)
    found = [(pyarrow.fs.LocalFileSystem(), os.fspath(orphan), orphan.as_uri(), 3)]
    dataset._sweep(found)
    dataset._sweep(found)

    assert not orphan.exists()


def test_the_sweep_propagates_nonabsence_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    import yggdryl

    from rekep.iceberg.dataset import IcebergDataset

    class Refused:
        @staticmethod
        def unlink() -> None:
            raise PermissionError("refused")

    class IOBase:
        @staticmethod
        def from_fs(_filesystem: object, _path: str) -> Refused:
            return Refused()

    monkeypatch.setattr(yggdryl, "IOBase", IOBase)
    dataset = object.__new__(IcebergDataset)
    with pytest.raises(PermissionError, match="refused"):
        dataset._sweep([(object(), "orphan.avro", "mock://bound/orphan.avro", 3)])


def test_an_unknown_mtime_is_spared_by_the_orphan_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import datetime

    from rekep.iceberg import dataset as module
    from rekep.iceberg.dataset import IcebergDataset

    class FileSystem:
        @staticmethod
        def get_file_info(_selector: pyarrow.fs.FileSelector) -> list[pyarrow.fs.FileInfo]:
            return [
                pyarrow.fs.FileInfo(
                    "root/uncommitted.parquet",
                    pyarrow.fs.FileType.File,
                    size=4,
                )
            ]

    filesystem = FileSystem()
    monkeypatch.setattr(module, "_store_of", lambda _table, _directory: (filesystem, "root"))
    dataset = object.__new__(IcebergDataset)
    dataset.__dict__.update(
        iceberg_table=object(),
        refresh=lambda: dataset,
        _live=lambda _table: (set(), set()),
        _data_path=lambda _table: "file:///root",
    )

    assert dataset._orphans(datetime.timedelta(days=3), metadata=False) == []
    assert dataset._orphans(datetime.timedelta(0), metadata=False)[0][1] == (
        "root/uncommitted.parquet"
    )


@pytest.mark.skipif(os.name != "nt", reason="Windows path normalization")
def test_native_file_io_resolves_windows_paths_and_file_uris(tmp_path: Path) -> None:
    target = tmp_path / "metadata.json"

    assert Path(IcebergFileIO.parse_location(os.fspath(target))[2]) == target
    assert Path(IcebergFileIO.parse_location(target.as_uri())[2]) == target
