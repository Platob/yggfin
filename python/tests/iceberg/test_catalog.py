"""Catalog and namespace CRUD against a real, fully local catalog."""

from pathlib import Path
from typing import Annotated

import pyarrow.fs
import pytest
from pyiceberg.io.pyarrow import PyArrowFile

from rekep import Convertible, Field, scalar
from rekep.arrow_file_io import ArrowFileIO
from rekep.iceberg import IcebergCatalog, IcebergDataset
from rekep.iceberg.catalog import PYARROW_FILE_IO
from rekep.urls import S3


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    size: int
    """Quantity."""


class CustomArrowFileIO(ArrowFileIO):
    """A distinct configured FileIO that keeps Windows URI handling."""


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

    assert seen["warehouse"] == "s3://bucket/wh"
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


@pytest.mark.parametrize("scheme", sorted(S3))
def test_dataset_locations_configure_one_file_io_without_entering_stored_paths(
    monkeypatch: pytest.MonkeyPatch, scheme: str
) -> None:
    """Table-specific storage still configures FileIO when the warehouse is local."""
    import pyiceberg.catalog
    from pyiceberg.table.locations import load_location_provider

    from rekep.arrow_file_io import ArrowFileIO

    query = "endpoint_override=minio%3A9000&scheme=http&region=eu-west-1"
    prefix = f"{scheme}://key:secret@bucket"
    seen = {}

    def loaded(name: str, **properties: str) -> object:
        seen.update(properties)
        return object()

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", loaded)
    dataset = IcebergDataset(
        field=Quote.into_field("trading.quotes"),
        properties={"type": "in-memory", "warehouse": "file:///local/warehouse"},
        location=f"{prefix}/tables/quotes?{query}",
        table_properties={
            "write.data.path": f"{prefix}/data?{query}",
            "write.metadata.path": f"{prefix}/metadata?{query}",
        },
    )

    assert dataset.location == f"{scheme}://bucket/tables/quotes"
    assert dataset.table_properties == {
        "write.data.path": f"{scheme}://bucket/data",
        "write.metadata.path": f"{scheme}://bucket/metadata",
    }
    assert dataset.properties["s3.endpoint"] == "http://minio:9000"
    assert dataset.properties["s3.region"] == "eu-west-1"
    assert dataset.properties["s3.access-key-id"] == "key"
    assert dataset.properties["s3.secret-access-key"] == "secret"
    assert all(
        "secret" not in location and "?" not in location
        for location in (dataset.location, *dataset.table_properties.values())
    )

    provider = load_location_provider(dataset.location, dataset.table_properties)
    targets = [provider.new_data_location(name) for name in ("one.parquet", "two.parquet")]
    assert targets == [
        f"{scheme}://bucket/data/one.parquet",
        f"{scheme}://bucket/data/two.parquet",
    ]
    assert len({ArrowFileIO.parse_location(target)[2] for target in targets}) == 2

    _ = dataset.store.catalog
    assert seen["warehouse"] == "file:///local/warehouse"
    assert seen["s3.endpoint"] == "http://minio:9000"
    assert seen["s3.access-key-id"] == "key"


def test_dataset_locations_refuse_two_explicit_s3_stores() -> None:
    with pytest.raises(ValueError, match="conflicting 's3.endpoint'"):
        IcebergDataset(
            field=Quote.into_field("trading.quotes"),
            location="s3://bucket/tables/quotes?endpoint_override=first%3A9000",
            table_properties={
                "write.data.path": "s3://bucket/data?endpoint_override=second%3A9000"
            },
        )


def test_a_shared_catalog_refuses_a_second_s3_store() -> None:
    catalog = IcebergCatalog(properties={"warehouse": "file:///local/warehouse"})
    catalog.dataset(
        "trading.first",
        field=Quote.into_field(),
        location="s3://first:secret@minio-one:9000/bucket/first",
    )

    with pytest.raises(ValueError, match="separate IcebergCatalog"):
        catalog.dataset(
            "trading.second",
            field=Quote.into_field(),
            location="s3://second:secret2@minio-two:9000/bucket/second",
        )

    assert catalog.properties["s3.endpoint"] == "http://minio-one:9000"
    assert catalog.properties["s3.access-key-id"] == "first"


def test_an_open_catalog_receives_explicit_table_location_settings_before_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pyiceberg.catalog

    table = object()

    class Loaded:
        def __init__(self, properties: dict[str, str]) -> None:
            self.properties = properties

        def load_table(self, _name: str) -> object:
            assert self.properties["s3.endpoint"] == "http://minio:9000"
            assert self.properties["s3.access-key-id"] == "key"
            return table

    monkeypatch.setattr(
        pyiceberg.catalog,
        "load_catalog",
        lambda _name, **properties: Loaded(properties),
    )
    catalog = IcebergCatalog(
        name="already-open",
        properties={"warehouse": "file:///local/warehouse"},
    )
    _ = catalog.catalog

    dataset = catalog.dataset(
        "trading.quotes",
        field=Quote.into_field(),
        location=(
            "s3n://key:secret@bucket/tables/quotes?endpoint_override=minio%3A9000&scheme=http"
        ),
    )

    assert dataset.store is catalog
    assert catalog.properties["s3.endpoint"] == "http://minio:9000"
    assert catalog.catalog.properties["s3.secret-access-key"] == "secret"
    assert dataset.iceberg_table is table


@pytest.mark.integration
@pytest.mark.parametrize("scheme", sorted(S3))
def test_explicit_create_location_stages_distinct_objects_without_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scheme: str
) -> None:
    import datetime

    import pyarrow
    import pyarrow.fs

    from rekep.arrow_file_io import ArrowFileIO

    @scalar
    class DailyQuote(Convertible):
        symbol: str
        day: Annotated[datetime.date, Field.partition_key()]

    remote = pyarrow.fs._MockFileSystem()
    remote.create_dir("bucket/metadata", recursive=True)
    remote.create_dir("bucket/data/day=2026-08-14", recursive=True)
    original = ArrowFileIO._initialize_fs
    opened: list[dict[str, str]] = []

    def initialized(
        self: ArrowFileIO, scheme: str, netloc: str | None = None
    ) -> pyarrow.fs.FileSystem:
        if scheme in S3:
            opened.append(dict(self.properties))
            return remote
        return original(self, scheme, netloc)

    monkeypatch.setattr(ArrowFileIO, "_initialize_fs", initialized)
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    dataset = IcebergDataset(
        field=DailyQuote.into_field("trading.remote_quotes"),
        catalog="remote-location",
        properties={
            "type": "sql",
            "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
            "warehouse": warehouse.as_uri(),
        },
    )
    query = "endpoint_override=minio%3A9000&scheme=http&region=eu-west-1"
    prefix = f"{scheme}://key:secret@bucket"
    assert not dataset.exists, "the already-open catalog receives the table-specific settings"
    dataset.create_with_field(
        DailyQuote.into_field("trading.remote_quotes"),
        location=f"{prefix}/tables/quotes?{query}",
        properties={
            "write.data.path": f"{prefix}/data?{query}",
            "write.metadata.path": f"{prefix}/metadata?{query}",
        },
    )

    table = dataset.iceberg_table
    assert table.location() == f"{scheme}://bucket/tables/quotes"
    assert table.properties["write.data.path"] == f"{scheme}://bucket/data"
    assert table.properties["write.metadata.path"] == f"{scheme}://bucket/metadata"
    assert opened and opened[0]["s3.endpoint"] == "http://minio:9000"
    assert opened[0]["s3.access-key-id"] == "key"

    day = datetime.date(2026, 8, 14)
    source = pyarrow.Table.from_pydict(
        {"symbol": ["A", "B"], "day": [day, day]},
        schema=DailyQuote.into_field().into_arrow_schema(),
    )
    dataset.overwrite_arrow_table(source, merge_by=False, commit_row_size=1)
    files = dataset.data_files().column("file_path").to_pylist()
    assert len(files) == 2
    assert len(set(files)) == 2
    assert all(path.startswith(f"{scheme}://bucket/data/day=2026-08-14/") for path in files)
    assert all("?" not in path and "secret" not in path for path in files)
    assert dataset.read_arrow_table().num_rows == 2
    dataset.close()


def test_a_named_file_io_wins(tmp_path: Path) -> None:
    named = IcebergCatalog(name="test", properties={"type": "in-memory", "py-io-impl": "x.Y"})
    assert named.properties["py-io-impl"] == "x.Y"


def test_a_named_file_io_is_wrapped_with_output_ownership(tmp_path: Path) -> None:
    from rekep.arrow_file_io import TRACKED_FILE_IO, TrackedFileIO

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

    owned = IcebergDataset(field=Quote.into_field("trading.quotes"))
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
    assert dataset.namespace == "trading"
    assert dataset.field.name == "trading.quotes"


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


def test_maintenance_reaches_the_store_the_catalog_was_configured_with() -> None:
    """`resolve` reads the location and the environment, and a canonical
    location has had the endpoint and the credentials taken out of it -- so a
    sweep resolved that way looks on AWS for a bucket that is on MinIO."""
    from rekep.iceberg.dataset import _store_of

    configured = pyarrow.fs.SubTreeFileSystem("/warehouse", pyarrow.fs.LocalFileSystem())

    class Io:
        @staticmethod
        def new_input(location: str) -> PyArrowFile:
            return PyArrowFile(location=location, path="wh/db/t/data", fs=configured)

    class Table:
        io = Io()

    filesystem, base = _store_of(Table(), "s3://bucket/wh/db/t/data")

    assert filesystem is configured
    assert base == "wh/db/t/data"

    # A FileIO with no Arrow filesystem behind it leaves the location as all
    # there is to go on.
    class Bare:
        @staticmethod
        def new_input(location: str) -> object:
            return object()

    class Unbacked:
        io = Bare()

    assert _store_of(Unbacked(), "s3://bucket/wh")[1] == "bucket/wh"


def test_the_sweep_evicts_by_the_key_the_file_io_stored_under() -> None:
    """On an object store that key is not the location: the endpoint, the
    access key and the region tell two stores serving one path apart."""
    from rekep.arrow_file_io import CONTENT_CACHE
    from rekep.iceberg.dataset import IcebergDataset

    io = ArrowFileIO({"s3.endpoint": "http://minio:9000", "s3.region": "eu-west-1"})
    location = "s3://bucket/wh/db/t/metadata/x.avro"
    identity = io.content_identity(location)
    assert identity != location
    CONTENT_CACHE.put(identity, b"stale")

    deleted: list[str] = []

    class FileSystem:
        @staticmethod
        def delete_file(path: str) -> None:
            deleted.append(path)

    class Table:
        pass

    table = Table()
    table.io = io
    dataset = object.__new__(IcebergDataset)
    dataset.__dict__["iceberg_table"] = table
    dataset._sweep([(FileSystem(), "bucket/wh/db/t/metadata/x.avro", location, 1)])

    assert deleted == ["bucket/wh/db/t/metadata/x.avro"]
    assert CONTENT_CACHE.peek(identity) is None
