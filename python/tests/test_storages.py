"""`Storages`: one Iceberg catalog per layer, and tables addressed across them."""

from pathlib import Path

import pytest

from rekep import Storages, scalar
from rekep.iceberg import IcebergCatalog
from rekep.storages import LAYERS, split_identifier

from .conftest import catalog_properties


@scalar
class Row:
    """One row."""

    key: str
    """What the row is."""


def _mapping(tmp_path: Path, layers=LAYERS) -> dict[str, dict]:
    """One local catalog per layer: its own database and its own warehouse."""
    return {
        layer: {"name": layer, "properties": catalog_properties(tmp_path, layer)}
        for layer in layers
    }


def _loaded(catalog: IcebergCatalog) -> bool:
    """Whether the pyiceberg catalog behind a handle has been opened."""
    return "catalog" in catalog.__dict__


@pytest.fixture
def storages(tmp_path: Path):
    with Storages.from_dict(_mapping(tmp_path)) as opened:
        yield opened


def test_the_layers_are_the_order_data_moves_through_them() -> None:
    assert LAYERS == ("bronze", "silver", "gold")


def test_one_mapping_per_layer_builds_one_catalog_per_layer(tmp_path: Path) -> None:
    """Each layer is its own catalog, and nothing is opened by building them."""
    storages = Storages.from_dict(_mapping(tmp_path))

    for layer in LAYERS:
        catalog = storages.catalog(layer)
        assert catalog is getattr(storages, layer)
        assert isinstance(catalog, IcebergCatalog)
        assert catalog.name == layer
        assert catalog.properties["uri"].endswith(f"{layer}.db")
        assert not _loaded(catalog)
    assert len({id(storages.catalog(layer)) for layer in LAYERS}) == 3


@pytest.mark.parametrize(
    ("layers", "extra", "refused"),
    [
        (("bronze", "silver"), {}, "missing gold"),
        ((), {}, "missing bronze, silver, gold"),
        (LAYERS, {"platinum": {}}, "unknown platinum"),
        (
            ("bronze", "silver"),
            {"platinum": {}, "copper": {}},
            "missing gold; unknown copper, platinum",
        ),
    ],
    ids=["missing", "empty", "unknown", "both"],
)
def test_a_layer_missing_or_unknown_is_refused_before_a_catalog_is_opened(
    tmp_path: Path, layers, extra, refused: str
) -> None:
    mapping = {**_mapping(tmp_path, layers), **extra}

    with pytest.raises(ValueError) as raised:
        Storages.from_dict(mapping)

    assert str(raised.value) == (
        f"expected one catalog per layer (bronze, silver, gold): {refused}"
    )
    # Refused before any catalog was built, so no database was created.
    assert not list(tmp_path.glob("*.db"))


def test_an_unknown_layer_names_no_catalog(storages: Storages) -> None:
    for layer in ("platinum", "Bronze", "", "record_keeping"):
        with pytest.raises(ValueError, match="expected a layer of bronze, silver, gold, got"):
            storages.catalog(layer)


@pytest.mark.parametrize(
    ("identifier", "split"),
    [
        ("bronze.record_keeping.log_messages", ("bronze", "record_keeping.log_messages")),
        ("silver.record_keeping.fix_messages", ("silver", "record_keeping.fix_messages")),
        ("gold.desk.eu.positions", ("gold", "desk.eu.positions")),
    ],
)
def test_an_identifier_is_its_layer_and_the_table_the_catalog_names(
    identifier: str, split: tuple[str, str]
) -> None:
    assert split_identifier(identifier) == split


@pytest.mark.parametrize(
    "identifier",
    [
        "record_keeping.log_messages",
        "platinum.record_keeping.log_messages",
        "bronze.log_messages",
        "bronze",
        "bronze.",
        "",
        "Bronze.record_keeping.log_messages",
    ],
)
def test_an_identifier_without_a_layer_namespace_and_table_is_refused(identifier: str) -> None:
    with pytest.raises(ValueError) as raised:
        split_identifier(identifier)

    assert str(raised.value) == (
        "expected <layer>.<namespace>.<table> with a layer of bronze, silver, gold, "
        f"got {identifier!r}"
    )


def test_a_dataset_is_routed_to_the_catalog_its_layer_names(storages: Storages) -> None:
    dataset = storages.dataset("silver.record_keeping.fix_messages", field=Row.into_field())

    assert dataset.store is storages.silver
    assert (dataset.namespace, dataset.name) == ("record_keeping", "fix_messages")
    assert dataset.identifier == "record_keeping.fix_messages"
    # Addressing a table opens no catalog beside the one that holds it.
    assert not _loaded(storages.bronze) and not _loaded(storages.gold)


def test_a_dataset_refuses_an_identifier_before_reaching_a_catalog(storages: Storages) -> None:
    with pytest.raises(ValueError, match="expected <layer>.<namespace>.<table>"):
        storages.dataset("record_keeping.fix_messages", field=Row.into_field())
    assert not any(_loaded(storages.catalog(layer)) for layer in LAYERS)


def test_tables_lists_every_layer_s_tables_under_its_layer(storages: Storages) -> None:
    assert list(storages.tables()) == []

    for identifier in (
        "silver.record_keeping.books",
        "bronze.record_keeping.log_messages",
        "silver.record_keeping.fix_messages",
    ):
        dataset = storages.dataset(identifier, field=Row.into_field())
        try:
            dataset.create_with()
        finally:
            dataset.close()

    # Layer by layer, in the order data moves through them.
    assert list(storages.tables()) == [
        "bronze.record_keeping.log_messages",
        *sorted(["silver.record_keeping.books", "silver.record_keeping.fix_messages"]),
    ]
    # One table is one catalog's: the same name in another layer is another table.
    assert storages.bronze.tables() == ["record_keeping.log_messages"]
    assert storages.gold.tables() == []


def test_closing_releases_every_opened_catalog_and_opens_none(tmp_path: Path) -> None:
    storages = Storages.from_dict(_mapping(tmp_path))
    storages.silver.namespaces()
    assert _loaded(storages.silver)

    storages.close()

    assert not any(_loaded(storages.catalog(layer)) for layer in LAYERS)
    assert sorted(path.name for path in tmp_path.glob("*.db")) == ["silver.db"]


def test_the_context_manager_closes_the_catalogs_on_the_way_out(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="inside"):
        with Storages.from_dict(_mapping(tmp_path)) as storages:
            for layer in LAYERS:
                storages.catalog(layer).namespaces()
            assert all(_loaded(storages.catalog(layer)) for layer in LAYERS)
            raise RuntimeError("inside")

    assert not any(_loaded(storages.catalog(layer)) for layer in LAYERS)
