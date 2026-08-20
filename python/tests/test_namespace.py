import pytest

from rekep.namespace import Namespace, unique_uri


def test_a_root_namespace_is_its_own_path() -> None:
    assert Namespace(name="iceberg").path() == "iceberg"


def test_child_nests_one_level() -> None:
    root = Namespace(name="iceberg")
    child = root.child("trading")
    assert child.parent is root
    assert child.path() == "iceberg.trading"


def test_levels_are_root_first() -> None:
    leaf = Namespace(name="iceberg").child("trading").child("orders")
    assert leaf.levels() == ["iceberg", "trading", "orders"]


def test_depth_counts_every_level() -> None:
    assert Namespace(name="iceberg").depth() == 1
    assert Namespace(name="iceberg").child("trading").depth() == 2


def test_of_builds_the_same_chain_as_nested_child_calls() -> None:
    via_of = Namespace.of("iceberg", "trading", "orders")
    via_child = Namespace(name="iceberg").child("trading").child("orders")
    assert via_of.path() == via_child.path() == "iceberg.trading.orders"


def test_of_refuses_no_levels() -> None:
    with pytest.raises(ValueError, match="at least one level"):
        Namespace.of()


def test_path_accepts_a_separator_override() -> None:
    ns = Namespace.of("bucket", "key", separator=".")
    assert ns.path(separator="/") == "bucket/key"


def test_child_separator_defaults_to_the_parents() -> None:
    root = Namespace(name="bucket", separator="/")
    assert root.child("key").path() == "bucket/key"


def test_str_is_the_path() -> None:
    assert str(Namespace.of("a", "b")) == "a.b"


def test_round_trips_through_json() -> None:
    ns = Namespace.of("iceberg", "trading", "orders")
    assert Namespace.from_json(ns.into_json()) == ns


# -- unique_uri ---------------------------------------------------------


def test_unique_uri_joins_namespace_and_name() -> None:
    assert unique_uri("job", "trading", "orders") == "job://trading/orders"


def test_unique_uri_without_a_namespace_is_just_the_name() -> None:
    assert unique_uri("job", None, "orders") == "job://orders"


def test_unique_uri_splits_a_dotted_namespace_into_levels() -> None:
    assert unique_uri("dataset", "iceberg.trading", "orders") == "dataset://iceberg/trading/orders"


def test_unique_uri_scheme_keeps_same_name_from_colliding_across_kinds() -> None:
    job_uri = unique_uri("job", "trading", "orders")
    dataset_uri = unique_uri("dataset", "trading", "orders")
    assert job_uri != dataset_uri
