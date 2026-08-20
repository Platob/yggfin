import pytest

from rekep.namespace import Namespace, ResourceUri


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


# -- ResourceUri ---------------------------------------------------------


def test_a_uri_is_the_service_then_the_path() -> None:
    """One spelling: the service is a path part, never a scheme of its own."""
    uri = ResourceUri.of("datasets", "warehouse", "trading", "orders")
    assert str(uri) == "rekep:/datasets/warehouse/trading/orders"
    assert uri.path() == "warehouse/trading/orders", "the path alone carries no service"


def test_levels_are_read_right_to_left() -> None:
    """A shorter path is a less qualified name, not a different shape."""
    full = ResourceUri.of("datasets", "warehouse", "trading", "orders")
    assert (full.catalog(), full.namespace(), full.name()) == ("warehouse", "trading", "orders")

    short = ResourceUri.of("datasets", "trading", "orders")
    assert (short.catalog(), short.namespace(), short.name()) == (None, "trading", "orders")

    bare = ResourceUri.of("datasets", "orders")
    assert (bare.catalog(), bare.namespace(), bare.name()) == (None, "default", "orders")


def test_a_branch_is_a_fragment_not_another_resource() -> None:
    uri = ResourceUri.of("datasets", "trading", "orders", branch="dev")
    assert str(uri) == "rekep:/datasets/trading/orders#dev"
    assert uri.at(None).path() == uri.path(), "same resource, different ref"


def test_every_spelling_parses_to_the_same_identity() -> None:
    spellings = [
        "rekep:/datasets/warehouse/trading/orders#dev",
        "rekep:/datasets//warehouse/trading/orders#dev",
        "rekep:/datasets/warehouse/trading/orders#dev",
        "/datasets/warehouse/trading/orders#dev",
    ]
    parsed = {ResourceUri.parse(text) for text in spellings}
    assert len(parsed) == 1
    assert str(parsed.pop()) == "rekep:/datasets/warehouse/trading/orders#dev"


def test_the_scheme_names_the_service_as_a_path_part() -> None:
    assert ResourceUri.parse("rekep:/jobs/pipeline/l2r").service == "jobs"
    assert str(ResourceUri.parse("rekep:/jobs/pipeline/l2r")) == "rekep:/jobs/pipeline/l2r"


def test_a_short_scheme_is_gone_not_merely_discouraged() -> None:
    """`ds:`/`job:`/`dag:` were a second spelling of one identity; a parser
    that still accepted them would keep two forms alive in every log line."""
    for old in ("ds:/trading/orders", "job:/pipeline/l2r", "dag:/pipeline/trading"):
        with pytest.raises(ValueError, match="unknown scheme"):
            ResourceUri.parse(old)


def test_a_bare_path_needs_to_be_told_its_service() -> None:
    assert ResourceUri.parse("trading/orders", service="datasets").name() == "orders"
    with pytest.raises(ValueError, match="no service"):
        ResourceUri.parse("trading/orders")


def test_a_foreign_scheme_is_refused_by_name() -> None:
    with pytest.raises(ValueError, match="unknown scheme 's3'"):
        ResourceUri.parse("s3://bucket/key")


def test_a_uri_needs_at_least_a_name() -> None:
    with pytest.raises(ValueError, match="at least a name"):
        ResourceUri.of("datasets")


def test_a_job_and_a_dataset_sharing_a_name_do_not_collide() -> None:
    job = ResourceUri.of("jobs", "trading", "orders")
    dataset = ResourceUri.of("datasets", "trading", "orders")
    assert str(job) != str(dataset)
    assert (job.service, dataset.service) == ("jobs", "datasets")


def test_child_goes_one_level_deeper() -> None:
    assert ResourceUri.of("datasets", "trading").child("orders").path() == "trading/orders"
