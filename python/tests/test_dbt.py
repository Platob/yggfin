"""The dbt project under `data/dbt`, and the plugin it reads and commits through.

dbt owns the SQL and this repository owns the Iceberg seam, so what is pinned
here is that seam: the declaration a model's configuration builds, the tables
its sources may name, and -- as an integration -- the products a build actually
commits and what a replay of the same build does not write twice.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import pyarrow
import pytest
from dbt.cli.main import dbtRunner

from rekep import cli
from rekep.dbt import COMMITTED, catalog_settings, committed, declared_field
from rekep.deploy import TABLES
from rekep.iceberg import IcebergCatalog, partition_keys, primary_keys, sort_keys

ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT / "data" / "dbt"
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"

#: The tables ingestion publishes, by name: what a source may read and what no
#: model may write.
INGESTED = {shape.table: shape for shape in TABLES}

#: What the models commit, and the rows the checked-in 111-line fixture answers
#: for each. One event per message that carried an order identity and a
#: lifecycle fact, one row per chain, and one occurrence per execution the
#: bridge relayed into a chain.
PRODUCTS = {
    "orders.events": 62,
    "orders.current": 6,
    "executions.fills": 7,
}

#: The whole route, in order: capture to raw rows, raw rows to FIX, FIX to
#: products.
WORKFLOW = ("parse_messages", "parse_fix", "build_dbt")


@pytest.fixture(scope="session")
def manifest(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """The parsed project: every model, source and test, with configs resolved.

    `dbt parse` reads the project and its profile and touches no catalog, so
    this is the structural check and not a run.
    """
    target = tmp_path_factory.mktemp("dbt-parse")
    parsed = dbtRunner().invoke(
        [
            "parse",
            "--project-dir",
            str(PROJECT),
            "--profiles-dir",
            str(PROJECT),
            "--target-path",
            str(target),
            "--log-path",
            str(target / "logs"),
            "--log-level",
            "none",
        ]
    )
    assert parsed.success, parsed.exception
    return parsed.result


def models(manifest: Any) -> dict[str, Any]:
    """Every model, by its own name."""
    return {
        node.name: node for node in manifest.nodes.values() if str(node.resource_type) == "model"
    }


def published(manifest: Any) -> dict[str, Any]:
    """Every model that commits to Iceberg, by the table it commits to."""
    return {
        node.config.extra["table"]: node
        for node in models(manifest).values()
        if node.config.materialized == "iceberg"
    }


# -- the project -------------------------------------------------------------


def test_the_project_and_its_profile_name_each_other() -> None:
    project = (PROJECT / "dbt_project.yml").read_text(encoding="utf-8")
    profiles = (PROJECT / "profiles.yml").read_text(encoding="utf-8")

    assert "profile: rekep" in project
    assert profiles.startswith("#")
    assert "\nrekep:\n" in f"\n{profiles}"
    assert "module: rekep.dbt" in profiles
    assert "send_anonymous_usage_stats: false" in project, "a build reports nowhere"


def test_the_project_pulls_no_dbt_package() -> None:
    """Everything a build needs is in the checkout: `dbt deps` reaches nothing."""
    assert not list(PROJECT.glob("packages.yml"))
    assert not list(PROJECT.glob("dependencies.yml"))


def test_every_source_is_a_table_this_repository_publishes(manifest: Any) -> None:
    named = {source.meta["table"] for source in manifest.sources.values()}

    assert named == set(INGESTED), "a source reads an ingested table, never another store"
    for source in manifest.sources.values():
        assert source.meta["plugin"] == "rekep"


def test_every_projected_column_is_one_the_published_shape_carries(manifest: Any) -> None:
    """The projection is pushed into scan planning, so a name that drifted out
    of the contract would silently narrow every product that reads it."""
    for source in manifest.sources.values():
        table = source.meta["table"]
        carried = {member.name for member in INGESTED[table].into_field()}
        projected = set(source.meta.get("columns") or ())
        documented = set(source.columns)

        assert projected <= carried, f"{table} does not carry {sorted(projected - carried)}"
        assert documented <= carried, f"{table} does not carry {sorted(documented - carried)}"
        assert not projected or documented <= projected, "a documented column is read"


def test_no_model_writes_a_table_ingestion_owns(manifest: Any) -> None:
    """A product is derived; the two ingested tables are written by their tasks."""
    assert not set(published(manifest)) & set(INGESTED)


def test_the_published_models_are_the_declared_products(manifest: Any) -> None:
    assert set(published(manifest)) == set(PRODUCTS)
    for table, node in published(manifest).items():
        declared = node.config.extra
        assert declared["plugin"] == "rekep"
        assert declared["mode"] in ("append", "overwrite")
        assert declared["primary_key"], f"{table} is committed on a key"


def test_every_key_a_model_declares_is_tested_as_one(manifest: Any) -> None:
    """The Iceberg key and the dbt tests agree: a key is unique and never null."""
    tested: dict[tuple[str, str], set[str]] = {}
    for node in manifest.nodes.values():
        # A singular test is a query of its own and carries no metadata; a
        # generic one names the test it is an instance of.
        metadata = getattr(node, "test_metadata", None)
        if str(node.resource_type) != "test" or metadata is None:
            continue
        if node.column_name and node.attached_node:
            model = node.attached_node.split(".")[-1]
            tested.setdefault((model, node.column_name), set()).add(metadata.name)

    for table, node in published(manifest).items():
        for column in node.config.extra["primary_key"]:
            assert tested.get((node.name, column), set()) >= {"unique", "not_null"}, (
                f"{table} keys on {column}, which is not tested as a key"
            )


def test_what_a_model_declares_non_null_is_what_it_tests(manifest: Any) -> None:
    """A column Iceberg refuses a null in is a column dbt says so about: the
    declaration and the test are two readings of one decision."""
    for table, node in published(manifest).items():
        declared = set(node.config.extra["primary_key"]) | set(
            node.config.extra.get("not_null") or ()
        )

        assert declared == _tested(manifest, node.name, "not_null"), (
            f"{table} declares {sorted(declared)} non-null"
        )


def _tested(manifest: Any, model: str, name: str) -> set[str]:
    """Every column of `model` carrying the generic test `name`."""
    return {
        node.column_name
        for node in manifest.nodes.values()
        if str(node.resource_type) == "test"
        and getattr(node, "test_metadata", None) is not None
        and node.test_metadata.name == name
        and node.column_name
        and (node.attached_node or "").split(".")[-1] == model
    }


def test_a_check_that_spans_two_products_warns_rather_than_fails(manifest: Any) -> None:
    """A singular test is a quality signal: a capture that starts mid-stream
    holds executions whose order was accepted before its first line, and that
    is the capture's shape rather than the build's mistake."""
    singular = [
        node
        for node in manifest.nodes.values()
        if str(node.resource_type) == "test" and getattr(node, "test_metadata", None) is None
    ]

    assert [node.name for node in singular] == ["every_fill_belongs_to_a_known_order"]
    assert all(str(node.config.severity).casefold() == "warn" for node in singular)


def test_the_products_dag_announces_the_tables_the_models_commit(manifest: Any) -> None:
    """The DAG's Assets are read from its source rather than from Airflow, so
    this holds on every platform."""
    declared = ast.parse((ROOT / "tasks" / "airflow" / "products.py").read_text(encoding="utf-8"))
    announced = next(
        ast.literal_eval(statement.value)
        for statement in declared.body
        if isinstance(statement, ast.Assign)
        and getattr(statement.targets[0], "id", "") == "PUBLISHED"
    )

    assert set(announced) == set(published(manifest))


# -- the declaration a model's configuration builds --------------------------


def staged() -> pyarrow.Schema:
    """The Arrow schema a staged model comes back as: DuckDB spells no width."""
    return pyarrow.schema(
        [
            pyarrow.field("eventkey", pyarrow.binary()),
            pyarrow.field("orderkey", pyarrow.binary()),
            pyarrow.field("timepartition", pyarrow.timestamp("us", tz="UTC")),
            pyarrow.field("lastqty", pyarrow.float64()),
        ]
    )


def test_the_declaration_is_the_staged_schema_under_the_configuration() -> None:
    field = declared_field(
        staged(),
        "orders.events",
        arrow_types={"eventkey": "fixed_size_binary[16]", "orderkey": "fixed_size_binary[16]"},
        keys=["eventkey"],
        not_null=["orderkey", "timepartition"],
        partition_by={"timepartition": "day"},
        sort_by=["orderkey", "timepartition"],
    )

    assert field.name == "orders.events"
    assert primary_keys(field) == ["eventkey"]
    assert partition_keys(field) == {"timepartition": "day"}
    assert sort_keys(field) == {"orderkey": "asc", "timepartition": "asc"}
    assert field.into_arrow_schema().field("eventkey").type == pyarrow.binary(16)
    assert not field["eventkey"].nullable
    assert not field["orderkey"].nullable
    assert field["lastqty"].nullable, "a column no configuration names stays as staged"


def test_a_column_the_model_does_not_select_is_refused() -> None:
    """A key that is not in the select list would otherwise be a silent no-op."""
    with pytest.raises(ValueError, match="does not select"):
        declared_field(staged(), "orders.events", keys=["orderkey", "ordertime"])
    with pytest.raises(ValueError, match="lastpx, ordertime"):
        declared_field(
            staged(),
            "orders.events",
            arrow_types={"lastpx": "double"},
            partition_by=["ordertime"],
        )


def test_a_catalog_is_a_mapping_or_the_json_an_environment_carries() -> None:
    declared = {"name": "rekep", "properties": {"type": "sql"}}

    assert catalog_settings(declared) == declared
    assert catalog_settings(json.dumps(declared)) == declared
    with pytest.raises(TypeError, match="mapping"):
        catalog_settings(["rekep"])


def test_what_a_build_committed_is_reported_once() -> None:
    """The runner reads the rows off the plugin, so a second read is a second
    build's, not this one's again."""
    COMMITTED.clear()
    COMMITTED["orders.events"] = 62

    assert committed() == {"orders.events": 62}
    assert committed() == {}


# -- the products, over the checked-in fixture -------------------------------


@pytest.mark.integration
def test_the_products_are_built_from_the_fixture_and_a_replay_writes_nothing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole route, and the same route again.

    Every relative location -- the project, its staging directory -- is
    spelled from the repository root, which is where a run starts, so that is
    where this one starts too.
    """
    monkeypatch.chdir(ROOT)
    catalog = {
        "name": "rekep",
        "properties": {
            "type": "sql",
            "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
            "warehouse": (tmp_path / "warehouse").as_uri(),
        },
    }

    def ran(name: str) -> dict[str, Any]:
        argv = [
            "task",
            "run",
            str(ROOT / "tasks" / name / f"{name}.json"),
            "--parameter",
            f"catalog={json.dumps(catalog)}",
        ]
        if name == "parse_messages":
            argv += ["--parameter", f"filesystem={json.dumps(FIXTURE.as_uri())}"]
        assert cli.main(argv) == 0, name
        return json.loads(capsys.readouterr().out)

    landed = {name: ran(name) for name in WORKFLOW}
    built = landed["build_dbt"]

    assert built["targets"] == {
        "orders_events": "orders.events",
        "orders_current": "orders.current",
        "executions_fills": "executions.fills",
    }
    assert built["rows"] == PRODUCTS
    assert built["written"] == sum(PRODUCTS.values())
    assert built["skipped"] == 0, "no node was skipped, so every test ran"
    assert built["models"] == 4 and built["tests"] > 0
    assert len(json.dumps(built)) < 4096, "XCom carries a summary, never a payload"

    store = IcebergCatalog.from_dict(catalog)
    try:
        assert {
            name: store.dataset(name).read_arrow_table().num_rows for name in PRODUCTS
        } == PRODUCTS
        events = store.dataset("orders.events").read_arrow_table()
        assert events.schema.field("eventkey").type == pyarrow.binary(16)
        assert events.column("eventkey").null_count == 0
        assert len(set(events.column("eventkey").to_pylist())) == PRODUCTS["orders.events"]
        current = store.dataset("orders.current").read_arrow_table()
        assert sum(current.column("eventcount").to_pylist()) == PRODUCTS["orders.events"]
        fills = store.dataset("executions.fills").read_arrow_table()
        assert min(fills.column("lastqty").to_pylist()) > 0, "a fill states what it executed"
    finally:
        store.close()

    replayed = ran("build_dbt")

    assert replayed["rows"] == {
        "orders.events": 0,
        "executions.fills": 0,
        # An overwrite carries every row it built, and lands the same six.
        "orders.current": PRODUCTS["orders.current"],
    }
    store = IcebergCatalog.from_dict(catalog)
    try:
        assert {
            name: store.dataset(name).read_arrow_table().num_rows for name in PRODUCTS
        } == PRODUCTS, "a replay of the same capture adds no row"
    finally:
        store.close()
