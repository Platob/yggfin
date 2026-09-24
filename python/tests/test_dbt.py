"""The dbt project under `data/dbt`, and the plugin it reads and commits through.

dbt owns the SQL and this repository owns the Iceberg seam, so what is pinned
here is that seam: the declaration a model's configuration builds, the tables
its sources may name, and -- as an integration -- the products a build actually
commits and what a replay of the same build does not write twice.
"""

from __future__ import annotations

import collections
import json
import logging
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow
import pytest
from dbt.cli.main import dbtRunner

from rekep.dbt import (
    CATALOG,
    OPENED,
    Plugin,
    catalog_settings,
    declared_field,
    released,
)
from rekep.deploy import TABLES
from rekep.fix import FixCodec, fix_registry
from rekep.iceberg import IcebergCatalog, partition_keys, primary_keys, sort_keys
from rekep.pipeline import parse_fix_raw, parse_fix_refined, parse_messages
from rekep.times import window_of

ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT / "data" / "dbt"
FIXTURE = ROOT / "data" / "capture" / "ulbridge.log"

#: The tables ingestion publishes, by name: what a source may read and what no
#: model may write.
INGESTED = {shape.table: shape for shape in TABLES}

#: What the models commit, and the rows the checked-in 144-line fixture answers
#: for each. One event per message that carried an order identity and a
#: lifecycle fact, one row per chain, and one occurrence per execution the
#: bridge relayed into a business-identifier chain.
PRODUCTS = {
    "orders.events": 16,
    "orders.current": 8,
    "executions.fills": 7,
}

#: The day the fixture was captured on: the window every ingestion stage lands.
WINDOW = window_of("2026-08-14", "2026-08-14")

#: The business identifier whose order the products are read to the cent for.
CHAIN = "00026877711XOEA0"

#: What the codec answers for one `OrdStatus(39)` code. The normalized
#: spellings the macros fold on are the codec's and this repository holds no
#: list of them, so a parsed message is the only thing that can say what they
#: are -- which is what makes the mapping below a reading rather than a guess.
SETTLED = {
    "0": "20NEW",
    "2": "80FILLED",
    "3": "80DONEDAY",
    "4": "90CANCELED",
    "5": "70REPLACED",
    "8": "95REJECTED",
    "B": "80CALCULAT",
    "C": "95EXPIRED",
    "D": "20ACCEPTED",
}

#: The macro file the folds read their vocabulary out of.
MACROS = PROJECT / "macros" / "rekep.sql"


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

    assert named <= set(INGESTED), "a source reads an ingested table, never another store"
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
    """A product is derived; an ingested table is written by its `rekep.pipeline` stage."""
    assert not set(published(manifest)) & set(INGESTED)


def test_the_published_models_are_the_declared_products(manifest: Any) -> None:
    assert set(published(manifest)) == set(PRODUCTS)
    for table, node in published(manifest).items():
        declared = node.config.extra
        assert declared["plugin"] == "rekep"
        assert declared["mode"] == "overwrite", f"{table} is replaced on its key on a rebuild"
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


def spelled(macro: str) -> set[str]:
    """The values one state macro names."""
    body = MACROS.read_text(encoding="utf-8").split(f"macro {macro}()", 1)[1]
    return set(re.findall(r"\'([^\']+)\'", body.split("endmacro", 1)[0]))


def test_the_states_a_fold_names_are_the_ones_the_codec_answers() -> None:
    """A hand-written vocabulary drifts silently; this is what notices.

    The codec is the oracle: one message per status code, parsed by the
    bundled dictionary, against the spellings `orders.current` opens and closes
    on. `5` Replaced is deliberately not terminal -- a replaced chain continues
    under its replacement -- so its absence is pinned too.
    """
    codec = FixCodec(fix_registry())
    answered = {}
    for code in SETTLED:
        line = b"8=FIX.4.4|35=8|11=X|37=Y|17=Z|150=F|39=" + code.encode() + b"|10=000|"
        (message,) = codec.parse_line(line)
        answered[code] = message.by_name("state").as_py()

    assert answered == SETTLED
    terminal = spelled("rekep_terminal_states")
    opening = spelled("rekep_opening_states")
    assert terminal == {SETTLED[code] for code in ("2", "3", "B", "4", "8", "C")}, (
        "a state no longer settles the way the fold reads it"
    )
    assert opening == {SETTLED[code] for code in ("0", "D")}
    assert SETTLED["5"] not in terminal, "a replaced chain continues under its replacement"


def test_the_drift_check_lists_every_state_the_folds_name(manifest: Any) -> None:
    """The reported vocabulary covers the one the folds act on, so a capture
    carrying a state this project reads is never reported as drift."""
    listed = {
        value
        for node in manifest.nodes.values()
        if getattr(node, "test_metadata", None) is not None
        and node.test_metadata.name == "accepted_values"
        for value in node.test_metadata.kwargs.get("values", ())
    }

    assert listed, "no accepted_values test reports an unexpected state"
    assert spelled("rekep_terminal_states") | spelled("rekep_opening_states") <= listed
    assert all(str(node.config.severity).casefold() == "warn" for node in _accepted(manifest))


def _accepted(manifest: Any) -> list[Any]:
    """Every `accepted_values` test the project declares."""
    return [
        node
        for node in manifest.nodes.values()
        if getattr(node, "test_metadata", None) is not None
        and node.test_metadata.name == "accepted_values"
    ]


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


def test_the_catalog_a_build_read_through_is_closed_when_it_is_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dbt says nothing to a plugin when a build ends, so the caller running
    `dbtRunner` in its own process closes it.

    An Iceberg catalog is a live connection, and over SQLite it is an open
    file: Windows will not delete one, so a build into a temporary warehouse
    fails on its way out rather than in any model -- which POSIX, where an
    open file unlinks, never shows.
    """
    import pyiceberg.catalog

    class Opened:
        closed = 0

        def close(self) -> None:
            self.closed += 1

    opened = Opened()
    monkeypatch.delenv(CATALOG, raising=False)
    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", lambda *_args, **_kwargs: opened)
    OPENED.clear()
    plugin = Plugin(
        name="rekep",
        plugin_config={"catalog": {"name": "rekep", "properties": {}}},
    )

    assert OPENED == [], "asking for a plugin opens nothing"
    held = plugin.catalog()
    assert held.catalog is opened, "the handle loads on first use, not on from_dict"
    assert OPENED == [plugin]

    assert released() == 1
    assert (OPENED, opened.closed) == ([], 1)
    assert released() == 0, "nothing is held, so nothing is closed a second time"
    # A build after this one reads through a handle of its own, not a closed one.
    assert plugin.catalog() is not held
    assert released() == 1


# -- the products, over the checked-in fixture -------------------------------


#: What dbt calls a node that did not settle. A test the project declares as
#: a warning is a quality signal about the capture, not a failed build.
FAILURES = ("error", "fail", "runtime error")


def built(target: Path, caplog: pytest.LogCaptureFixture) -> dict[str, int]:
    """`dbt build` over the project in this process: the rows each table took.

    The rows are read off the plugin's one INFO record per commit, because
    dbt's own results count nodes and not rows. `target` holds the build's
    target path, which the staged Parquet goes with.
    """
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="rekep.dbt"):
        invoked = dbtRunner().invoke(
            [
                "build",
                "--project-dir",
                str(PROJECT),
                "--profiles-dir",
                str(PROJECT),
                "--target-path",
                str(target / "target"),
                "--log-path",
                str(target / "logs"),
                "--log-level",
                "none",
            ]
        )
    # The catalog the build read and committed through is this process's to
    # close, and one plugin held it.
    assert released() == 1
    assert invoked.exception is None, invoked.exception
    nodes = list(invoked.result.results)
    failed = [node.node.name for node in nodes if str(node.status) in FAILURES]
    assert invoked.success and not failed, failed
    assert not [node.node.name for node in nodes if str(node.status) == "skipped"], (
        "no node was skipped, so every test ran"
    )
    kinds = collections.Counter(str(node.node.resource_type) for node in nodes)
    assert kinds["model"] == 4 and kinds["test"] > 0, kinds
    committed: collections.Counter[str] = collections.Counter()
    for record in caplog.records:
        if record.name == "rekep.dbt" and (
            matched := re.fullmatch(r"\w+ overwrote (\d+) rows into (\S+)", record.getMessage())
        ):
            committed[matched.group(2)] += int(matched.group(1))
    return dict(committed)


@pytest.mark.integration
def test_the_products_are_built_from_the_fixture_and_a_replay_adds_no_row(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole route, and the build again.

    The three ingestion stages land the fixture's day through `rekep.pipeline`,
    and dbt builds the project from the repository root, which is where every
    relative location the profile spells starts; `REKEP_DBT_CATALOG` points it
    at this test's catalog.
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
    store = IcebergCatalog.from_dict(catalog)
    try:
        parse_messages(FIXTURE.as_uri(), store, WINDOW)
        parse_fix_raw(store, WINDOW)
        parse_fix_refined(store, WINDOW)
    finally:
        store.close()
    monkeypatch.setenv(CATALOG, json.dumps(catalog))

    assert built(tmp_path, caplog) == PRODUCTS

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
        # One order read to the cent: two partial fills and the one that
        # closed it, folded into its current state and its three fills.
        order = pyarrow.compute.equal(current.column("orderid"), CHAIN)
        assert [
            (row["cumqty"], row["leavesqty"], row["avgpx"], row["state"], row["eventcount"])
            for row in current.filter(order).to_pylist()
        ] == [(Decimal(600), Decimal(0), Decimal("83.08"), "80FILLED", 4)]
        chain = fills.filter(pyarrow.compute.equal(fills.column("orderid"), CHAIN))
        assert sorted(chain.column("lastqty").to_pylist()) == [Decimal(q) for q in (21, 57, 75)]
        refined = store.dataset("fix.refined").read_arrow_table()
        expired = refined.filter(pyarrow.compute.equal(refined.column("state"), "95EXPIRED"))
        assert expired.num_rows == 1
        expiry = expired.to_pylist()[0]
        expiry_event = events.filter(
            pyarrow.compute.equal(events.column("eventkey"), expiry["curruuid"])
        )
        assert expiry_event.num_rows == 1
        assert expiry_event.column("eventtime").to_pylist() == [expiry["currunix"]]
        assert expiry_event.column("state").to_pylist() == ["95EXPIRED"]
        expiry_current = current.filter(
            pyarrow.compute.equal(current.column("last_eventkey"), expiry["curruuid"])
        )
        assert expiry_current.num_rows == 1
        assert expiry_current.column("updatedat").to_pylist() == [expiry["currunix"]]
        assert expiry_current.column("state").to_pylist() == ["95EXPIRED"]
        assert expiry["transacttime"] != expiry["currunix"]
    finally:
        store.close()

    # Every model is committed on its key, so a rebuild carries every row it
    # built and the tables hold each one once.
    assert built(tmp_path, caplog) == PRODUCTS
    store = IcebergCatalog.from_dict(catalog)
    try:
        assert {
            name: store.dataset(name).read_arrow_table().num_rows for name in PRODUCTS
        } == PRODUCTS, "a replay of the same capture adds no row"
    finally:
        store.close()
