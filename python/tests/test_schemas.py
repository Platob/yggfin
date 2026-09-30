"""The contracts under `schemas/` are what `tools/schemas_dump.py` writes from the tasks' fields.

Pinned here: no file drifted from the fields it was written from, nothing
else lives there, every Iceberg contract loads through PyIceberg's own models
and reads back as the field it states, and every `schema.yml` is a dbt
version 2 sources file whose columns and tests are the contract's.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder

from rekep import MarketDataKind, Side, State
from rekep.deploy import TABLES, Deployed
from rekep.iceberg import (
    CONTRACT_KEYS,
    iceberg_contract,
    iceberg_contract_field,
    partition_keys,
    primary_keys,
    sort_keys,
)
from rekep.storages import LAYERS

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS = ROOT / "schemas"

#: What a dbt source column may declare, and what a source table may.
DBT_COLUMN_KEYS = {"name", "data_type", "description", "data_tests"}
DBT_TABLE_KEYS = {"name", "description", "meta", "columns"}
DBT_SOURCE_KEYS = {"name", "description", "database", "schema", "tables"}


def tool() -> ModuleType:
    """`tools/schemas_dump.py`, which is a script and not a package module."""
    path = ROOT / "tools" / "schemas_dump.py"
    spec = importlib.util.spec_from_file_location("schemas_dump", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def dump() -> ModuleType:
    return tool()


@pytest.fixture(scope="module")
def published(dump: ModuleType) -> dict[Path, str]:
    """Every file the tool writes, with the text it writes into it."""
    return dict(dump.published())


def contract_path(table: Deployed) -> Path:
    return SCHEMAS / table.layer / f"{table.name.replace('.', '/')}.json"


def contract(table: Deployed) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(contract_path(table).read_text(encoding="utf-8"))
    return loaded


def sources(layer: str) -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load(
        (SCHEMAS / layer / "schema.yml").read_text(encoding="utf-8")
    )
    return loaded


def test_no_published_file_drifted_from_the_fields(published: dict[Path, str]) -> None:
    """Regenerate with `tools/schemas_dump.py` and review the diff when this fails."""
    drifted = [
        str(path.relative_to(ROOT))
        for path, text in published.items()
        if not path.is_file() or path.read_text(encoding="utf-8") != text
    ]
    assert not drifted, f"stale, run tools/schemas_dump.py: {drifted}"


def test_schemas_holds_only_what_the_tool_writes(published: dict[Path, str]) -> None:
    held = {path for path in SCHEMAS.rglob("*") if path.is_file()} - {SCHEMAS / "README.md"}
    assert held == {path for path in published if SCHEMAS in path.parents}


def test_one_contract_per_table_and_one_source_file_per_layer(published: dict[Path, str]) -> None:
    contracts = {path for path in published if path.suffix == ".json"}
    layers = {path for path in published if path.name == "schema.yml"}
    assert contracts == {contract_path(table) for table in TABLES}
    assert layers == {SCHEMAS / layer / "schema.yml" for layer in LAYERS}


@pytest.mark.parametrize("table", TABLES, ids=[table.table for table in TABLES])
def test_a_contract_is_pyiceberg_model_json(table: Deployed) -> None:
    document = contract(table)

    assert list(document) == list(CONTRACT_KEYS) == ["schema", "partition-spec", "sort-order"]
    schema = Schema.model_validate(document["schema"])
    spec = PartitionSpec.model_validate(document["partition-spec"])
    order = SortOrder.model_validate(document["sort-order"])

    # Every table is keyed, laid out and ordered the same way.
    assert schema.identifier_field_names() == {"curruuid"}
    assert schema.find_field("curruuid").required
    assert [
        (schema.find_column_name(field.source_id), str(field.transform)) for field in spec.fields
    ] == [("currunix", "hour")]
    assert [
        (schema.find_column_name(field.source_id), field.direction.value) for field in order.fields
    ] == [("currunix", "asc"), ("seqnum", "asc"), ("curruuid", "asc")]
    assert [field.field_id for field in schema.fields] == list(range(1, len(schema.fields) + 1))


@pytest.mark.parametrize("table", TABLES, ids=[table.table for table in TABLES])
def test_a_contract_reads_back_as_the_field_it_states(table: Deployed) -> None:
    """A contract read back and republished is the same bytes, so anything the
    reader drops shows up as a diff rather than as a silent loss."""
    text = contract_path(table).read_text(encoding="utf-8")
    field = iceberg_contract_field(text, table.name)

    assert f"{iceberg_contract(field)}\n" == text
    assert primary_keys(field) == ["curruuid"]
    assert partition_keys(field) == {"currunix": "hour"}
    assert list(sort_keys(field)) == ["currunix", "seqnum", "curruuid"]


def test_the_two_fix_tables_are_one_shape() -> None:
    """What the parse answered and what the walk restated are the same row."""
    raw, refined = (
        next(table for table in TABLES if table.table == name)
        for name in ("bronze.record_keeping.fix_messages", "silver.record_keeping.fix_messages")
    )
    assert contract(raw) == contract(refined)


def test_a_fix_row_holds_none_of_the_text_it_was_read_from() -> None:
    """`body`, `msgthreadid` and `loglevel` stay on the line, which `srcuuids`
    joins back to; the bracket's other captures fill FIX fields of their names."""
    columns = {
        table.table: [member["name"] for member in contract(table)["schema"]["fields"]]
        for table in TABLES
    }
    lines = columns["bronze.record_keeping.log_messages"]
    messages = columns["bronze.record_keeping.fix_messages"]

    assert lines[15:] == [
        "body",
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    ]
    for column in ("body", "msgthreadid", "loglevel"):
        assert column not in messages, column
    for column in ("srcuuids", "msgsessionid", "msgctxid", "msgseqnum", "msgpluginid", "exprunix"):
        assert column in messages, column
    # Every table opens with the event columns the text read states on a line.
    for table, names in columns.items():
        assert "currunix" in names[:2] and "state" in names, table


@pytest.mark.parametrize("layer", LAYERS)
def test_a_layer_is_a_dbt_v2_sources_file(layer: str) -> None:
    document = sources(layer)

    assert set(document) == {"version", "sources"}
    assert document["version"] == 2
    assert isinstance(document["sources"], list) and document["sources"]
    for source in document["sources"]:
        assert set(source) <= DBT_SOURCE_KEYS
        assert source["name"].startswith(layer)
        assert source["database"] == layer
        assert isinstance(source["tables"], list)
        for table in source["tables"]:
            assert set(table) <= DBT_TABLE_KEYS
            assert isinstance(table["name"], str) and table["columns"]
            for column in table["columns"]:
                assert set(column) <= DBT_COLUMN_KEYS, column
                assert isinstance(column["name"], str) and isinstance(column["data_type"], str)
                for test in column.get("data_tests", []):
                    assert test in ("not_null", "unique") or set(test) == {"accepted_values"}


@pytest.mark.parametrize("table", TABLES, ids=[table.table for table in TABLES])
def test_a_source_table_is_its_contract(table: Deployed, dump: ModuleType) -> None:
    """The dbt columns are the contract's, in order, typed and tested as it states."""
    namespace, name = table.name.split(".", 1)
    (source,) = [held for held in sources(table.layer)["sources"] if held["schema"] == namespace]
    (declared,) = [held for held in source["tables"] if held["name"] == name]
    members = contract(table)["schema"]["fields"]

    assert source["name"] == table.layer
    assert [column["name"] for column in declared["columns"]] == [
        member["name"] for member in members
    ]
    assert declared["meta"]["primary_key"] == ["curruuid"]
    assert declared["meta"]["partitioned_by"] == ["hour(currunix)"]
    assert declared["meta"]["written_by"] == dump.WRITERS[table.table]
    for column, member in zip(declared["columns"], members, strict=True):
        assert column["data_type"] == dump.iceberg_type(member["type"])
        tests = column.get("data_tests", [])
        assert ("not_null" in tests) is member["required"], column["name"]
        assert ("unique" in tests) is (column["name"] == "curruuid"), column["name"]
    enums = {"state": State, "side": Side, "marketdatakind": MarketDataKind}
    assert "state" in [column["name"] for column in declared["columns"]]
    for column in declared["columns"]:
        accepted = [test for test in column.get("data_tests", []) if isinstance(test, dict)]
        if column["name"] not in enums:
            assert not accepted, column["name"]
            continue
        (held,) = accepted
        values = [int(member) for member in enums[column["name"]]]
        assert held["accepted_values"]["values"] == values, column["name"]


def test_gold_declares_its_source_and_no_table() -> None:
    (source,) = sources("gold")["sources"]
    assert (source["name"], source["database"], source["tables"]) == ("gold", "gold", [])
