"""Write every table's published contract: Iceberg JSON, dbt YAML and a page.

The tables are `rekep.deploy.TABLES`, and each one's shape is the field its
task declares it with -- the native read's own row, the FIX registry's fixed
row, the book fold's -- narrowed to what Iceberg stores. Nothing here states a
column; it writes down what those fields already say, three ways:

- `schemas/<layer>/<namespace>/<table>.json`, the Iceberg contract: schema,
  partition spec and sort order, as PyIceberg's own model JSON;
- `schemas/<layer>/schema.yml`, the layer as a dbt source: every table, every
  top-level column with its Iceberg type, description and tests;
- `docs/tables/<layer>/<table>.md`, the page a reader looks a column up on.

Run from the repository root whenever a shape changes:

    uv run --project python python tools/schemas_dump.py

`python/tests/test_schemas.py` fails on any drift between a file and this.
"""

from __future__ import annotations

import json
import pathlib
from collections.abc import Iterator
from typing import Any

import yaml

from rekep import State
from rekep.deploy import TABLES, Deployed
from rekep.iceberg import iceberg_contract
from rekep.storages import LAYERS

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "schemas"
PAGES = ROOT / "docs" / "tables"

#: What each layer holds, in a sentence a dbt source and a page both state.
LAYER_DESCRIPTIONS = {
    "bronze": (
        "What was read, as it was read: the captured lines, and every FIX frame "
        "parsed out of them. Keyed on each row's own identity, laid out by the hour "
        "of `currunix`."
    ),
    "silver": (
        "What a walk settled: one row per FIX event with its lifecycle, the books "
        "folded from them, and the order, quote and execution events flattened out "
        "of the books."
    ),
    "gold": (
        "The consumers' layer: aggregates built from silver. No task here writes "
        "it; it is created empty so a product built on silver has a catalog of its own."
    ),
}

#: What each table holds, in a sentence.
TABLE_DESCRIPTIONS = {
    "bronze.record_keeping.log_messages": (
        "One row per captured line: the event the read settled over it, the line past "
        "its row header as `body`, and one column per row-header capture."
    ),
    "bronze.record_keeping.fix_messages": (
        "One row per FIX message parsed out of a stored line, nothing walked: a "
        "message logged again at another hop is folded into the one row its identity "
        "names, and `seqnum` and `prevuuid` are empty."
    ),
    "silver.record_keeping.fix_messages": (
        "One row per FIX event, walked: the chain it belongs to, the step it stands "
        "at, the lines it was logged on, and the state, creation and expiry its chain "
        "folded forward."
    ),
    "silver.record_keeping.books": (
        "One row per book the fold answered over the silver FIX events of a window, "
        "from no depth before its start: both sides' live depth and deltas, and the "
        "executions."
    ),
    "silver.record_keeping.orders": "The order deltas of the books, one row per order event.",
    "silver.record_keeping.quotes": "The quote deltas of the books, one row per quote event.",
    "silver.record_keeping.executions": "The executions the books carry, one row per execution.",
}

#: The task that writes each table.
WRITERS = {
    "bronze.record_keeping.log_messages": "parse_log_messages",
    "bronze.record_keeping.fix_messages": "parse_fix_messages_raw",
    "silver.record_keeping.fix_messages": "parse_fix_messages_refined",
    "silver.record_keeping.books": "parse_books",
    "silver.record_keeping.orders": "parse_orders",
    "silver.record_keeping.quotes": "parse_quotes",
    "silver.record_keeping.executions": "parse_executions",
}


#: The page each task is documented on, where it is not the task's own name.
TASK_PAGES = {
    "parse_log_messages": "parse-log-messages",
    "parse_fix_messages_raw": "parse-fix-messages-raw",
    "parse_fix_messages_refined": "parse-fix-messages-refined",
    "parse_books": "parse-books",
    "parse_orders": "parse-orders-quotes-executions",
    "parse_quotes": "parse-orders-quotes-executions",
    "parse_executions": "parse-orders-quotes-executions",
}


def contract(table: Deployed) -> dict[str, Any]:
    """One table's Iceberg contract, as the JSON document it is written as."""
    loaded: dict[str, Any] = json.loads(iceberg_contract(table.into_field()))
    return loaded


def iceberg_type(dtype: Any) -> str:
    """An Iceberg type as the one line a column table spells it in."""
    if isinstance(dtype, str):
        return dtype
    kind = dtype["type"]
    if kind == "list":
        return f"list<{iceberg_type(dtype['element'])}>"
    if kind == "map":
        return f"map<{iceberg_type(dtype['key'])}, {iceberg_type(dtype['value'])}>"
    if kind == "struct":
        members = ", ".join(
            f"{member['name']}: {iceberg_type(member['type'])}" for member in dtype["fields"]
        )
        return f"struct<{members}>"
    return str(kind)


def layout(document: dict[str, Any]) -> dict[str, list[str]]:
    """The key, the partition transforms and the sort order a contract states."""
    schema = document["schema"]
    names = {member["id"]: member["name"] for member in schema["fields"]}
    return {
        "primary_key": [names[held] for held in schema.get("identifier-field-ids", [])],
        "partitioned_by": [
            f"{spec['transform']}({names[spec['source-id']]})"
            for spec in document["partition-spec"]["fields"]
        ],
        "sorted_by": [names[order["source-id"]] for order in document["sort-order"]["fields"]],
    }


def dbt_column(member: dict[str, Any], key: list[str]) -> dict[str, Any]:
    """One top-level column as a dbt source column."""
    column: dict[str, Any] = {
        "name": member["name"],
        "data_type": iceberg_type(member["type"]),
    }
    if member.get("doc"):
        column["description"] = member["doc"]
    tests: list[Any] = []
    if member["required"]:
        tests.append("not_null")
    if member["name"] in key:
        tests.append("unique")
    if member["name"] == "state":
        tests.append(
            {"accepted_values": {"values": [int(state) for state in State], "quote": False}}
        )
    if tests:
        column["data_tests"] = tests
    return column


def dbt_layer(layer: str, documents: dict[str, dict[str, Any]]) -> str:
    """One layer as a dbt `schema.yml`: one source per namespace."""
    namespaces: dict[str, list[dict[str, Any]]] = {}
    for table in TABLES:
        if table.layer != layer:
            continue
        namespace, name = table.name.split(".", 1)
        document = documents[table.table]
        held = layout(document)
        namespaces.setdefault(namespace, []).append(
            {
                "name": name,
                "description": TABLE_DESCRIPTIONS[table.table],
                "meta": {"written_by": WRITERS[table.table], **held},
                "columns": [
                    dbt_column(member, held["primary_key"])
                    for member in document["schema"]["fields"]
                ],
            }
        )
    sources = [
        {
            "name": layer if len(namespaces) < 2 else f"{layer}_{namespace}",
            "description": LAYER_DESCRIPTIONS[layer],
            "database": layer,
            "schema": namespace,
            "tables": tables,
        }
        for namespace, tables in namespaces.items()
    ] or [
        {
            "name": layer,
            "description": LAYER_DESCRIPTIONS[layer],
            "database": layer,
            "schema": "record_keeping",
            "tables": [],
        }
    ]
    return yaml.safe_dump(
        {"version": 2, "sources": sources},
        sort_keys=False,
        allow_unicode=True,
        width=100,
    )


def page(table: Deployed, document: dict[str, Any]) -> str:
    """One table's documentation page: what it holds, its layout, every column."""
    held = layout(document)
    writer = WRITERS[table.table]
    short = table.name.split(".", 1)[1]
    lines = [
        f"# {table.table}",
        "",
        TABLE_DESCRIPTIONS[table.table],
        "",
        "| | |",
        "| --- | --- |",
        f"| written by | [`{writer}`](../../tasks/{TASK_PAGES[writer]}.md) |",
        f"| key | {', '.join(f'`{name}`' for name in held['primary_key']) or 'none'} |",
        f"| partitioned by | {', '.join(f'`{name}`' for name in held['partitioned_by'])} |",
        f"| sorted by | {', '.join(f'`{name}`' for name in held['sorted_by'])} |",
        f"| columns | {len(document['schema']['fields'])} |",
        f"| Iceberg contract | `schemas/{table.layer}/{table.name.replace('.', '/')}.json` |",
        (
            f"| dbt source | `{{{{ source('{table.layer}', '{short}') }}}}`,"
            f" `schemas/{table.layer}/schema.yml` |"
        ),
        f"| sample rows | [{table.table}](../../samples/{table.layer}/{short}.md) |",
        "",
        "## Columns",
        "",
        "| column | type | required | description |",
        "| --- | --- | :---: | --- |",
    ]
    for member in document["schema"]["fields"]:
        described = " ".join((member.get("doc") or "").split()).replace("|", "\\|")
        typed = iceberg_type(member["type"]).replace("|", "\\|")
        required = "yes" if member["required"] else ""
        lines.append(f"| `{member['name']}` | `{typed}` | {required} | {described} |")
    if any(member["name"] == "state" for member in document["schema"]["fields"]):
        lines += [
            "",
            "## `state` codes",
            "",
            "`state` stores the code of a lifecycle-sorted enum: the codes order from the",
            "first state to the terminal ones, and a code's hundreds are its rank.",
            "[States](../states.md) lists every member.",
        ]
    return "\n".join(lines) + "\n"


def states_page() -> str:
    """The page listing every state a `state` column stores, by code."""
    lines = [
        "# States",
        "",
        "`state` is stored as the `int32` code of a lifecycle-sorted enum on every",
        "table that carries it. The hundreds of a code are its rank, so the codes",
        "order from the first state to the terminal ones and a band of ranks is one",
        "question: below `8000` a thing can still change, `8000`-`8999` it ended having",
        "done what was asked, `9000`-`9499` someone stopped it, `9500`-`9999` it could",
        "not be done. A state added to a rank takes the next free number of it, so no",
        "stored code moves.",
        "",
        "| code | name | rank | meaning |",
        "| ---: | --- | ---: | --- |",
    ]
    for state in State:
        described = " ".join(state.description.split()).replace("|", "\\|")
        lines.append(f"| {int(state)} | `{state.name}` | {state.rank} | {described} |")
    return "\n".join(lines) + "\n"


def published() -> Iterator[tuple[pathlib.Path, str]]:
    """Every file this writes, with the text it holds."""
    documents = {table.table: contract(table) for table in TABLES}
    for table in TABLES:
        path = SCHEMAS / table.layer / f"{table.name.replace('.', '/')}.json"
        yield path, json.dumps(documents[table.table], indent=2, ensure_ascii=False) + "\n"
        yield (
            PAGES / table.layer / f"{table.name.split('.', 1)[1]}.md",
            page(table, documents[table.table]),
        )
    for layer in LAYERS:
        yield SCHEMAS / layer / "schema.yml", dbt_layer(layer, documents)
    yield PAGES / "states.md", states_page()


def main() -> None:
    for path, text in published():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print(path.relative_to(ROOT))


if __name__ == "__main__":
    main()
