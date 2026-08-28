"""Build documentation artifacts that derive from repository data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from rekep import Execution, Field, FixMsg, Instrument, Message, Order
from rekep.fix import FixRegistry, record_document
from rekep.market import Book
from rekep.text.message import SESSION_FIELDS

#: The six persisted contracts, in the order a row reaches them: text first,
#: then the market products a transcribed row is translated into. `stage` is
#: what the page groups them under and `source` is the product upstream, so
#: the lineage a page draws is this table and not a second hand-kept copy.
PRODUCTS: tuple[tuple[type, str, str, str | None], ...] = (
    (Message, "message", "text", None),
    (FixMsg, "fixmsg", "text", "message"),
    (Instrument, "instrument", "market", "fixmsg"),
    (Order, "order", "market", "fixmsg"),
    (Execution, "execution", "market", "fixmsg"),
    (Book, "book", "market", "order"),
)


def on_post_build(config: Any) -> None:
    """Write the browser artifacts from the code this checkout holds."""
    root = Path(config.config_file_path).resolve().parent
    assets = Path(config.site_dir) / "assets"
    assets.mkdir(parents=True, exist_ok=True)
    _write(assets / "fix-registry.json", _registry_catalog(root))
    _write(assets / "product-lineage.json", _product_catalog())


def _write(target: Path, payload: Any) -> None:
    """One artifact, as compact as JSON gets: the browser reads it, not a person."""
    target.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def _registry_catalog(root: Path) -> dict[str, Any]:
    """The browser catalog from the registry used by this checkout."""
    registry = FixRegistry(cache_dir=root / "data" / "fix", offline=True)
    return {
        "versions": list(registry.versions),
        "components": [
            {**entry.into_dict(), "slug": entry.slug}
            for entry in sorted(registry.component_records().values(), key=lambda one: one.name)
        ],
        "fields": [
            record_document(entry)
            for entry in sorted(
                registry.field_records().values(),
                key=lambda one: (
                    one.fix.tag is None,
                    one.fix.tag or 0,
                    one.fix.canonical,
                ),
            )
        ],
    }


def _product_catalog() -> dict[str, Any]:
    """Every product's columns, and what upstream each column is read from.

    Built from `Field.from_class` rather than from the checked-in documents
    under `schemas/`: those are dumped from these same classes, and a page
    that described the dump would describe whatever the last dump was rather
    than what this checkout does.

    Enumerations are collected once and referenced by name. `Currency` alone
    carries several hundred values, and six products naming it inline is the
    same table six times in a file the browser downloads.
    """
    enums: dict[str, Any] = {}
    products = []
    for pyclass, key, stage, source in PRODUCTS:
        document = Field.from_class(pyclass).into_dict()
        _lift_session_header(document, key)
        products.append(
            {
                "key": key,
                "name": document["name"],
                "stage": stage,
                "source": source,
                "description": document.get("description") or "",
                "namespace": document["metadata"]["namespace"],
                "version": document["metadata"]["version"],
                "columns": [_column(member, enums) for member in document["fields"]],
            }
        )
    return {"products": products, "enums": enums}


def _lift_session_header(document: dict[str, Any], key: str) -> None:
    """Give the raw stage's seven header columns the tags they are lifted from.

    `Message` is protocol-neutral, so its declaration carries no FIX origin --
    the annotation lives on `FixMsg`, where a column *is* a FIX field. But the
    raw stage does lift seven of them, and `SESSION_FIELDS` is where the code
    says which and from what tag. Read from there rather than matched on the
    column names: the names agreeing with the standard's is what makes the
    guess look safe, and a guess that looks safe is still not the declaration.
    """
    if key != "message":
        return
    tags = dict(SESSION_FIELDS)
    for member in document["fields"]:
        tag = tags.get(member["name"])
        if tag is not None:
            member["fix"] = {"tag": tag, "name": member["name"]}


def _column(member: dict[str, Any], enums: dict[str, Any]) -> dict[str, Any]:
    """One column as a lineage row: its type, its origin, and its role.

    `fix` is the whole of the origin a widget can show honestly -- the tag and
    the name a value is read from -- so the registry's version and message-type
    lists are dropped here. They belong to the field, and the registry pages
    already hold them.
    """
    column: dict[str, Any] = {
        "name": member["name"],
        "type": member["type"],
        "description": member.get("description") or "",
    }
    if member.get("nullable"):
        column["nullable"] = True
    origin = member.get("fix") or {}
    if origin.get("tag") or origin.get("name"):
        column["fix"] = {"tag": origin.get("tag"), "name": origin.get("name")}
    enum = member.get("enum") or {}
    if enum.get("name"):
        name = enum["name"]
        if name not in enums:
            enums[name] = json.loads(enum.get("values") or "{}")
        column["enum"] = name
    iceberg = member.get("iceberg") or {}
    if iceberg.get("primary_key"):
        column["key"] = "primary"
    if iceberg.get("partition_key"):
        column["partition"] = iceberg["partition_key"]
        column["derived_from"] = iceberg.get("derived_from")
    unit = (member.get("metadata") or {}).get("unit")
    if unit:
        column["unit"] = unit
    return column
