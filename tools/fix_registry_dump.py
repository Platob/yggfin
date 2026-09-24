"""Publish the FIX section's two browser assets from rekep's registry.

The pages under `docs/fix/` read a projection of the dictionary rather than
the dictionary itself, because a browser cannot open an `IOBase` folder. Two
files, because six thousand definitions carry five megabytes of code sets and
a page load does not owe a reader that:

- `fix-registry.json` is the index every widget needs to resolve a key;
- `fix-details.json` is the members, code sets and lineage one entry needs
  only when somebody opens it.

Run from the repository root whenever the bundled registry changes:

    uv run --project python python tools/fix_registry_dump.py
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

from rekep import Field
from rekep.fix import FixRegistry, fix_registry

ASSETS = pathlib.Path("docs/assets")
CATEGORIES = ("fields", "components", "groups")


def metadata_records(field: Field, key: str) -> list[dict[str, Any]]:
    """One validated FIX metadata document as rows, or none.

    The document is the collection itself, in the order the specification
    states it, so nothing is unwrapped out of a named member first.
    """
    held = field.fix.get(key)
    return [] if held is None else json.loads(held)


def definitions(registry: FixRegistry) -> list[tuple[str, Field]]:
    """Every stored definition, preserving its registry category."""
    document = json.loads(registry.into_json())
    return [
        (category, Field.from_json(json.dumps(stringly(held))))
        for category in CATEGORIES
        for held in document[category]
    ]


def stringly(declaration: dict[str, Any]) -> dict[str, Any]:
    """Restore metadata documents to the compact strings a Field holds."""
    metadata = declaration.get("metadata")
    if not isinstance(metadata, dict):
        return declaration
    return {
        **declaration,
        "metadata": {
            key: value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
            for key, value in metadata.items()
        },
    }


def members(field: Field) -> list[dict[str, Any]]:
    """A repeating group's members, exploded and flattened natively."""
    if not field.dtype.is_nested:
        return []
    return [
        {
            "path": member.name,
            "tag": member.fix.tag,
            "name": member.display or member.name,
            "type": str(member.dtype.into_arrow()),
        }
        for expanded in field.explode_fields()
        for member in expanded.unnest_fields()
    ]


def main() -> int:
    """Write both assets, and report what each cost."""
    registry = fix_registry()

    index: list[dict[str, Any]] = []
    fields: dict[str, dict[str, Any]] = {}
    for category, field in definitions(registry):
        fix = field.fix
        row: dict[str, Any] = {
            "id": fix.id,
            "tag": fix.tag,
            "name": field.name,
            "display": field.display or field.name,
            "dialect": ", ".join(fix.branches) or "standard",
            "category": category,
            "shape": "group" if category == "groups" else "field",
            "type": str(field.dtype.into_arrow()),
            "nullable": field.nullable,
            "description": fix.description or field.comment or "",
        }
        # Absent rather than empty: an index of six thousand rows pays for
        # every key it repeats.
        for key, value in (("names", list(fix.names)), ("tags", list(fix.tags))):
            if value:
                row[key] = value
        if fix.codeset:
            row["codeset"] = fix.codeset
        index.append(row)

        deep = {
            key: value
            for key, value in (
                ("members", members(field)),
                ("codeset", fix.codeset),
                ("lineage", metadata_records(field, "lineage")),
            )
            if value
        }
        if deep:
            fields[str(fix.id)] = deep

    index.sort(key=lambda row: (row["tag"] is None, row["tag"] or 0, row["name"]))
    published = {
        "source": "rekep bundle",
        "dialects": sorted({row["dialect"] for row in index}),
        "fields": index,
    }
    details = {
        "fields": fields,
        "codesets": {name: registry.codeset(name) for name in registry.codeset_names()},
    }
    for name, held in (("fix-registry.json", published), ("fix-details.json", details)):
        target = ASSETS / name
        target.write_text(json.dumps(held, separators=(",", ":")) + "\n", encoding="utf-8")
        print(f"  wrote {target} ({target.stat().st_size:,} bytes)")
    print(f"{len(index):,} definitions, {len(fields):,} with deep records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
