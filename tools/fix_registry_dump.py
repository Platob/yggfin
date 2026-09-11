"""Publish the FIX section's two browser assets from rekep's registry.

The pages under `docs/fix/` read a projection of the dictionary rather than
the dictionary itself, because a browser cannot open an `IOBase` folder. Two
files, because six thousand definitions carry five megabytes of code sets and
a page load does not owe a reader that:

- `fix-registry.json` is the index every widget needs to resolve a key;
- `fix-details.json` is the members, code sets and lineage one entry needs
  only when somebody opens it.

Run from the repository root whenever the bundled registry changes:

    python tools/fix_registry_dump.py
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

from rekep import Field
from rekep.fix import fix_registry

ASSETS = pathlib.Path("docs/assets")


def records(field: Field, key: str, collection: str) -> list[dict[str, Any]]:
    """One validated FIX metadata document as rows, or none."""
    held = field.fix.get(key)
    return [] if held is None else json.loads(held).get(collection, [])


def shape_of(field: Field, shape: str) -> str:
    """The Arrow type a row shows.

    A group's own type is its whole nested subtree, which one definition
    spells in two hundred kilobytes. The members are published beside it, so
    the row states the shape and leaves the tree to them.
    """
    return "list<struct>" if shape == "group" else str(field.dtype.into_arrow())


def members(field: Field) -> list[dict[str, Any]]:
    """A repeating group's members, exploded and flattened natively."""
    if not field.dtype.is_nested:
        return []
    return [
        {
            "path": member.name,
            "tag": member.fix.tag,
            "name": member.display or member.name,
            "type": "list<struct>" if member.dtype.is_nested else str(member.dtype.into_arrow()),
        }
        for expanded in field.explode_fields()
        for member in expanded.unnest_fields()
    ]


def shown(registry: Any) -> list[tuple[Field, str]]:
    """Every definition a widget shows, beside the shape it is shown as.

    Iterating a registry walks its tagged scalars, and a repeating group is a
    named definition rather than one of them -- so the groups are asked for by
    name, or the browser would show none.
    """
    scalars = [(field, "field") for field in registry]
    groups = [(field, "group") for field in registry.definitions("groups")]
    return scalars + groups


def main() -> int:
    """Write both assets, and report what each cost."""
    registry = fix_registry()

    index: list[dict[str, Any]] = []
    detail: dict[str, dict[str, Any]] = {}
    for field, shape in shown(registry):
        fix = field.fix
        row: dict[str, Any] = {
            "tag": fix.tag,
            "name": field.name,
            "display": field.display or field.name,
            "branch": fix.branch or "standard",
            "shape": shape,
            "type": shape_of(field, shape),
            "nullable": field.nullable,
            "description": fix.description or field.comment or "",
        }
        # Absent rather than empty: an index of six thousand rows pays for
        # every key it repeats.
        for key, value in (("aliases", list(fix.aliases)), ("tags", list(fix.tags))):
            if value:
                row[key] = value
        index.append(row)

        deep = {
            key: value
            for key, value in (
                ("members", members(field)),
                ("codes", records(field, "codes", "codes")),
                ("lineage", records(field, "lineage", "entries")),
            )
            if value
        }
        if deep and fix.tag is not None:
            detail[str(fix.tag)] = deep

    index.sort(key=lambda row: row["tag"] or 0)
    published = {
        "source": "rekep bundle",
        "branches": sorted({row["branch"] for row in index}),
        "fields": index,
    }
    for name, held in (("fix-registry.json", published), ("fix-details.json", detail)):
        target = ASSETS / name
        target.write_text(json.dumps(held, separators=(",", ":")) + "\n", encoding="utf-8")
        print(f"  wrote {target} ({target.stat().st_size:,} bytes)")
    print(f"{len(index):,} definitions, {len(detail):,} with deep records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
