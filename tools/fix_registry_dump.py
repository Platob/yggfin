"""Publish the FIX section's two browser assets from `config/fix`.

The pages under `docs/fix/` read a projection of the dictionary rather than
the dictionary itself, because a browser cannot open an `IOBase` folder. Two
files, because six thousand definitions carry five megabytes of code sets and
a page load does not owe a reader that:

- `fix-registry.json` is the index every widget needs to resolve a key;
- `fix-details.json` is the members, code sets and lineage one entry needs
  only when somebody opens it.

Run from the repository root whenever `config/fix` changes:

    python tools/fix_registry_dump.py
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

from yggdryl import Field, IOBase
from yggdryl.fix import FixRegistry

ASSETS = pathlib.Path("docs/assets")
SOURCE = "file:config/fix"


def records(field: Field, key: str, collection: str) -> list[dict[str, Any]]:
    """One validated FIX metadata document as rows, or none."""
    held = field.fix.get(key)
    return [] if held is None else json.loads(held).get(collection, [])


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
    registry = FixRegistry.from_handle(IOBase.from_uri(SOURCE))
    registry.with_crate_fields()

    index: list[dict[str, Any]] = []
    detail: dict[str, dict[str, Any]] = {}
    for field in registry:
        fix = field.fix
        row: dict[str, Any] = {
            "tag": fix.tag,
            "name": field.name,
            "display": field.display or field.name,
            "branch": fix.branch or "standard",
            "shape": "group" if field.dtype.is_nested else "field",
            "type": str(field.dtype.into_arrow()),
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
        "source": "config/fix",
        "branches": sorted({row["branch"] for row in index}),
        "fields": index,
    }
    for name, held in (("fix-registry.json", published), ("fix-details.json", detail)):
        target = ASSETS / name
        target.write_text(json.dumps(held, separators=(",", ":")) + "\n", encoding="utf-8")
        print(f"  ✓ {target} ({target.stat().st_size:,} bytes)")
    print(f"{len(index):,} definitions, {len(detail):,} with deep records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
