"""Build documentation artifacts that derive from repository data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from rekep.fix import FixRegistry


def on_post_build(config: Any) -> None:
    """Write the browser catalog from the registry used by this checkout."""
    root = Path(config.config_file_path).resolve().parent
    registry = FixRegistry(cache_dir=root / "data" / "fix", offline=True)
    payload = {
        "versions": list(registry.versions),
        "components": [
            {**entry.into_dict(), "slug": entry.slug}
            for entry in sorted(
                registry.component_entries().values(), key=lambda one: one.name
            )
        ],
        "fields": [
            entry.into_dict()
            for entry in sorted(
                registry.field_entries().values(),
                key=lambda one: (one.tag is None, one.tag or 0, one.name),
            )
        ],
    }
    target = Path(config.site_dir) / "assets" / "fix-registry.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
