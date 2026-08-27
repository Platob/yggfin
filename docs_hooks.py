"""Build documentation artifacts that derive from repository data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from rekep.fix import FixRegistry, record_document


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
            record_document(entry)
            for entry in sorted(
                registry.field_entries().values(),
                key=lambda one: (one.fix.tag is None, one.fix.tag or 0, one.fix.canonical),
            )
        ],
    }
    target = Path(config.site_dir) / "assets" / "fix-registry.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
