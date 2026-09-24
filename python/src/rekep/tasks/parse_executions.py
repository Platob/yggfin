"""Flatten one window of a committed `market.books` snapshot into `market.executions`."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from rekep.tasks.events import flattened

#: The table this task writes.
TARGETS = ("market.executions",)


def run(
    *,
    books: str,
    snapshot_id: int | None,
    start: Any,
    end: Any,
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Replace `[start, end)` of `market.executions` from one `books` snapshot."""
    return flattened(
        "executions", books=books, snapshot_id=snapshot_id, start=start, end=end, catalog=catalog
    )
