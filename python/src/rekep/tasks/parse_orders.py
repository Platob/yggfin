"""Flatten one window of a committed `market.books` snapshot into `market.orders`."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from rekep.tasks.events import flattened

#: The table this task writes.
TARGETS = ("market.orders",)


def run(
    *,
    books: str,
    snapshot_id: int | None,
    start: Any,
    end: Any,
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Replace `[start, end)` of `market.orders` from one `books` snapshot."""
    return flattened(
        "orders", books=books, snapshot_id=snapshot_id, start=start, end=end, catalog=catalog
    )
