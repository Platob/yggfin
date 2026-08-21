"""A capture with a known mix of event kinds, and a catalog to land it in."""

from __future__ import annotations

from pathlib import Path

import pytest

from rekep.market import EventType

#: One line per kind, repeated -- so every count below is derived from this and
#: then pinned against a literal, rather than both moving together.
KINDS: dict[EventType, str] = {
    EventType.EXECUTION: "8=FIX.4.4\x0135=8\x0117=e{i}\x01",
    EventType.ORDER: "sent NewOrderSingle AAPL {i}@10.0",
    EventType.BOOK_SIDE: "8=FIX.4.4\x0135=X\x01268={i}\x01",
    EventType.UNKNOWN: "heartbeat {i}",
}

#: Lines per kind, per hour, in the capture the fixtures build.
PER_HOUR = 6
HOURS = 2
EXPECTED_ROWS = PER_HOUR * HOURS * len(KINDS)


def write_capture(folder: Path, name: str = "a.log", day: str = "2026-08-14") -> Path:
    """A log of `EXPECTED_ROWS` lines, spread over two hours and every kind."""
    folder.mkdir(parents=True, exist_ok=True)
    lines = [
        f"{day} 0{hour}:{index:02d}:0{slot}.167_520 [t-1] [Bridge] " + template.format(i=index)
        for hour in range(HOURS)
        for index in range(PER_HOUR)
        for slot, template in enumerate(KINDS.values())
    ]
    path = folder / name
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.fixture
def capture(tmp_path: Path) -> Path:
    """A folder holding one log."""
    folder = tmp_path / "capture"
    write_capture(folder)
    return folder


@pytest.fixture
def catalog(tmp_path: Path) -> dict[str, str]:
    """A local SQLite catalog, so the whole flow runs with nothing to reach."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        "warehouse": warehouse.as_uri(),
    }
