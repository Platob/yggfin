"""What a test that needs a catalog builds: a local one, reaching nothing."""

from __future__ import annotations

from pathlib import Path


def catalog_properties(tmp_path: Path, name: str = "warehouse") -> dict[str, str]:
    """A SQLite catalog and a file warehouse under `tmp_path`, so a test reaches nothing.

    `name` is what keeps two catalogs in one test apart: each needs a database
    *and* a warehouse directory of its own, or two tables of the same name land
    on the same files and a comparison compares one of them with itself.
    """
    warehouse = tmp_path / name
    warehouse.mkdir(parents=True, exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{(tmp_path / f'{name}.db').as_posix()}",
        "warehouse": warehouse.as_uri(),
    }
