"""Shared test isolation: local catalogs and no cloud discovery."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _keep_aws_discovery_off_the_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give Arrow inert credentials so unit tests never probe EC2 metadata."""
    for name in (
        "S3_ENDPOINT_URL",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "S3_SESSION_TOKEN",
        "S3_REGION",
        "AWS_ENDPOINT_URL_S3",
        "AWS_ENDPOINT_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "rekep-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "rekep-test")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


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
