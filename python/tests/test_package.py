"""The retained package surface is importable and native-field based."""

from __future__ import annotations

import importlib
import pathlib
import tomllib
from typing import Annotated

import pytest
from yggdryl import Field as YggdrylField

import rekep
from rekep import Field, IOBase, TextOptions, scalar
from rekep.fields import (
    PARTITION_KEY,
    PRIMARY_KEY,
    SORT_KEY,
    partition_key,
    primary_key,
    sort_key,
)
from rekep.fix import fix_registry, global_registry, registry_path

PYPROJECT = pathlib.Path(__file__).parent.parent / "pyproject.toml"

PACKAGES = (
    "rekep",
    "rekep.fields",
    "rekep.fix",
    "rekep.iceberg",
    "rekep.tasks",
    "rekep.text",
)


def test_the_package_version_is_the_one_the_build_publishes() -> None:
    """Two spellings of one number, which is exactly how they drift apart."""
    declared = tomllib.loads(PYPROJECT.read_text())["project"]["version"]
    assert rekep.__version__ == declared


@pytest.mark.parametrize("package", PACKAGES)
def test_everything_exported_is_importable(package: str) -> None:
    """`__all__` is a promise; a name that moved without it is an ImportError."""
    module = importlib.import_module(package)
    assert module.__all__, f"{package} publishes nothing"
    for name in module.__all__:
        assert hasattr(module, name), f"{package}.__all__ names {name!r}, which is not there"


def test_scalar_is_the_only_public_decorator_name() -> None:
    """One public decorator keeps declarations on one searchable spelling."""
    assert callable(scalar)
    assert "scalar" in rekep.__all__
    assert "field" not in rekep.__all__
    assert not hasattr(rekep, "field")


@scalar
class Row:
    """One row, declaring each protocol key exactly once."""

    unix: Annotated[int, primary_key(), sort_key()]
    """When."""

    hour: Annotated[int, partition_key()]
    """Which hour."""


def test_the_protocol_keys_are_the_ones_a_declaration_writes() -> None:
    """The published spelling of a key, against the one a field actually stores.

    `SORT_KEY` is exported and referenced nowhere else in this repository, so
    a rename of the metadata key would leave the constant behind, still
    exported, still wrong, and nothing would fail.
    """
    metadata = dict(Row.field().field("unix").metadata)
    assert metadata[SORT_KEY] == "asc"
    assert metadata[PRIMARY_KEY] == "true"
    hour = Row.field().field("hour")
    assert hour.is_partition
    assert hour.metadata["field:partition"] == "true"
    assert PARTITION_KEY not in hour.metadata
    assert partition_key("day")["metadata"][PARTITION_KEY] == "day"


def test_rekep_reexports_the_native_yggdryl_field() -> None:
    """There is no project Field implementation or compatibility subclass."""
    assert Field is YggdrylField


def test_rekep_installs_its_bundled_registry_as_the_process_default() -> None:
    bundled = fix_registry()

    assert registry_path().is_dir()
    assert len(bundled) == 6262
    assert global_registry() == bundled
    assert IOBase.__module__.startswith("yggdryl")
    assert TextOptions.__module__.startswith("yggdryl")


def test_the_scheduling_dependencies_are_installed_wherever_they_can_be() -> None:
    """A skipped Airflow suite must not be able to read as a green one.

    `tests/test_marimo_operator.py` opens with `importorskip("airflow")`, so
    dropping the `airflow` group would delete the operator and DAG tests from
    the run without failing anything. This is the one assertion that notices,
    on every platform Airflow supports.
    """
    import importlib.util
    import sys

    if sys.platform == "win32":  # pragma: no cover - Airflow is POSIX-only
        pytest.skip("Airflow does not run on Windows")

    for name in ("airflow", "airflow.providers.standard.hooks.subprocess", "marimo"):
        assert importlib.util.find_spec(name) is not None, (
            f"{name} is missing: the operator, DAG and runner tests would silently skip. "
            "Sync the default groups (dev, runner, airflow)."
        )
