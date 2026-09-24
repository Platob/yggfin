"""The retained package surface is importable and native-field based."""

from __future__ import annotations

import importlib
import pathlib
import sys

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - the back-port is what the older interpreter has
    import tomli as tomllib
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
from rekep.fix import fix_crate_fields, fix_registry, global_registry, registry_path

PYPROJECT = pathlib.Path(__file__).parent.parent / "pyproject.toml"

PACKAGES = (
    "rekep",
    "rekep.dbt",
    "rekep.fields",
    "rekep.fix",
    "rekep.iceberg",
    "rekep.market",
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
    metadata = dict(Row.into_field().field("unix").metadata)
    assert metadata[SORT_KEY] == "asc"
    assert metadata[PRIMARY_KEY] == "true"
    hour = Row.into_field().field("hour")
    assert hour.is_partition
    assert hour.metadata["FIELD:partition"] == "true"
    assert PARTITION_KEY not in hour.metadata
    assert partition_key("day")["metadata"][PARTITION_KEY] == "day"


def test_rekep_reexports_the_native_yggdryl_field() -> None:
    """There is no project Field implementation or compatibility subclass."""
    assert Field is YggdrylField


def test_rekep_installs_its_bundled_registry_as_the_process_default() -> None:
    bundled = fix_registry()

    assert registry_path().is_dir()
    assert len(bundled) == 7781
    assert global_registry() == bundled
    assert IOBase.__module__.startswith("yggdryl")
    assert TextOptions.__module__.startswith("yggdryl")


def test_the_bundled_dictionary_is_the_crates_own_where_it_restates_the_crate() -> None:
    """The bundle is yggdryl's `config/fix`, copied whole and never edited.

    Three of its documents restate definitions the core seeds on every
    registry it builds -- the crate's scalar columns and its two Map groups.
    A committed copy of one is free to drift the day the core restates it,
    so each is compared with the field the installed crate answers: the same
    tag, datatype and nullability, or the vendored copy is stale and the
    re-vendoring is what fixes it, not an edit.
    """
    import json

    owned = {member.name: member for member in fix_crate_fields()}
    root = registry_path()
    held: list[str] = []
    for category in ("fields", "components", "groups"):
        folder = root / category
        if not folder.is_dir():
            continue
        for path in folder.glob("*.json"):
            document = json.loads(path.read_text(encoding="utf-8"))
            for one in document if isinstance(document, list) else [document]:
                if not isinstance(one, dict) or one.get("name") not in owned:
                    continue
                stated = json.loads(owned[one["name"]].into_json())
                # A list-valued metadata entry is written to the store as the
                # list it is and answered by a field as the JSON text it
                # holds: one value, two spellings.
                stated["metadata"] = {
                    key: json.loads(value) if isinstance(one["metadata"].get(key), list) else value
                    for key, value in stated["metadata"].items()
                }
                assert stated == one, f"{category}/{path.name}:{one['name']} drifted from the crate"
                held.append(one["name"])

    # The crate's scalar columns, and the two Map groups it seeds beside them.
    assert sorted(held) == sorted(owned)
    # And the registry is the same one with them or without: the core seeds
    # what they restate, so nothing is added and nothing is lost.
    assert len(fix_registry()) == 7781


def test_the_scheduling_dependencies_are_installed_wherever_they_can_be() -> None:
    """A skipped Airflow suite must not be able to read as a green one.

    `tests/test_rekep_operator.py` opens with `importorskip("airflow")`, so
    dropping the `airflow` group would delete the operator and DAG tests from
    the run without failing anything. This is the one assertion that notices,
    on every platform Airflow supports -- and that the `runner` group the
    operator launches every task under can run `build_dbt`.
    """
    import importlib.util
    import sys

    if sys.platform == "win32":  # pragma: no cover - Airflow is POSIX-only
        pytest.skip("Airflow does not run on Windows")

    for name in (
        "airflow",
        "airflow.providers.standard.hooks.subprocess",
        "airflow.providers.amazon.aws.operators.eks",
        "airflow.providers.cncf.kubernetes.operators.pod",
        "dbt.cli.main",
    ):
        assert importlib.util.find_spec(name) is not None, (
            f"{name} is missing: the operator and DAG tests would silently skip. "
            "Sync the default groups (dev, runner, airflow)."
        )
