"""The retained package surface is importable and native-field based."""

from __future__ import annotations

import importlib
import pathlib
import tomllib
from typing import Annotated

import pytest
from yggdryl import Field as YggdrylField

import rekep
from rekep import Field, scalar
from rekep.fields import (
    PARTITION_KEY,
    PRIMARY_KEY,
    SORT_KEY,
    partition_key,
    primary_key,
    sort_key,
)

PYPROJECT = pathlib.Path(__file__).parent.parent / "pyproject.toml"

PACKAGES = (
    "rekep",
    "rekep.fields",
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
    assert dict(Row.field().field("hour").metadata)[PARTITION_KEY] == "identity"


def test_rekep_reexports_the_native_yggdryl_field() -> None:
    """There is no project Field implementation or compatibility subclass."""
    assert Field is YggdrylField
