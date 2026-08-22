"""What `import rekep` publishes, and the exports nothing inside it calls.

An exported name with no internal caller is not dead -- it is the half of the
API that only a consumer uses -- but it is also the half nothing else here
would notice breaking. So the ones a grep across `src/`, `tests/`, `docs/`,
`schemas/` and `benchmarks/` found no second reference to are pinned here.
"""

from __future__ import annotations

import importlib
import pathlib
import tomllib
from typing import Annotated

import rekep
from rekep import Field, field
from rekep.fields import PARTITION_KEY, PRIMARY_KEY, SORT_KEY
from rekep.fix import BASE_URL, CACHE_DIRECTORY, FixRegistry

PYPROJECT = pathlib.Path(__file__).parent.parent / "pyproject.toml"


def test_the_package_version_is_the_one_the_build_publishes() -> None:
    """Two spellings of one number, which is exactly how they drift apart."""
    declared = tomllib.loads(PYPROJECT.read_text())["project"]["version"]
    assert rekep.__version__ == declared


def test_everything_exported_is_importable() -> None:
    """`__all__` is a promise; a name that moved without it is an ImportError."""
    for name in rekep.__all__:
        assert hasattr(rekep, name), f"rekep.__all__ names {name!r}, which is not there"


@field
class Row:
    """One row, declaring each protocol key exactly once."""

    unix: Annotated[int, Field.primary_key(), Field.sort_key()]
    """When."""

    hour: Annotated[int, Field.partition_key()]
    """Which hour."""


def test_the_protocol_keys_are_the_ones_a_declaration_writes() -> None:
    """The published spelling of a key, against the one a field actually stores.

    `SORT_KEY` is exported and referenced nowhere else in this repository, so
    a rename of the metadata key would leave the constant behind, still
    exported, still wrong, and nothing would fail.
    """
    metadata = dict(Row.FIELD.field("unix").metadata)
    assert metadata[SORT_KEY] == "asc"
    assert metadata[PRIMARY_KEY] == "true"
    assert dict(Row.FIELD.field("hour").metadata)[PARTITION_KEY] == "identity"


def test_the_registry_defaults_are_the_exported_ones() -> None:
    """Both are exported and referenced nowhere else; they are the defaults."""
    registry = FixRegistry()
    assert registry.base_url == BASE_URL.rstrip("/")
    assert str(registry.cache_dir) == str(pathlib.Path(CACHE_DIRECTORY))


def test_every_submodule_imports_on_its_own() -> None:
    """A module reachable only through `rekep/__init__` still has to import."""
    for name in ("rekep.fix", "rekep.logs", "rekep.market", "rekep.tasks", "rekep.fields"):
        assert importlib.import_module(name) is not None
