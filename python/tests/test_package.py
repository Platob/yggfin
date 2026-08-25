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

import pytest

import rekep
import rekep.fix.columns
import rekep.fix.rules
import rekep.market.fix
from rekep import Field, FixMsg, scalar
from rekep.fields import PARTITION_KEY, PRIMARY_KEY, SORT_KEY
from rekep.fix import (
    BASE_URL,
    CACHE_DIRECTORY,
    CODECS,
    COMMON,
    FLAT,
    NO_PROTOCOL,
    QUOTE,
    SESSION,
    FixRegistry,
    Rule,
    Rules,
)

PYPROJECT = pathlib.Path(__file__).parent.parent / "pyproject.toml"

#: Every package here that publishes an `__all__`. A subpackage's is as much a
#: promise as the root's, and it is the one a `from rekep.fix import ...` reads
#: -- so a module reachable only through an `__init__` still has to import.
PACKAGES = (
    "rekep",
    "rekep.enums",
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
    """The old decorator spelling must not linger as a compatibility API."""
    assert callable(scalar)
    assert "scalar" in rekep.__all__
    assert "field" not in rekep.__all__
    assert not hasattr(rekep, "field")


@scalar
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
    metadata = dict(Row.into_field().field("unix").metadata)
    assert metadata[SORT_KEY] == "asc"
    assert metadata[PRIMARY_KEY] == "true"
    assert dict(Row.into_field().field("hour").metadata)[PARTITION_KEY] == "identity"


def test_the_registry_defaults_are_the_exported_ones() -> None:
    """Both are exported and referenced nowhere else; they are the defaults."""
    registry = FixRegistry()
    assert registry.base_url == BASE_URL.rstrip("/")
    assert str(registry.cache_dir) == str(pathlib.Path(CACHE_DIRECTORY))


# -- the FIX surface a consumer reads a parsed log through --------------------


def test_the_published_column_list_is_the_one_the_parser_lifts_by() -> None:
    """`FLAT` is exported and referenced nowhere else here, because the package
    lifts tags through `columns.COLUMNS` instead -- so a tag that moved in one
    and not the other would hand every consumer a column name the table does
    not have, and nothing inside would notice.
    """
    assert dict(FLAT) == dict(rekep.fix.columns.COLUMNS)
    assert (len(SESSION), len(COMMON), len(QUOTE), len(FLAT)) == (33, 26, 18, 77)
    assert len(dict(FLAT)) == len(FLAT), "one tag, one column"


def test_every_codec_a_rule_may_name_is_one_the_parser_reads() -> None:
    """`CODECS` is exported and referenced nowhere else, so a fourth reading
    added to the parser would leave the published list short -- and a rule
    document naming the new one would validate against a list that never grew.
    """
    assert set(CODECS) == set(rekep.fix.rules.CODEC_KEYS)
    assert {rule.codec for rule in Rules().rules} <= set(CODECS)


def test_the_name_for_no_protocol_is_one_constant_and_not_three() -> None:
    """`OTHER` is the fall-through rule's protocol, a parsed line's default and
    the value the `protocol_code` column holds for most of a capture. Spelled out at
    each of those it would be three constants, and only one of them published.
    """
    assert Rule().protocol == NO_PROTOCOL
    assert FixMsg().protocol_code == NO_PROTOCOL
    assert Rules().rule("nothing declares this").protocol == NO_PROTOCOL


def test_the_reader_of_a_fix_timestamp_is_one_function_under_every_spelling() -> None:
    """`unix_of` reads a FIX timestamp, so it lives with the datatypes; it is
    published from `rekep.market` as well, because a caller reading market
    events has no reason to import the protocol. A second copy behind one of
    those spellings would be two answers to "when", agreeing until they did
    not.
    """
    assert rekep.fix.unix_of is rekep.fix.fields.unix_of
    assert rekep.market.unix_of is rekep.fix.fields.unix_of
    assert rekep.market.fix.unix_of is rekep.fix.fields.unix_of
