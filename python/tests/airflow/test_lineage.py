"""Lineage metadata needs no Airflow install; nothing here imports it."""

from rekep import record
from rekep.airflow import lineage
from rekep.airflow.lineage import asset_name, asset_uri, documentation_of, metadata_of, tags_of
from rekep.models import Log
from rekep.records import Record


@record
class Report(Record):
    """A daily report."""

    day: str
    """ISO date the report covers."""


def test_asset_uri_is_stable_and_scheme_qualified() -> None:
    assert asset_uri(Log) == "rekep://rekep.models.log.Log"
    assert asset_uri(Log) == asset_uri(Log)


def test_asset_name_is_the_class_name() -> None:
    assert asset_name(Log) == "Log"


def test_metadata_carries_the_contract() -> None:
    metadata = metadata_of(Log)
    assert metadata["record"] == "rekep.models.log.Log"
    assert metadata["description"] == "One parsed line of a trading log."
    assert "unix: int64" in metadata["fields"]
    assert metadata["rekep_version"]


def test_metadata_is_flat_strings() -> None:
    """Airflow renders asset extras as a flat table; nesting is unreadable there."""
    assert all(isinstance(v, str) for v in metadata_of(Log).values())


def test_tags_cover_both_directions_and_the_scheme() -> None:
    assert tags_of([Log], [Report]) == ["Log", "Report", "rekep"]


def test_tags_deduplicate() -> None:
    assert tags_of([Log], [Log]) == ["Log", "rekep"]


def test_documentation_tables() -> None:
    docs = documentation_of([Log], [Report])
    assert "### Consumes" in docs and "### Produces" in docs
    assert "`Log`" in docs and "`Report`" in docs
    assert "One parsed line of a trading log." in docs


def test_documentation_omits_an_empty_section() -> None:
    docs = documentation_of([], [Report])
    assert "### Consumes" not in docs
    assert "### Produces" in docs


def test_scheme_constant_is_what_the_uris_use() -> None:
    assert asset_uri(Report).startswith(f"{lineage.ASSET_SCHEME}://")
