"""The merge logic the decorators are made of; no Airflow needed."""

from rekep.airflow.decorators import _with_assets, _with_lineage
from rekep.models import Log


def test_no_records_changes_nothing() -> None:
    kwargs = {"tags": ["mine"], "doc_md": "docs"}
    assert _with_lineage(kwargs, (), (), docs_key="doc_md") is kwargs
    assert _with_assets(kwargs, (), ()) is kwargs


def test_lineage_appends_to_the_callers_tags() -> None:
    merged = _with_lineage({"tags": ["mine"]}, [Log], (), docs_key="doc_md")
    assert set(merged["tags"]) >= {"mine", "Log", "rekep"}


def test_lineage_appends_to_the_callers_docs() -> None:
    merged = _with_lineage({"doc_md": "# Mine"}, [Log], (), docs_key="doc_md")
    assert merged["doc_md"].startswith("# Mine")
    assert "### Consumes" in merged["doc_md"]


def test_lineage_writes_docs_when_the_caller_has_none() -> None:
    merged = _with_lineage({}, (), [Log], docs_key="doc_md")
    assert merged["doc_md"].startswith("### Produces")


def test_the_callers_kwargs_are_not_mutated() -> None:
    kwargs = {"tags": ["mine"]}
    _with_lineage(kwargs, [Log], (), docs_key="doc_md")
    assert kwargs == {"tags": ["mine"]}


def test_assets_extend_inlets_and_outlets(monkeypatch) -> None:
    """Asset construction is swapped out so this runs without Airflow."""
    from rekep.airflow import lineage

    monkeypatch.setattr(lineage, "asset_of", lineage.asset_uri)
    merged = _with_assets({"outlets": ["existing"]}, [Log], [Log])
    assert merged["inlets"] == [lineage.asset_uri(Log)]
    assert merged["outlets"] == ["existing", lineage.asset_uri(Log)]
