"""What a record adds to Airflow's own arguments; no Airflow needed."""

from rekep.airflow import lineage
from rekep.models import Log


def test_no_records_changes_nothing() -> None:
    """A job that declares no lineage gets Airflow's arguments untouched."""
    given = {"tags": ["mine"], "doc_md": "docs"}
    assert lineage.dag_arguments((), (), **given) == given
    assert lineage.task_arguments((), (), **given) == given


def test_lineage_appends_to_the_callers_tags() -> None:
    merged = lineage.dag_arguments([Log], (), tags=["mine"])
    assert set(merged["tags"]) >= {"mine", "Log", "rekep"}


def test_lineage_appends_to_the_callers_docs() -> None:
    merged = lineage.dag_arguments([Log], (), doc_md="# Mine")
    assert merged["doc_md"].startswith("# Mine")
    assert "### Consumes" in merged["doc_md"]


def test_lineage_writes_docs_when_the_caller_has_none() -> None:
    assert lineage.dag_arguments((), [Log])["doc_md"].startswith("### Produces")


def test_a_task_also_gets_inlets_and_outlets(monkeypatch) -> None:
    """Asset construction is swapped out so this runs without Airflow."""
    monkeypatch.setattr(lineage, "asset_of", lineage.asset_uri)
    merged = lineage.task_arguments([Log], [Log], outlets=["existing"])
    assert merged["inlets"] == [lineage.asset_uri(Log)]
    assert merged["outlets"] == ["existing", lineage.asset_uri(Log)]
    assert "doc_md" in merged, "a task carries the lineage docs too"
