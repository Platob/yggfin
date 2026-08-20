"""What a record adds to Airflow's own arguments; no Airflow needed."""

from rekep.airflow import lineage
from rekep.models import Log


def test_no_records_derives_nothing() -> None:
    """A task that declares no lineage gets Airflow's arguments as they came,
    bar the tags, which are flattened for Airflow whatever else happens."""
    given = {"doc_md": "docs", "retries": 2}
    assert lineage.dag_arguments((), (), **given) == given
    assert lineage.task_arguments((), (), **given) == given
    assert lineage.dag_arguments((), (), tags={"mine": "yes"}, **given)["tags"] == ["mine=yes"]


def test_lineage_adds_to_the_callers_tags() -> None:
    merged = lineage.dag_arguments([Log], (), tags={"mine": "yes"})
    assert set(merged["tags"]) >= {"mine=yes", "Log=consumes", "generator=rekep"}


def test_a_declared_tag_wins_over_the_derived_one() -> None:
    """An explicit declaration is a decision; a derivation is a default."""
    merged = lineage.dag_arguments([Log], (), tags={"generator": "mine"})
    assert "generator=mine" in merged["tags"]
    assert "generator=rekep" not in merged["tags"]


def test_a_task_gets_no_tags_at_all() -> None:
    """Airflow tags a dag; an operator handed one refuses to parse."""
    assert "tags" not in lineage.task_arguments((), (), tags={"mine": "yes"})


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
