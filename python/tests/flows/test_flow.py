import json
import pathlib
from collections.abc import Iterator

import pyarrow
import pytest

import rekep.flows
from rekep.flows import Flow, Passthrough, load, load_all
from rekep.models import Log
from rekep.records import record

SAMPLE = pathlib.Path(__file__).parent.parent / "data" / "app_sample.txt"
REPO_FLOWS = pathlib.Path(__file__).parents[3] / "stacks" / "flows"


@record
class Doubler(Flow):
    """Every batch twice, for telling transform output from input."""

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        for batch in batches:
            yield batch
            yield batch


# -- the class --------------------------------------------------------------


def test_flow_is_abstract() -> None:
    with pytest.raises(TypeError, match="abstract"):
        Flow(name="nope")


def test_passthrough_is_the_identity() -> None:
    batch = pyarrow.RecordBatch.from_pydict({"a": [1, 2, 3]})
    (out,) = list(Passthrough(name="p").arrow_transform(iter([batch])))
    assert out is batch


def test_flow_is_a_record() -> None:
    flow = Passthrough(name="p", schedule="@daily", consumes=["rekep.models.Log"])
    assert Passthrough.from_json(flow.into_json()) == flow


def test_lineage_paths_resolve_to_record_classes() -> None:
    flow = Passthrough(name="p", produces=["rekep.models.Log"])
    assert flow.produced_records() == [Log]
    assert flow.consumed_records() == []


def test_a_non_record_lineage_path_is_refused() -> None:
    flow = Passthrough(name="p", consumes=["pathlib.Path"])
    with pytest.raises(TypeError, match="not a Record"):
        flow.consumed_records()


# -- run --------------------------------------------------------------------


def test_run_extracts_transforms_and_counts() -> None:
    flow = Passthrough(name="p", source=SAMPLE.as_uri())
    assert flow.run() == 24


def test_transform_output_is_what_load_sees() -> None:
    assert Doubler(name="d", source=SAMPLE.as_uri()).run() == 48


def test_run_without_a_source_says_what_to_override() -> None:
    with pytest.raises(NotImplementedError, match="override extract"):
        Passthrough(name="p").run()


# -- side files -------------------------------------------------------------


def test_load_builds_the_declared_class(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "flow.json"
    path.write_text(json.dumps({"flow": "rekep.flows.Passthrough", "name": "j"}))
    flow = load(path)
    assert isinstance(flow, Passthrough)
    assert flow.name == "j"


def test_load_renders_jinja_with_the_environment(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BUCKET", "s3://lake")
    path = tmp_path / "flow.yaml"
    path.write_text('flow: rekep.flows.Passthrough\nname: y\nsource: "{{ env.BUCKET }}/app.txt"\n')
    assert load(path).source == "s3://lake/app.txt"


def test_load_passes_extra_context(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "flow.yaml"
    path.write_text('flow: rekep.flows.Passthrough\nname: "{{ suffix }}"\n')
    assert load(path, suffix="rendered").name == "rendered"


def test_load_requires_a_flow_key(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "flow.yaml"
    path.write_text("name: anonymous\n")
    with pytest.raises(ValueError, match="declares no"):
        load(path)


def test_load_refuses_a_non_flow_class(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "flow.yaml"
    path.write_text("flow: rekep.models.Log\nname: x\n")
    with pytest.raises(TypeError, match="not a Flow subclass"):
        load(path)


def test_load_refuses_the_abstract_base(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "flow.yaml"
    path.write_text("flow: rekep.flows.Flow\nname: x\n")
    with pytest.raises(TypeError, match="abstract"):
        load(path)


def test_load_all_reads_a_directory(tmp_path: pathlib.Path) -> None:
    (tmp_path / "b.yaml").write_text("flow: rekep.flows.Passthrough\nname: b\n")
    (tmp_path / "a.json").write_text(json.dumps({"flow": "rekep.flows.Passthrough", "name": "a"}))
    (tmp_path / "notes.txt").write_text("not a flow")
    flows = load_all(tmp_path)
    assert [flow.name for flow in flows] == ["a", "b"], "sorted, and .txt ignored"


def test_the_shipped_side_files_load(monkeypatch: pytest.MonkeyPatch) -> None:
    """Whatever is committed under stacks/flows must actually parse."""
    monkeypatch.setenv("REKEP_SOURCE_URL", "file:///dev/null")
    flows = load_all(REPO_FLOWS)
    assert flows, "stacks/flows has no side files"
    assert all(isinstance(flow, rekep.flows.Flow) for flow in flows)
