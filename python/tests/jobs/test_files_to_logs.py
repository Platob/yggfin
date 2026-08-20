import pathlib

from rekep.jobs import FilesToLogs
from rekep.models import Log, ParsedMessage

SAMPLE = pathlib.Path(__file__).parent.parent / "data" / "app_sample.txt"


def test_produces_defaults_to_log() -> None:
    job = FilesToLogs(uri="job:/f2l")
    assert job.produces == ["rekep.models.Log"]
    assert job.produced_records() == [Log]


def test_produces_override_wins() -> None:
    job = FilesToLogs(uri="job:/f2l", produces=["rekep.models.ParsedMessage"])
    assert job.produced_records() == [ParsedMessage]


def test_arrow_transform_is_the_identity() -> None:
    import pyarrow

    batch = pyarrow.RecordBatch.from_pydict({"a": [1, 2, 3]})
    (out,) = list(FilesToLogs(uri="job:/f2l").arrow_transform(iter([batch])))
    assert out is batch


def test_run_parses_the_sample_file() -> None:
    job = FilesToLogs(uri="job:/f2l", source=SAMPLE.as_uri())
    assert job.run() == 24


def test_a_run_event_reports_log_as_the_output() -> None:
    from rekep.run import RunState

    job = FilesToLogs(uri="job:/pipeline/f2l", source=SAMPLE.as_uri())
    event = job.into_run_event(RunState.COMPLETE)
    assert event.outputs[0].namespace == "pipeline"
    assert event.outputs[0].name == "log"


def test_is_a_record_and_round_trips() -> None:
    job = FilesToLogs(uri="job:/f2l", schedule="@daily", source=SAMPLE.as_uri())
    assert FilesToLogs.from_json(job.into_json()) == job
