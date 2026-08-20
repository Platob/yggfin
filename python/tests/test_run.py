import datetime
import uuid

from rekep.job import Job
from rekep.run import InputDataset, OutputDataset, Run, RunEvent, RunState


def test_run_id_defaults_to_a_fresh_uuid() -> None:
    run = Run()
    assert uuid.UUID(run.run_id)
    assert Run().run_id != run.run_id


def test_run_event_round_trips_through_json() -> None:
    event = RunEvent(
        event_type=RunState.START,
        event_time=datetime.datetime(2026, 8, 19, tzinfo=datetime.UTC),
        run=Run(run_id="00000000-0000-0000-0000-000000000000"),
        job=Job(name="demo", namespace="trading"),
        outputs=[OutputDataset(namespace="trading", name="orders")],
    )
    assert RunEvent.from_json(event.into_json()) == event


def test_run_event_defaults_carry_producer_and_schema_url() -> None:
    from rekep.run import PRODUCER, SCHEMA_URL

    event = RunEvent(
        event_type=RunState.COMPLETE,
        event_time=datetime.datetime.now(datetime.UTC),
        run=Run(),
        job=Job(name="demo"),
    )
    assert event.producer == PRODUCER
    assert event.schema_url == SCHEMA_URL
    assert event.inputs == []
    assert event.outputs == []


def test_input_and_output_dataset_carry_their_own_facets() -> None:
    inp = InputDataset(namespace="trading", name="orders", input_facets={"inputStatistics": {}})
    out = OutputDataset(namespace="trading", name="orders", output_facets={"outputStatistics": {}})
    assert inp.namespace == out.namespace == "trading"
    assert inp.input_facets == {"inputStatistics": {}}
    assert out.output_facets == {"outputStatistics": {}}
