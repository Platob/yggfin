"""Lineage: who is told, and what it costs when nobody is."""

import pytest

from rekep.dataset import Dataset
from rekep.job import Passthrough
from rekep.lineage import Collector, Lineage, LineageClient
from rekep.run import Run, RunState


class Sink:
    """The smallest thing that satisfies the protocol."""

    def __init__(self) -> None:
        self.seen = 0

    def emit(self, event: object) -> None:
        self.seen += 1


# -- the protocol is a duck ------------------------------------------------


def test_anything_with_emit_is_a_client() -> None:
    assert isinstance(Sink(), LineageClient)
    assert isinstance(Collector(), LineageClient)


def test_something_without_emit_is_not() -> None:
    assert not isinstance(object(), LineageClient)


def test_a_client_need_not_be_a_collector() -> None:
    sink = Sink()
    Passthrough(name="p").with_lineage(sink)
    assert Passthrough(name="p").with_lineage(sink).lineage_client() is sink


# -- the boundary ----------------------------------------------------------


def run_of(client: object, **kwargs: object) -> Lineage:
    return Lineage(client=client, job=Passthrough(name="p"), **kwargs)


def test_every_event_of_one_run_shares_its_id() -> None:
    collector = Collector()
    run = run_of(collector)
    run.start()
    run.complete()
    assert len({event.run.run_id for event in collector.events}) == 1


def test_complete_may_carry_different_references_than_start() -> None:
    """A row count is not knowable until the work is done."""
    from rekep.run import OutputDataset

    collector = Collector()
    run = run_of(collector, outputs=[OutputDataset(namespace="n", name="d")])
    run.start()
    run.complete(outputs=[OutputDataset(namespace="n", name="d", output_facets={"rows": 3})])
    assert collector.of(RunState.START)[0].outputs[0].output_facets == {}
    assert collector.of(RunState.COMPLETE)[0].outputs[0].output_facets == {"rows": 3}


def test_a_failure_becomes_an_error_message_facet() -> None:
    collector = Collector()
    run = run_of(collector)
    run.start()
    run.fail(ValueError("no catalog"))
    (failed,) = collector.of(RunState.FAIL)
    assert failed.run.facets["errorMessage"] == {
        "message": "ValueError: no catalog",
        "programmingLanguage": "PYTHON",
    }


def test_a_failure_without_an_exception_carries_no_facet() -> None:
    collector = Collector()
    run = run_of(collector)
    run.fail()
    assert collector.of(RunState.FAIL)[0].run.facets == {}


def test_the_run_id_survives_the_error_facet_being_attached() -> None:
    """`fail` replaces the run to add a facet; it must stay the same run."""
    collector = Collector()
    run = run_of(collector, run=Run())
    before = run.run.run_id
    run.start()
    run.fail(RuntimeError("boom"))
    assert {event.run.run_id for event in collector.events} == {before}


# -- collecting ------------------------------------------------------------


def test_a_collector_keeps_order_and_filters_by_state() -> None:
    collector = Collector()
    run = run_of(collector)
    run.start()
    run.complete()
    assert len(collector) == 2
    assert [event.event_type for event in collector.events] == [
        RunState.START,
        RunState.COMPLETE,
    ]
    assert len(collector.of(RunState.COMPLETE)) == 1


# -- opting out ------------------------------------------------------------


@pytest.mark.parametrize(
    "resource",
    [Dataset(record="rekep.models.Log", name="d"), Passthrough(name="p")],
    ids=["dataset", "job"],
)
def test_nothing_is_bound_until_something_binds_it(resource: object) -> None:
    assert resource.lineage_client() is None


def test_binding_twice_keeps_the_last_client() -> None:
    first, second = Collector(), Collector()
    dataset = Dataset(record="rekep.models.Log", name="d").with_lineage(first)
    assert dataset.with_lineage(second).lineage_client() is second
