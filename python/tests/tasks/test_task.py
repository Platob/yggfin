"""A task is a document: what it says, and what it refuses to say."""

from __future__ import annotations

import pytest

from rekep.tasks import ParseLogs, Task, TaskRun


def test_a_document_says_which_task_it_is() -> None:
    built = Task.from_dict({"kind": "parse_logs", "source": "/logs"})
    assert isinstance(built, ParseLogs)
    assert built.source == "/logs"


def test_a_task_round_trips_through_every_document_format(tmp_path) -> None:
    """The same `from_yaml` that reads a schema contract reads one of these."""
    task = ParseLogs(source="/logs", namespace="captures", limit=10)
    assert Task.from_json(task.into_json()) == task
    assert ParseLogs.from_json(task.into_json()) == task

    document = tmp_path / "parse_logs.yml"
    task.into_yaml(str(document))
    assert Task.from_yaml(str(document)) == task
    assert Task.from_file(str(document)) == task, "and by the extension alone"


def test_the_kind_is_the_class_and_not_a_caller_s_word_for_it() -> None:
    assert ParseLogs(source="/logs").kind == "parse_logs"
    assert ParseLogs(source="/logs").into_dict()["kind"] == "parse_logs"


def test_a_document_with_no_kind_is_refused_by_name() -> None:
    with pytest.raises(ValueError, match="add a `kind`"):
        Task.from_dict({"source": "/logs"})


def test_a_document_naming_an_unknown_kind_says_what_there_is() -> None:
    with pytest.raises(ValueError, match="parse_logs"):
        Task.from_dict({"kind": "nonsense"})


def test_a_subclass_refuses_a_document_for_a_different_task() -> None:
    """Rather than quietly building the wrong task out of the right fields."""
    with pytest.raises(ValueError, match="parse_logs"):
        ParseLogs.from_dict({"kind": "something_else", "source": "/logs"})


def test_a_kind_is_reachable_by_existing_rather_than_by_registering() -> None:
    assert Task.KINDS["parse_logs"] is ParseLogs

    class Nothing(Task):
        KIND = "nothing"

    assert Task.KINDS["nothing"] is Nothing
    assert isinstance(Task.from_dict({"kind": "nothing"}), Nothing)


def test_a_base_task_says_it_does_not_say_what_it_does() -> None:
    with pytest.raises(NotImplementedError, match="does not say what it does"):
        Task().run()


def test_a_report_adds_up_what_it_landed() -> None:
    run = TaskRun(task="t", rows=10, written={"a": 3, "b": 4}, skipped=3, seconds=1.5)
    assert run.landed == 7
    assert run.rows == run.landed + run.skipped, "every row is landed or already stored"


def test_a_report_reads_as_one_line() -> None:
    text = str(TaskRun(task="parse_logs", rows=10, written={"a.b": 7}, skipped=3, seconds=1.25))
    assert "parse_logs" in text and "10 read" in text and "7 written" in text
    assert "3 already stored" in text and "1.25s" in text and "a.b=7" in text


def test_a_report_that_skipped_nothing_does_not_say_so() -> None:
    assert "already stored" not in str(TaskRun(task="t", rows=1, written={"a": 1}))


def test_a_report_round_trips_as_a_document() -> None:
    run = TaskRun(task="t", rows=10, written={"a": 3}, skipped=7, seconds=0.5)
    assert TaskRun.from_json(run.into_json()) == run
