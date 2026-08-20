"""Finding a declared class by name -- what replaced the dotted path."""

from __future__ import annotations

from typing import Any

import pytest

from rekep import classes
from rekep.dag import Dag
from rekep.job import Job, Passthrough
from rekep.models import Log, ParsedMessage
from rekep.records import Record, record


@record
class Widget(Record):
    """Something nothing else declares."""

    name: str
    """Its name."""


# -- names -------------------------------------------------------------------


def test_a_class_is_declared_by_writing_it() -> None:
    """`@record` is the declaration; there is no second list to keep in step."""
    assert classes.find("widget", Record) is Widget


def test_the_class_name_and_its_snake_form_are_one_key() -> None:
    """A class that answered to two spellings would be two things to whoever
    writes them down."""
    assert classes.find("ParsedMessage", Record) is ParsedMessage
    assert classes.find("parsed_message", Record) is ParsedMessage


def test_a_record_is_found_by_its_uri_too() -> None:
    assert classes.find("rekep:///records/log", Record) is Log
    assert Record.locate(str(Log.record_uri())) is Log


def test_a_record_uri_is_the_records_own_name() -> None:
    assert str(Log.record_uri()) == "rekep:///records/log"
    assert Log.record_name() == "log", "the same name the table takes"


# -- what a lookup refuses ---------------------------------------------------


def test_an_undeclared_name_says_what_is_declared() -> None:
    """The cause is almost always a module nobody imported, and a list of
    names is what tells you so."""
    with pytest.raises(KeyError, match="no record named 'nowhere'") as refused:
        classes.find("nowhere", Record)
    assert "Declared: " in str(refused.value)
    assert "log" in str(refused.value)


def test_a_name_declared_as_something_else_says_so() -> None:
    with pytest.raises(TypeError, match="is Log, not a Job"):
        classes.find("log", Job)


def test_a_resource_uri_is_not_a_class_name() -> None:
    """A job's identity names the job, not the class that implements it."""
    with pytest.raises(ValueError, match="not a class"):
        classes.find("rekep:///jobs/pipeline/files_to_logs", Job)


def test_two_classes_of_one_name_are_refused_at_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Declaring both is fine -- two test modules may each write a `Venue`.
    Only whoever looks the name up has a problem, and they are told which
    classes to choose between."""

    class Twin(Record):
        pass

    entries = list(classes.DECLARED.get("widget", []))
    monkeypatch.setitem(classes.DECLARED, "widget", [*entries, Twin])
    with pytest.raises(ValueError, match="more than one record"):
        classes.find("widget", Record)


# -- the bases themselves ----------------------------------------------------


def test_a_job_class_is_found_by_name() -> None:
    assert classes.find("passthrough", Job) is Passthrough
    assert Job.locate("job") is Job


def test_a_dag_class_is_found_by_name() -> None:
    assert Dag.locate("dag") is Dag


def test_declared_lists_a_kind() -> None:
    everything = classes.declared(Record)
    assert Log in everything and Passthrough in everything
    assert classes.declared(Dag) == [Dag]


# -- modules a deployment adds -----------------------------------------------


def test_a_missing_name_imports_the_declared_modules_first(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """Importing is declaring, so `$REKEP_MODULES` is how a deployment's own
    records become findable without a dotted path in every reference."""
    module = tmp_path / "extra_records.py"
    module.write_text(
        "from rekep.records import Record, record\n"
        "\n"
        "@record\n"
        "class Outsider(Record):\n"
        '    """Declared elsewhere."""\n'
        "\n"
        "    name: str\n"
        '    """Its name."""\n',
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv(classes.MODULES_VAR, "extra_records")
    monkeypatch.setattr(classes, "_IMPORTED", set())

    assert classes.find("outsider", Record).record_name() == "outsider"


def test_a_module_that_cannot_be_imported_says_where_it_came_from(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(classes.MODULES_VAR, "no_such_module_anywhere")
    monkeypatch.setattr(classes, "_IMPORTED", set())
    with pytest.raises(ImportError, match=r"REKEP_MODULES"):
        classes.find("nowhere", Record)
