"""The `rekep` command, run in this process so a failure is a traceback."""

import dataclasses
import json
from pathlib import Path

import pyarrow
import pytest

from rekep import Field, cli
from rekep.fields import field_of


def run(*argv: str) -> int:
    return cli.main(list(argv))


def test_help_is_hierarchical_and_argument_errors_are_concise(
    capsys: pytest.CaptureFixture,
) -> None:
    with pytest.raises(SystemExit) as stopped:
        run("fields", "load")
    assert stopped.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "target" in captured.err
    assert "rekep fields load --help" in captured.err

    with pytest.raises(SystemExit) as stopped:
        run("fields", "--help")
    assert stopped.value.code == 0
    help_text = capsys.readouterr().out
    assert "COMMAND" in help_text and "dump" in help_text and "load" in help_text


def test_version_is_available_without_entering_a_command(capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as stopped:
        run("--version")
    assert stopped.value.code == 0
    assert cli.__version__ in capsys.readouterr().out


# -- dumping ----------------------------------------------------------------


def test_dump_writes_the_declaration_to_stdout(capsysbinary: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "tests.test_cli:Venue") == 0
    written = capsysbinary.readouterr().out
    assert written.decode() == f"{field_of(Venue).into_json(indent=2)}\n"
    assert Field.from_json(written.decode()) == field_of(Venue)


def test_dump_takes_a_dotted_class_too(capsysbinary: pytest.CaptureFixture) -> None:
    """`module:Attribute` is what an entry point writes; the dot is what a docstring does."""
    assert run("fields", "dump", "--pyclass", "tests.test_cli.Venue") == 0
    assert Field.from_json(capsysbinary.readouterr().out.decode()) == field_of(Venue)


def test_dump_writes_json_to_the_target(tmp_path: Path) -> None:
    target = tmp_path / "log.json"
    assert (
        run(
            "fields",
            "dump",
            "--pyclass",
            "tests.test_cli:Venue",
            "--target",
            str(target),
        )
        == 0
    )
    assert target.read_text() == f"{field_of(Venue).into_json(indent=2)}\n"
    assert Field.from_json(target.read_text()) == field_of(Venue)


def test_only_the_document_reaches_stdout(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """So a dump with no target pipes, and one with a target says where it went."""
    target = tmp_path / "venue.json"
    assert (
        run(
            "fields",
            "dump",
            "--pyclass",
            "tests.test_cli:Venue",
            "--target",
            str(target),
        )
        == 0
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert str(target) in captured.err


def test_dump_takes_a_plain_dataclass(capsysbinary: pytest.CaptureFixture) -> None:
    """The CLI projects an undecorated dataclass through the same field adapter."""
    assert run("fields", "dump", "--pyclass", "tests.test_cli:Venue") == 0
    dumped = Field.from_json(capsysbinary.readouterr().out.decode())
    assert [member.name for member in dumped] == ["mic", "country"]
    assert dumped.field("country").nullable is True


@dataclasses.dataclass
class Venue:
    """A venue, declared without the decorator."""

    mic: str
    country: str | None = None


NOT_A_FIELD: dict[str, object] = {}


def test_a_class_that_is_not_a_shape_is_refused(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "tests.test_cli:NOT_A_FIELD") == 1
    assert "does not name a field" in capsys.readouterr().err


def test_a_missing_attribute_names_the_module(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "rekep.cli:Nothing") == 1
    assert "has no 'Nothing'" in capsys.readouterr().err


def test_a_missing_module_is_reported(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "nowhere.at.all:Shape") == 1
    assert "nowhere" in capsys.readouterr().err


def test_a_spec_that_names_no_class_says_how_to_write_one(capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "dump", "--pyclass", "Venue") == 1
    assert "module:Attribute" in capsys.readouterr().err


# -- loading ----------------------------------------------------------------


def test_a_document_that_does_not_build_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """Parsing is not the check -- building is."""
    broken = tmp_path / "broken.json"
    broken.write_text(
        json.dumps(
            {
                "name": "Broken",
                "dtype": {"type": "struct", "fields": [{"name": "x"}]},
                "nullable": False,
            }
        )
    )
    assert run("fields", "load", "--target", str(broken)) == 1
    assert "missing field `dtype`" in capsys.readouterr().err


def test_a_document_with_an_unknown_type_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    broken = tmp_path / "broken.json"
    broken.write_text(
        json.dumps(
            {
                "name": "Broken",
                "dtype": {
                    "type": "struct",
                    "fields": [{"name": "x", "dtype": {"type": "int65"}, "nullable": False}],
                },
                "nullable": False,
            }
        )
    )
    assert run("fields", "load", "--target", str(broken)) == 1
    assert "int65" in capsys.readouterr().err


def test_a_document_extension_does_not_select_another_codec(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    unknown = tmp_path / "log.txt"
    unknown.write_text(field_of(Venue).into_json())
    assert run("fields", "load", "--target", str(unknown)) == 0
    assert "Venue: 2 columns, builds" in capsys.readouterr().out


def test_a_missing_document_is_reported(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    assert run("fields", "load", "--target", str(tmp_path / "nowhere.json")) == 1
    assert "nowhere.json" in capsys.readouterr().err


# -- the round trip the CLI exists for --------------------------------------


def test_dump_then_load_is_the_contract_workflow(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """What CI runs: publish the declaration, then check the file builds."""
    target = tmp_path / "venue.json"
    assert (
        run(
            "fields",
            "dump",
            "--pyclass",
            "tests.test_cli:Venue",
            "--target",
            str(target),
        )
        == 0
    )
    assert run("fields", "load", "--target", str(target)) == 0
    assert Field.from_json(target.read_text()) == field_of(Venue)
    assert "builds" in capsys.readouterr().out


def test_native_document_codecs_consume_and_return_text(tmp_path: Path) -> None:
    target = tmp_path / "shape.json"
    shape = field_of(pyarrow.schema([("a", pyarrow.int32())]), "Shape")
    target.write_text(shape.into_json())
    assert Field.from_json(target.read_text()) == shape


# -- running a task ----------------------------------------------------------

#: A task application small enough to read, carrying everything the runner
#: contracts on: definitions from the document, a record on `stderr`, a print
#: on `stdout` that must not reach the payload, and a `Stage` result.
APPLICATION = """
import marimo

app = marimo.App()

with app.setup:
    import pathlib

    from rekep.logs import Stage, configure
    from rekep.tasks import Task


@app.cell
def parameters():
    _defaults = Task.from_json(str(pathlib.Path(__file__).with_suffix(".json"))).parameters
    source = _defaults["source"]
    rows = _defaults["rows"]
    secret = _defaults["secret"]
    log_level = _defaults["log_level"]
    return log_level, rows, secret, source


@app.cell
def _(log_level):
    records = configure(log_level)
    return (records,)


@app.cell
def _(records, rows, secret, source):
    print("an application writes to stdout")
    stage = Stage("sample", sources={"input": source}, targets={"output": "out.rows"})
    stage.says("holding %d characters of configuration", len(secret))
    result = stage.finished(read=rows, written=rows, rows=rows)
    return (result,)
"""

DOCUMENT = json.dumps(
    {
        "name": "sample",
        "application": "sample.py",
        "parameters": {
            "source": "data/capture",
            "rows": 1,
            "secret": "unset",
            "log_level": "INFO",
        },
    }
)


def task(tmp_path: Path, application: str = APPLICATION, document: str = DOCUMENT) -> str:
    """One task directory, as the repository lays them out."""
    directory = tmp_path / "sample"
    directory.mkdir()
    (directory / "sample.py").write_text(application, encoding="utf-8")
    (directory / "sample.json").write_text(document, encoding="utf-8")
    return str(directory / "sample.json")


def test_a_task_run_publishes_the_same_document_to_stdout_and_the_result_file(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    published = tmp_path / "result.json"

    assert run("task", "run", task(tmp_path), "--result-file", str(published)) == 0

    captured = capsys.readouterr()
    assert json.loads(captured.out) == json.loads(published.read_text(encoding="utf-8"))
    assert captured.out.count("\n") == 1, "one line, so a pipeline reads one document"
    assert published.read_text(encoding="utf-8") == captured.out.strip()


def test_the_result_carries_every_key_a_route_reads(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    assert run("task", "run", task(tmp_path)) == 0

    result = json.loads(capsys.readouterr().out)
    assert set(result) == {
        "task",
        "read",
        "written",
        "skipped",
        "sources",
        "targets",
        "window",
        "elapsed_ms",
        "rows",
    }
    assert result["task"] == "sample"
    assert (result["read"], result["written"], result["skipped"]) == (1, 1, 0)
    assert result["window"] == {"start": None, "end": None}


def test_only_the_result_reaches_stdout(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """An application may print anything; the payload is the document alone."""
    assert run("task", "run", task(tmp_path)) == 0

    captured = capsys.readouterr()
    assert json.loads(captured.out)
    assert "an application writes to stdout" in captured.err
    assert "an application writes to stdout" not in captured.out
    assert "sample finished: 1 read" in captured.err, "and the records are there too"


def test_both_override_forms_apply_with_the_command_line_last(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """A document is what a caller wrote; a `--parameter` is what a person typed."""
    document = task(tmp_path)
    overrides = tmp_path / "parameters.json"
    overrides.write_text(json.dumps({"rows": 2, "source": "from-the-file"}), encoding="utf-8")

    assert (
        run(
            "task",
            "run",
            document,
            "--parameters-file",
            str(overrides),
            "--parameter",
            "rows=3",
        )
        == 0
    )

    result = json.loads(capsys.readouterr().out)
    assert result["read"] == 3, "the command line wins over the file"
    assert result["sources"] == {"input": "from-the-file"}, "which wins over the document"


def test_a_parameter_is_read_as_json_then_as_text(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    assert run("task", "run", task(tmp_path), "--parameter", "rows=4") == 0
    assert json.loads(capsys.readouterr().out)["rows"] == 4

    again = tmp_path / "again"
    again.mkdir()
    assert run("task", "run", task(again), "--parameter", "source=data/two") == 0
    assert json.loads(capsys.readouterr().out)["sources"] == {"input": "data/two"}


def test_a_parameter_without_a_value_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    assert run("task", "run", task(tmp_path), "--parameter", "rows") == 1
    assert "name=value" in capsys.readouterr().err


def test_a_configured_secret_is_never_written_anywhere_a_reader_looks(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """The runner echoes no parameter: what a task holds stays in the task."""
    published = tmp_path / "result.json"

    assert (
        run(
            "task",
            "run",
            task(tmp_path),
            "--parameter",
            'secret="s3://key:hunter2@bucket"',
            "--result-file",
            str(published),
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "hunter2" not in captured.out
    assert "hunter2" not in captured.err
    assert "hunter2" not in published.read_text(encoding="utf-8")
    assert "holding 23 characters" in captured.err, "and the task still got it"


def test_an_application_exporting_no_app_is_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    assert run("task", "run", task(tmp_path, application="version = 1\n")) == 1
    assert "exports no marimo app" in capsys.readouterr().err


def test_an_application_publishing_no_result_is_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    application = APPLICATION.replace("    return (result,)", "    return")
    application = application.replace("    result = stage.finished", "    stage.finished")

    assert run("task", "run", task(tmp_path, application=application)) == 1
    assert "defines no result" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("published", "reported"),
    [
        ("3", "is a mapping"),
        ('{"task": "sample"}', "is missing"),
        ('{**stage.finished(read=1, written=1), "read": -1}', "not negative"),
        ('{**stage.finished(read=1, written=1), "read": True}', "as an integer"),
        ('{**stage.finished(read=1, written=1), "window": {}}', "start and an end"),
        ('{**stage.finished(read=1, written=1), "targets": {"a": 1}}', "role=name text"),
    ],
    ids=["scalar", "short", "negative", "boolean", "window", "targets"],
)
def test_a_malformed_result_is_refused_before_it_is_published(
    tmp_path: Path, capsys: pytest.CaptureFixture, published: str, reported: str
) -> None:
    application = APPLICATION.replace(
        "result = stage.finished(read=rows, written=rows, rows=rows)", f"result = {published}"
    )
    target = tmp_path / "result.json"

    assert (
        run("task", "run", task(tmp_path, application=application), "--result-file", str(target))
        == 1
    )
    assert reported in capsys.readouterr().err
    assert not target.exists(), "nothing is published when the shape is wrong"


def test_a_result_naming_another_task_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    application = APPLICATION.replace('Stage("sample"', 'Stage("other"')

    assert run("task", "run", task(tmp_path, application=application)) == 1
    assert "returned 'other', not a sample run" in capsys.readouterr().err


def test_one_document_may_run_under_a_discriminated_name(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    application = APPLICATION.replace('Stage("sample"', 'Stage("sample_replay"')

    assert run("task", "run", task(tmp_path, application=application)) == 0
    assert json.loads(capsys.readouterr().out)["task"] == "sample_replay"


def test_a_failing_cell_leaves_no_result_and_says_where_it_raised(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    application = APPLICATION.replace(
        "    stage = Stage(", "    raise RuntimeError('the venue refused')\n    stage = Stage("
    )
    published = tmp_path / "result.json"

    assert (
        run("task", "run", task(tmp_path, application=application), "--result-file", str(published))
        == 1
    )

    captured = capsys.readouterr()
    assert "the venue refused" in captured.err
    assert "sample.py" in captured.err, "the traceback names the line that raised"
    assert not published.exists()
    assert list(published.parent.glob("*.partial")) == []


def test_a_published_result_replaces_the_previous_one_whole(tmp_path: Path) -> None:
    """`_publish` writes beside the target and renames, so a reader sees one
    complete document or the one that was there before."""
    published = tmp_path / "deep" / "result.json"

    cli._publish(published, '{"first": 1}')
    cli._publish(published, '{"second": 2}')

    assert json.loads(published.read_text(encoding="utf-8")) == {"second": 2}
    assert sorted(path.name for path in published.parent.iterdir()) == ["result.json"]


def test_a_parameters_file_that_is_not_an_object_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    overrides = tmp_path / "parameters.json"
    overrides.write_text("[1, 2]", encoding="utf-8")

    assert run("task", "run", task(tmp_path), "--parameters-file", str(overrides)) == 1
    assert "JSON object of parameters" in capsys.readouterr().err
