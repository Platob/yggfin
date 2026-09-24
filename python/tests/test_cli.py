"""The `rekep` command, run in this process so a failure is a traceback."""

import dataclasses
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow
import pytest

from rekep import cli
from rekep.fields import field_of
from rekep.iceberg import iceberg_contract, iceberg_contract_field
from rekep.logs import Stage
from rekep.tasks import TASKS, Task

from .tasks.test_task import replaced


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
    assert written.decode() == f"{iceberg_contract(field_of(Venue))}\n"
    assert json.loads(written)["schema"]["fields"] == [
        {"id": 1, "name": "mic", "type": "string", "required": True},
        {"id": 2, "name": "country", "type": "string", "required": False},
    ]


def test_dump_takes_a_dotted_class_too(capsysbinary: pytest.CaptureFixture) -> None:
    """`module:Attribute` is what an entry point writes; the dot is what a docstring does."""
    assert run("fields", "dump", "--pyclass", "tests.test_cli.Venue") == 0
    written = capsysbinary.readouterr().out.decode()
    assert written == f"{iceberg_contract(field_of(Venue))}\n"


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
    assert target.read_text() == f"{iceberg_contract(field_of(Venue))}\n"
    assert iceberg_contract(iceberg_contract_field(target.read_text(), "Venue")) + "\n" == (
        target.read_text()
    )


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
    dumped = iceberg_contract_field(capsysbinary.readouterr().out.decode(), "Venue")
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
                "schema": {
                    "type": "struct",
                    "fields": [{"id": 1, "name": "x", "type": "string", "required": True}],
                    "schema-id": 0,
                    "identifier-field-ids": [99],
                },
                "partition-spec": {"spec-id": 0, "fields": []},
                "sort-order": {"order-id": 0, "fields": []},
            }
        )
    )
    assert run("fields", "load", "--target", str(broken)) == 1
    assert "Could not find field with id: 99" in capsys.readouterr().err


def test_a_document_in_the_previous_field_format_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """`fields load` no longer routes it; `Field.from_json` still reads declarations."""
    stale = tmp_path / "stale.json"
    stale.write_text(
        json.dumps(
            {
                "name": "Venue",
                "dtype": {
                    "type": "struct",
                    "fields": [{"name": "mic", "dtype": {"type": "utf8"}, "nullable": False}],
                },
                "nullable": False,
            }
        )
    )
    assert run("fields", "load", "--target", str(stale)) == 1
    assert "missing schema, partition-spec, sort-order" in capsys.readouterr().err


def test_a_document_with_an_unknown_type_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    broken = tmp_path / "broken.json"
    broken.write_text(
        json.dumps(
            {
                "schema": {
                    "type": "struct",
                    "fields": [{"id": 1, "name": "x", "type": "int65", "required": True}],
                },
                "partition-spec": {"spec-id": 0, "fields": []},
                "sort-order": {"order-id": 0, "fields": []},
            }
        )
    )
    assert run("fields", "load", "--target", str(broken)) == 1
    assert "int65" in capsys.readouterr().err


def test_a_document_extension_does_not_select_another_codec(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    unknown = tmp_path / "log.txt"
    unknown.write_text(iceberg_contract(field_of(Venue)))
    assert run("fields", "load", "--target", str(unknown)) == 0
    # A contract names no struct, so `load` names it after the file holding it.
    assert "log: 2 columns, builds" in capsys.readouterr().out


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
    assert iceberg_contract_field(target.read_text(), "Venue").into_arrow_schema().names == [
        "mic",
        "country",
    ]
    assert "builds" in capsys.readouterr().out


def test_contract_document_codecs_consume_and_return_text(tmp_path: Path) -> None:
    target = tmp_path / "shape.json"
    shape = field_of(pyarrow.schema([("a", pyarrow.int32())]), "Shape")
    target.write_text(f"{iceberg_contract(shape)}\n")
    rebuilt = iceberg_contract_field(target.read_text(), "Shape")
    assert rebuilt.into_arrow_schema().equals(shape.into_arrow_schema())
    assert f"{iceberg_contract(rebuilt)}\n" == target.read_text()


# -- the bundled tasks -------------------------------------------------------


def test_the_task_list_is_every_bundled_task_as_json(capsys: pytest.CaptureFixture) -> None:
    assert run("tasks", "list") == 0

    assert json.loads(capsys.readouterr().out) == [
        {"name": task.name, "summary": task.summary, "targets": list(task.targets)}
        for task in TASKS
    ]


def test_the_task_help_lists_every_task_with_its_summary(capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as stopped:
        run("tasks", "--help")
    assert stopped.value.code == 0

    # argparse wraps a long summary, so the words are compared, not the lines.
    listed = " ".join(capsys.readouterr().out.split())
    for task in TASKS:
        assert f"{task.name} {' '.join(task.summary.split())}" in listed


@pytest.mark.parametrize("name", [task.name for task in TASKS])
def test_a_task_help_lists_its_defaults(capsys: pytest.CaptureFixture, name: str) -> None:
    with pytest.raises(SystemExit) as stopped:
        run("tasks", name, "--help")
    assert stopped.value.code == 0

    shown = capsys.readouterr().out
    assert all(action in shown for action in ("show", "run", "deploy"))
    for parameter, value in Task(name).parameters.items():
        assert f"\n  {parameter} = {json.dumps(value)}\n" in shown


@pytest.mark.parametrize("name", [task.name for task in TASKS])
def test_show_without_overrides_is_the_shipped_defaults(
    capsys: pytest.CaptureFixture, name: str
) -> None:
    assert run("tasks", name, "show") == 0
    assert json.loads(capsys.readouterr().out) == Task(name).parameters


def test_the_parameters_file_applies_over_the_defaults_and_the_command_line_last(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """A file is what a caller wrote; a `--parameter` is what a person typed."""
    overrides = tmp_path / "parameters.json"
    overrides.write_text(
        json.dumps({"start": "2026-08-13", "filesystem": "file:from-the-file"}), encoding="utf-8"
    )

    assert (
        run(
            "tasks",
            "parse_messages",
            "show",
            "--parameters-file",
            str(overrides),
            "--parameter",
            "start=2026-08-14",
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {
        **Task("parse_messages").parameters,
        "filesystem": "file:from-the-file",
        "start": "2026-08-14",
    }


@pytest.mark.parametrize(
    ("spelled", "read"),
    [
        ("snapshot_id=3", 3),
        ("snapshot_id=null", None),
        ('books="market.books"', "market.books"),
        ("books=market.books", "market.books"),
        ("books=a=b", "a=b"),
        ("start=2026-08-14", "2026-08-14"),
        ("start=", ""),
        ('catalog={"name": "other", "properties": {}}', {"name": "other", "properties": {}}),
    ],
)
def test_a_parameter_is_read_as_json_then_as_text(
    capsys: pytest.CaptureFixture, spelled: str, read: object
) -> None:
    assert run("tasks", "parse_orders", "show", "--parameter", spelled) == 0
    assert json.loads(capsys.readouterr().out)[spelled.partition("=")[0]] == read


@pytest.mark.parametrize("action", ["show", "run", "deploy"])
def test_an_undeclared_parameter_is_one_line_and_runs_nothing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, action: str
) -> None:
    seen = replaced(monkeypatch)

    assert run("tasks", "parse_messages", action, "--parameter", "rows=1") == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.strip().count("\n") == 0
    assert (
        "parse_messages takes no rows; it takes filesystem, rowheader, start, end, catalog"
        in captured.err
    )
    assert seen == []


def test_an_undeclared_name_in_the_parameters_file_is_refused_too(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    overrides = tmp_path / "parameters.json"
    overrides.write_text(json.dumps({"source": "data/capture"}), encoding="utf-8")

    assert run("tasks", "parse_messages", "show", "--parameters-file", str(overrides)) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "parse_messages takes no source" in captured.err


def test_a_parameter_without_a_value_is_refused(capsys: pytest.CaptureFixture) -> None:
    assert run("tasks", "parse_messages", "run", "--parameter", "start") == 1
    assert "a parameter is name=value, not 'start'" in capsys.readouterr().err


def test_a_parameters_file_that_is_not_an_object_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    overrides = tmp_path / "parameters.json"
    overrides.write_text("[1, 2]", encoding="utf-8")

    assert run("tasks", "parse_messages", "show", "--parameters-file", str(overrides)) == 1
    assert "JSON object of parameters" in capsys.readouterr().err


def test_a_run_prints_one_compact_line_and_publishes_the_same_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    seen = replaced(monkeypatch)
    published = tmp_path / "deep" / "result.json"

    assert run("tasks", "parse_messages", "run", "--result-file", str(published)) == 0

    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert captured.out == json.dumps(result, ensure_ascii=False, separators=(",", ":")) + "\n"
    assert published.read_text(encoding="utf-8") == captured.out.strip()
    assert sorted(path.name for path in published.parent.iterdir()) == ["result.json"]
    assert set(result) == set(Stage.KEYS)
    assert (result["task"], result["read"], result["written"]) == ("parse_messages", 1, 1)
    assert seen == [Task("parse_messages").parameters]


def test_a_run_takes_the_parameters_the_command_resolved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    seen = replaced(monkeypatch)
    overrides = tmp_path / "parameters.json"
    overrides.write_text(json.dumps({"start": "2026-08-13", "end": "2026-08-15"}), "utf-8")

    assert (
        run(
            "tasks",
            "parse_messages",
            "run",
            "--parameters-file",
            str(overrides),
            "--parameter",
            "start=2026-08-14",
        )
        == 0
    )
    assert seen == [
        {**Task("parse_messages").parameters, "start": "2026-08-14", "end": "2026-08-15"}
    ]


def test_only_the_result_reaches_stdout(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A task may print anything; the payload is the result alone."""
    replaced(monkeypatch)

    assert run("tasks", "parse_messages", "run") == 0

    captured = capsys.readouterr()
    assert json.loads(captured.out)
    assert "a task writes to stdout" in captured.err
    assert "a task writes to stdout" not in captured.out
    assert "parse_messages finished: 1 read" in captured.err, "and the records are there too"


def test_a_configured_secret_is_never_written_anywhere_a_reader_looks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """The command echoes no parameter: what a task holds stays in the task."""
    seen = replaced(monkeypatch)
    published = tmp_path / "result.json"

    assert (
        run(
            "tasks",
            "parse_messages",
            "run",
            "--parameter",
            "filesystem=s3://key:hunter2@bucket",
            "--result-file",
            str(published),
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "hunter2" not in captured.out
    assert "hunter2" not in captured.err
    assert "hunter2" not in published.read_text(encoding="utf-8")
    assert seen[0]["filesystem"] == "s3://key:hunter2@bucket", "and the task still got it"
    assert "holding 23 characters" in captured.err


@pytest.mark.parametrize(
    ("answer", "reported"),
    [
        (lambda stage: 3, "is a mapping"),
        (lambda stage: {"task": "parse_messages"}, "is missing"),
        (lambda stage: {**stage.finished(read=1, written=1), "read": -1}, "not negative"),
        (lambda stage: {**stage.finished(read=1, written=1), "read": True}, "as an integer"),
        (lambda stage: {**stage.finished(read=1, written=1), "window": {}}, "start and an end"),
        (
            lambda stage: {**stage.finished(read=1, written=1), "targets": {"a": 1}},
            "role=name text",
        ),
    ],
    ids=["scalar", "short", "negative", "boolean", "window", "targets"],
)
def test_a_malformed_result_is_refused_before_it_is_published(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
    answer: Any,
    reported: str,
) -> None:
    replaced(monkeypatch, answer)
    published = tmp_path / "result.json"

    assert run("tasks", "parse_messages", "run", "--result-file", str(published)) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert reported in captured.err
    assert list(tmp_path.iterdir()) == [], "nothing is published when the shape is wrong"


def test_a_result_naming_another_task_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    replaced(monkeypatch, task="parse_fix_raw")

    assert run("tasks", "parse_messages", "run") == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "returned 'parse_fix_raw', not a parse_messages run" in captured.err


def test_a_failing_run_publishes_nothing_and_says_where_it_raised(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    def refused(stage: Stage) -> Any:
        raise RuntimeError("the venue refused")

    replaced(monkeypatch, refused)
    published = tmp_path / "result.json"

    assert run("tasks", "parse_messages", "run", "--result-file", str(published)) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Traceback" in captured.err
    assert "the venue refused" in captured.err
    assert "test_cli.py" in captured.err, "the traceback names the line that raised"
    assert list(tmp_path.iterdir()) == [], "no result and no partial one"


def test_a_published_result_replaces_the_previous_one_whole(tmp_path: Path) -> None:
    """`_publish` writes beside the target and renames, so a reader sees one
    complete document or the one that was there before."""
    published = tmp_path / "deep" / "result.json"

    cli._publish(published, '{"first": 1}')
    cli._publish(published, '{"second": 2}')

    assert json.loads(published.read_text(encoding="utf-8")) == {"second": 2}
    assert sorted(path.name for path in published.parent.iterdir()) == ["result.json"]


@pytest.mark.parametrize(
    "argv",
    [
        ("tasks",),
        ("tasks", "parse_messages"),
        ("tasks", "parse_message", "run"),
        ("tasks", "parse_messages", "deploy", "--table", "logs.messages"),
    ],
    ids=["no-task", "no-action", "unknown", "prefix"],
)
def test_a_command_the_tasks_do_not_take_is_an_argument_error(
    capsys: pytest.CaptureFixture, argv: tuple[str, ...]
) -> None:
    with pytest.raises(SystemExit) as stopped:
        run(*argv)
    assert stopped.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "--help" in captured.err


@pytest.mark.parametrize(
    ("level", "recorded"), [((), True), (("--log-level", "ERROR"), False)], ids=["default", "error"]
)
def test_a_run_records_at_info_unless_the_command_line_says_otherwise(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    level: tuple[str, ...],
    recorded: bool,
) -> None:
    """The runner configures the records, not the task: a task log is read
    afterwards at INFO, and `--log-level` is what a person turns it with."""
    replaced(monkeypatch)

    assert run(*level, "tasks", "parse_messages", "run") == 0

    err = capsys.readouterr().err
    assert ("INFO rekep.logs parse_messages finished" in err) is recorded


@pytest.mark.parametrize("command", [("show",), ("deploy", "--dry-run")], ids=["show", "deploy"])
def test_a_command_that_runs_nothing_records_at_warning(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, command: tuple[str, ...]
) -> None:
    """Only a run is read afterwards at INFO; a person at a terminal reads the
    console, so every other command keeps the package's records to WARNING."""
    import logging

    assert run("tasks", "build_dbt", *command) == 0
    capsys.readouterr()

    assert logging.getLogger("rekep").level == logging.WARNING


@pytest.mark.parametrize(
    ("options", "parameters", "level"),
    [
        ((), (), "INFO"),
        (("--log-level", "ERROR"), (), "ERROR"),
        (("--log-level", "ERROR"), ("--parameter", "log_level=DEBUG"), "DEBUG"),
    ],
    ids=["shipped", "command-line", "parameter"],
)
def test_a_task_declaring_its_level_takes_the_command_line_one(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    options: tuple[str, ...],
    parameters: tuple[str, ...],
    level: str,
) -> None:
    """An explicit `--log-level` beats the shipped `log_level`; the parameter,
    named itself, beats both."""
    seen: list[str] = []

    def stand_in(**taken: Any) -> dict[str, Any]:
        seen.append(taken["log_level"])
        return Stage("optimize_iceberg").finished(read=0, written=0)

    monkeypatch.setattr(Task("optimize_iceberg").module, "run", stand_in)

    assert run(*options, "tasks", "optimize_iceberg", "run", *parameters) == 0
    capsys.readouterr()
    assert seen == [level]


@pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM is a POSIX signal")
def test_sigterm_ends_a_run_as_an_exit_and_closes_what_it_opened(tmp_path: Path) -> None:
    """A pod's container runs `rekep` as its first process, which the kernel
    does not stop for a signal it has no handler for."""
    import signal
    import subprocess
    import time

    started, closed = tmp_path / "started", tmp_path / "closed"
    script = f"""
import time
from rekep import cli
from rekep.tasks import Task

def run(**_):
    try:
        open({str(started)!r}, "w").close()
        time.sleep(60)
    finally:
        open({str(closed)!r}, "w").close()

Task("parse_messages").module.run = run
raise SystemExit(cli.main(["tasks", "parse_messages", "run"]))
"""
    child = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    deadline = time.monotonic() + 30
    while not started.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert started.exists(), "the run never started"
    began = time.monotonic()
    child.send_signal(signal.SIGTERM)

    assert child.wait(timeout=20) == 128 + signal.SIGTERM
    assert time.monotonic() - began < 10
    assert closed.exists(), "the run unwound rather than being killed"


def test_a_command_leaves_the_callers_sigterm_handler_as_it_found_it(
    capsys: pytest.CaptureFixture,
) -> None:
    import signal

    before = signal.getsignal(signal.SIGTERM)
    assert run("tasks", "list") == 0
    capsys.readouterr()
    assert signal.getsignal(signal.SIGTERM) is before
