"""Run ingestion tasks and manage their Iceberg table contracts."""

from __future__ import annotations

import argparse
import contextlib
import importlib
import inspect
import json
import os
import pathlib
import signal
import sys
import threading
import traceback
import urllib.parse
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from rekep import __version__
from rekep.console import Console
from rekep.fields import Field, field_of
from rekep.iceberg import (
    iceberg_contract,
    iceberg_contract_field,
    partition_keys,
    primary_keys,
)
from rekep.logs import COMMAND_LEVEL, TASK_LEVEL, configure
from rekep.resources import read_bytes, resource
from rekep.tasks import TASKS, Task

CONSOLE = Console(stream="stderr")


class CommandFormatter(argparse.RawDescriptionHelpFormatter):
    """Compact command help with scannable section names."""

    def __init__(self, prog: str) -> None:
        super().__init__(prog, max_help_position=30)

    def start_section(self, heading: str | None) -> None:
        super().start_section(heading.upper() if heading else heading)


class CommandParser(argparse.ArgumentParser):
    """An argument parser whose failures use the shared terminal console."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("formatter_class", CommandFormatter)
        # An option is spelled whole: a prefix such as `--table` would
        # otherwise be read as `--table-property`.
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)

    def error(self, message: str) -> None:
        CONSOLE.fail(message)
        CONSOLE.note(f"run `{self.prog} --help`")
        raise SystemExit(2)


def main(argv: list[str] | None = None) -> int:
    """Run one command; return the exit code rather than raising it."""
    parser = _parser()
    arguments = parser.parse_args(argv)
    configure(arguments.log_level or COMMAND_LEVEL)
    with _terminable():
        try:
            return arguments.run(arguments)
        except (
            AttributeError,
            ImportError,
            KeyError,
            OSError,
            TypeError,
            ValueError,
        ) as error:
            CONSOLE.fail(f"{type(error).__name__}: {error}")
            return 1


@contextlib.contextmanager
def _terminable() -> Iterator[None]:
    """SIGTERM ends the command as an exit, so what it opened is closed.

    A pod's container runs the command as its first process, which a signal
    it installs no handler for does not stop: deleting the pod would wait out
    its grace period and kill it. Scoped to the command, so a caller of `main`
    keeps its own handler.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.signal(signal.SIGTERM, _terminated)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _terminated(signum: int, frame: Any) -> None:
    raise SystemExit(128 + signum)


def dump(arguments: argparse.Namespace) -> int:
    """Write a declared table contract as a document.

    A contract is answered by a class's own field, or by a function that
    builds one: the FIX row is the second, because no class declares it --
    it is the dictionary's row with the capture's own columns in front.
    """
    held = _imported(arguments.pyclass)
    shape = field_of(held() if inspect.isfunction(held) else held)
    payload = f"{iceberg_contract(shape)}\n"
    if arguments.target:
        output = resource(arguments.target)
        try:
            output.write_bytes(payload.encode())
        finally:
            output.close()
        CONSOLE.ok(f"{arguments.pyclass} {CONSOLE.glyph('arrow')} {arguments.target}")
        return 0
    sys.stdout.write(payload)
    return 0


def load(arguments: argparse.Namespace) -> int:
    """Read and validate a table contract document."""
    document = read_bytes(arguments.target).decode()
    # A contract names no struct -- the catalog owns a table's name -- so the
    # file that holds it does, and its stem is what a reader already calls it.
    shape = iceberg_contract_field(document, _stem(arguments.target))
    schema = shape.into_arrow_schema()
    print(f"{shape.name or '<unnamed>'}: {len(schema.names)} columns, builds")
    for member in shape:
        print(f"  {member.name}: {member.dtype.into_arrow()}{_marks(member)}")
    print(f"  primary keys: {primary_keys(shape) or '-'}")
    print(f"  partition keys: {partition_keys(shape) or '-'}")
    return 0


def _stem(target: str) -> str:
    """The name a target spells, whether it is a path or a URI.

    A presigned URL carries slashes in its query; only the path names the file.
    """
    spelled = urllib.parse.urlsplit(target).path or target
    return pathlib.PurePosixPath(spelled.replace("\\", "/").rstrip("/")).stem


def _marks(member: Field) -> str:
    """What a column is besides its type."""
    marks = []
    metadata = member.iceberg
    if str(metadata.get("primary_key") or "").casefold() == "true":
        marks.append("primary key")
    if member.is_partition:
        marks.append("partition identity")
    elif (partition := str(metadata.get("partition_key") or "")) and (
        partition.casefold() != "false"
    ):
        marks.append(f"partition {partition}")
    if field_id := metadata.get("field_id"):
        marks.append(f"id {field_id}")
    if member.nullable:
        marks.append("nullable")
    return f"  [{', '.join(marks)}]" if marks else ""


def _imported(spec: str) -> Any:
    """Return the class or field named by ``module:Attribute``."""
    module_name, separator, attribute = spec.partition(":")
    if not separator:
        module_name, _, attribute = spec.rpartition(".")
    if not module_name or not attribute:
        raise ValueError(
            f"{spec!r} does not name a class: write it as module:Attribute, "
            "for instance rekep.text:Message"
        )
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attribute)
    except AttributeError as error:
        raise AttributeError(f"{module_name} has no {attribute!r}") from error


def _write_json(document: Any) -> None:
    """Write one machine-readable command result to stdout."""
    json.dump(document, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def list_tasks(arguments: argparse.Namespace) -> int:
    """Write every bundled task, in the order the graph runs them."""
    _write_json(
        [
            {"name": task.name, "summary": task.summary, "targets": list(task.targets)}
            for task in TASKS
        ]
    )
    return 0


def show_task(arguments: argparse.Namespace) -> int:
    """Write the parameters a run of one task would take."""
    _write_json(Task(arguments.task).resolved(_overrides(arguments)))
    return 0


def run_task(arguments: argparse.Namespace) -> int:
    """Run one bundled task in this process and write its result."""
    task = Task(arguments.task)
    overrides = _overrides(arguments)
    # A name the task does not take is a mistake in the command, said in one
    # line before anything runs rather than as a traceback out of the run.
    declared = task.resolved(overrides)
    # A task log is read after the fact, so a run records at INFO unless the
    # command line said otherwise. A task declaring `log_level` configures its
    # own records, so a level the command line names reaches it as that
    # parameter, unless the parameter itself was named.
    if arguments.log_level and "log_level" in declared and "log_level" not in overrides:
        overrides["log_level"] = arguments.log_level
    configure(arguments.log_level or TASK_LEVEL)
    CONSOLE.note(f"{task.name} {CONSOLE.glyph('arrow')} {', '.join(task.targets) or 'catalog'}")
    try:
        # A task's own output -- dbt's, a library's print -- is a log line, so
        # stdout carries the one result document and nothing else.
        with contextlib.redirect_stdout(sys.stderr):
            result = task.run(overrides)
    except Exception as error:
        traceback.print_exc()
        CONSOLE.fail(f"{task.name}: {type(error).__name__}: {error}")
        return 1
    payload = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    if arguments.result_file:
        _publish(pathlib.Path(arguments.result_file), payload)
    print(payload)
    CONSOLE.ok(task.name)
    return 0


def deploy_task(arguments: argparse.Namespace) -> int:
    """Create the tables one task writes, ahead of its first run."""
    task = Task(arguments.task)
    overrides = _overrides(arguments)
    task.resolved(overrides)
    table_properties = _settings(arguments.table_property)
    try:
        document = task.deploy(
            overrides, table_properties=table_properties, dry_run=arguments.dry_run
        )
    except Exception as error:
        # A catalog that cannot be reached fails in its own driver's terms,
        # which is the traceback; the line under it says whose deploy it was.
        traceback.print_exc()
        CONSOLE.fail(f"{task.name}: {type(error).__name__}: {error}")
        return 1
    for table, outcome in document["tables"].items():
        line = f"{table} {CONSOLE.glyph('arrow')} {outcome}"
        (CONSOLE.warn if outcome == "missing" else CONSOLE.ok)(line)
    if not document["tables"]:
        CONSOLE.note(f"{task.name} declares no table to create ahead of a run")
    _write_json(document)
    return 0


def _overrides(arguments: argparse.Namespace) -> dict[str, Any]:
    """The parameters file, under each repeated ``--parameter``."""
    overrides: dict[str, Any] = {}
    if arguments.parameters_file:
        overrides.update(_parameters_document(arguments.parameters_file))
    for spelled in arguments.parameter or ():
        name, separator, value = spelled.partition("=")
        if not separator:
            raise ValueError(f"a parameter is name=value, not {spelled!r}")
        overrides[name] = _parameter(value)
    return overrides


def _parameters_document(path: str) -> Mapping[str, Any]:
    """Read one JSON mapping of task overrides."""
    document = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, Mapping):
        raise TypeError(f"{path} is a JSON object of parameters")
    return document


def _publish(path: pathlib.Path, payload: str) -> None:
    """Write ``payload`` where a reader sees the whole document or none of it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f"{path.name}.{os.getpid()}.partial")
    with staged.open("w", encoding="utf-8") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(staged, path)


def _settings(spelled: Sequence[str] | None) -> dict[str, str]:
    """Return repeated ``NAME=VALUE`` options as a mapping."""
    settings = {}
    for option in spelled or ():
        name, separator, value = option.partition("=")
        if not separator:
            raise ValueError(f"a property is name=value, not {option!r}")
        settings[name] = value
    return settings


def _parameter(value: str) -> Any:
    """Read one command-line task parameter as JSON, then text."""
    try:
        return json.loads(value)
    except ValueError:
        return value


def _overridable(parser: argparse.ArgumentParser) -> None:
    """The two ways a command line sets a task's parameters."""
    parser.add_argument(
        "--parameter",
        action="append",
        default=None,
        metavar="NAME=VALUE",
        help="override one parameter; repeatable, values read as JSON then as text",
    )
    parser.add_argument(
        "--parameters-file",
        default=None,
        metavar="PATH",
        help="JSON object of overrides, applied under any --parameter",
    )


def _defaults(task: Task) -> str:
    """One task's parameters and their defaults, as its help lists them."""
    return "parameters (defaults):\n" + "\n".join(
        f"  {name} = {json.dumps(value)}" for name, value in task.parameters.items()
    )


def _parser() -> argparse.ArgumentParser:
    parser = CommandParser(
        prog="rekep",
        description=__doc__.splitlines()[0],
        epilog="""examples:
  rekep tasks list
  rekep tasks parse_messages run --parameter start=2026-08-14 --parameter end=2026-08-14
  rekep tasks parse_messages deploy
  rekep fields dump --pyclass rekep.text:Message""",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--log-level",
        default=None,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help=(
            f"records this package writes to stderr; default {COMMAND_LEVEL}, "
            f"and {TASK_LEVEL} for `tasks <name> run`, where it also sets a task's "
            "own `log_level` parameter"
        ),
    )
    commands = parser.add_subparsers(
        dest="command", required=True, title="commands", metavar="COMMAND"
    )

    tasks = commands.add_parser(
        "tasks",
        help="run and deploy the bundled pipeline tasks",
        description="Show, run and deploy the pipeline tasks bundled in rekep.tasks.",
        epilog="""examples:
  rekep tasks list
  rekep tasks parse_messages show
  rekep tasks parse_messages deploy --dry-run
  rekep tasks parse_messages run --parameter start=2026-08-14 --parameter end=2026-08-14""",
    )
    named = tasks.add_subparsers(dest="task", required=True, title="tasks", metavar="TASK")
    named.add_parser("list", help="every bundled task as JSON").set_defaults(run=list_tasks)
    for task in TASKS:
        one = named.add_parser(
            task.name, help=task.summary, description=task.summary, epilog=_defaults(task)
        )
        actions = one.add_subparsers(
            dest="action", required=True, title="commands", metavar="COMMAND"
        )
        showing = actions.add_parser("show", help="print the parameters a run would take")
        _overridable(showing)
        showing.set_defaults(run=show_task)
        running = actions.add_parser("run", help="run once and print the result")
        _overridable(running)
        running.add_argument(
            "--result-file",
            default=None,
            metavar="PATH",
            help="where the result document is published atomically",
        )
        running.set_defaults(run=run_task)
        deploying = actions.add_parser(
            "deploy", help="create the tables it writes that its catalog lacks"
        )
        _overridable(deploying)
        deploying.add_argument(
            "--table-property", action="append", default=None, metavar="NAME=VALUE"
        )
        deploying.add_argument(
            "--dry-run", action="store_true", help="report what is missing, create nothing"
        )
        deploying.set_defaults(run=deploy_task)

    field_commands = commands.add_parser(
        "fields",
        help="publish table contracts",
        description="Dump and validate Iceberg table contract documents.",
    )
    actions = field_commands.add_subparsers(
        dest="action", required=True, title="commands", metavar="COMMAND"
    )
    dumping = actions.add_parser("dump", help="write a class's field as a table contract")
    dumping.add_argument(
        "--pyclass",
        required=True,
        help="class, field, or field builder as module:Attribute",
    )
    dumping.add_argument(
        "--target", default=None, help="contract JSON path or URI; stdout when omitted"
    )
    dumping.set_defaults(run=dump)
    loading = actions.add_parser("load", help="read and validate a table contract document")
    loading.add_argument("--target", required=True, help="contract JSON path or URI")
    loading.set_defaults(run=load)
    return parser


__all__ = ["main"]


if __name__ == "__main__":  # pragma: no cover - the console script calls main()
    raise SystemExit(main())
