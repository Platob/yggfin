"""Run ingestion tasks and manage their native field and Iceberg declarations."""

from __future__ import annotations

import argparse
import contextlib
import importlib
import importlib.util
import json
import os
import pathlib
import sys
import traceback
from collections.abc import Mapping, Sequence
from typing import Any

from rekep import __version__
from rekep.console import Console
from rekep.deploy import TABLES, deploy
from rekep.fields import Field, arrow_type, field_of, fields
from rekep.iceberg import IcebergCatalog, partition_keys, primary_keys
from rekep.logs import COMMAND_LEVEL, Stage, configure
from rekep.resources import read_bytes, resource
from rekep.tasks import Task

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
        super().__init__(*args, **kwargs)

    def error(self, message: str) -> None:
        CONSOLE.fail(message)
        CONSOLE.note(f"run `{self.prog} --help`")
        raise SystemExit(2)


def main(argv: list[str] | None = None) -> int:
    """Run one command; return the exit code rather than raising it."""
    parser = _parser()
    arguments = parser.parse_args(argv)
    configure(arguments.log_level)
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


def dump(arguments: argparse.Namespace) -> int:
    """Write a Python class's native field as a document."""
    shape = field_of(_imported(arguments.pyclass))
    payload = f"{shape.into_json(indent=2)}\n"
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
    """Read and validate a native field document."""
    shape = Field.from_json(read_bytes(arguments.target).decode())
    schema = shape.into_arrow_schema()
    print(f"{shape.name or '<unnamed>'}: {len(schema.names)} columns, builds")
    for member in fields(shape):
        print(f"  {member.name}: {arrow_type(member)}{_marks(member)}")
    if shape.dtype.id == "struct":
        print(f"  primary keys: {primary_keys(shape) or '-'}")
        print(f"  partition keys: {partition_keys(shape) or '-'}")
    return 0


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
    if sources := member.partition.sources:
        transform = member.partition.transform or "identity"
        marks.append(f"derived {transform}({', '.join(sources)})")
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


def run_task(arguments: argparse.Namespace) -> int:
    """Execute one task document's Marimo application in this process."""
    document = pathlib.Path(arguments.document).resolve()
    task = Task.from_json(str(document))
    application = task.into_application_path(document)
    parameters = dict(task.parameters)
    if arguments.parameters_file:
        parameters.update(_parameters_document(arguments.parameters_file))
    for spelled in arguments.parameter or ():
        name, separator, value = spelled.partition("=")
        if not separator:
            raise ValueError(f"a parameter is name=value, not {spelled!r}")
        parameters[name] = _parameter(value)
    CONSOLE.warn(f"{task.name} {CONSOLE.glyph('arrow')} {application.name}")
    app = _application(application)
    try:
        with contextlib.redirect_stdout(sys.stderr):
            _, definitions = app.run(defs=parameters)
    except Exception as error:
        traceback.print_exc()
        CONSOLE.fail(f"{task.name}: {type(error).__name__}: {error}")
        return 1
    if "result" not in definitions:
        raise ValueError(f"{application} defines no result")
    result = Stage.validated(definitions["result"])
    named = result["task"]
    if named != task.name and not named.startswith(f"{task.name}_"):
        raise ValueError(f"{application} returned {named!r}, not a {task.name} run")
    payload = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    if arguments.result_file:
        _publish(pathlib.Path(arguments.result_file), payload)
    print(payload)
    CONSOLE.ok(task.name)
    return 0


def _application(path: pathlib.Path) -> Any:
    """Import the Marimo ``app`` exported by ``path``."""
    if importlib.util.find_spec("marimo") is None:
        raise ImportError(
            "running a task needs marimo: uv sync --project python --locked --group runner"
        )
    specification = importlib.util.spec_from_file_location(path.stem, path)
    if specification is None or specification.loader is None:
        raise ImportError(f"{path} is not an importable module")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    app = getattr(module, "app", None)
    if app is None:
        raise AttributeError(f"{path} exports no marimo app")
    return app


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


def deploy_tables(arguments: argparse.Namespace) -> int:
    """Create the raw-message Iceberg table ahead of ingestion."""
    settings = _catalog_settings(arguments)
    catalog = settings["catalog"]
    try:
        done = deploy(
            catalog,
            table_properties=settings["table_properties"],
            branch=settings["branch"],
            tables=arguments.table or None,
            dry_run=arguments.dry_run,
        )
        document = {"catalog": catalog.into_dict(), "tables": done}
    finally:
        catalog.close()
    for table, outcome in done.items():
        line = f"{table} {CONSOLE.glyph('arrow')} {outcome}"
        (CONSOLE.warn if outcome == "missing" else CONSOLE.ok)(line)
    _write_json(document)
    return 0


def _catalog_settings(arguments: argparse.Namespace) -> dict[str, Any]:
    """Build catalog and table settings from a task document and overrides."""
    parameters: dict[str, Any] = {}
    if arguments.document:
        document = pathlib.Path(arguments.document).resolve()
        parameters = dict(Task.from_json(str(document)).parameters)
    configured = parameters.get("catalog") or {}
    if not isinstance(configured, Mapping):
        raise TypeError("task catalog must be a mapping with name and properties")
    unexpected = sorted(set(configured) - {"name", "properties"})
    if unexpected:
        raise TypeError(
            "task catalog accepts only name and properties; unexpected " + ", ".join(unexpected)
        )
    catalog = IcebergCatalog.from_dict(configured)
    if arguments.catalog:
        catalog.name = arguments.catalog
    catalog.properties.update(_settings(arguments.property))
    settings = {
        "catalog": catalog,
        "table_properties": dict(parameters.get("table_properties") or {}),
        "branch": arguments.branch or parameters.get("branch"),
    }
    settings["table_properties"].update(_settings(arguments.table_property))
    return settings


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


def _parser() -> argparse.ArgumentParser:
    parser = CommandParser(
        prog="rekep",
        description=__doc__.splitlines()[0],
        epilog="""examples:
  rekep task run tasks/parse_messages/parse_messages.json
  rekep fields dump --pyclass rekep.text:Message
  rekep iceberg deploy tasks/parse_messages/parse_messages.json""",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--log-level",
        default=COMMAND_LEVEL,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help=f"records this package writes to stderr; default {COMMAND_LEVEL}",
    )
    commands = parser.add_subparsers(
        dest="command", required=True, title="commands", metavar="COMMAND"
    )

    tasks = commands.add_parser(
        "task", help="run task applications", description="Execute project task documents."
    )
    running = tasks.add_subparsers(
        dest="action", required=True, title="commands", metavar="COMMAND"
    ).add_parser("run", help="execute one task document's Marimo application")
    running.add_argument("document", help="path to a task JSON document under tasks/")
    running.add_argument(
        "--parameter",
        action="append",
        default=None,
        metavar="NAME=VALUE",
        help="override one parameter; repeatable, values read as JSON then as text",
    )
    running.add_argument(
        "--parameters-file",
        default=None,
        metavar="PATH",
        help="JSON object of overrides, applied under any --parameter",
    )
    running.add_argument(
        "--result-file",
        default=None,
        metavar="PATH",
        help="where the result document is published atomically",
    )
    running.set_defaults(run=run_task)

    iceberg = commands.add_parser(
        "iceberg",
        help="deploy ingestion tables",
        description="Create the Iceberg table the ingestion task writes.",
    )
    deploying = iceberg.add_subparsers(
        dest="action", required=True, title="commands", metavar="COMMAND"
    ).add_parser("deploy", help="create each declared table that is not there yet")
    deploying.add_argument(
        "document", nargs="?", default=None, help="task JSON carrying catalog settings"
    )
    deploying.add_argument("--catalog", default=None, help="catalog name")
    deploying.add_argument("--property", action="append", default=None, metavar="NAME=VALUE")
    deploying.add_argument("--table-property", action="append", default=None, metavar="NAME=VALUE")
    deploying.add_argument("--branch", default=None, help="branch tables are created on")
    deploying.add_argument(
        "--table",
        action="append",
        default=None,
        choices=[shape.table for shape in TABLES],
        metavar="NAME",
    )
    deploying.add_argument("--dry-run", action="store_true")
    deploying.set_defaults(run=deploy_tables)

    field_commands = commands.add_parser(
        "fields", help="publish fields", description="Dump and validate native field documents."
    )
    actions = field_commands.add_subparsers(
        dest="action", required=True, title="commands", metavar="COMMAND"
    )
    dumping = actions.add_parser("dump", help="write a class's field as a document")
    dumping.add_argument("--pyclass", required=True, help="class as module:Attribute")
    dumping.add_argument("--target", default=None, help="JSON path or URI; stdout when omitted")
    dumping.set_defaults(run=dump)
    loading = actions.add_parser("load", help="read and validate a field document")
    loading.add_argument("--target", required=True, help="JSON path or URI")
    loading.set_defaults(run=load)
    return parser


__all__ = ["main"]


if __name__ == "__main__":  # pragma: no cover - the console script calls main()
    raise SystemExit(main())
