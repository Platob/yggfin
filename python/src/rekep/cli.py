"""Build declarations, run tasks, and manage the FIX registry."""

from __future__ import annotations

import argparse
import dataclasses
import importlib
import json
import pathlib
import sys
from collections.abc import Sequence
from typing import Any

from rekep import __version__
from rekep.console import Console
from rekep.deploy import TABLES, deploy
from rekep.fields import Field, StructField
from rekep.filesystems import read_bytes
from rekep.fix.classify import KeyReport, apply_report, classify, count_files
from rekep.fix.entries import (
    ANY_VERSION,
    Alias,
    ComponentRecord,
    record_copy,
    refuse_record,
)
from rekep.fix.fields import fix_field, namespaced_field
from rekep.fix.registry import DEFAULT_SOURCES, FixRegistry
from rekep.fix.shell import shell
from rekep.fix.store import document_of, field_document
from rekep.logs import COMMAND_LEVEL, configure
from rekep.tasks import Task

#: Where everything a person reads goes. `stderr`, so a dump piped into a file
#: is the document and nothing else -- the styling never lands in the payload.
#: Named rather than held, so this resolves to whatever `sys.stderr` is when a
#: line is written and not to whatever it was at import.
CONSOLE = Console(stream="stderr")

#: Formats a declaration can be written as, and the extensions they are
#: inferred from. `Convertible` owns the readers and writers; this is only the
#: spelling `--format` accepts.
FORMATS: dict[str, tuple[str, ...]] = {
    "json": (".json",),
    "yaml": (".yaml", ".yml"),
}


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
    # Before the command runs, so a record from configuration-time work is not
    # the one thing the level does not reach.
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
        # A traceback is for a defect here; a bad path or a class that is not a
        # shape is a thing the caller can fix, and it gets one line saying so.
        CONSOLE.fail(f"{type(error).__name__}: {error}")
        return 1


def dump(arguments: argparse.Namespace) -> int:
    """Write a Python class's declaration as a document."""
    shape = Field.from_class(_imported(arguments.pyclass))
    spelling = arguments.format or _format_of(arguments.target)
    payload = getattr(shape, f"into_{spelling}")(arguments.target)
    if payload is None:
        CONSOLE.ok(f"{arguments.pyclass} {CONSOLE.glyph('arrow')} {arguments.target}")
        return 0
    sys.stdout.buffer.write(payload)
    return 0


def load(arguments: argparse.Namespace) -> int:
    """Read a document back and build what it declares.

    Parsing is not the check -- building is. A document can be valid YAML and
    still name a type Arrow does not have, a `fixed_size_list` with no width or
    a map with a nullable key, and every one of those is a contract two systems
    would read differently.
    """
    shape = Field.from_file(arguments.target)
    schema = shape.into_arrow_schema()
    print(f"{shape.name or '<unnamed>'}: {len(schema.names)} columns, builds")
    for member in getattr(shape, "fields", ()):
        print(f"  {member.name}: {member.dtype}{_marks(member)}")
    if isinstance(shape, StructField):
        print(f"  primary keys: {shape.primary_keys() or '-'}")
        print(f"  partition keys: {shape.partition_keys() or '-'}")
    return 0


def open_shell(arguments: argparse.Namespace) -> int:
    """Drive the registry from a prompt rather than from flags."""
    return shell(arguments.store, console=Console(stream="stderr"))


def _marks(member: Field) -> str:
    """What a column is besides its type: a key, a partition, an Iceberg id."""
    marks = []
    if member.is_primary_key:
        marks.append("primary key")
    if member.is_partition_key:
        marks.append(f"partition {member.partition_transform}")
    if member.field_id is not None:
        marks.append(f"id {member.field_id}")
    if member.nullable:
        marks.append("nullable")
    return f"  [{', '.join(marks)}]" if marks else ""


def _imported(spec: str) -> Any:
    """The class or field `spec` names: `module:Attribute`, or `module.Attribute`.

    Both spellings, because the first is what an entry point uses and the
    second is what a docstring does. The colon is unambiguous, so it is tried
    first; a dotted name falls back to importing everything up to the last dot.
    """
    module_name, separator, attribute = spec.partition(":")
    if not separator:
        module_name, _, attribute = spec.rpartition(".")
    if not module_name or not attribute:
        raise ValueError(
            f"{spec!r} does not name a class: write it as module:Attribute, "
            "for instance rekep.text.fixmsg:FixMsg"
        )
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attribute)
    except AttributeError as error:
        raise AttributeError(f"{module_name} has no {attribute!r}") from error


def _format_of(target: str | None) -> str:
    """The format a target's extension names, defaulting to YAML."""
    if target:
        for spelling, suffixes in FORMATS.items():
            if target.endswith(suffixes):
                return spelling
    return "yaml"


# -- the FIX registry --------------------------------------------------------
#
# Adding a newly observed alias or a newly observed namespaced field is a supported
# operation, not a hand edit of a JSON file. Every verb here goes through
# `FixRegistry`, which schema-checks the change, re-runs the alias-collision
# check against the whole store, and refuses the write rather than leaving it
# half applied.


def _registry(arguments: argparse.Namespace) -> FixRegistry:
    """The store a registry command edits, offline and never scraping."""
    return FixRegistry(cache_dir=arguments.store, offline=True)


def _write_json(document: Any) -> None:
    """Write one machine-readable command result to stdout."""
    json.dump(document, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def registry_versions(arguments: argparse.Namespace) -> int:
    """Write every stored FIX version and its field count."""
    registry = _registry(arguments)
    _write_json(
        [
            {"version": version, "fields": len(registry.fields(version))}
            for version in registry.versions
        ]
    )
    return 0


def registry_coverage(arguments: argparse.Namespace) -> int:
    """Write source answer counts for stored fields."""
    _write_json(_registry(arguments).source_coverage())
    return 0


def find_fields(arguments: argparse.Namespace) -> int:
    """Write distinct field records matching one query."""
    registry = _registry(arguments)
    entries: list[Field] = []
    for member in registry.search(
        arguments.query,
        version=arguments.version,
        limit=arguments.limit,
    ):
        entry = registry.field(member.fix.get("tag") or member.name)
        if entry is not None:
            entries.append(entry)
    _write_json([entry.into_dict() for entry in entries])
    return 0


def show_field(arguments: argparse.Namespace) -> int:
    """Write one complete field record."""
    entry = _registry(arguments).field(arguments.field)
    if entry is None:
        CONSOLE.fail(f"no FIX field {arguments.field!r} in this registry")
        return 1
    _write_json(entry.into_dict())
    return 0


def list_components(arguments: argparse.Namespace) -> int:
    """Write component identities, optionally filtered by name."""
    wanted = arguments.query.casefold()
    entries = sorted(_registry(arguments).component_records().values(), key=lambda item: item.name)
    _write_json(
        [
            {"name": entry.name, "versions": list(entry.versions)}
            for entry in entries
            if not wanted or wanted in entry.name.casefold()
        ]
    )
    return 0


def show_component(arguments: argparse.Namespace) -> int:
    """Write one complete component record."""
    try:
        entry = _registry(arguments).merged_component(arguments.component)
    except KeyError:
        CONSOLE.fail(f"no FIX component {arguments.component!r} in this registry")
        return 1
    _write_json(entry.into_dict())
    return 0


def dump_registry(arguments: argparse.Namespace) -> int:
    """Write a deterministic archive of one registry."""
    with CONSOLE.spinner(f"writing {arguments.output}"):
        written = _registry(arguments).into_zip(arguments.output)
    CONSOLE.ok(f"wrote {written}")
    return 0


def add_field(arguments: argparse.Namespace) -> int:
    """Register one field identity the store does not have yet."""
    registry = _registry(arguments)
    entry = registry.add_field(_field_entry(arguments))
    CONSOLE.ok(
        f"added {entry.fix.canonical} {CONSOLE.glyph('arrow')} {field_document(entry.fix.key)}"
    )
    return 0


def update_field(arguments: argparse.Namespace) -> int:
    """Replace one stored field identity, keeping the aliases it already has."""
    registry = _registry(arguments)
    held = registry.resolve(arguments.name) if arguments.name else None
    fresh = _field_entry(arguments)
    if held is not None and not arguments.declaration:
        fresh = record_copy(fresh)
        fresh.fix.named_aliases = held.fix.named_aliases or fresh.fix.named_aliases
    entry = registry.update_field(fresh)
    CONSOLE.ok(
        f"updated {entry.fix.canonical} {CONSOLE.glyph('arrow')} {field_document(entry.fix.key)}"
    )
    return 0


def promote_field(arguments: argparse.Namespace) -> int:
    """Register a rendered field and the column it is lifted into, in one call."""
    registry = _registry(arguments)
    entry = registry.promote_field(
        arguments.name,
        arguments.column,
        type=arguments.type,
        description=arguments.description,
        aliases=tuple(arguments.alias),
    )
    CONSOLE.ok(f"promoted {entry.fix.canonical} {CONSOLE.glyph('arrow')} column {entry.fix.column}")
    return 0


def remove_field(arguments: argparse.Namespace) -> int:
    """Delete one field identity, saying so when the store did not have it."""
    if not _registry(arguments).remove_field(arguments.name):
        CONSOLE.fail(f"no FIX field {arguments.name!r} in this registry")
        return 1
    CONSOLE.ok(f"removed {arguments.name}")
    return 0


def alias_field(arguments: argparse.Namespace) -> int:
    """Record spellings one field has been observed under, with their provenance."""
    registry = _registry(arguments)
    entry = registry.alias_field(
        arguments.name,
        *(
            Alias(name=alias, source=arguments.source, occurrences=arguments.occurrences)
            for alias in arguments.alias
        ),
    )
    CONSOLE.ok(f"{entry.fix.canonical} answers to {', '.join(entry.fix.spellings())}")
    return 0


def remove_component(arguments: argparse.Namespace) -> int:
    """Delete one component identity, saying so when the store did not have it."""
    if not _registry(arguments).remove_component(arguments.name):
        CONSOLE.fail(f"no FIX component {arguments.name!r} in this registry")
        return 1
    CONSOLE.ok(f"removed {arguments.name}")
    return 0


def add_component(arguments: argparse.Namespace) -> int:
    """Register one component identity from a document holding its member trees."""
    registry = _registry(arguments)
    entry = registry.add_component(_component_entry(arguments))
    CONSOLE.ok(f"added {entry.name} {CONSOLE.glyph('arrow')} components/{entry.slug}")
    return 0


def update_component(arguments: argparse.Namespace) -> int:
    """Replace one stored component identity from such a document."""
    registry = _registry(arguments)
    entry = registry.update_component(_component_entry(arguments))
    CONSOLE.ok(f"updated {entry.name} {CONSOLE.glyph('arrow')} components/{entry.slug}")
    return 0


def _component_entry(arguments: argparse.Namespace) -> ComponentRecord:
    """One component identity out of the document `--declaration` names.

    Whichever format it is written in: `Convertible` reads the extension, so a
    declaration travels as JSON, YAML or TOML without a flag saying which.
    """
    return ComponentRecord.from_file(arguments.declaration)


def check_registry(arguments: argparse.Namespace) -> int:
    """Report everything inconsistent about a store; nothing means it is sound."""
    with CONSOLE.spinner("checking every identity"):
        problems = _registry(arguments).check()
    for problem in problems:
        CONSOLE.fail(problem)
    if not problems:
        CONSOLE.ok("this store is sound")
    return 1 if problems else 0


def scrape_registry(arguments: argparse.Namespace) -> int:
    """Scrape and atomically replace a complete local registry."""
    configuration = {
        name: value
        for name, value in (
            ("timeout", arguments.timeout),
            ("max_workers", arguments.max_workers),
            ("retries", arguments.retries),
            ("backoff", arguments.backoff),
        )
        if value is not None
    }
    source_urls = {
        "nanoconda": arguments.nanoconda_url,
        "onixs": arguments.onixs_url,
        "quickfix": arguments.quickfix_url,
    }
    if any(source_urls.values()):
        configuration["sources"] = tuple(
            dataclasses.replace(source, url=source_urls[source.name] or source.url)
            for source in DEFAULT_SOURCES
        )
    configuration["announce"] = CONSOLE.note
    target = arguments.output or "~/.config/fix"
    with CONSOLE.spinner(f"scraping into {target}"):
        registry = FixRegistry.scrape(arguments.output, **configuration)
    if arguments.conflicts:
        registry.conflicts.into_json(arguments.conflicts)
    CONSOLE.ok(
        f"{registry.cache_dir} holds {len(registry.field_records())} fields and "
        f"{len(registry.component_records())} components"
    )
    return 0


def _field_entry(arguments: argparse.Namespace) -> Field:
    """One field identity out of the flags that describe it.

    A record is a `Field`, so `--declaration` is the same document every other
    declaration in this package is written as -- what `rekep fields dump`
    writes and what a shard holds. `FORMATS` above says which spelling the
    name means, `document_of` decodes it, and `refuse_record` applies every
    refusal a stored record meets.
    """
    if arguments.declaration:
        payload = read_bytes(arguments.declaration)
        document = document_of(payload, f".{_format_of(arguments.declaration)}")
        return refuse_record(Field.from_dict(document))
    record = (
        fix_field(arguments.name, int(arguments.tag), arguments.type or None)
        if arguments.tag
        else namespaced_field(arguments.name, arguments.type or None)
    )
    fix = record.fix
    fix.versions = list(arguments.version or [ANY_VERSION])
    if arguments.description:
        record.description = arguments.description
    if arguments.column:
        fix.column = arguments.column
    if arguments.alias:
        fix.named_aliases = [{"name": alias} for alias in arguments.alias]
    return refuse_record(record)


# -- classifying a capture's key names ---------------------------------------


def classify_keys(arguments: argparse.Namespace) -> int:
    """Count every key name a capture spells, and say what each one is."""
    registry = FixRegistry(cache_dir=arguments.store, offline=True)
    counts = count_files(
        arguments.source,
        pattern=arguments.pattern,
        recursive=not arguments.flat,
        plugins=arguments.plugins,
        limit=arguments.limit,
    )
    report = classify(counts, registry)
    if arguments.report:
        report.into_json(arguments.report)
        CONSOLE.ok(f"{arguments.source} {CONSOLE.glyph('arrow')} {arguments.report}")
    _write_json(report.into_dict())
    return 0


def apply_keys(arguments: argparse.Namespace) -> int:
    """Register what a report found, through the registry's own verbs."""
    registry = FixRegistry(cache_dir=arguments.store, offline=True)
    report = KeyReport.from_json(arguments.report)
    applied = apply_report(
        registry,
        report,
        aliases=arguments.aliases,
        namespace=arguments.namespace,
        minimum=arguments.minimum,
    )
    for line in applied:
        CONSOLE.ok(line)
    if not applied:
        CONSOLE.warn("nothing to apply: name --aliases, --namespace, or both")
    return 0


def run_task(arguments: argparse.Namespace) -> int:
    """Execute one task document's notebook, through Papermill.

    The one reproducible command a local run is: the same YAML Airflow hands
    to `PapermillOperator`, the same notebook, the same parameters -- so a run
    on a laptop and a run on a worker differ in nothing but the machine.
    """
    try:
        import papermill
    except ImportError as error:  # pragma: no cover - papermill is a dev tool
        raise ImportError(
            "running a task needs papermill and a kernel to run the notebook under: "
            "uv run --project python --group runner rekep task run ..."
        ) from error

    document = pathlib.Path(arguments.document).resolve()
    task = Task.from_yaml(str(document))
    parameters = dict(task.parameters)
    for spelled in arguments.parameter or ():
        name, _, value = spelled.partition("=")
        if not _:
            raise ValueError(f"a parameter is name=value, not {spelled!r}")
        parameters[name] = _parameter(value)
    target = arguments.output or f"{task.name}.executed.ipynb"
    CONSOLE.warn(f"{task.name} {CONSOLE.glyph('arrow')} {target}")
    executed = papermill.execute_notebook(
        str(task.into_notebook_path(document)),
        target,
        parameters=parameters,
        kernel_name=arguments.kernel,
        progress_bar=False,
    )
    for cell in executed.get("cells", []):
        for output in cell.get("outputs", []):
            text = output.get("data", {}).get("text/plain")
            if text and cell is executed["cells"][-1]:
                print(text)
    CONSOLE.ok(task.name)
    return 0


def deploy_tables(arguments: argparse.Namespace) -> int:
    """Create the pipeline's Iceberg tables ahead of the jobs that fill them.

    Settings come from a task document when one is named, because the catalog
    a deployment should create tables in is the catalog the pipeline will
    write to -- naming it twice is how the two drift.
    """
    settings = _catalog_settings(arguments)
    done = deploy(
        settings["catalog"],
        properties=settings["properties"],
        table_properties=settings["table_properties"],
        branch=settings["branch"],
        tables=arguments.table or None,
        dry_run=arguments.dry_run,
    )
    for table, outcome in done.items():
        line = f"{table} {CONSOLE.glyph('arrow')} {outcome}"
        (CONSOLE.warn if outcome == "missing" else CONSOLE.ok)(line)
    _write_json({"catalog": settings["catalog"], "tables": done})
    return 0


def _catalog_settings(arguments: argparse.Namespace) -> dict[str, Any]:
    """Catalog, its properties, table properties and branch, flags last."""
    parameters: dict[str, Any] = {}
    if arguments.document:
        document = pathlib.Path(arguments.document).resolve()
        parameters = dict(Task.from_yaml(str(document)).parameters)
    settings = {
        "catalog": arguments.catalog or parameters.get("catalog") or "default",
        "properties": dict(parameters.get("catalog_properties") or {}),
        "table_properties": dict(parameters.get("table_properties") or {}),
        "branch": arguments.branch or parameters.get("branch"),
    }
    settings["properties"].update(_settings(arguments.property))
    settings["table_properties"].update(_settings(arguments.table_property))
    return settings


def _settings(spelled: Sequence[str] | None) -> dict[str, str]:
    """Repeated `NAME=VALUE` options as the mapping they spell."""
    settings = {}
    for option in spelled or ():
        name, separator, value = option.partition("=")
        if not separator:
            raise ValueError(f"a property is name=value, not {option!r}")
        settings[name] = value
    return settings


def _parameter(value: str) -> Any:
    """One command-line parameter as the value it spells.

    JSON first, so `--parameter limit=100` is a number and
    `--parameter static_values={"bridge":"b1"}` is a mapping; anything JSON
    refuses is the string it already was, which is what a path or a table
    name is.
    """
    try:
        return json.loads(value)
    except ValueError:
        return value


def _parser() -> argparse.ArgumentParser:
    parser = CommandParser(
        prog="rekep",
        description=__doc__.splitlines()[0],
        epilog="""examples:
  rekep fields load --target schemas/rekep/fixmsg.yaml
  rekep iceberg deploy tasks/parse_fix/parse_fix.yml
  rekep fix registry check --store data/fix
  rekep fix shell --store data/fix""",
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
        "task", help="run notebook tasks", description="Execute project task documents."
    )
    running = tasks.add_subparsers(
        dest="action", required=True, title="commands", metavar="COMMAND"
    ).add_parser("run", help="execute one task document's notebook")
    running.add_argument("document", help="path to a task YAML under tasks/")
    running.add_argument(
        "--parameter",
        action="append",
        default=None,
        metavar="NAME=VALUE",
        help="override one parameter; repeatable, values read as JSON then as text",
    )
    running.add_argument("--output", default=None, help="where to write the executed notebook")
    running.add_argument("--kernel", default="python3", help="Jupyter kernel to execute under")
    running.set_defaults(run=run_task)

    iceberg = commands.add_parser(
        "iceberg",
        help="deploy the pipeline's tables",
        description="Create the Iceberg namespaces and tables the pipeline writes.",
    )
    deploying = iceberg.add_subparsers(
        dest="action", required=True, title="commands", metavar="COMMAND"
    ).add_parser("deploy", help="create every declared table that is not there yet")
    deploying.add_argument(
        "document",
        nargs="?",
        default=None,
        help="task YAML to read catalog, properties and branch from",
    )
    deploying.add_argument("--catalog", default=None, help="catalog name; overrides the document")
    deploying.add_argument(
        "--property",
        action="append",
        default=None,
        metavar="NAME=VALUE",
        help="one catalog property; repeatable, applied over the document's",
    )
    deploying.add_argument(
        "--table-property",
        action="append",
        default=None,
        metavar="NAME=VALUE",
        help="one property every created table gets; repeatable",
    )
    deploying.add_argument("--branch", default=None, help="branch tables are created on")
    deploying.add_argument(
        "--table",
        action="append",
        default=None,
        choices=[shape.table for shape in TABLES],
        metavar="NAME",
        help=f"deploy only this table; repeatable, one of {', '.join(_.table for _ in TABLES)}",
    )
    deploying.add_argument(
        "--dry-run",
        action="store_true",
        help="report which tables are missing without creating any",
    )
    deploying.set_defaults(run=deploy_tables)

    fields = commands.add_parser(
        "fields",
        help="publish Arrow declarations",
        description="Dump and validate portable Arrow declaration documents.",
    )
    actions = fields.add_subparsers(
        dest="action", required=True, title="commands", metavar="COMMAND"
    )

    dumping = actions.add_parser("dump", help="write a class's declaration as a document")
    dumping.add_argument(
        "--pyclass",
        required=True,
        help="the class to dump, as module:Attribute (rekep.text.fixmsg:FixMsg)",
    )
    dumping.add_argument(
        "--format",
        choices=sorted(FORMATS),
        default=None,
        help="output format; inferred from --target, else yaml",
    )
    dumping.add_argument(
        "--target",
        default=None,
        help="path or URI to write; stdout when it is left out",
    )
    dumping.set_defaults(run=dump)

    loading = actions.add_parser("load", help="read a document back and build what it declares")
    loading.add_argument("--target", required=True, help="path or URI of the document to read")
    loading.set_defaults(run=load)

    fix = commands.add_parser(
        "fix",
        help="manage the FIX dictionary",
        description="Inspect, edit, validate, and refresh FIX registry stores.",
    )
    protocol = fix.add_subparsers(
        dest="protocol", required=True, title="commands", metavar="COMMAND"
    )
    registry = protocol.add_parser(
        "registry",
        help="manage a registry store",
        description="Scriptable FIX registry reads, writes, validation, and refresh.",
    )
    verbs = registry.add_subparsers(
        dest="action", required=True, title="commands", metavar="COMMAND"
    )

    def verb(name: str, help_text: str, run: Any) -> argparse.ArgumentParser:
        """One registry verb, with the store every one of them takes."""
        action = verbs.add_parser(name, help=help_text)
        action.add_argument(
            "--store",
            required=True,
            help="path or URI of the registry store: a directory, or a .zip of one",
        )
        action.set_defaults(run=run)
        return action

    def described(action: argparse.ArgumentParser) -> argparse.ArgumentParser:
        """The flags that describe a field identity, shared by add and update."""
        source = action.add_mutually_exclusive_group(required=True)
        source.add_argument("--name", help="the field's canonical name")
        source.add_argument(
            "--declaration",
            help="complete JSON or YAML field record; other field flags are ignored",
        )
        action.add_argument(
            "--tag",
            type=int,
            default=None,
            help="its FIX tag; leave it out for a rendered field FIX never numbered",
        )
        action.add_argument("--type", default="", help="its FIX datatype, for instance String")
        action.add_argument("--description", default="", help="one factual line about it")
        action.add_argument(
            "--version",
            action="append",
            default=[],
            help="a FIX version it is declared for; repeatable, "
            f"and {ANY_VERSION!r} when it holds for all of them",
        )
        action.add_argument(
            "--column",
            default="",
            help="the parsed-log column it is lifted into, when the log declares one",
        )
        action.add_argument(
            "--alias", action="append", default=[], help="another spelling; repeatable"
        )
        return action

    verb("versions", "write stored versions and field counts as JSON", registry_versions)
    verb("coverage", "write source coverage as JSON", registry_coverage)
    finding = verb("find", "search fields and write their records as JSON", find_fields)
    finding.add_argument("query", help="tag, name, or description to search")
    finding.add_argument("--version", default=None, help="search only one FIX version")
    finding.add_argument("--limit", type=int, default=20, help="maximum matches to write")
    showing = verb("show", "write one complete field record as JSON", show_field)
    showing.add_argument("field", help="field name, alias, or tag")
    components = verb("components", "write component identities as JSON", list_components)
    components.add_argument("query", nargs="?", default="", help="optional name filter")
    component = verb("component", "write one complete component record as JSON", show_component)
    component.add_argument("component", help="component name or alias")
    verb("check", "report everything inconsistent about a store", check_registry)

    described(verb("add-field", "register a field identity the store does not have", add_field))
    described(verb("update-field", "replace a stored field identity", update_field))

    promoting = verb(
        "promote", "register a rendered field and its parsed-log column in one call", promote_field
    )
    promoting.add_argument("--name", required=True, help="the field's canonical name")
    promoting.add_argument(
        "--column", required=True, help="the parsed-log column the field is lifted into"
    )
    promoting.add_argument(
        "--type",
        default="",
        help="its FIX datatype; unsaid keeps what the entry holds, or String",
    )
    promoting.add_argument("--description", default="", help="one factual line about it")
    promoting.add_argument(
        "--alias", action="append", default=[], help="another spelling; repeatable"
    )

    verb("remove-field", "delete a field identity", remove_field).add_argument(
        "--name", required=True, help="the field to remove, by any name it answers to"
    )

    aliasing = verb("alias-field", "record spellings a field was observed under", alias_field)
    aliasing.add_argument("--name", required=True, help="the field, by any name it answers to")
    aliasing.add_argument(
        "--alias", action="append", required=True, help="a spelling to record; repeatable"
    )
    aliasing.add_argument("--source", default="", help="which capture the spelling was counted in")
    aliasing.add_argument(
        "--occurrences", type=int, default=0, help="how many times it was counted there"
    )

    for name, run in (("add-component", add_component), ("update-component", update_component)):
        action = verb(name, f"{name.split('-')[0]} a component identity", run)
        action.add_argument(
            "--declaration",
            required=True,
            help="path of a document holding the entry's name and per-version members",
        )
    verb("remove-component", "delete a component identity", remove_component).add_argument(
        "--name", required=True, help="the component to remove"
    )

    dumping = verb("dump", "write a deterministic registry archive", dump_registry)
    dumping.add_argument("--output", required=True, help="target .zip path or URI")

    interactive = protocol.add_parser(
        "shell",
        help="drive the registry from a prompt rather than from flags",
        description="Open a bounded, guided view of one FIX registry store.",
        epilog="""inside the prompt:
  find PartyRole
  show 452
  help show""",
    )
    interactive.add_argument(
        "--store",
        required=True,
        help="path or URI of the registry store: a directory, or a .zip of one",
    )
    interactive.set_defaults(run=open_shell)

    counting = protocol.add_parser(
        "classify", help="count a capture's key names and say what each one is"
    )
    counting.add_argument("--source", required=True, help="a capture file or a folder of them")
    counting.add_argument("--store", required=True, help="the registry to classify against")
    counting.add_argument("--pattern", default="*", help="which files under --source to read")
    counting.add_argument(
        "--flat", action="store_true", help="read only --source itself, not what is under it"
    )
    counting.add_argument(
        "--plugins",
        default=None,
        help="a regular expression a line's plugincode must match, for instance ^UL",
    )
    counting.add_argument(
        "--limit", type=int, default=None, help="stop after this many lines, for a sample"
    )
    counting.add_argument("--report", default=None, help="where to write the report as JSON")
    counting.set_defaults(run=classify_keys)

    applying = protocol.add_parser("apply", help="register what a classification report found")
    applying.add_argument("--store", required=True, help="the registry to write to")
    applying.add_argument("--report", required=True, help="a report written by `fix classify`")
    applying.add_argument(
        "--aliases", action="store_true", help="record each near miss against the field it means"
    )
    applying.add_argument(
        "--namespace", action="store_true", help="declare each name FIX never numbered"
    )
    applying.add_argument(
        "--minimum",
        type=int,
        default=0,
        help="skip anything counted fewer times than this",
    )
    applying.set_defaults(run=apply_keys)

    scraping = verbs.add_parser("scrape", help="replace a local store from the FIX sources")
    scraping.add_argument(
        "--output",
        "--store",
        dest="output",
        default=None,
        metavar="PATH",
        help="local target directory; defaults to ~/.config/fix",
    )
    scraping.add_argument(
        "--conflicts",
        default=None,
        metavar="PATH",
        help="write the attributed conflict report as JSON",
    )
    scraping.add_argument("--nanoconda-url", default=None, help="Nanoconda source URL")
    scraping.add_argument("--onixs-url", default=None, help="OnixS source URL")
    scraping.add_argument("--quickfix-url", default=None, help="QuickFIX source URL")
    scraping.add_argument("--timeout", type=float, default=None, help="request timeout in seconds")
    scraping.add_argument(
        "--max-workers", type=int, default=None, help="maximum concurrent source requests"
    )
    scraping.add_argument("--retries", type=int, default=None, help="retries per source request")
    scraping.add_argument(
        "--backoff", type=float, default=None, help="initial retry backoff in seconds"
    )
    scraping.set_defaults(run=scrape_registry)
    return parser


if __name__ == "__main__":  # pragma: no cover - the console script calls main()
    raise SystemExit(main())
