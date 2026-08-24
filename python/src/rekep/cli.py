"""The `rekep` command: publish a declaration, and check one loads."""

from __future__ import annotations

import argparse
import dataclasses
import importlib
import json
import pathlib
import sys
from typing import Any

from rekep.fields import Field, StructField, field_of
from rekep.fix.entries import ANY_VERSION, STANDARD, VENDOR, Alias, ComponentEntry, FieldEntry
from rekep.fix.registry import FixRegistry

#: Formats a declaration can be written as, and the extensions they are
#: inferred from. `Convertible` owns the readers and writers; this is only the
#: spelling `--format` accepts.
FORMATS: dict[str, tuple[str, ...]] = {
    "json": (".json",),
    "yaml": (".yaml", ".yml"),
    "toml": (".toml",),
}


def main(argv: list[str] | None = None) -> int:
    """Run one command; return the exit code rather than raising it."""
    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        return arguments.run(arguments)
    except (
        AttributeError,
        FileNotFoundError,
        ImportError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        # A traceback is for a defect here; a bad path or a class that is not a
        # shape is a thing the caller can fix, and it gets one line saying so.
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1


def dump(arguments: argparse.Namespace) -> int:
    """Write a Python class's declaration as a document."""
    shape = field_of(_imported(arguments.pyclass))
    spelling = arguments.format or _format_of(arguments.target)
    payload = getattr(shape, f"into_{spelling}")(arguments.target)
    if payload is None:
        print(f"{arguments.pyclass} -> {arguments.target}", file=sys.stderr)
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
        print(f"  {member.name}: {member.arrow_type}{_marks(member)}")
    if isinstance(shape, StructField):
        print(f"  primary keys: {shape.primary_keys() or '-'}")
        print(f"  partition keys: {shape.partition_keys() or '-'}")
    return 0


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
            "for instance rekep.text.log:Log"
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
# Adding a newly observed alias or a newly observed vendor field is a supported
# operation, not a hand edit of a JSON file. Every verb here goes through
# `FixRegistry`, which schema-checks the change, re-runs the alias-collision
# check against the whole store, and refuses the write rather than leaving it
# half applied.


def _registry(arguments: argparse.Namespace) -> FixRegistry:
    """The store a registry command edits, offline and never scraping."""
    return FixRegistry(cache_dir=arguments.store, offline=True)


def add_field(arguments: argparse.Namespace) -> int:
    """Register one field identity the store does not have yet."""
    registry = _registry(arguments)
    entry = registry.add_field(_field_entry(arguments))
    print(f"added {entry.name} -> fields/{entry.slug}.json", file=sys.stderr)
    return 0


def update_field(arguments: argparse.Namespace) -> int:
    """Replace one stored field identity, keeping the aliases it already has."""
    registry = _registry(arguments)
    held = registry.resolve(arguments.name)
    fresh = _field_entry(arguments)
    if held is not None:
        fresh = dataclasses.replace(fresh, aliases=held.aliases or fresh.aliases)
    entry = registry.update_field(fresh)
    print(f"updated {entry.name} -> fields/{entry.slug}.json", file=sys.stderr)
    return 0


def remove_field(arguments: argparse.Namespace) -> int:
    """Delete one field identity, saying so when the store did not have it."""
    if not _registry(arguments).remove_field(arguments.name):
        print(f"no FIX field {arguments.name!r} in this registry", file=sys.stderr)
        return 1
    print(f"removed {arguments.name}", file=sys.stderr)
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
    print(f"{entry.name} answers to {list(entry.spellings())}", file=sys.stderr)
    return 0


def remove_component(arguments: argparse.Namespace) -> int:
    """Delete one component identity, saying so when the store did not have it."""
    if not _registry(arguments).remove_component(arguments.name):
        print(f"no FIX component {arguments.name!r} in this registry", file=sys.stderr)
        return 1
    print(f"removed {arguments.name}", file=sys.stderr)
    return 0


def add_component(arguments: argparse.Namespace) -> int:
    """Register one component identity from a document holding its member trees."""
    registry = _registry(arguments)
    entry = registry.add_component(_component_entry(arguments))
    print(f"added {entry.name} -> components/{entry.slug}.json", file=sys.stderr)
    return 0


def update_component(arguments: argparse.Namespace) -> int:
    """Replace one stored component identity from such a document."""
    registry = _registry(arguments)
    entry = registry.update_component(_component_entry(arguments))
    print(f"updated {entry.name} -> components/{entry.slug}.json", file=sys.stderr)
    return 0


def _component_entry(arguments: argparse.Namespace) -> ComponentEntry:
    """One component identity out of the document `--declaration` names."""
    return ComponentEntry.from_dict(json.loads(pathlib.Path(arguments.declaration).read_text()))


def check_registry(arguments: argparse.Namespace) -> int:
    """Report everything inconsistent about a store; nothing means it is sound."""
    problems = _registry(arguments).check()
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


def migrate_registry(arguments: argparse.Namespace) -> int:
    """Rewrite a store one file per identity, refusing a migration that loses one."""
    migrated = _registry(arguments).migrate(arguments.target)
    print(f"{arguments.store} -> {arguments.target}", file=sys.stderr)
    print(
        f"{len(migrated.field_entries())} fields, {len(migrated.component_entries())} components",
        file=sys.stderr,
    )
    return 0


def _field_entry(arguments: argparse.Namespace) -> FieldEntry:
    """One field identity out of the flags that describe it."""
    variant: dict[str, Any] = {}
    if arguments.type:
        variant["type"] = arguments.type
    if arguments.description:
        variant["description"] = arguments.description
    return FieldEntry(
        name=arguments.name,
        tag=arguments.tag,
        kind=STANDARD if arguments.tag else VENDOR,
        aliases=tuple(Alias(name=alias) for alias in arguments.alias),
        variants={version: dict(variant) for version in (arguments.version or [ANY_VERSION])},
        column=arguments.column or "",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rekep", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    fields = commands.add_parser("fields", help="declarations and the documents they publish")
    actions = fields.add_subparsers(dest="action", required=True)

    dumping = actions.add_parser("dump", help="write a class's declaration as a document")
    dumping.add_argument(
        "--pyclass",
        required=True,
        help="the class to dump, as module:Attribute (rekep.text.log:Log)",
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

    fix = commands.add_parser("fix", help="the FIX dictionary this package carries")
    protocol = fix.add_subparsers(dest="protocol", required=True)
    registry = protocol.add_parser("registry", help="edit and check a FIX registry store")
    verbs = registry.add_subparsers(dest="action", required=True)

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
        action.add_argument("--name", required=True, help="the field's canonical name")
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

    described(verb("add-field", "register a field identity the store does not have", add_field))
    described(verb("update-field", "replace a stored field identity", update_field))
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
            help="path of a JSON document holding the entry's name and per-version members",
        )
    verb("remove-component", "delete a component identity", remove_component).add_argument(
        "--name", required=True, help="the component to remove"
    )

    verb("check", "report everything inconsistent about a store", check_registry)
    verb("migrate", "rewrite a store one file per identity", migrate_registry).add_argument(
        "--target", required=True, help="where to write the migrated store"
    )
    return parser


if __name__ == "__main__":  # pragma: no cover - the console script calls main()
    raise SystemExit(main())
