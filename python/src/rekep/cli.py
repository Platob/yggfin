"""The `rekep` command: publish a declaration, and check one loads.

Two things a contract needs from outside Python. `fields dump` writes a class's
shape as a document -- which is what keeps `schemas/` in step with the code
that declares it, in CI or in a pre-commit hook rather than by hand. `fields
load` reads one back and builds it, which is the check that matters: a contract
that no longer parses is a contract two systems no longer share.

    rekep fields dump --pyclass rekep.logs.log:Log --target schemas/rekep/log.yaml
    rekep fields load --target schemas/rekep/log.yaml

Both are thin: everything they do is `Field.from_file`, `field_of` and the
`into_*` methods, so the command line can never do something the library
cannot, or do it differently.
"""

from __future__ import annotations

import argparse
import importlib
import sys
from typing import Any

from rekep.fields import Field, StructField, field_of

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
    """Write a Python class's declaration as a document.

    The format is `--format`, or the target's extension when it says one, or
    YAML -- which is the only default a `--target`-less call can have, and the
    one the contracts in this repository are written in. `--format` wins over
    the extension where they disagree: it was typed, and the extension was
    merely there.

    Only the document ever reaches stdout, so a dump with no target pipes; the
    line saying where a file went is on stderr.
    """
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
            "for instance rekep.logs.log:Log"
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rekep", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    fields = commands.add_parser("fields", help="declarations and the documents they publish")
    actions = fields.add_subparsers(dest="action", required=True)

    dumping = actions.add_parser("dump", help="write a class's declaration as a document")
    dumping.add_argument(
        "--pyclass",
        required=True,
        help="the class to dump, as module:Attribute (rekep.logs.log:Log)",
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
    return parser


if __name__ == "__main__":  # pragma: no cover - the console script calls main()
    raise SystemExit(main())
