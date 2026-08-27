"""The interactive FIX registry terminal."""

from __future__ import annotations

import dataclasses
import difflib
import sys
from collections.abc import Callable
from functools import cache
from typing import Any

from rekep.console import Console
from rekep.fix import quickfix
from rekep.fix.entries import ANY_VERSION, NAMESPACE, STANDARD, Alias, ComponentEntry, FieldEntry
from rekep.fix.fields import FIX_SCALARS
from rekep.fix.registry import FixRegistry
from rekep.fix.store import field_document

#: Answers that confirm a write; every other answer leaves the store unchanged.
YES = ("y", "yes")

#: How many rows a listing shows before it says how many more there are.
PAGE = 20


def terminal_reader(prompt: str) -> str:
    """Read stdin after writing the styled prompt to stderr."""
    print(prompt, end="", file=sys.stderr, flush=True)
    return input()


@dataclasses.dataclass
class Shell:
    """One open registry store, and the prompt that edits it."""

    registry: FixRegistry
    console: Console = dataclasses.field(default_factory=lambda: Console(stream="stderr"))
    #: Where answers come from. `input` at a prompt; a list of lines in a test,
    #: which is what makes every branch here reachable without a terminal.
    reader: Callable[[str], str] = terminal_reader

    def run(self) -> int:
        """Read commands until `quit`, end of input, or an interrupt."""
        self._banner()
        while True:
            try:
                line = self._ask(f"{self.console.glyph('prompt')} ").strip()
            except (EOFError, KeyboardInterrupt):
                self.console.line()
                return 0
            if not line:
                continue
            verb, _, rest = line.partition(" ")
            command = self.into_commands().get(verb.lower())
            if command is None:
                self.console.fail(f"no command {verb!r}; `help` lists them")
                near = difflib.get_close_matches(verb.lower(), self.into_commands(), n=1)
                if near:
                    self.console.note(f"did you mean `{near[0]}`?")
                continue
            try:
                if command(self, rest.strip()) is False:
                    return 0
            except (EOFError, KeyboardInterrupt):
                self.console.line()
                self.console.warn("cancelled; nothing was written")
            except (KeyError, OSError, TypeError, ValueError) as error:
                self.console.fail(f"{type(error).__name__}: {error}")
        return 0

    # -- the commands -------------------------------------------------------

    @classmethod
    def into_commands(cls) -> dict[str, Callable[[Shell, str], Any]]:
        """`{verb: what it runs}`, which is also what `help` prints."""
        return {
            "help": cls._help,
            "versions": cls._versions,
            "find": cls._find,
            "show": cls._show,
            "components": cls._components,
            "component": cls._component,
            "add-field": cls._add_field_declaration,
            "update-field": cls._update_field_declaration,
            "add-component": cls._add_component,
            "update-component": cls._update_component,
            "remove-component": cls._remove_component,
            "add": cls._add,
            "edit": cls._edit,
            "alias": cls._alias,
            "remove": cls._remove,
            "check": cls._check,
            "load": cls._load,
            "dump": cls._dump,
            "quit": cls._quit,
            "exit": cls._quit,
        }

    @classmethod
    @cache
    def into_help(cls) -> tuple[tuple[str, str], ...]:
        """One line per verb, in the order `help` prints them."""
        return (
            ("help [verb]", "commands, or details for one verb"),
            ("versions", "every FIX version this store holds"),
            ("find <text>", "search fields by tag, name or description"),
            ("show <name|tag>", "one field, every version of it"),
            ("components [text]", "component identities, filtered by name"),
            ("component <name>", "one component's newest member tree"),
            ("add-field <path>", "register a field declaration file"),
            ("update-field <path>", "replace a field from a declaration file"),
            ("add-component <path>", "register a component declaration"),
            ("update-component <path>", "replace a component declaration"),
            ("remove-component <name>", "delete a component identity"),
            ("add", "register a field through guided prompts"),
            ("edit <name>", "change a field through guided prompts"),
            ("alias <name>", "record another spelling a capture used"),
            ("remove <name>", "delete a field identity"),
            ("check", "report everything inconsistent about this store"),
            ("load <path>", "open another store, replacing this one"),
            ("dump <path>", "write this store to a .zip archive"),
            ("quit", "leave"),
        )

    def _help(self, rest: str) -> None:
        """Show the command index or one command's usage."""
        wanted = rest.strip().lower()
        if wanted:
            found = next(
                ((usage, text) for usage, text in self.into_help() if usage.split()[0] == wanted),
                None,
            )
            if found is None:
                self.console.fail(f"no command {wanted!r}")
                return
            usage, text = found
            self.console.panel(
                wanted,
                [
                    _detail(self.console, "usage", usage),
                    _detail(self.console, "does", text),
                ],
            )
            self.console.line()
            return

        groups = (
            ("browse", {"versions", "find", "show", "components", "component"}),
            ("edit fields", {"add", "edit", "alias", "remove", "add-field", "update-field"}),
            ("edit components", {"add-component", "update-component", "remove-component"}),
            ("store", {"check", "load", "dump"}),
            ("session", {"help", "quit"}),
        )
        for title, verbs in groups:
            rows = [
                (self.console.style(usage, "yellow"), text)
                for usage, text in self.into_help()
                if usage.split()[0] in verbs
            ]
            self.console.rule(title)
            self.console.table(("command", "does"), rows)
        self.console.line()

    def _versions(self, rest: str) -> None:
        """Which FIX versions this store answers for, and how many fields each has."""
        del rest
        rows = []
        with self.console.spinner("reading the store"):
            for version in self.registry.versions:
                rows.append(
                    (
                        self.console.style(version, "yellow"),
                        str(len(self.registry.fields(version))),
                    )
                )
        self.console.rule("versions")
        self.console.table(("version", "fields"), rows)
        self.console.line()

    def _find(self, rest: str) -> None:
        """Fields matching `rest` by tag, name or description, best first."""
        if not rest:
            self.console.warn("say what to look for: `find PartyRole`")
            return
        with self.console.spinner(f"searching for {rest!r}"):
            found = self.registry.search(rest, limit=PAGE + 1)
        shown = found[:PAGE]
        count = f"{PAGE}+" if len(found) > PAGE else str(len(shown))
        self.console.rule(f"{count} match{'' if count == '1' else 'es'}")
        self.console.table(
            ("tag", "name", "type", "description"),
            [
                (
                    self.console.style(member.fix.get("tag", "-"), "yellow"),
                    self.console.style(member.name, "white"),
                    member.fix.get("type", "-"),
                    _clipped(member.description, max(20, self.console.width - 40)),
                )
                for member in shown
            ],
        )
        if len(found) > PAGE:
            self.console.note("more matches; narrow the query")
        self.console.line()

    def _show(self, rest: str) -> None:
        """One field identity: its reading, the versions declaring it, its values."""
        entry = self._entry(rest)
        if entry is None:
            return
        rows = [
            _detail(self.console, "tag", entry.tag or "-"),
            _detail(self.console, "kind", entry.kind),
            _detail(self.console, "type", entry.type or "-"),
            _detail(self.console, "column", entry.column or "-"),
            _detail(self.console, "versions", ", ".join(entry.versions)),
            _detail(self.console, "spellings", ", ".join(entry.spellings())),
            _detail(
                self.console,
                "about",
                _clipped(entry.description, max(20, self.console.width - 18)) or "-",
            ),
        ]
        self.console.panel(entry.name, rows)
        values = entry.values
        if values:
            self.console.table(
                ("value", "means", "symbol"),
                [
                    (
                        self.console.style(one.value, "yellow"),
                        _clipped(one.meaning, max(20, self.console.width - 44)),
                        one.aliases[0] if one.aliases else "-",
                    )
                    for one in values[:PAGE]
                ],
            )
            if len(values) > PAGE:
                self.console.note(
                    f"{len(values) - PAGE} more; `rekep fix registry show` writes complete JSON"
                )
        self.console.line()

    def _components(self, rest: str) -> None:
        """Component identities, filtered by name when `rest` says one."""
        wanted = rest.strip().lower()
        entries = sorted(
            (
                entry
                for entry in self.registry.component_entries().values()
                if not wanted or wanted in entry.name.lower()
            ),
            key=lambda entry: entry.name,
        )
        self.console.rule(f"{len(entries)} component{'' if len(entries) == 1 else 's'}")
        self.console.table(
            ("name", "versions"),
            [
                (self.console.style(entry.name, "white"), ", ".join(entry.versions))
                for entry in entries[:PAGE]
            ],
        )
        if len(entries) > PAGE:
            self.console.note(f"{len(entries) - PAGE} more; narrow it with `components <text>`")
        self.console.line()

    def _component(self, rest: str) -> None:
        """One component's member tree, for the newest version that declares it."""
        if not rest:
            self.console.warn("name one: `component Parties`")
            return
        try:
            entry = self.registry.merged_component(rest)
        except KeyError:
            self.console.fail(f"no component {rest!r} in this store")
            near = difflib.get_close_matches(
                rest,
                self.registry.component_entries(),
                n=3,
                cutoff=0.5,
            )
            if near:
                self.console.note(f"did you mean {', '.join(near)}?")
            return
        version = entry.newest
        declared = entry.into_component(version)
        self.console.panel(
            f"{entry.name} @ {version}",
            [f"{len(quickfix.members_of(declared))} top-level members"],
        )
        members = list(quickfix.walk(declared))
        for member, path in members[:PAGE]:
            required = "optional" if member.nullable is not False else "required"
            tag = member.fix.tag or 0
            self.console.line(
                "  "
                + "  " * len(path)
                + self.console.style(self.console.glyph("bullet"), "grey")
                + " "
                + self.console.style(member.name, "white")
                + self.console.style(f" <{tag}>" if tag else "", "yellow")
                + self.console.style(f"  {required}", "grey")
            )
        if len(members) > PAGE:
            self.console.note(
                f"{len(members) - PAGE} more; `rekep fix registry component` writes complete JSON"
            )
        self.console.line()

    def _add_component(self, rest: str) -> None:
        """Register the component declaration at `rest`."""
        entry = self._component_declaration(rest)
        if entry is None:
            return
        if not self._confirm(f"add {entry.name}"):
            self.console.warn("nothing was written")
            return
        stored = self.registry.add_component(entry)
        self.console.ok(f"added {stored.name}")

    def _update_component(self, rest: str) -> None:
        """Replace the component declaration at `rest`."""
        entry = self._component_declaration(rest)
        if entry is None:
            return
        if not self._confirm(f"update {entry.name}"):
            self.console.warn("nothing was written")
            return
        stored = self.registry.update_component(entry)
        self.console.ok(f"updated {stored.name}")

    def _remove_component(self, rest: str) -> None:
        """Delete one component after showing what the name resolves to."""
        if not rest:
            self.console.warn("name one: `remove-component Parties`")
            return
        try:
            entry = self.registry.merged_component(rest)
        except KeyError:
            self.console.fail(f"no component {rest!r} in this store")
            return
        self.console.panel(
            entry.name,
            [
                _detail(self.console, "versions", ", ".join(entry.versions)),
                _detail(self.console, "members", len(entry.members)),
            ],
        )
        if not self._confirm(f"remove {entry.name}"):
            self.console.warn("kept")
            return
        if self.registry.remove_component(entry.name):
            self.console.ok(f"removed {entry.name}")
        else:  # pragma: no cover - resolved immediately above
            self.console.fail(f"{entry.name} was not in this store")

    def _component_declaration(self, path: str) -> ComponentEntry | None:
        """Read and preview one component document."""
        if not path:
            self.console.warn("say which: `add-component parties.json`")
            return None
        path = _unquoted(path)
        entry = ComponentEntry.from_file(path)
        self.console.panel(
            entry.name,
            [
                _detail(self.console, "versions", ", ".join(entry.versions)),
                _detail(self.console, "members", len(entry.members)),
                _detail(self.console, "source", path),
            ],
        )
        return entry

    # -- changing it --------------------------------------------------------

    def _add_field_declaration(self, rest: str) -> None:
        """Register the complete field declaration at `rest`."""
        self._store_field_declaration(rest, update=False)

    def _update_field_declaration(self, rest: str) -> None:
        """Replace the complete field declaration at `rest`."""
        self._store_field_declaration(rest, update=True)

    def _store_field_declaration(self, path: str, *, update: bool) -> None:
        """Preview, confirm, and store one complete field document."""
        if not path:
            self.console.warn("say which: `add-field field.json`")
            return
        path = _unquoted(path)
        entry = FieldEntry.from_file(path)
        self._field_panel(entry, source=path)
        verb = "update" if update else "add"
        if not self._confirm(f"{verb} {entry.name}"):
            self.console.warn("nothing was written")
            return
        stored = self.registry.update_field(entry) if update else self.registry.add_field(entry)
        self.console.ok(f"{'updated' if update else 'added'} {stored.name}")

    def _add(self, rest: str) -> None:
        """Build one field identity by answering for each part of it."""
        entry = self._built(None, name=rest)
        if entry is None:
            return
        self.registry.add_field(entry)
        self.console.ok(f"added {entry.name} {self.console.glyph('arrow')} {field_document(entry)}")

    def _edit(self, rest: str) -> None:
        """Change one stored identity, keeping every part left unanswered."""
        held = self._entry(rest)
        if held is None:
            return
        entry = self._built(held)
        if entry is None:
            return
        self.registry.update_field(dataclasses.replace(entry, aliases=held.aliases))
        self.console.ok(f"updated {entry.name}")

    def _built(self, held: FieldEntry | None, name: str = "") -> FieldEntry | None:
        """One field identity, question by question, confirmed before it is returned.

        `held` supplies every default when there is one, so editing is
        answering only what changes; `None` is an addition, where a bare Enter
        means "leave it out" rather than "as it was".
        """
        console = self.console
        console.rule("field")
        name = self._ask_for("name", held.name if held else name)
        if not name:
            console.warn("a field needs a name")
            return None
        tag = self._ask_for(
            "tag (blank for a field FIX never numbered)", str(held.tag or "") if held else ""
        )
        if tag and not tag.isdigit():
            console.warn(f"{tag!r} is not a tag")
            return None
        version = self._ask_for(
            "versions it is declared for, comma separated (`*` holds for all of them)",
            ", ".join(held.versions) if held else self._newest(),
        )
        versions = tuple(part.strip() for part in version.split(",") if part.strip())
        if not versions:
            console.warn("a field is declared for at least one version")
            return None
        console.note(f"types: {', '.join(sorted(FIX_SCALARS)[:12])}{console.glyph('ellipsis')}")
        datatype = self._ask_for("FIX datatype", (held.type if held else "") or "String")
        described = self._ask_for("one factual line about it", held.description if held else "")
        column = self._ask_for(
            "parsed-log column, when the log declares one", held.column if held else ""
        )
        entry = FieldEntry(
            name=name,
            tag=int(tag) if tag else None,
            kind=STANDARD if tag else NAMESPACE,
            aliases=held.aliases if held else (),
            versions=versions,
            type=datatype,
            description=described,
            values=held.values if held else (),
            column=column,
        )
        self._field_panel(entry)
        if not self._confirm("write it"):
            console.warn("nothing was written")
            return None
        return entry

    def _field_panel(self, entry: FieldEntry, *, source: str = "") -> None:
        """Show one complete field record before a write."""
        rows = [
            _detail(self.console, "tag", entry.tag or "-"),
            _detail(self.console, "kind", entry.kind),
            _detail(self.console, "versions", ", ".join(entry.versions)),
            _detail(self.console, "type", entry.type or "-"),
            _detail(self.console, "description", _clipped(entry.description, 60) or "-"),
            _detail(self.console, "column", entry.column or "-"),
        ]
        if source:
            rows.append(_detail(self.console, "source", source))
        self.console.panel(entry.name, rows)

    def _alias(self, rest: str) -> None:
        """Record another spelling one field has been observed under."""
        entry = self._entry(rest)
        if entry is None:
            return
        spelling = self._ask_for("the spelling to record", "")
        if not spelling:
            self.console.warn("an alias needs a name")
            return
        source = self._ask_for("which capture it was counted in", "")
        counted = self._ask_for("how many times", "0")
        if not counted.isdigit():
            self.console.warn("the occurrence count must be a non-negative whole number")
            return
        proposed = Alias(name=spelling, source=source, occurrences=int(counted))
        self.console.panel(
            "alias",
            [
                _detail(self.console, "field", entry.name),
                _detail(self.console, "spelling", proposed.name),
                _detail(self.console, "source", proposed.source or "-"),
                _detail(self.console, "occurrences", proposed.occurrences),
            ],
        )
        if not self._confirm("record it"):
            self.console.warn("nothing was written")
            return
        updated = self.registry.alias_field(
            entry.name,
            proposed,
        )
        self.console.ok(f"{updated.name} answers to {', '.join(updated.spellings())}")

    def _remove(self, rest: str) -> None:
        """Delete one field identity, after saying which one it resolved to."""
        entry = self._entry(rest)
        if entry is None:
            return
        if not self._confirm(f"remove {entry.name}"):
            self.console.warn("kept")
            return
        if self.registry.remove_field(entry.name):
            self.console.ok(f"removed {entry.name}")
        else:
            self.console.fail(f"{entry.name} was not in this store")

    def _check(self, rest: str) -> None:
        """Everything inconsistent about this store; nothing means it is sound."""
        del rest
        with self.console.spinner("checking every identity"):
            problems = self.registry.check()
        if not problems:
            self.console.ok("this store is sound")
            return
        self.console.rule(f"{len(problems)} problem{'' if len(problems) == 1 else 's'}")
        for problem in problems:
            self.console.fail(problem)
        self.console.line()

    def _load(self, rest: str) -> None:
        """Open another store, so one session can read two."""
        if not rest:
            self.console.warn("say which: `load ../data/fix`")
            return
        rest = _unquoted(rest)
        with self.console.spinner(f"opening {rest}"):
            registry = FixRegistry(cache_dir=rest, offline=True)
            versions = registry.versions
        self.registry = registry
        self.console.ok(f"{rest} {self.console.glyph('arrow')} {len(versions)} versions")

    def _dump(self, rest: str) -> None:
        """Write this store into one archive, which is how it travels."""
        if not rest:
            self.console.warn("say where: `dump ../data/fix.zip`")
            return
        rest = _unquoted(rest)
        self.console.panel(
            "archive",
            [
                _detail(self.console, "store", self.registry.cache_dir),
                _detail(self.console, "target", rest),
            ],
        )
        if not self._confirm("write it"):
            self.console.warn("nothing was written")
            return
        with self.console.spinner(f"writing {rest}"):
            written = self.registry.into_zip(rest)
        self.console.ok(f"wrote {written}")

    def _quit(self, rest: str) -> bool:
        """Leave, which is what `False` means to the loop."""
        del rest
        self.console.note("bye")
        return False

    # -- asking -------------------------------------------------------------

    def _banner(self) -> None:
        """What this is, and where its store is, before the first prompt."""
        console = self.console
        console.line()
        console.panel(
            "REKEP / FIX REGISTRY",
            [
                _detail(
                    console,
                    "store",
                    _clipped(str(self.registry.cache_dir), max(20, console.width - 22)),
                ),
                _detail(console, "versions", len(self.registry.versions)),
                _detail(console, "fields", len(self.registry.field_entries())),
                _detail(console, "components", len(self.registry.component_entries())),
                _detail(console, "command", f"type {console.style('help', 'yellow')}"),
            ],
        )
        console.line()

    def _newest(self) -> str:
        """Which version a new field is declared for unless the answer says another."""
        versions = self.registry.versions
        return versions[0] if versions else ANY_VERSION

    def _ask(self, prompt: str) -> str:
        """One answer, with the prompt styled where the terminal allows it."""
        return self.reader(self.console.style(prompt, "bold", "orange"))

    def _ask_for(self, question: str, default: str = "") -> str:
        """One answer, where a bare Enter keeps `default`."""
        shown = f" [{self.console.style(default, 'grey')}]" if default else ""
        answer = self._ask(f"  {question}{shown} {self.console.glyph('prompt')} ").strip()
        return answer or default

    def _confirm(self, question: str) -> bool:
        """A yes/no, defaulting to no: a write is never what silence meant."""
        answer = self._ask(f"  {question}? [y/N] {self.console.glyph('prompt')} ").strip().lower()
        return answer in YES

    def _entry(self, name: str) -> FieldEntry | None:
        """The identity `name` resolves to, saying what else it could have meant."""
        if not name:
            self.console.warn("name a field: `show PartyRole`")
            return None
        entry = self.registry.entry(name)
        if entry is not None:
            return entry
        self.console.fail(f"no field {name!r} in this store")
        near = self.registry.search(name, limit=5)
        if near:
            self.console.note(f"did you mean {', '.join(member.name for member in near)}?")
        return None


def _clipped(text: str, width: int) -> str:
    """`text` cut to `width`, with an ellipsis where it was cut."""
    text = " ".join(str(text).split())
    suffix = "..."
    return text if len(text) <= width else text[: max(0, width - len(suffix))] + suffix


def _unquoted(text: str) -> str:
    """One shell argument, accepting matching quotes around paths with spaces."""
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    return text


def _detail(console: Console, name: str, value: Any) -> str:
    """One aligned label and value inside a shell panel."""
    return f"{console.style(f'{name:<12}', 'grey')} {value}"


def shell(
    store: str,
    console: Console | None = None,
    reader: Callable[[str], str] = terminal_reader,
) -> int:
    """Open `store` and drive it from a prompt; the exit code the CLI returns."""
    return Shell(
        registry=FixRegistry(cache_dir=store, offline=True),
        console=console or Console(stream="stderr"),
        reader=reader,
    ).run()


#: Re-exported so a caller building an entry document has the same names the
#: prompt uses, without importing two modules to do it.
__all__ = ["ComponentEntry", "FieldEntry", "Shell", "shell"]
