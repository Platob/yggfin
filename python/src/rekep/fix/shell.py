"""`rekep fix shell`: the registry's verbs, driven by a prompt instead of flags.

Editing a dictionary of seven thousand identities from a shell is a lot of
typing to get one field right, and every mistake is a rejected write and a
retyped command. Here the same verbs run against a store held open: what is in
it is browsable, a change is built one answered question at a time with the
store's own vocabulary offered as it goes, and nothing is written until the
whole entry has been shown back.

Everything a command does goes through `FixRegistry`, exactly as the flag-driven
verbs do. This is a way of *calling* them, never a second implementation.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence
from functools import cache
from typing import Any

from rekep.console import Console
from rekep.fix.entries import ANY_VERSION, NAMESPACE, STANDARD, Alias, ComponentEntry, FieldEntry
from rekep.fix.fields import FIX_SCALARS
from rekep.fix.registry import FixRegistry

#: What a bare Enter means at a yes/no question, per question.
YES = ("y", "yes")
NO = ("n", "no")

#: How many rows a listing shows before it says how many more there are.
PAGE = 20


@dataclasses.dataclass
class Shell:
    """One open registry store, and the prompt that edits it."""

    registry: FixRegistry
    console: Console = dataclasses.field(default_factory=Console)
    #: Where answers come from. `input` at a prompt; a list of lines in a test,
    #: which is what makes every branch here reachable without a terminal.
    reader: Callable[[str], str] = input

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
            ("help", "every verb and what it does"),
            ("versions", "every FIX version this store holds"),
            ("find <text>", "search fields by tag, name or description"),
            ("show <name|tag>", "one field, every version of it"),
            ("components [text]", "component identities, filtered by name"),
            ("component <name>", "one component's members, version by version"),
            ("add", "register a field, one answered question at a time"),
            ("edit <name>", "change a stored field, keeping what you do not retype"),
            ("alias <name>", "record another spelling a capture used"),
            ("remove <name>", "delete a field identity"),
            ("check", "report everything inconsistent about this store"),
            ("load <path>", "open another store, replacing this one"),
            ("dump <path>", "write this store to a directory or a .zip"),
            ("quit", "leave"),
        )

    def _help(self, rest: str) -> None:
        """Every verb and what it does."""
        del rest
        self.console.rule("commands")
        self.console.table(
            ("verb", "does"),
            [(self.console.style(verb, "bright_cyan"), text) for verb, text in self.into_help()],
        )
        self.console.line()

    def _versions(self, rest: str) -> None:
        """Which FIX versions this store answers for, and how many fields each has."""
        del rest
        rows = []
        with self.console.spinner("reading the store"):
            for version in self.registry.versions:
                rows.append(
                    (
                        self.console.style(version, "bright_cyan"),
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
            found = self.registry.search(rest, limit=PAGE)
        self.console.rule(f"{len(found)} match{'' if len(found) == 1 else 'es'}")
        self.console.table(
            ("tag", "name", "type", "description"),
            [
                (
                    self.console.style(member.fix.get("tag", "-"), "yellow"),
                    self.console.style(member.name, "bright_cyan"),
                    member.fix.get("type", "-"),
                    _clipped(member.description, max(20, self.console.width - 40)),
                )
                for member in found
            ],
        )
        self.console.line()

    def _show(self, rest: str) -> None:
        """One field identity, and what each version says about it."""
        entry = self._entry(rest)
        if entry is None:
            return
        rows = [
            f"{self.console.style('tag', 'grey')}      {entry.tag if entry.tag else '-'}",
            f"{self.console.style('kind', 'grey')}     {entry.kind}",
            f"{self.console.style('column', 'grey')}   {entry.column or '-'}",
            f"{self.console.style('spellings', 'grey')} {', '.join(entry.spellings())}",
        ]
        self.console.panel(entry.name, rows)
        self.console.table(
            ("version", "name", "type", "description"),
            [
                (
                    self.console.style(version, "bright_cyan"),
                    str(variant.get("name") or entry.name),
                    str(variant.get("type") or "-"),
                    _clipped(
                        str(variant.get("description") or ""), max(20, self.console.width - 44)
                    ),
                )
                for version, variant in entry.variants.items()
            ],
        )
        self.console.line()

    def _components(self, rest: str) -> None:
        """Component identities, filtered by name when `rest` says one."""
        wanted = rest.strip().lower()
        entries = [
            entry
            for entry in self.registry.component_entries().values()
            if not wanted or wanted in entry.name.lower()
        ]
        self.console.rule(f"{len(entries)} component{'' if len(entries) == 1 else 's'}")
        self.console.table(
            ("name", "versions"),
            [
                (self.console.style(entry.name, "bright_cyan"), ", ".join(entry.versions))
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
        entry = self.registry.merged_component(rest)
        version = entry.versions[0]
        declared = entry.into_component(version)
        self.console.panel(
            f"{entry.name} @ {version}", [f"{len(declared.members)} top-level members"]
        )
        for member, path in _members(declared.members):
            required = "required" if member.required else "optional"
            tag = getattr(member, "tag", 0)
            self.console.line(
                "  "
                + "  " * len(path)
                + self.console.style(self.console.glyph("bullet"), "grey")
                + " "
                + self.console.style(member.name, "bright_cyan")
                + self.console.style(f" <{tag}>" if tag else "", "yellow")
                + self.console.style(f"  {required}", "grey")
            )
        self.console.line()

    # -- changing it --------------------------------------------------------

    def _add(self, rest: str) -> None:
        """Build one field identity by answering for each part of it."""
        entry = self._built(None, name=rest)
        if entry is None:
            return
        self.registry.add_field(entry)
        self.console.ok(f"added {entry.name} {self.console.glyph('arrow')} fields/{entry.slug}")

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
            "version (`*` holds for all of them)",
            held.versions[0] if held else self._newest(),
        )
        variant = dict((held.variant(version) if held else None) or {})
        console.note(f"types: {', '.join(sorted(FIX_SCALARS)[:12])}{console.glyph('ellipsis')}")
        datatype = self._ask_for("FIX datatype", str(variant.get("type") or "String"))
        described = self._ask_for(
            "one factual line about it", str(variant.get("description") or "")
        )
        column = self._ask_for(
            "parsed-log column, when the log declares one", held.column if held else ""
        )
        entry = FieldEntry(
            name=name,
            tag=int(tag) if tag else None,
            kind=STANDARD if tag else NAMESPACE,
            aliases=held.aliases if held else (),
            variants={version: {**variant, "type": datatype, "description": described}},
            column=column,
        )
        console.panel(
            entry.name,
            [
                f"{console.style('tag', 'grey')}         {entry.tag if entry.tag else '-'}",
                f"{console.style('kind', 'grey')}        {entry.kind}",
                f"{console.style('version', 'grey')}     {version}",
                f"{console.style('type', 'grey')}        {datatype}",
                f"{console.style('description', 'grey')} {_clipped(described, 60) or '-'}",
                f"{console.style('column', 'grey')}      {column or '-'}",
            ],
        )
        if not self._confirm("write it"):
            console.warn("nothing was written")
            return None
        return entry

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
        updated = self.registry.alias_field(
            entry.name,
            Alias(
                name=spelling,
                source=source,
                occurrences=int(counted) if counted.isdigit() else 0,
            ),
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
        """Open another store, so one session can compare or migrate two."""
        if not rest:
            self.console.warn("say which: `load ../data/fix`")
            return
        with self.console.spinner(f"opening {rest}"):
            registry = FixRegistry(cache_dir=rest, offline=True)
            versions = registry.versions
        self.registry = registry
        self.console.ok(f"{rest} {self.console.glyph('arrow')} {len(versions)} versions")

    def _dump(self, rest: str) -> None:
        """Write this store somewhere else: a directory, or a `.zip` of one."""
        if not rest:
            self.console.warn("say where: `dump ../data/fix.zip`")
            return
        with self.console.spinner(f"writing {rest}"):
            written = (
                self.registry.into_zip(rest)
                if str(rest).endswith(".zip")
                else self.registry.migrate(rest).cache_dir
            )
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
            "rekep fix",
            [
                f"{console.style('store', 'grey')}    {self.registry.cache_dir}",
                f"{console.style('versions', 'grey')} {len(self.registry.versions)}",
                f"{console.style('help', 'grey')}     type {console.style('help', 'bright_cyan')}",
            ],
        )
        console.line()

    def _newest(self) -> str:
        """Which version a new field is declared for unless the answer says another."""
        versions = self.registry.versions
        return versions[0] if versions else ANY_VERSION

    def _newest(self) -> str:
        """Which version a new field is declared for unless the answer says another."""
        versions = self.registry.versions
        return versions[0] if versions else ANY_VERSION

    def _ask(self, prompt: str) -> str:
        """One answer, with the prompt styled where the terminal allows it."""
        return self.reader(self.console.style(prompt, "bold", "cyan"))

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
        entry = self.registry.resolve(name)
        if entry is not None:
            return entry
        self.console.fail(f"no field {name!r} in this store")
        near = self.registry.search(name, limit=5)
        if near:
            self.console.note(f"did you mean {', '.join(member.name for member in near)}?")
        return None


def _members(
    members: Sequence[Any], path: tuple[str, ...] = ()
) -> list[tuple[Any, tuple[str, ...]]]:
    """Every member under `members`, with the groups it sits inside."""
    found: list[tuple[Any, tuple[str, ...]]] = []
    for member in members:
        found.append((member, path))
        found.extend(_members(getattr(member, "members", ()), (*path, member.name)))
    return found


def _clipped(text: str, width: int) -> str:
    """`text` cut to `width`, with an ellipsis where it was cut."""
    text = " ".join(str(text).split())
    return text if len(text) <= width else text[: max(1, width - 1)] + "…"


def shell(store: str, console: Console | None = None, reader: Callable[[str], str] = input) -> int:
    """Open `store` and drive it from a prompt; the exit code the CLI returns."""
    return Shell(
        registry=FixRegistry(cache_dir=store, offline=True),
        console=console or Console(),
        reader=reader,
    ).run()


#: Re-exported so a caller building an entry document has the same names the
#: prompt uses, without importing two modules to do it.
__all__ = ["ComponentEntry", "FieldEntry", "Shell", "shell"]
