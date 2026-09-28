"""The documentation promises what the package does, in the package's own names.

Pinned here: every page is reachable from the navigation, no page names the
native dependency beneath `rekep`, every example compiles and imports only
`rekep`, every task call binds to the task's signature, and every table has
its column page and its sample page. Under `-m integration`, every example
that asserts runs from the repository root, and the sample pages are what the
tasks land today.
"""

from __future__ import annotations

import ast
import contextlib
import hashlib
import importlib.util
import inspect
import json
import os
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import yaml

from rekep import Storages, pipeline
from rekep.deploy import TABLES, deploy

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
FENCE = re.compile(r"^```python\n(.*?)^```", re.MULTILINE | re.DOTALL)
JSON_FENCE = re.compile(r"^```json\n(.*?)^```", re.MULTILINE | re.DOTALL)

#: The dependency beneath `rekep`, which documentation never names: a reader
#: imports `rekep`, and the package re-exports what they need.
NATIVE = re.compile(r"yggdryl", re.IGNORECASE)

#: The one page whose bash fences name the native serving binary.
XMLA_PAGE = DOCS / "storages" / "xmla.md"

#: A command of a `rekep` console script, which the package does not install.
COMMAND = re.compile(r"\brekep\s+(?:tasks|fields)\b")

#: The skill that teaches an agent the library.
SKILL = ROOT / ".claude" / "skills" / "rekep"

#: Every page a reader copies an example or a command from.
PAGES = [
    ROOT / "README.md",
    SKILL / "SKILL.md",
    ROOT / "config" / "README.md",
    ROOT / "data" / "README.md",
    ROOT / "schemas" / "README.md",
    *sorted(DOCS.rglob("*.md")),
]

#: Everything published as documentation, which names nothing beneath `rekep`.
PUBLISHED = [
    ROOT / "README.md",
    *sorted(path for path in DOCS.rglob("*") if path.is_file()),
    *sorted(path for path in (ROOT / "schemas").rglob("*") if path.is_file()),
    *sorted(path for path in SKILL.rglob("*") if path.is_file()),
]


def code_fences(page: Path, pattern: re.Pattern[str] = FENCE) -> Iterator[str]:
    """Read one page's fenced examples."""
    yield from pattern.findall(page.read_text(encoding="utf-8"))


def navigation() -> list[str]:
    """Every page `mkdocs.yml` navigates to, relative to `docs/`."""
    config = yaml.load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)

    def pages(value: object) -> Iterator[str]:
        if isinstance(value, str) and value.endswith(".md"):
            yield value
        elif isinstance(value, list):
            for child in value:
                yield from pages(child)
        elif isinstance(value, dict):
            for child in value.values():
                yield from pages(child)

    return list(pages(config["nav"]))


def tool(name: str) -> ModuleType:
    """One script of `tools/`, which is not a package."""
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_navigation_names_existing_pages() -> None:
    declared = navigation()
    assert declared
    assert [page for page in declared if not (DOCS / page).is_file()] == []
    assert len(declared) == len(set(declared))


def test_every_page_is_in_the_navigation() -> None:
    """A page nobody navigates to is a page nobody reads."""
    pages = {
        page.relative_to(DOCS).as_posix()
        for page in DOCS.rglob("*.md")
        if page.parent != DOCS / "assets"
    }
    assert pages - set(navigation()) == set()


def fenced_lines(text: str, language: str) -> set[int]:
    """The line numbers, from one, inside `language` fences of a page."""
    held: set[int] = set()
    fenced = False
    for number, line in enumerate(text.splitlines(), 1):
        if fenced and line.startswith("```"):
            fenced = False
        elif fenced:
            held.add(number)
        elif line.strip() == f"```{language}":
            fenced = True
    return held


def test_no_documentation_names_the_native_dependency() -> None:
    """The one exception is the XMLA page's command line, which names the
    serving binary inside its bash fences."""
    named = []
    for path in PUBLISHED:
        text = path.read_text(encoding="utf-8", errors="replace")
        exempt = fenced_lines(text, "bash") if path == XMLA_PAGE else set()
        named += [
            f"{path.relative_to(ROOT)}:{number}"
            for number, line in enumerate(text.splitlines(), 1)
            if NATIVE.search(line) and number not in exempt
        ]
    assert not named, named
    xmla = XMLA_PAGE.read_text(encoding="utf-8")
    serving = fenced_lines(xmla, "bash")
    assert any(
        NATIVE.search(line) for number, line in enumerate(xmla.splitlines(), 1) if number in serving
    ), "the XMLA page states the serving command line"


def test_every_table_has_its_column_page_and_its_samples() -> None:
    declared = set(navigation())
    for table in TABLES:
        short = table.name.split(".", 1)[1]
        for section in ("tables", "samples"):
            assert f"{section}/{table.layer}/{short}.md" in declared, (section, table.table)
    assert "tables/states.md" in declared


def test_every_task_has_its_page() -> None:
    declared = set(navigation())
    tasks = [name for name in pipeline.__all__ if name.startswith("parse_")]
    # The three flatteners share one page; every other task has its own.
    flatteners = {task.__name__ for task in pipeline.FLATTENERS.values()}
    flattened = "parse-orders-quotes-executions.md"
    pages = {
        name: flattened if name in flatteners else f"{name.replace('_', '-')}.md" for name in tasks
    }
    assert {f"tasks/{page}" for page in pages.values()} <= declared
    for name, page in pages.items():
        assert f"`{name}" in (DOCS / "tasks" / page).read_text(encoding="utf-8"), name


def test_python_examples_compile() -> None:
    examples = [
        (page, index, source) for page in PAGES for index, source in enumerate(code_fences(page))
    ]

    assert len(examples) >= 30
    for page, index, source in examples:
        ast.parse(source, filename=f"{page.relative_to(ROOT)}#{index}")


def test_json_examples_parse() -> None:
    examples = [(page, source) for page in PAGES for source in code_fences(page, JSON_FENCE)]

    assert examples
    for page, source in examples:
        try:
            json.loads(source)
        except json.JSONDecodeError as error:
            raise AssertionError(f"invalid JSON in {page.relative_to(ROOT)}: {error}") from error


def test_a_storages_example_names_every_layer() -> None:
    """A JSON example keyed by layer is a whole `Storages.from_dict` mapping."""
    for page in PAGES:
        for source in code_fences(page, JSON_FENCE):
            held = json.loads(source)
            if "bronze" in held:
                assert set(held) == {"bronze", "silver", "gold"}, page


def test_the_capture_is_the_bytes_its_page_pins() -> None:
    """`data/capture/ulbridge.log` is the capture every documented count
    reads, and the digest its page states is what says so."""
    page = (ROOT / "data" / "README.md").read_text(encoding="utf-8")
    (digest,) = re.findall(r"^```text\n([0-9a-f]{64})\n```", page, re.MULTILINE)
    capture = (ROOT / "data" / "capture" / "ulbridge.log").read_bytes()
    assert hashlib.sha256(capture).hexdigest() == digest


def test_public_python_uses_the_rekep_surface() -> None:
    """Examples and tools import their product, not what is beneath it."""
    sources = [(page, source) for page in PAGES for source in code_fences(page)]
    sources += [(path, path.read_text(encoding="utf-8")) for path in (ROOT / "tools").glob("*.py")]

    assert sources
    for path, source in sources:
        tree = ast.parse(source, filename=str(path))
        modules = [
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        ] + [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        assert not [module for module in modules if NATIVE.match(module)], path


def test_no_page_spells_a_rekep_command() -> None:
    """The package is called from Python and installs no console script, so a
    `rekep tasks` or `rekep fields` line hands its reader a command that is
    not there: a task is a `rekep.pipeline` call."""
    spelled = [
        f"{page.relative_to(ROOT)}:{number}: {line.strip()}"
        for page in PAGES
        for number, line in enumerate(page.read_text(encoding="utf-8").splitlines(), 1)
        if COMMAND.search(line)
    ]

    assert not spelled, "\n".join(spelled)


#: What a page calls into the processing surface, bound to what each one takes.
CALLED = {
    **{name: getattr(pipeline, name) for name in pipeline.__all__ if name.startswith("parse_")},
    "deploy": deploy,
    "from_dict": Storages.from_dict,
}


def test_every_documented_task_call_binds_to_its_signature() -> None:
    """A page that hands a task a keyword it does not take compiles, so each
    call is bound to the function it names instead."""
    bound = 0
    for page in PAGES:
        for index, source in enumerate(code_fences(page)):
            for node in ast.walk(ast.parse(source)):
                if not isinstance(node, ast.Call):
                    continue
                called = node.func.attr if isinstance(node.func, ast.Attribute) else None
                called = node.func.id if isinstance(node.func, ast.Name) else called
                if called not in CALLED:
                    continue
                if any(isinstance(argument, ast.Starred) for argument in node.args) or any(
                    keyword.arg is None for keyword in node.keywords
                ):
                    continue
                try:
                    inspect.signature(CALLED[called]).bind(
                        *[None] * len(node.args), **{keyword.arg: None for keyword in node.keywords}
                    )
                except TypeError as error:
                    raise AssertionError(
                        f"{page.relative_to(ROOT)}#{index}: {called}: {error}"
                    ) from error
                bound += 1
    assert bound >= 20, "the pages call the tasks"


#: Every example that asserts, which is every example complete enough to run.
EXAMPLES = [
    pytest.param(source, id=f"{page.relative_to(ROOT)}#{index}")
    for page in PAGES
    for index, source in enumerate(code_fences(page))
    if re.search(r"^\s*assert\b", source, re.MULTILINE)
]


@contextlib.contextmanager
def at_the_root() -> Iterator[None]:
    """Run from the repository root, where every example's relative path resolves."""
    held = Path.cwd()
    os.chdir(ROOT)
    try:
        yield
    finally:
        os.chdir(held)


@pytest.mark.integration
@pytest.mark.parametrize("source", EXAMPLES)
def test_an_asserting_example_runs(source: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # A module of its own, registered, because a dataclass reads its class's
    # module back out of `sys.modules`.
    example = ModuleType("docs_example")
    monkeypatch.setitem(sys.modules, example.__name__, example)
    with at_the_root():
        exec(compile(source, "<example>", "exec"), example.__dict__)


@pytest.mark.integration
def test_the_samples_are_what_the_tasks_land() -> None:
    """Regenerate with `tools/samples_dump.py` and review the diff when this fails."""
    drifted = [
        str(path.relative_to(ROOT))
        for path, text in tool("samples_dump").published()
        if not path.is_file() or path.read_text(encoding="utf-8") != text
    ]
    assert not drifted, f"stale, run tools/samples_dump.py: {drifted}"
