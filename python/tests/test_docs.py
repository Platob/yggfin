"""The small documentation surface matches the supported ingestion path."""

from __future__ import annotations

import ast
import json
import re
import shlex
from collections.abc import Iterator
from pathlib import Path

import yaml

from rekep import Field, Message
from rekep.cli import _parser
from rekep.tasks import Task

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
FENCE = re.compile(r"^```python\n(.*?)^```", re.MULTILINE | re.DOTALL)
JSON_FENCE = re.compile(r"^```json\n(.*?)^```", re.MULTILINE | re.DOTALL)
SHELL_FENCE = re.compile(r"^```bash\n(.*?)^```", re.MULTILINE | re.DOTALL)
INVOKED = re.compile(r"(?:^|\s)rekep\s")
LOOP = re.compile(r"^\s*for (\w+) in ([^;]+); do\s*$")

#: Every page a reader copies a command from.
PAGES = [
    ROOT / "README.md",
    ROOT / "airflow" / "README.md",
    ROOT / "config" / "README.md",
    ROOT / "data" / "README.md",
    ROOT / "data" / "dbt" / "README.md",
    *sorted(DOCS.rglob("*.md")),
]


def code_fences(page: Path, pattern: re.Pattern[str]) -> Iterator[str]:
    """Read fenced examples, including an exact repository snippet."""
    for source in pattern.findall(page.read_text(encoding="utf-8")):
        if match := re.fullmatch(r'\s*--8<-- "([^"]+)"\s*', source):
            snippet = (ROOT / match.group(1)).resolve()
            assert snippet.is_relative_to(ROOT), f"{page} includes a file outside the repository"
            source = snippet.read_text(encoding="utf-8")
        yield source


def test_python_examples_compile() -> None:
    examples = [
        (page, index, source)
        for page in sorted(DOCS.rglob("*.md"))
        for index, source in enumerate(code_fences(page, FENCE))
    ]

    assert len(examples) >= 6
    for page, index, source in examples:
        ast.parse(source, filename=f"{page.relative_to(DOCS)}#{index}")


def test_json_examples_parse() -> None:
    examples = [
        (page, source)
        for page in sorted(DOCS.rglob("*.md"))
        for source in code_fences(page, JSON_FENCE)
    ]

    assert examples
    for page, source in examples:
        try:
            json.loads(source)
        except json.JSONDecodeError as error:
            raise AssertionError(f"invalid JSON in {page.relative_to(DOCS)}: {error}") from error


def test_navigation_names_existing_pages() -> None:
    config = yaml.load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)

    def pages(value: object):
        if isinstance(value, str) and value.endswith(".md"):
            yield value
        elif isinstance(value, list):
            for child in value:
                yield from pages(child)
        elif isinstance(value, dict):
            for child in value.values():
                yield from pages(child)

    declared = list(pages(config["nav"]))
    assert declared
    assert all((DOCS / page).is_file() for page in declared)


def test_docs_publish_the_native_message_and_market_contracts() -> None:
    config = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    message_schema = (ROOT / "schemas" / "rekep" / "message.json").read_text(encoding="utf-8")
    fix_schema = (ROOT / "schemas" / "rekep" / "fixmsg.json").read_text(encoding="utf-8")

    assert Field.__name__ == "Field"
    assert [member.name for member in Message.into_field()] == [
        "currunix",
        "curruuid",
        "currhashcode",
        "crosscode",
        "seqnum",
        "body",
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    ]
    assert "pipeline/tasks/parse-fix-raw.md" in config
    assert "pipeline/tasks/parse-fix-refined.md" in config
    for kind in ("books", "orders", "quotes", "executions"):
        assert f"pipeline/tasks/parse-{kind}.md" in config
    assert "pipeline/tasks/parse-fix.md" not in config
    assert "pipeline/tasks/build-dbt.md" in config
    assert "market/" not in config
    assert sorted(path.name for path in (ROOT / "schemas" / "rekep").glob("*.json")) == [
        "book.json",
        "fixmsg.json",
        "marketevent.json",
        "message.json",
    ]
    # The FIX contract is the native projected event row. A line's text
    # remains solely in Message, linked by the event row's `srcuuids`.
    assert '"name": "body"' in message_schema
    assert '"name": "msgtype"' in fix_schema
    assert '"name": "srcuuids"' in fix_schema
    for capture in ("msgthreadid", "loglevel", "body"):
        assert f'"name": "{capture}"' not in fix_schema
    book = json.loads((ROOT / "schemas" / "rekep" / "book.json").read_text(encoding="utf-8"))
    market_event = json.loads(
        (ROOT / "schemas" / "rekep" / "marketevent.json").read_text(encoding="utf-8")
    )
    book_names = [member["name"] for member in book["schema"]["fields"]]
    event_names = [member["name"] for member in market_event["schema"]["fields"]]
    assert book_names[-3:] == ["bid", "ask", "executions"]
    assert event_names == book_names[:-3]


def test_docs_record_the_measured_message_rates() -> None:
    benchmark = (DOCS / "storage" / "benchmarks.md").read_text(encoding="utf-8")
    task = (DOCS / "pipeline" / "tasks" / "parse-messages.md").read_text(encoding="utf-8")

    assert "70,000" in benchmark
    assert "timestamp[us, UTC]" in benchmark
    assert "fastest of two warmed runs" in benchmark
    assert "buffered()" in benchmark
    assert "300,000 rows" in benchmark
    # The decoder limit stays documented where a reader meets it, and the
    # staging answer stays beside it: both were removed from the docs once.
    assert "concatenated gzip members" in task
    assert "staging locally is not a substitute" in task


def test_fix_schema_stays_owned_by_the_runtime_registry() -> None:
    raw = (DOCS / "pipeline" / "tasks" / "parse-fix-raw.md").read_text(encoding="utf-8")
    refined = (DOCS / "pipeline" / "tasks" / "parse-fix-refined.md").read_text(encoding="utf-8")
    schemas = (ROOT / "schemas" / "README.md").read_text(encoding="utf-8")

    assert "parse_text_arrow_reader" in raw
    assert "128-column" in raw
    assert "FixMsg" in raw and "FixMsg" in refined
    assert "fix_schema_carrying" not in raw
    assert "fix_lifecycle_arrow_reader" in refined
    assert "fix_window_filter" in refined
    assert "not alternate implementations" in schemas
    assert "`fixmsg.json`" in schemas
    assert "iceberg_contract" in schemas


def test_public_python_uses_the_rekep_surface() -> None:
    """Examples, DAGs and tools import their product, not its runtime."""
    roots = [ROOT / "README.md", ROOT / "schemas", DOCS, ROOT / "airflow", ROOT / "tools"]
    sources = []
    for root in roots:
        for path in [root] if root.is_file() else root.rglob("*"):
            if path.is_file() and path.suffix == ".py":
                sources.append((path, path.read_text(encoding="utf-8")))
            elif path.is_file() and path.suffix == ".md":
                sources.extend((path, source) for source in code_fences(path, FENCE))

    assert sources
    for path, source in sources:
        tree = ast.parse(source, filename=str(path))
        modules = [
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        ] + [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        assert not any(
            module == "yggdryl" or module.startswith("yggdryl.") for module in modules
        ), path


def test_each_task_page_publishes_its_defaults_verbatim() -> None:
    """Pasted defaults and repository snippets state what a run takes."""
    pages = {
        "pipeline/tasks/parse-messages.md": "parse_messages",
        "pipeline/tasks/parse-fix-raw.md": "parse_fix_raw",
        "pipeline/tasks/parse-fix-refined.md": "parse_fix_refined",
        "pipeline/tasks/parse-books.md": "parse_books",
        "pipeline/tasks/parse-orders.md": "parse_orders",
        "pipeline/tasks/parse-quotes.md": "parse_quotes",
        "pipeline/tasks/parse-executions.md": "parse_executions",
        "pipeline/tasks/build-dbt.md": "build_dbt",
    }

    for page, name in pages.items():
        shown = [json.loads(source) for source in code_fences(DOCS / page, JSON_FENCE)]
        assert Task(name).parameters in shown, f"{page} no longer shows {name}.json as it is"


def shell_commands() -> Iterator[tuple[Path, list[str]]]:
    """Every `rekep` invocation in a shell fence, as the arguments after `rekep`.

    A command inside a `for` loop is read once per value the loop names. A
    command spelled with a placeholder, `<name>`, states a form rather than a
    command and is left out.
    """
    for page in PAGES:
        for fence in SHELL_FENCE.findall(page.read_text(encoding="utf-8")):
            loops: dict[str, list[str]] = {}
            for line in fence.replace("\\\n", " ").splitlines():
                if looped := LOOP.match(line):
                    loops[looped.group(1)] = looped.group(2).split()
                    continue
                if "<" in line or not INVOKED.search(line):
                    continue
                spellings = [line]
                for variable, values in loops.items():
                    spellings = [
                        re.sub(rf"\$\{{{variable}\}}|\${variable}\b", value, spelled)
                        for spelled in spellings
                        for value in values
                    ]
                for spelled in dict.fromkeys(spellings):
                    words = shlex.split(spelled, comments=True)
                    if "rekep" not in words:
                        continue
                    words = words[words.index("rekep") + 1 :]
                    for end, word in enumerate(words):
                        if word in {"&", "&&", ";", "|", "||"}:
                            words = words[:end]
                            break
                    yield page, words


def test_documented_commands_parse() -> None:
    """A documented command is one the CLI takes, naming what its task declares."""
    commands = list(shell_commands())

    assert commands
    for page, words in commands:
        where = f"{page.relative_to(ROOT)}: rekep {shlex.join(words)}"
        try:
            arguments = _parser().parse_args(words)
        except SystemExit as ended:
            # `--help` and `--version` answer and exit cleanly; a refusal does not.
            assert ended.code == 0, where
            continue
        if getattr(arguments, "command", None) != "tasks" or arguments.task == "list":
            continue
        declared = Task(arguments.task).parameters
        for spelled in arguments.parameter or ():
            name = spelled.partition("=")[0]
            assert name in declared, f"{where} sets {name}, which the task does not take"
