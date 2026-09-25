"""The small documentation surface matches the supported ingestion path."""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import re
from collections.abc import Iterator
from pathlib import Path

import yaml

from rekep import Field, Message, pipeline
from rekep.deploy import deploy

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
FENCE = re.compile(r"^```python\n(.*?)^```", re.MULTILINE | re.DOTALL)
JSON_FENCE = re.compile(r"^```json\n(.*?)^```", re.MULTILINE | re.DOTALL)

#: A command of the `rekep` console script, which the package does not install.
COMMAND = re.compile(r"\brekep\s+(?:tasks|fields)\b")

#: Every page a reader copies a command from.
PAGES = [
    ROOT / "README.md",
    ROOT / ".claude" / "skills" / "rekep" / "SKILL.md",
    ROOT / "config" / "README.md",
    ROOT / "data" / "README.md",
    ROOT / "schemas" / "README.md",
    *sorted(DOCS.rglob("*.md")),
]


def code_fences(page: Path, pattern: re.Pattern[str]) -> Iterator[str]:
    """Read one page's fenced examples."""
    yield from pattern.findall(page.read_text(encoding="utf-8"))


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


def test_the_capture_is_the_bytes_its_page_pins() -> None:
    """`data/capture/ulbridge.log` is the core's own capture at the pinned
    release, and the digest its page states is what says so."""
    page = (ROOT / "data" / "README.md").read_text(encoding="utf-8")
    (digest,) = re.findall(r"^```text\n([0-9a-f]{64})\n```", page, re.MULTILINE)
    capture = (ROOT / "data" / "capture" / "ulbridge.log").read_bytes()
    assert hashlib.sha256(capture).hexdigest() == digest


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
    task = (DOCS / "pipeline" / "parse-messages.md").read_text(encoding="utf-8")

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
    raw = (DOCS / "pipeline" / "parse-fix-raw.md").read_text(encoding="utf-8")
    refined = (DOCS / "pipeline" / "parse-fix-refined.md").read_text(encoding="utf-8")
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
    """Examples and tools import their product, not its runtime."""
    roots = [ROOT / "README.md", ROOT / "schemas", DOCS, ROOT / "tools"]
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


def test_no_page_spells_a_rekep_command() -> None:
    """The package is called from Python and installs no console script, so a
    `rekep tasks` or `rekep fields` line hands its reader a command that is not
    there: a stage is a `rekep.pipeline` call and a contract an
    `iceberg_contract` one."""
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
}


def test_every_documented_stage_call_binds_to_its_signature() -> None:
    """A page that hands a stage a keyword it does not take compiles, so each
    call is bound to the function it names instead."""
    bound = 0
    for page in PAGES:
        for index, source in enumerate(code_fences(page, FENCE)):
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
    assert bound, "the pages call the stages"
