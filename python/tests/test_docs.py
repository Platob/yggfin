"""The small documentation surface matches the supported ingestion path."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import yaml

from rekep import Field, Message

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
FENCE = re.compile(r"^```python\n(.*?)^```", re.MULTILINE | re.DOTALL)
JSON_FENCE = re.compile(r"^```json\n(.*?)^```", re.MULTILINE | re.DOTALL)


def test_python_examples_compile() -> None:
    examples = [
        (page, index, source)
        for page in sorted(DOCS.rglob("*.md"))
        for index, source in enumerate(FENCE.findall(page.read_text(encoding="utf-8")))
    ]

    assert len(examples) >= 6
    for page, index, source in examples:
        ast.parse(source, filename=f"{page.relative_to(DOCS)}#{index}")


def test_json_examples_parse() -> None:
    examples = [
        (page, source)
        for page in sorted(DOCS.rglob("*.md"))
        for source in JSON_FENCE.findall(page.read_text(encoding="utf-8"))
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


def test_docs_publish_the_native_message_contracts() -> None:
    config = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    schema = (ROOT / "schemas" / "rekep" / "message.json").read_text(encoding="utf-8")

    assert Field.__name__ == "Field"
    assert [member.name for member in Message.field()] == [
        "url",
        "rownum",
        "timestamp",
        "timepartition",
        "threadId",
        "sessionUid",
        "msgCtxId",
        "seqNum",
        "plugin",
        "level",
        "bodyhash",
        "body",
    ]
    assert "pipeline/tasks/parse-fix.md" in config
    assert "market/" not in config
    assert sorted(path.name for path in (ROOT / "schemas" / "rekep").glob("*.json")) == [
        "fix-message.json",
        "message.json",
    ]
    # An Iceberg contract carries no Arrow metadata, so FIX vocabulary can only
    # leak into the raw product as a column -- which is what this looks for.
    assert '"name": "body"' in schema
    assert '"name": "msgtype"' not in schema


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
    task = (DOCS / "pipeline" / "tasks" / "parse-fix.md").read_text(encoding="utf-8")
    schemas = (ROOT / "schemas" / "README.md").read_text(encoding="utf-8")

    assert "parse_arrow_reader" in task
    assert "iceberg_fix_field" in task
    assert "not alternate implementations" in schemas
    assert "`fix-message.json`" in schemas
    assert "iceberg_contract" in schemas


def test_public_scripts_and_documentation_use_only_the_rekep_name() -> None:
    roots = [ROOT / "README.md", ROOT / "schemas", DOCS, ROOT / "tasks", ROOT / "tools"]
    suffixes = {".md", ".json", ".js", ".py"}
    files = []
    for root in roots:
        files.extend([root] if root.is_file() else root.rglob("*"))

    exposed = [path for path in files if path.is_file() and path.suffix in suffixes]
    assert exposed
    for path in exposed:
        assert "yggdryl" not in path.read_text(encoding="utf-8").casefold(), path


def test_each_task_page_publishes_its_document_verbatim() -> None:
    """A pasted document is only documentation while it still matches.

    `mkdocs.yml` enables `pymdownx.snippets` so a page can include a file from
    the checkout, but these two pages paste the JSON instead, which nothing
    stops from drifting. This is what stops it.
    """
    pages = {
        "pipeline/tasks/parse-messages.md": "parse_messages",
        "pipeline/tasks/parse-fix.md": "parse_fix",
    }

    for page, name in pages.items():
        document = json.loads((ROOT / "tasks" / name / f"{name}.json").read_text(encoding="utf-8"))
        shown = [
            json.loads(source)
            for source in JSON_FENCE.findall((DOCS / page).read_text(encoding="utf-8"))
        ]
        assert document in shown, f"{page} no longer shows {name}.json as it is"
