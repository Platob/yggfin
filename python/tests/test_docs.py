"""The small documentation surface matches the supported ingestion path."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import yaml
from yggdryl import Field as YggdrylField

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

    assert Field is YggdrylField
    assert [member.name for member in Message.field()] == [
        "url",
        "rownum",
        "timestamp",
        "timepartition",
        "threadname",
        "branch",
        "level",
        "mimetype",
        "msgtype",
        "msgdirection",
        "msghash",
        "body",
    ]
    assert "pipeline/tasks/parse-fix.md" in config
    assert "market/" not in config
    assert sorted(path.name for path in (ROOT / "schemas" / "rekep").glob("*.json")) == [
        "fix-message.json",
        "message.json",
    ]
    assert "fix:name" not in schema


def test_docs_record_the_measured_message_rates() -> None:
    benchmark = (DOCS / "storage" / "benchmarks.md").read_text(encoding="utf-8")
    task = (DOCS / "pipeline" / "tasks" / "parse-messages.md").read_text(encoding="utf-8")

    assert "70,000" in benchmark
    assert "timestamp[us, UTC]" in benchmark
    assert "fastest of two warmed runs" in benchmark
    assert "buffered()" in benchmark
    assert "300,000 rows" in benchmark
    assert "Concatenated gzip" in task
    assert "yggdryl-text-streaming.md" in task


def test_fix_schema_stays_owned_by_the_runtime_registry() -> None:
    task = (DOCS / "pipeline" / "tasks" / "parse-fix.md").read_text(encoding="utf-8")
    schemas = (ROOT / "schemas" / "README.md").read_text(encoding="utf-8")

    assert "parse_arrow_reader" in task
    assert "Field.from_arrow_schema" in task
    assert "second registry" in schemas
    assert "`fix-message.json`" in schemas
