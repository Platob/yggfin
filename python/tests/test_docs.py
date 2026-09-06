"""The small documentation surface matches the supported ingestion path."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import yaml
from yggdryl import Field as YggdrylField

from rekep import Field, Message

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
FENCE = re.compile(r"^```python\n(.*?)^```", re.MULTILINE | re.DOTALL)


def test_python_examples_compile() -> None:
    examples = [
        (page, index, source)
        for page in sorted(DOCS.rglob("*.md"))
        for index, source in enumerate(FENCE.findall(page.read_text(encoding="utf-8")))
    ]

    assert len(examples) >= 6
    for page, index, source in examples:
        ast.parse(source, filename=f"{page.relative_to(DOCS)}#{index}")


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


def test_docs_publish_only_the_native_message_contract() -> None:
    config = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    schema = (ROOT / "schemas" / "rekep" / "message.yaml").read_text(encoding="utf-8")

    assert Field is YggdrylField
    assert [member.name for member in Message.field()] == [
        "sourceurl",
        "sourcerownum",
        "timestamp",
        "threadname",
        "plugin",
        "level",
        "body",
    ]
    assert "fix/" not in config and "market/" not in config
    assert list((ROOT / "schemas" / "rekep").glob("*.yaml")) == [
        ROOT / "schemas" / "rekep" / "message.yaml"
    ]
    assert "fix:name" not in schema


def test_docs_record_the_measured_message_rates() -> None:
    benchmark = (DOCS / "storage" / "benchmarks.md").read_text(encoding="utf-8")
    task = (DOCS / "pipeline" / "tasks" / "parse-messages.md").read_text(encoding="utf-8")

    assert "200,000" in benchmark
    assert "97,813" in benchmark
    assert "fastest of three warmed runs" in benchmark
    assert "buffered()" in benchmark
    assert "300,000 rows" in benchmark
    assert "Concatenated gzip" in task
    assert "yggdryl-text-streaming.md" in task


def test_next_fix_session_starts_from_yggdryl() -> None:
    prompt = (DOCS / "prompts" / "yggdryl-fix-refactor.md").read_text(encoding="utf-8")

    assert "yggdryl.fix.FixRegistry" in prompt
    assert "Do not restore" in prompt
    assert "Rust first" in prompt
