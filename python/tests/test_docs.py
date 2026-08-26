"""The docs' code examples, against the package they document.

Nothing imports a documentation page, so a rename leaves an example that
raises on its first line and says so to nobody -- which is what happened to
`from rekep import FieldRules`. Every `python` fence under `docs/` is compiled
and every name it imports from this package is looked up, which is one import
of `rekep` for the price of walking a dozen files.
"""

from __future__ import annotations

import ast
import importlib
import re
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[2] / "docs"

WORKFLOW_STEPS = (
    ("parse-messages", "parse_messages"),
    ("parse-fix", "parse_fix"),
    ("flatten-instruments", "flatten_instruments"),
    ("parse-market", "parse_market"),
    ("flatten-orders", "flatten_orders"),
    ("flatten-executions", "flatten_executions"),
)

#: A fenced block and the language it claims, for the `python` ones.
_FENCE = re.compile(r"^```(\w+)\n(.*?)^```", re.MULTILINE | re.DOTALL)


def examples() -> list[tuple[str, str]]:
    """`(where it is, the source)` for every python fence under `docs/`."""
    found = []
    for page in sorted(DOCS.rglob("*.md")):
        for index, match in enumerate(_FENCE.finditer(page.read_text())):
            if match[1] == "python":
                found.append((f"{page.relative_to(DOCS)}#{index}", match[2]))
    return found


EXAMPLES = examples()


def test_the_docs_carry_examples() -> None:
    """Or this file passes by having nothing to check."""
    assert len(EXAMPLES) >= 8


def test_fix_transcribe_uses_the_published_registry_in_both_directions() -> None:
    page = (DOCS / "fix" / "transcribe.md").read_text(encoding="utf-8")
    script = (DOCS / "javascripts" / "fix-transcribe.js").read_text(encoding="utf-8")
    config = (DOCS.parent / "mkdocs.yml").read_text(encoding="utf-8")

    assert 'data-source="../../assets/fix-registry.json"' in page
    for direction in ("decode", "encode"):
        assert f"data-{direction}-form" in page
        assert f"data-{direction}-rows" in page
        assert f"data-{direction}-debug" in page
        assert f"data-{direction}-protocol" in page
        assert f"data-{direction}-structure" in page
    assert "field.encoded" in script
    assert "field.decoded" in script
    assert "protocolOf" in script
    assert "structureOf" in script
    assert "expandPayloadPairs" in script
    assert '"fix-wrapper"' in script
    assert 'return marked || "#"' in script
    assert "stylesheets/fix-transcribe.css" in config
    assert "javascripts/fix-transcribe.js" in config


def test_home_page_uses_the_animated_rkp_trigram() -> None:
    page = (DOCS / "index.md").read_text(encoding="utf-8")
    logo = (DOCS / "assets" / "rkp-logo.svg").read_text(encoding="utf-8")
    config = (DOCS.parent / "mkdocs.yml").read_text(encoding="utf-8")

    assert 'class="rkp-hero"' in page
    assert 'src="assets/rkp-logo.svg"' in page
    assert 'aria-labelledby="rkp-title rkp-desc"' in logo
    assert 'viewBox="0 0 420 280"' in logo
    assert "prefers-reduced-motion:no-preference" in logo
    assert all(color in logo for color in ("#f23b3b", "#ff8a00", "#ffd43b"))
    assert "rkp-frame" not in logo
    assert "rkp-grid" not in logo
    assert "logo: assets/rkp-logo.svg" in config
    assert "stylesheets/home.css" in config


def test_registry_docs_keep_the_cli_discoverable_and_results_bounded() -> None:
    browser = (DOCS / "fix" / "registry.md").read_text(encoding="utf-8")
    page = (DOCS / "fix" / "shell.md").read_text(encoding="utf-8")
    script = (DOCS / "javascripts" / "fix-registry.js").read_text(encoding="utf-8")
    styles = (DOCS / "stylesheets" / "fix-registry.css").read_text(encoding="utf-8")
    config = (DOCS.parent / "mkdocs.yml").read_text(encoding="utf-8")

    assert "Registry CLI: fix/shell.md" in config
    assert "rekep fix registry show" in page
    assert "rekep fix shell --store" in page
    assert "const PAGE_SIZE = 20" in script
    assert 'placeholder="Name, tag, MsgType, or member"' in browser
    assert "field.description" in script
    assert "member.tag ?? field?.tag" in script
    assert "fix-registry__description--row" in script
    assert "fix-registry__description--member" in script
    assert ".fix-registry__tag" in styles


@pytest.mark.parametrize(("page_name", "task_name"), WORKFLOW_STEPS)
def test_every_workflow_step_has_a_runnable_command(page_name: str, task_name: str) -> None:
    page = (DOCS / "pipeline" / "tasks" / f"{page_name}.md").read_text(encoding="utf-8")
    workflow = (DOCS / "pipeline" / "operations" / "run.md").read_text(encoding="utf-8")
    document = DOCS.parent / "tasks" / task_name / f"{task_name}.yml"

    assert document.is_file()
    assert "## Run this step" in page
    assert "uv run --project python --with papermill rekep task run" in page
    assert f"tasks/{task_name}/{task_name}.yml" in page
    assert f"--output {task_name}.executed.ipynb" in page
    assert f"tasks/{task_name}/{task_name}.yml" in workflow


@pytest.mark.parametrize(("where", "source"), EXAMPLES, ids=[one for one, _ in EXAMPLES])
def test_an_example_parses_and_imports_what_it_names(where: str, source: str) -> None:
    tree = ast.parse(source, filename=where)
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not (node.module or "").startswith("rekep"):
            continue
        module = importlib.import_module(node.module)
        for alias in node.names:
            assert hasattr(module, alias.name), f"{where}: {node.module} has no {alias.name}"
