"""The docs' code examples, against the package they document.

Nothing imports a documentation page, so a rename leaves an example that
raises on its first line and says so to nobody -- which is what happened to
`from rekep import FieldRules`. Every `python` fence under `docs/` is compiled
and every name it imports from this package is looked up, which is one import
of `rekep` for the price of walking a dozen files.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import re
import subprocess
import sys
import tempfile
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
    assert "function codecs(field)" in script, "the lookups are derived from the values"
    assert "field.encoded" not in script and "field.decoded" not in script
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
    assert 'viewBox="0 0 420 230"' in logo
    assert "prefers-reduced-motion:no-preference" in logo
    assert all(color in logo for color in ("#ef4444", "#f97316"))
    assert "#ffd43b" not in logo
    assert "ORDERED MARKET DATA" in logo
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


#: A fence that reaches a bucket, a network scrape, or a path this checkout has
#: not got cannot be run here. It is still parsed and still checked for free
#: names; only its printed output goes unverified.
def _bound(tree: ast.AST) -> set[str]:
    """Every name a fence binds: assignments, imports, definitions, parameters."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
        elif isinstance(node, ast.Import | ast.ImportFrom):
            names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            names.add(node.name)
    return names


def pages() -> list[tuple[str, list[str]]]:
    """`(page, its python fences in order)` -- in order, because a later fence
    on a page continues the one above it and a reader runs them that way."""
    found = []
    for page in sorted(DOCS.rglob("*.md")):
        fences = [m[2] for m in _FENCE.finditer(page.read_text()) if m[1] == "python"]
        if fences:
            found.append((str(page.relative_to(DOCS)), fences))
    return found


_OUTSIDE = (
    "s3://bucket",
    "FixRegistry.scrape",
    "FixRegistry()",
    "TextFile.from_path",
)


@pytest.mark.integration
@pytest.mark.parametrize(("page", "fences"), pages(), ids=[one for one, _ in pages()])
def test_a_printed_output_is_what_the_code_prints(page: str, fences: list[str]) -> None:
    """The ```text after a fence is a claim about this checkout, and a claim
    nothing runs is one a rename or a changed default quietly falsifies -- the
    `properties_of` block on the Iceberg page printed a dict that had gained a
    key.

    Fences carry forward, so stdout does too: the block after fence *n* is the
    tail of everything the page has printed up to it.
    """
    with tempfile.TemporaryDirectory() as sandbox:
        # A reader runs these from the checkout, so a relative `schemas/` or
        # `data/` has to resolve -- but an example that *writes* is not a
        # licence to write into the repository, and one of them lands a
        # `catalog.db` and a warehouse where it is run. So: linked in, run
        # elsewhere, and whatever they create goes with the directory.
        root = Path(sandbox)
        for shared in ("schemas", "data", "python"):
            source_path = DOCS.parent / shared
            if source_path.exists():
                (root / shared).symlink_to(source_path, target_is_directory=True)
        carried: list[str] = []
        for index, source in enumerate(fences):
            outside = any(mark in source for mark in _OUTSIDE)
            carried.append("pass" if outside else source)
            stated = _stated_output(page, index)
            if stated is None or outside:
                continue
            run = subprocess.run(  # noqa: S603
                [sys.executable, "-c", "\n".join(carried)],
                capture_output=True,
                text=True,
                timeout=180,
                cwd=root,
            )
            assert run.returncode == 0, f"{page}#{index} raised:\n{run.stderr[-2000:]}"
            assert run.stdout.strip().endswith(stated), (
                f"{page}#{index} prints\n{run.stdout.strip()[-2000:]}\nnot\n{stated}"
            )


def _stated_output(page: str, index: int) -> str | None:
    """The `text` fence immediately after python fence `index`, if there is one."""
    blocks = [(m[1], m[2]) for m in _FENCE.finditer((DOCS / page).read_text())]
    python = [position for position, (lang, _) in enumerate(blocks) if lang == "python"]
    at = python[index]
    if at + 1 < len(blocks) and blocks[at + 1][0] == "text":
        return blocks[at + 1][1].strip()
    return None


@pytest.mark.parametrize(("page", "fences"), pages(), ids=[one for one, _ in pages()])
def test_a_page_never_reads_a_name_nothing_on_it_wrote(page: str, fences: list[str]) -> None:
    """An example naming a variable no fence ever assigns cannot be run by the
    reader it was written for, and nothing else here would notice: compiling
    accepts a free name, and every one of these pages compiled while
    `shape.cast_arrow(reader)` had no `reader` anywhere on it.

    Names carry forward between fences on one page and never between pages.
    """
    carried: set[str] = set(dir(builtins))
    for index, source in enumerate(fences):
        tree = ast.parse(source, filename=f"{page}#{index}")
        used = {
            node.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
        }
        free = sorted(used - _bound(tree) - carried)
        assert not free, f"{page}#{index} reads {free}, which no fence on this page writes"
        carried |= _bound(tree)


@pytest.mark.parametrize(("where", "source"), EXAMPLES, ids=[one for one, _ in EXAMPLES])
def test_an_example_parses_and_imports_what_it_names(where: str, source: str) -> None:
    tree = ast.parse(source, filename=where)
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not (node.module or "").startswith("rekep"):
            continue
        module = importlib.import_module(node.module)
        for alias in node.names:
            assert hasattr(module, alias.name), f"{where}: {node.module} has no {alias.name}"
