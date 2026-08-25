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


@pytest.mark.parametrize(("where", "source"), EXAMPLES, ids=[one for one, _ in EXAMPLES])
def test_an_example_parses_and_imports_what_it_names(where: str, source: str) -> None:
    tree = ast.parse(source, filename=where)
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not (node.module or "").startswith("rekep"):
            continue
        module = importlib.import_module(node.module)
        for alias in node.names:
            assert hasattr(module, alias.name), f"{where}: {node.module} has no {alias.name}"
