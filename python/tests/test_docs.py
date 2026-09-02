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
from types import ModuleType

import pytest

from rekep.tasks import Task

DOCS = Path(__file__).resolve().parents[2] / "docs"

WORKFLOW_TASKS = (
    ("parse-messages", "parse_messages/parse_messages.yml"),
    ("parse-fix", "parse_fix/parse_fix.yml"),
    ("parse-instruments", "parse_instruments/parse_instruments.yml"),
    ("parse-market", "parse_market/parse_market.yml"),
    ("flatten-orders", "flatten_orders/flatten_orders.yml"),
    ("flatten-executions", "flatten_executions/flatten_executions.yml"),
)

TASK_DOCUMENTS = (
    *(document for _, document in WORKFLOW_TASKS),
    "optimize_iceberg/optimize_iceberg.yml",
)

#: A fenced block and the language it claims, for the `python` ones.
_FENCE = re.compile(r"^```(\w+)\n(.*?)^```", re.MULTILINE | re.DOTALL)


def examples() -> list[tuple[str, str]]:
    """`(where it is, the source)` for every python fence under `docs/`."""
    found = []
    for page in sorted(DOCS.rglob("*.md")):
        for index, match in enumerate(_FENCE.finditer(page.read_text(encoding="utf-8"))):
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
    assert all(f'code: "{code}"' in script for code in ("FIX", "FIXML", "UL")), (
        "the page classifies by the three shapes rekep.fix.rules classifies by"
    )
    assert 'return marked || "#"' in script
    assert "stylesheets/fix-transcribe.css" in config
    assert "javascripts/fix-transcribe.js" in config


def test_home_page_uses_the_animated_rkp_trigram() -> None:
    page = (DOCS / "index.md").read_text(encoding="utf-8")
    logo = (DOCS / "assets" / "rkp-logo.svg").read_text(encoding="utf-8")
    config = (DOCS.parent / "mkdocs.yml").read_text(encoding="utf-8")

    assert 'class="rkp-hero"' in page
    assert 'src="assets/rkp-logo.svg#only-dark"' in page
    assert 'src="assets/rkp-logo-light.svg#only-light"' in page
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
    assert "Repeating groups: fix/repeating-groups.md" in config
    assert "Encode values: fix/encode.md" in config
    assert "Decode values: fix/decode.md" in config
    assert "rekep fix registry show" in page
    assert "rekep fix shell --store" in page
    assert "const PAGE_SIZE = 20" in script
    assert 'placeholder="Name, tag, MsgType, or member"' in browser
    assert "field.description" in script
    assert "member.tag ?? field?.tag" in script
    assert "fix-registry__description--row" in script
    assert "fix-registry__description--member" in script
    assert 'badge(field.type || "—", "type")' in script
    assert "data-summary-groups" in browser
    assert ".fix-registry__tag" in styles


def test_registry_docs_publish_arrow_types_and_derived_groups() -> None:
    catalog = _hooks()._registry_catalog(DOCS.parent)
    side = next(field for field in catalog["fields"] if field.get("tag") == 54)
    parties = next(group for group in catalog["groups"] if group["name"] == "NoPartyIDs")

    assert side["type"] == "string" and side["fix_type"] == "char"
    assert catalog["namespaces"][0] == "standard"
    assert catalog["coverage"]["fields"] == len(catalog["fields"])
    assert catalog["coverage"]["groups"] == len(catalog["groups"])
    assert len(catalog["groups"]) == 525
    assert parties["record_kind"] == "group"
    assert parties["namespace"] == "standard"
    assert parties["type"] == "list", "a component entry is the field document it declares"
    udf = [
        component
        for component in catalog["components"]
        if component["namespace"] == "fixtrading-udf"
    ]
    assert len(udf) == 126


def test_registry_catalog_keeps_duplicate_tags_namespace_addressable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tag shared by FIX and a UDF remains two definitions in the artifact."""
    hooks = _hooks()
    registry = hooks.FixRegistry(cache_dir=DOCS.parent / "data" / "fix")
    side = next(field for field in registry.field_records().values() if field.fix.tag == 54)
    component = next(iter(registry.component_records().values()))
    group = next(iter(registry.repeating_group_records().values()))

    class Registry:
        versions = ("5.0.SP2",)

        def __init__(self, **_kwargs: object) -> None:
            pass

        def namespaces(self) -> tuple[str, ...]:
            return ("standard", "fixtrading-udf")

        def field_records(self, namespace: str = "standard") -> dict[str, object]:
            return {f"{namespace}-Side": side}

        def component_records(self, namespace: str = "standard") -> dict[str, object]:
            return {f"{namespace}-component": component}

        def repeating_group_records(self, namespace: str = "standard") -> dict[str, object]:
            return {f"{namespace}-group": group}

        def source_manifest(self) -> tuple[dict[str, str], ...]:
            return (
                {
                    "source_id": "fix-udf",
                    "namespace": "fixtrading-udf",
                    "url": "https://example.test/udf.xml",
                    "version": "1",
                    "format": "orchestra",
                    "checksum": "bb",
                    "license_url": "https://example.test/terms",
                },
                {
                    "source_id": "fix-latest",
                    "namespace": "standard",
                    "url": "https://example.test/latest.xml",
                    "version": "EP309",
                    "format": "orchestra",
                    "checksum": "aa",
                    "license_url": "https://example.test/license",
                },
            )

    monkeypatch.setattr(hooks, "FixRegistry", Registry)
    catalog = hooks._registry_catalog(DOCS.parent)

    assert [(field["namespace"], field["tag"]) for field in catalog["fields"]] == [
        ("standard", 54),
        ("fixtrading-udf", 54),
    ]
    assert [source["source_id"] for source in catalog["sources"]] == [
        "fix-latest",
        "fix-udf",
    ]
    assert catalog["coverage"]["by_namespace"] == [
        {
            "namespace": "standard",
            "fields": 1,
            "components": 1,
            "groups": 1,
            "enumerations": 1,
        },
        {
            "namespace": "fixtrading-udf",
            "fields": 1,
            "components": 1,
            "groups": 1,
            "enumerations": 1,
        },
    ]
    assert [component["namespace"] for component in catalog["components"]] == [
        "standard",
        "fixtrading-udf",
    ]
    assert [group["namespace"] for group in catalog["groups"]] == [
        "standard",
        "fixtrading-udf",
    ]


def test_registry_browser_routes_duplicate_tags_by_namespace() -> None:
    browser = (DOCS / "fix" / "registry.md").read_text(encoding="utf-8")
    script = (DOCS / "javascripts" / "fix-registry.js").read_text(encoding="utf-8")
    styles = (DOCS / "stylesheets" / "fix-registry.css").read_text(encoding="utf-8")

    assert 'select name="namespace"' in browser
    assert "data-source-coverage" in browser and "data-namespace-coverage" in browser
    assert "fieldByIdentity" in script
    assert "fieldHref(field)" in script
    assert "!fieldByTag.has" in script, "the first, standard definition stays preferred"
    assert 'parameters.get("namespace")' in script
    assert "data-component-members" in script, "namespace routing keeps lazy nested trees"
    assert ".fix-registry__namespace" in styles
    assert "font-weight: 700" not in styles


def test_fix_component_docs_keep_nested_contracts_collapsible() -> None:
    """A struct and its repeating list stay one tree all the way to the page."""
    hooks = _hooks()
    catalog = hooks._product_catalog()
    fixmsg = next(product for product in catalog["products"] if product["key"] == "fixmsg")
    instrument = next(column for column in fixmsg["columns"] if column["name"] == "instrument")
    legs = next(column for column in instrument["fields"] if column["name"] == "legs")
    altids = next(column for column in fixmsg["columns"] if column["name"] == "altids")

    assert {column["name"] for column in instrument["fields"]} >= {
        "symbol",
        "securityid",
        "legs",
    }
    assert {column["name"] for column in legs["fields"]} >= {"symbol", "securityid"}
    assert [column["name"] for column in altids["fields"]] == ["key", "value"]

    lineage = (DOCS / "javascripts" / "product-lineage.js").read_text(encoding="utf-8")
    registry = (DOCS / "javascripts" / "fix-registry.js").read_text(encoding="utf-8")
    transcribe = (DOCS / "javascripts" / "fix-transcribe.js").read_text(encoding="utf-8")
    styles = (DOCS / "stylesheets" / "fix-registry.css").read_text(encoding="utf-8")

    assert 'class="product-lineage__nested"' in lineage
    assert "function leafColumns(columns)" in lineage
    assert "function expandedMembers(members, seen)" in registry
    assert "function hydrateComponentTree(event)" in registry
    assert "data-component-members" in registry
    assert 'class="fix-registry__tree-node' in registry
    assert 'class="fix-registry__references"' in registry
    assert 'class="fix-transcribe__group"' in transcribe
    assert "entry.groups.map((nested)" in transcribe
    assert "font-weight: 700" not in styles


@pytest.mark.parametrize(("page_name", "task_document"), WORKFLOW_TASKS)
def test_every_workflow_step_has_a_runnable_command(page_name: str, task_document: str) -> None:
    page = (DOCS / "pipeline" / "tasks" / f"{page_name}.md").read_text(encoding="utf-8")
    workflow = (DOCS / "pipeline" / "operations" / "run.md").read_text(encoding="utf-8")
    document = DOCS.parent / "tasks" / task_document
    task = Task.from_yaml(document)
    relative = document.relative_to(DOCS.parent).as_posix()

    assert document.is_file()
    assert "## Run this step" in page
    assert "uv run --project python --group runner rekep task run" in page
    assert relative in page
    application = task.into_application_path(document).relative_to(DOCS.parent).as_posix()
    assert application in page, "and the page names the application it runs"
    assert relative in workflow


def test_every_task_injects_one_catalog_document() -> None:
    """Task parameters keep catalog identity and properties atomic."""
    for task_document in TASK_DOCUMENTS:
        document = DOCS.parent / "tasks" / task_document
        task = Task.from_yaml(document)
        parameters = task.parameters
        assert "catalog_properties" not in parameters
        assert set(parameters["catalog"]) == {"name", "properties"}

        source = task.into_application_path(document).read_text(encoding="utf-8")
        assert "IcebergCatalog.from_dict(catalog)" in source
        assert "catalog_properties" not in source


def test_airflow_reuses_one_parse_fix_definition_for_every_category() -> None:
    document = DOCS.parent / "tasks" / "parse_fix" / "parse_fix.yml"
    configured = Task.from_yaml(document)
    assert configured.name == "parse_fix"
    assert configured.parameters["category"] == "market"
    assert "task_name" not in configured.parameters
    assert "target" not in configured.parameters

    source = configured.into_application_path(document).read_text(encoding="utf-8")
    parameters, _, body = source.partition("def _(log_level):")
    assert "task_name =" not in parameters
    assert "target =" not in parameters
    assert 'task_name = f"parse_fix_{category}"' in body
    assert 'target = f"fix.{category}"' in body

    dag = (DOCS.parent / "tasks" / "airflow" / "market_pipeline.py").read_text(encoding="utf-8")
    compact = "".join(dag.split())
    assert 'CATEGORIES=("market","misc","unknown")' in compact
    assert '"parse_fix",f"parse_fix_{category}",parameters={"category":category}' in compact
    for category in ("market", "misc", "unknown"):
        assert not (document.parent / f"parse_fix_{category}.yml").exists()


def test_workflow_tasks_commit_bounded_groups_of_batches() -> None:
    """The task documents expose one cadence and keep the row cap optional."""
    for page_name, task_document in WORKFLOW_TASKS:
        document = DOCS.parent / "tasks" / task_document
        task = Task.from_yaml(document)
        parameters = task.parameters
        assert parameters["commit_batch_num"] == 8
        assert parameters["commit_row_size"] is None

        # The document is the only place the cadence is written: the parameter
        # cell reads it back rather than repeating the number in Python.
        source = task.into_application_path(document).read_text(encoding="utf-8")
        assert 'commit_batch_num = _defaults["commit_batch_num"]' in source
        assert 'commit_row_size = _defaults["commit_row_size"]' in source
        assert "commit_batch_num = 8" not in source
        assert "commit_batch_num=commit_batch_num" in source

        page = (DOCS / "pipeline" / "tasks" / f"{page_name}.md").read_text(encoding="utf-8")
        assert "commit_batch_num: 8" in page
        assert "commit_row_size: null" in page


def test_deploying_the_tables_is_documented_where_a_deployment_is_read() -> None:
    """The command, on the two pages a person creating a catalog is already on."""
    bootstrap = (DOCS / "pipeline" / "operations" / "deploy.md").read_text(encoding="utf-8")
    workflow = (DOCS / "pipeline" / "operations" / "run.md").read_text(encoding="utf-8")

    for page in (bootstrap, workflow):
        assert "rekep iceberg deploy tasks/parse_fix/parse_fix.yml" in page
        assert "--dry-run" in page
    assert "rekep.deploy.TABLES" in bootstrap
    assert "rekep.deploy.TABLES" in workflow


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
        fences = [
            m[2] for m in _FENCE.finditer(page.read_text(encoding="utf-8")) if m[1] == "python"
        ]
        if fences:
            found.append((str(page.relative_to(DOCS)), fences))
    return found


_OUTSIDE = (
    "s3://bucket",
    "FixRegistry.scrape",
    "FixRegistry()",
    "TextFile.from_path",
)

_CHECKPOINT = "__REKEP_DOC_CHECKPOINT_9B173F9E__"


def _link_directory(link: Path, target: Path) -> None:
    """Expose one read-only checkout directory inside an example sandbox."""
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as error:
        if sys.platform != "win32" or getattr(error, "winerror", None) != 1314:
            raise
        # Directory junctions need no developer-mode privilege and exercise the
        # same examples on Windows instead of silently skipping the whole page.
        subprocess.run(  # noqa: S603
            ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(target)],
            check=True,
            capture_output=True,
        )


@pytest.mark.integration
@pytest.mark.parametrize(("page", "fences"), pages(), ids=[one for one, _ in pages()])
def test_a_printed_output_is_what_the_code_prints(page: str, fences: list[str]) -> None:
    """The ```text after a fence is a claim about this checkout, and a claim
    nothing runs is one a rename or a changed default quietly falsifies -- the
    `into_properties` block on the Iceberg page printed a dict that had gained a
    key.

    Fences carry forward, so stdout does too: the block after fence *n* is the
    tail of everything the page has printed up to it.
    """
    script: list[str] = []
    checks: list[tuple[int, str, str]] = []
    checked_script_length = 0
    for index, source in enumerate(fences):
        outside = any(mark in source for mark in _OUTSIDE)
        script.append("pass" if outside else source)
        stated = _stated_output(page, index)
        if stated is None or outside:
            continue
        marker = f"{_CHECKPOINT}{index}"
        script.append(f"print('\\n{marker}')")
        checks.append((index, stated, marker))
        checked_script_length = len(script)

    if not checks:
        return
    script = script[:checked_script_length]

    with tempfile.TemporaryDirectory() as sandbox:
        # A reader runs these from the checkout, so a relative `schemas/` or
        # `data/` has to resolve -- but an example that *writes* is not a
        # licence to write into the repository, and one of them lands a
        # `catalog.db` and a warehouse where it is run. So: linked in, run
        # elsewhere, and whatever they create goes with the directory.
        root = Path(sandbox)
        for shared in ("schemas", "data"):
            source_path = DOCS.parent / shared
            if source_path.exists():
                _link_directory(root / shared, source_path)

        # One interpreter follows the page in order. The old one-process-per-
        # assertion loop reran every earlier fence and made this suite quadratic.
        run = subprocess.run(  # noqa: S603
            [sys.executable, "-c", "\n".join(script)],
            capture_output=True,
            text=True,
            timeout=180,
            cwd=root,
        )
        assert run.returncode == 0, f"{page} raised:\n{run.stderr[-2000:]}"

        cumulative = ""
        position = 0
        for index, stated, marker in checks:
            delimiter = f"\n{marker}\n"
            checkpoint = run.stdout.find(delimiter, position)
            assert checkpoint >= 0, f"{page}#{index} did not reach its output checkpoint"
            cumulative += run.stdout[position:checkpoint]
            assert cumulative.strip().endswith(stated), (
                f"{page}#{index} prints\n{cumulative.strip()[-2000:]}\nnot\n{stated}"
            )
            position = checkpoint + len(delimiter)


def _stated_output(page: str, index: int) -> str | None:
    """The `text` fence immediately after python fence `index`, if there is one."""
    blocks = [(m[1], m[2]) for m in _FENCE.finditer((DOCS / page).read_text(encoding="utf-8"))]
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


def _hooks() -> ModuleType:
    """The build hooks, imported the way mkdocs imports them."""
    root = DOCS.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return importlib.import_module("docs_hooks")


def test_the_ascii_code_page_covers_each_physical_width() -> None:
    """The shared widget and its catalog carry every persisted ASCII base."""
    catalog = _hooks()._enum_catalog()["enums"]
    script = (DOCS / "javascripts" / "ascii-codes.js").read_text(encoding="utf-8")

    assert {(one["base"], one["byte_width"], one["stored"]) for one in catalog} == {
        ("Ascii32", 4, "int32"),
        ("Ascii64", 8, "int64"),
        ("Ascii128", 16, "fixed_size_binary[16]"),
    }
    assert all(f'value="{width}"' in script for width in (4, 8, 16))
    by_name = {one["name"]: one for one in catalog}
    for name in ("Protocol", "Plugin"):
        assert by_name[name]["base"] == "Ascii128"
        assert by_name[name]["stored"] == "fixed_size_binary[16]"
        assert all(len(member["value"]) == 32 for member in by_name[name]["members"])


def test_a_neutral_inverts_and_an_accent_is_left_alone() -> None:
    """The light variant of a diagram is derived, not drawn again, so the one
    rule that derives it is the thing worth pinning: depth flips, identity
    does not."""
    hooks = _hooks()

    assert hooks.relight_colour("#050505") == "#fbfbfb", "a near-black ground becomes near-white"
    assert hooks.relight_colour("#fafafa") == "#060606", "and the text on it becomes near-black"
    for accent in ("#f97316", "#ef4444"):
        assert hooks.relight_colour(accent) == accent, "brand colour is identity, not depth"


def test_the_card_a_third_party_mark_sits_on_is_not_relit() -> None:
    """The marks are reproduced unmodified, so the plate under them has to stay
    the ground they were drawn for. Relit with everything else it put the
    Apache and GitHub marks on black."""
    hooks = _hooks()
    drawing = (DOCS / "assets" / "workflow-run.svg").read_text(encoding="utf-8")

    assert ".brand-plate{fill:#ffffff" in drawing, "the authored plate is white"
    assert ".brand-plate{fill:#ffffff" in hooks.relight(drawing), "and stays white"
    assert ".label{font:600 18px system-ui,sans-serif;fill:#060606" in hooks.relight(drawing), (
        "while a label beside it does invert"
    )


def test_every_mark_is_carried_rather_than_referenced() -> None:
    """A browser renders `<img src="a.svg">` in a context that loads no
    external resource, so a mark referenced by path never arrives and the
    plate under it ships empty -- which is what these diagrams did."""
    hooks = _hooks()
    assets = DOCS / "assets"

    for name in hooks.DIAGRAMS:
        drawing = (assets / f"{name}.svg").read_text(encoding="utf-8")
        embedded = hooks.embed_marks(drawing, assets)
        assert 'href="logos/' not in embedded, f"{name}: a mark is still referenced by path"
        assert drawing.count("logos/") == 0 or "data:image/svg+xml;base64," in embedded
        # One `<symbol>` per mark, not one copy per placement: the Iceberg mark
        # appears eight times in one diagram and is eleven kilobytes.
        assert embedded.count("data:image/svg+xml;base64,") == len(
            {name for name, _ in hooks._MARK.findall(drawing)}
        )


@pytest.mark.parametrize("page", sorted(str(one.relative_to(DOCS)) for one in DOCS.rglob("*.md")))
def test_a_diagram_is_shown_in_both_schemes(page: str) -> None:
    """Material picks between two images by a URL fragment, so a diagram named
    without one is a diagram that shows in both schemes -- and every one of
    these is drawn for exactly one of them."""
    hooks = _hooks()
    text = (DOCS / page).read_text(encoding="utf-8")
    # Where a diagram is *shown*, not where it is named: the asset licence
    # table lists these filenames as prose and carries no image at all.
    found = re.findall(r"!\[[^\]]*\]\(([^)]+)\)|<img[^>]+src=\"([^\"]+)\"", text)
    sources = [one or other for one, other in found]

    for name in hooks.DIAGRAMS:
        dark = [one for one in sources if one.endswith(f"{name}.svg#only-dark")]
        light = [one for one in sources if one.endswith(f"{name}-light.svg#only-light")]
        plain = [one for one in sources if one.endswith(f"{name}.svg")]
        assert not plain, f"{page}: {name}.svg is shown with no scheme fragment"
        assert len(dark) == len(light), f"{page}: {name} is not shown once per scheme"
